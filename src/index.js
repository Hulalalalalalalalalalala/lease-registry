// In-process, time bounded lease registry with append-only log persistence.
// Time only moves when the injected clock moves; nothing expires on its own.
// Tokens are derived from the injected clock as well, never from wall time.

import fs from 'node:fs';

const LOG_VERSION = 1;

export class LeaseTakenError extends Error {
  constructor(resource, holder) {
    super(`lease for resource ${String(resource)} is already held`);
    this.name = 'LeaseTakenError';
    this.code = LeaseTakenError.code;
    this.resource = resource;
    this.holder = holder ?? null;
  }
}
LeaseTakenError.code = 'LEASE_TAKEN';

export class LogFileError extends Error {
  constructor(message, cause) {
    super(message);
    this.name = 'LogFileError';
    this.code = LogFileError.code;
    this.cause = cause ?? null;
  }
}
LogFileError.code = 'LOG_FILE_ERROR';

// Binary min-heap ordered by (expiresAt, insertion order). Every live lease
// has a node in the queue; renewals push a fresh node and retire the old one,
// so reclaiming expired leases only walks entries that are actually due
// instead of scanning the whole table.
class ExpiryQueue {
  constructor() {
    this.nodes = [];
  }

  get size() {
    return this.nodes.length;
  }

  push(node) {
    const nodes = this.nodes;
    nodes.push(node);
    let i = nodes.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (expiresBefore(nodes[i], nodes[parent])) {
        [nodes[i], nodes[parent]] = [nodes[parent], nodes[i]];
        i = parent;
      } else {
        break;
      }
    }
  }

  peek() {
    return this.nodes[0];
  }

  pop() {
    const nodes = this.nodes;
    const top = nodes[0];
    const last = nodes.pop();
    if (nodes.length > 0) {
      nodes[0] = last;
      let i = 0;
      for (;;) {
        const left = i * 2 + 1;
        const right = left + 1;
        let next = i;
        if (left < nodes.length && expiresBefore(nodes[left], nodes[next])) {
          next = left;
        }
        if (right < nodes.length && expiresBefore(nodes[right], nodes[next])) {
          next = right;
        }
        if (next === i) {
          break;
        }
        [nodes[i], nodes[next]] = [nodes[next], nodes[i]];
        i = next;
      }
    }
    return top;
  }
}

function expiresBefore(a, b) {
  if (a.expiresAt !== b.expiresAt) {
    return a.expiresAt < b.expiresAt;
  }
  return a.order < b.order;
}

let registrySequence = 0;

export function createRegistry({ ttlMs, clock = Date.now, logPath = null } = {}) {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new TypeError('ttlMs must be a positive finite number');
  }
  if (typeof clock !== 'function') {
    throw new TypeError('clock must be a function');
  }

  const defaultTtlMs = ttlMs;
  // resource -> FIFO array of queue positions. A group request listing the
  // same resource twice occupies two consecutive positions in that queue.
  const waitQueues = new Map();
  // requestId -> group, while the request is still queued.
  const pendingGroups = new Map();
  // token -> { resource, holder, expiresAt, ttlMs, legacy, heapSeq }
  const leases = new Map();
  // Every token this instance knows about, including replayed credentials
  // that were already released or reclaimed, so a freshly minted token can
  // never reuse a dead credential's string.
  const knownTokens = new Set();
  // resource -> tokens currently occupying it. Usually one entry; an atomic
  // group listing one resource several times holds one credential per
  // occurrence, all belonging to the same request.
  const slots = new Map();
  const expiryQueue = new ExpiryQueue();

  const counters = {
    granted: 0,
    renewed: 0,
    released: 0,
    reclaimed: 0,
  };

  // Per-instance identity makes tokens unique across registries living in the
  // same process; one instance can never present a token another owns.
  registrySequence += 1;
  const registryId = registrySequence;
  let tokenSequence = 0;
  let queueOrder = 0;
  let requestSequence = 0;
  let batchSequence = 0;

  function mintToken(now, seq = tokenSequence) {
    // Pure candidate: nothing is committed until the event is on disk, so a
    // failed append rolls all the way back. Pids and instance ids can coincide
    // after a restart, so skip any sequence that collides with a credential
    // reconstructed from the log. The caller threads the sequence through when
    // it mints several tokens for one atomic write.
    let token;
    do {
      seq += 1;
      token = `lease-${process.pid}-${registryId}-${seq}-${now.toString(36)}`;
    } while (knownTokens.has(token));
    return { token, seq };
  }

  function enqueue(token, lease) {
    expiryQueue.push({
      token,
      expiresAt: lease.expiresAt,
      heapSeq: lease.heapSeq,
      order: queueOrder++,
    });
  }

  function isLive(lease, now) {
    return lease !== undefined && lease.expiresAt > now;
  }

  function getSlots(resource) {
    let list = slots.get(resource);
    if (list === undefined) {
      list = [];
      slots.set(resource, list);
    }
    return list;
  }

  function addSlot(resource, token) {
    getSlots(resource).push(token);
  }

  function removeSlot(resource, token) {
    const list = slots.get(resource);
    if (list === undefined) {
      return false;
    }
    const index = list.indexOf(token);
    if (index === -1) {
      return false;
    }
    list.splice(index, 1);
    if (list.length === 0) {
      slots.delete(resource);
    }
    return true;
  }

  // Drop expired-but-unswept credentials from a resource that is about to be
  // granted: the resource is free, and a retaken lease must never be reported
  // as reclaimed later. Mirrors the single-resource retake rule.
  function dropStaleSlots(resource, now) {
    const list = slots.get(resource);
    if (list === undefined) {
      return;
    }
    for (let i = list.length - 1; i >= 0; i -= 1) {
      const token = list[i];
      if (!isLive(leases.get(token), now)) {
        leases.delete(token);
        list.splice(i, 1);
      }
    }
    if (list.length === 0) {
      slots.delete(resource);
    }
  }

  function liveSlotCount(resource, now) {
    const list = slots.get(resource);
    if (list === undefined) {
      return 0;
    }
    let live = 0;
    for (const token of list) {
      if (isLive(leases.get(token), now)) {
        live += 1;
      }
    }
    return live;
  }

  function liveHolder(resource, now) {
    const list = slots.get(resource);
    if (list === undefined) {
      return null;
    }
    for (const token of list) {
      const lease = leases.get(token);
      if (isLive(lease, now)) {
        return lease.holder;
      }
    }
    return null;
  }

  function appendEvents(events) {
    if (logPath === null) {
      return;
    }
    // One write for the whole batch: an operation that frees resources and
    // wakes requests either lands completely or not at all.
    let text = '';
    for (const event of events) {
      text += `${JSON.stringify(event)}\n`;
    }
    try {
      fs.appendFileSync(logPath, text);
    } catch (cause) {
      throw new LogFileError(
        `failed to append to lease log at ${logPath}`,
        cause,
      );
    }
  }

  function appendEvent(event) {
    appendEvents([event]);
  }

  function detachGroup(group) {
    for (const position of group.positions) {
      const queue = waitQueues.get(position.resource);
      if (queue === undefined) {
        continue;
      }
      const index = queue.indexOf(position);
      if (index !== -1) {
        queue.splice(index, 1);
        if (queue.length === 0) {
          waitQueues.delete(position.resource);
        }
      }
    }
    pendingGroups.delete(group.requestId);
  }

  // A queued request gives up once the clock reaches its deadline and is
  // never woken afterwards. Dropping it moves no counter and no ownership.
  function purgeExpiredWaiters(now) {
    const dead = [];
    for (const group of pendingGroups.values()) {
      if (group.deadline <= now) {
        dead.push(group);
      }
    }
    for (const group of dead) {
      detachGroup(group);
      group.ticket.status = 'expired';
    }
  }

  function enqueueGroup(resources, ttlList, holder, now, waitMs, single) {
    requestSequence += 1;
    const requestId = `wait-${registryId}-${requestSequence}`;
    const ticket = single
      // Exact baseline shape for single-resource callers.
      ? { status: 'waiting', resource: resources[0], holder, waitId: requestId }
      : { status: 'waiting', resources: [...resources], holder, waitId: requestId };
    const group = {
      requestId,
      arrival: requestSequence,
      holder,
      deadline: now + waitMs,
      single,
      ticket,
      positions: [],
    };
    // One position per listed occurrence, pushed onto each resource's queue
    // in the given order. Positions of one request land consecutively, so a
    // repeated resource lines up as many times as it was listed.
    resources.forEach((resource, index) => {
      const position = {
        group,
        resource,
        ttlMs: ttlList[index],
        granted: false,
        token: null,
        expiresAt: null,
      };
      group.positions.push(position);
      let queue = waitQueues.get(resource);
      if (queue === undefined) {
        queue = [];
        waitQueues.set(resource, queue);
      }
      queue.push(position);
    });
    pendingGroups.set(requestId, group);
    return group;
  }

  // Plan every grant one request needs (one per still-ungranted position),
  // touching no state. The caller commits only after all events are on disk.
  // batchId ties the credentials of one atomic batch together for replay.
  function planGroupGrants(group, now, seqCursor) {
    const batchId = nextBatchId();
    const grants = [];
    let seq = seqCursor;
    for (const position of group.positions) {
      if (position.granted) {
        continue;
      }
      const minted = mintToken(now, seq);
      seq = minted.seq;
      const expiresAt = now + position.ttlMs;
      grants.push({
        position,
        group,
        resource: position.resource,
        holder: group.holder,
        ttlMs: position.ttlMs,
        token: minted.token,
        seq: minted.seq,
        expiresAt,
        event: {
          v: LOG_VERSION,
          type: 'acquire',
          at: now,
          token: minted.token,
          resource: position.resource,
          holder: group.holder,
          ttlMs: position.ttlMs,
          expiresAt,
          requestId: group.requestId,
          batch: batchId,
        },
      });
    }
    return { group, grants, seq, batchId };
  }

  function applyPlannedGrants(plan, now) {
    for (const grant of plan.grants) {
      tokenSequence = grant.seq;
      knownTokens.add(grant.token);
      const lease = {
        resource: grant.resource,
        holder: grant.holder,
        expiresAt: grant.expiresAt,
        ttlMs: grant.ttlMs,
        legacy: false,
        heapSeq: 0,
        batch: plan.batchId,
      };
      leases.set(grant.token, lease);
      dropStaleSlots(grant.resource, now);
      addSlot(grant.resource, grant.token);
      enqueue(grant.token, lease);
      counters.granted += 1;

      grant.position.token = grant.token;
      grant.position.expiresAt = grant.expiresAt;
      grant.position.granted = true;
    }

    // The batch holds everything it asked for; it leaves every queue at once.
    const { group } = plan;
    const { ticket } = group;
    detachGroup(group);
    ticket.status = 'granted';
    const granted = group.positions.map((position) => ({
      resource: position.resource,
      holder: group.holder,
      token: position.token,
      expiresAt: position.expiresAt,
    }));
    if (group.single) {
      ticket.token = granted[0].token;
      ticket.expiresAt = granted[0].expiresAt;
    } else {
      // One credential per listed occurrence, each with its own expiry.
      ticket.leases = granted;
      // Aliases for callers that read the batch outcome as a flat list.
      ticket.results = granted;
    }
  }

  function positionPlanned(plans, position) {
    return plans.some((plan) =>
      plan.grants.some((grant) => grant.position === position));
  }

  // A queued request is fillable exactly when every position reaches the
  // front of its resource's queue and that resource is free once the
  // triggering event is applied. Only live credentials occupy: an expired
  // but unswept lease is already gone, while a surviving twin of a released
  // duplicate credential keeps the resource held.
  function groupFillable(group, plans, freedLiveCount, now) {
    for (const position of group.positions) {
      if (position.granted) {
        continue;
      }
      const queue = waitQueues.get(position.resource);
      if (queue === undefined) {
        return false;
      }
      const index = queue.indexOf(position);
      for (let i = 0; i < index; i += 1) {
        const ahead = queue[i];
        if (ahead.group === group) {
          // Own repeated listing: its positions are granted together.
          continue;
        }
        if (positionPlanned(plans, ahead)) {
          continue;
        }
        return false;
      }
      // A release vacates a live credential; a sweep only removes leases
      // that were already expired, so it frees no live occupancy here.
      let occupied = Math.max(
        0,
        liveSlotCount(position.resource, now)
          - (freedLiveCount.get(position.resource) ?? 0),
      );
      for (const plan of plans) {
        if (plan.group === group) {
          continue;
        }
        for (const grant of plan.grants) {
          if (grant.resource === position.resource) {
            occupied += 1;
          }
        }
      }
      if (occupied > 0) {
        return false;
      }
    }
    return true;
  }

  // Grant every queued request whose resources become available through the
  // given freeing event, cascading across resources and queues. Entries are
  // { resource, live }: a release vacates a live credential, a reclaim only
  // removes one that had already expired. Everything planned rides in the
  // triggering log write.
  function planWakeups(freedEntries, now) {
    const freedLiveCount = new Map();
    for (const entry of freedEntries) {
      if (entry.live) {
        freedLiveCount.set(
          entry.resource,
          (freedLiveCount.get(entry.resource) ?? 0) + 1,
        );
      }
    }
    const plans = [];
    let seq = tokenSequence;
    for (;;) {
      // Candidates are picked strictly by arrival order: the earliest
      // pending request whose batch is now complete wins, so no resource's
      // queue can jump ahead of an earlier request.
      let next = null;
      for (const group of pendingGroups.values()) {
        if (plans.some((plan) => plan.group === group)) {
          continue;
        }
        if (!groupFillable(group, plans, freedLiveCount, now)) {
          continue;
        }
        if (next === null || group.arrival < next.arrival) {
          next = group;
        }
      }
      if (next === null) {
        break;
      }
      const plan = planGroupGrants(next, now, seq);
      seq = plan.seq;
      plans.push(plan);
    }
    return plans;
  }

  function nextBatchId() {
    batchSequence += 1;
    return `batch-${registryId}-${batchSequence}`;
  }

  // Resolve the ttl argument into one positive finite ttl per occurrence.
  function resolveTtls(resources, ttlMs) {
    if (ttlMs === undefined) {
      return resources.map(() => defaultTtlMs);
    }
    if (Array.isArray(ttlMs)) {
      if (ttlMs.length !== resources.length) {
        throw new TypeError('ttlMs must match the resources list length');
      }
      return ttlMs.map((value) => {
        if (!Number.isFinite(value) || value <= 0) {
          throw new TypeError('ttlMs must be a positive finite number');
        }
        return value;
      });
    }
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new TypeError('ttlMs must be a positive finite number');
    }
    return resources.map(() => ttlMs);
  }

  // Persist and commit one credential per listed occurrence, all in a single
  // log write. A failed write leaves every resource exactly as before.
  function commitDirectGrants(resources, ttlList, holder, now) {
    const batchId = nextBatchId();
    const events = [];
    const grants = [];
    let seq = tokenSequence;
    resources.forEach((resource, index) => {
      const ttlMsValue = ttlList[index];
      const minted = mintToken(now, seq);
      seq = minted.seq;
      const expiresAt = now + ttlMsValue;
      events.push({
        v: LOG_VERSION,
        type: 'acquire',
        at: now,
        token: minted.token,
        resource,
        holder,
        ttlMs: ttlMsValue,
        expiresAt,
        batch: batchId,
      });
      grants.push({ resource, ttlMs: ttlMsValue, token: minted.token, expiresAt });
    });
    appendEvents(events);

    tokenSequence = seq;
    const result = [];
    for (const grant of grants) {
      knownTokens.add(grant.token);
      const lease = {
        resource: grant.resource,
        holder,
        expiresAt: grant.expiresAt,
        ttlMs: grant.ttlMs,
        legacy: false,
        heapSeq: 0,
        batch: batchId,
      };
      leases.set(grant.token, lease);
      dropStaleSlots(grant.resource, now);
      addSlot(grant.resource, grant.token);
      enqueue(grant.token, lease);
      counters.granted += 1;
      result.push({
        resource: grant.resource,
        holder,
        token: grant.token,
        expiresAt: grant.expiresAt,
      });
    }
    return result;
  }

  // Atomic multi-resource acquisition. Every listed occurrence gets its own
  // credential and independent expiry; either all of them are held, or none
  // are (the whole request queues instead). ttlMs may be one number or one
  // ttl per listed resource.
  function acquireAll(resources, holder, ttlMs = undefined, waitMs = undefined) {
    if (!Array.isArray(resources) || resources.length === 0) {
      throw new TypeError('resources must be a non-empty array');
    }
    const ttlList = resolveTtls(resources, ttlMs);
    const wantsWait = validateWait(waitMs);
    const now = clock();
    purgeExpiredWaiters(now);

    // Earlier waiters keep their place, and any live lease on a listed
    // resource blocks the whole batch; a resource listed twice is free for
    // this request only when it is not held at all.
    let conflict = null;
    for (const resource of resources) {
      if (waitQueues.has(resource) || liveSlotCount(resource, now) > 0) {
        conflict = resource;
        break;
      }
    }

    if (conflict !== null) {
      if (!wantsWait) {
        throw new LeaseTakenError(conflict, liveHolder(conflict, now));
      }
      return enqueueGroup(
        resources, ttlList, holder, now, waitMs, false,
      ).ticket;
    }

    return commitDirectGrants(resources, ttlList, holder, now);
  }

  function validateWait(waitMs) {
    const wantsWait = waitMs !== undefined && waitMs !== null;
    if (wantsWait && (!Number.isFinite(waitMs) || waitMs <= 0)) {
      throw new TypeError('waitMs must be a positive finite number');
    }
    return wantsWait;
  }

  function acquire(resourceOrResources, holder, ttlMs = undefined, waitMs = undefined) {
    // Array input is the multi-resource form: same entry point, atomic rules.
    if (Array.isArray(resourceOrResources)) {
      return acquireAll(resourceOrResources, holder, ttlMs, waitMs);
    }
    const resource = resourceOrResources;
    const effectiveTtl = ttlMs === undefined ? defaultTtlMs : ttlMs;
    if (!Number.isFinite(effectiveTtl) || effectiveTtl <= 0) {
      throw new TypeError('ttlMs must be a positive finite number');
    }
    const wantsWait = validateWait(waitMs);
    const now = clock();
    purgeExpiredWaiters(now);

    if (liveSlotCount(resource, now) > 0 || waitQueues.has(resource)) {
      if (!wantsWait) {
        throw new LeaseTakenError(resource, liveHolder(resource, now));
      }
      // Occupied or reserved: take a place in line instead of throwing. The
      // resource does not change hands just because a waiter showed up.
      return enqueueGroup(
        [resource], [effectiveTtl], holder, now, waitMs, true,
      ).ticket;
    }

    const [granted] = commitDirectGrants(
      [resource], [effectiveTtl], holder, now,
    );
    return granted;
  }

  function renew(token) {
    const lease = leases.get(token);
    if (lease === undefined || lease.legacy) {
      return false;
    }
    const now = clock();
    if (lease.expiresAt <= now) {
      return false;
    }
    purgeExpiredWaiters(now);
    const expiresAt = now + lease.ttlMs;
    appendEvent({ v: LOG_VERSION, type: 'renew', at: now, token, expiresAt });

    lease.expiresAt = expiresAt;
    lease.heapSeq += 1;
    enqueue(token, lease);
    counters.renewed += 1;
    return true;
  }

  function release(token) {
    const lease = leases.get(token);
    if (lease === undefined || lease.legacy) {
      return false;
    }
    const now = clock();
    if (lease.expiresAt <= now) {
      return false;
    }
    purgeExpiredWaiters(now);

    // The freed resource goes to the requests at the head of its queue; a
    // group only wakes when every copy it needs is free. All wakeup grants
    // ride in the same write as the release, so a failed log leaves the lease
    // and every queue untouched.
    const plans = planWakeups([{ resource: lease.resource, live: true }], now);
    const events = [{ v: LOG_VERSION, type: 'release', at: now, token }];
    for (const plan of plans) {
      for (const grant of plan.grants) {
        events.push(grant.event);
      }
    }
    appendEvents(events);

    // The record comes off immediately; its queued node retires lazily.
    leases.delete(token);
    removeSlot(lease.resource, token);
    counters.released += 1;
    for (const plan of plans) {
      applyPlannedGrants(plan, now);
    }
    return true;
  }

  // Cancelling only lands on a request that is still queued: an already
  // woken, expired or cancelled request reports false, as does an unknown id.
  function cancel(requestId) {
    purgeExpiredWaiters(clock());
    const group = pendingGroups.get(requestId);
    if (group === undefined) {
      return false;
    }
    detachGroup(group);
    group.ticket.status = 'cancelled';
    return true;
  }

  function sweep(now = clock()) {
    purgeExpiredWaiters(now);
    const due = [];
    while (expiryQueue.size > 0) {
      const node = expiryQueue.peek();
      if (node.expiresAt > now) {
        break;
      }
      expiryQueue.pop();
      const lease = leases.get(node.token);
      if (lease === undefined || lease.heapSeq !== node.heapSeq) {
        // Retired node: release, retake or renewal already superseded it.
        continue;
      }
      due.push(node);
    }

    if (due.length === 0) {
      return [];
    }

    // Every due node still carries the current lease record (retired nodes
    // were filtered above) and a current lease always occupies its resource.
    const reclaimable = due;

    // Reclaimed resources are handed to the head requests of their queues in
    // the same expiry order the reclaim happens; every wakeup grant rides
    // along in the same log write, so a failed append rolls everything back.
    const events = [{
      v: LOG_VERSION,
      type: 'reclaim',
      at: now,
      items: reclaimable.map((node) => ({
        token: node.token,
        resource: leases.get(node.token).resource,
      })),
    }];
    const plans = planWakeups(
      reclaimable.map((node) => ({
        resource: leases.get(node.token).resource,
        live: false,
      })),
      now,
    );
    for (const plan of plans) {
      for (const grant of plan.grants) {
        events.push(grant.event);
      }
    }

    try {
      appendEvents(events);
    } catch (error) {
      // The write never happened: put the popped nodes back so the queue is
      // byte-for-byte the same as before the call.
      for (const node of due) {
        expiryQueue.push(node);
      }
      throw error;
    }

    const freed = [];
    for (const node of reclaimable) {
      const lease = leases.get(node.token);
      leases.delete(node.token);
      if (removeSlot(lease.resource, node.token)) {
        counters.reclaimed += 1;
        freed.push(lease.resource);
      }
    }
    for (const plan of plans) {
      applyPlannedGrants(plan, now);
    }
    return freed;
  }

  function holder(resource) {
    const now = clock();
    purgeExpiredWaiters(now);
    return liveHolder(resource, now);
  }

  function stats() {
    let live = 0;
    const now = clock();
    purgeExpiredWaiters(now);
    for (const list of slots.values()) {
      for (const token of list) {
        if (isLive(leases.get(token), now)) {
          live += 1;
        }
      }
    }
    return {
      granted: counters.granted,
      renewed: counters.renewed,
      released: counters.released,
      reclaimed: counters.reclaimed,
      live,
    };
  }

  // Credentials of the atomic batch currently being replayed: a batch takes
  // over a resource as one unit, so its own earlier credentials must not be
  // treated as the stale lease it replaces.
  let replayBatchId = null;
  let replayBatchTokens = new Set();
  // Highest batch number seen in the log, so ids minted after a restart can
  // never collide with an earlier process's batches.
  let replayMaxBatchSeq = 0;

  replay();
  batchSequence = Math.max(batchSequence, replayMaxBatchSeq);

  return {
    acquire,
    acquireAll,
    acquireGroup: acquireAll,
    acquireMany: acquireAll,
    renew,
    release,
    sweep,
    holder,
    stats,
    cancel,
  };

  function replay() {
    if (logPath === null) {
      return;
    }

    let text;
    try {
      text = fs.readFileSync(logPath, 'utf8');
    } catch (cause) {
      if (cause.code === 'ENOENT') {
        // No log yet: start from an empty registry.
        return;
      }
      throw new LogFileError(
        `failed to read lease log at ${logPath}`,
        cause,
      );
    }

    const lines = text.split('\n');
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      if (line.trim() === '') {
        continue;
      }
      let event;
      try {
        event = JSON.parse(line);
      } catch (cause) {
        throw new LogFileError(
          `corrupt lease log entry on line ${i + 1} of ${logPath}`,
          cause,
        );
      }
      replayEvent(event, i + 1);
    }
  }

  function replayEvent(event, lineNumber) {
    if (event === null || typeof event !== 'object' || event.v !== LOG_VERSION) {
      throw new LogFileError(
        `unrecognized lease log version on line ${lineNumber} of ${logPath}`,
      );
    }

    switch (event.type) {
      case 'acquire': {
        if (
          typeof event.token !== 'string' ||
          !Number.isFinite(event.ttlMs) ||
          !Number.isFinite(event.expiresAt)
        ) {
          throw new LogFileError(
            `corrupt acquire entry on line ${lineNumber} of ${logPath}`,
          );
        }
        // Credentials minted together for one atomic batch superseded any
        // expired, unswept lease on the resource as one unit: only slots held
        // by a different batch count as the stale lease being retaken.
        if (event.batch !== undefined && event.batch !== null) {
          const logged = /^batch-[^-]+-(\d+)$/.exec(String(event.batch));
          if (logged !== null) {
            replayMaxBatchSeq = Math.max(
              replayMaxBatchSeq,
              Number(logged[1]),
            );
          }
          if (event.batch !== replayBatchId) {
            replayBatchId = event.batch;
            replayBatchTokens = new Set();
          }
        } else {
          replayBatchId = null;
          replayBatchTokens = new Set();
        }
        const occupantTokens = slots.get(event.resource);
        if (occupantTokens !== undefined) {
          for (const oldToken of [...occupantTokens]) {
            if (!replayBatchTokens.has(oldToken)) {
              leases.delete(oldToken);
              removeSlot(event.resource, oldToken);
            }
          }
        }
        const lease = {
          resource: event.resource,
          holder: event.holder,
          expiresAt: event.expiresAt,
          ttlMs: event.ttlMs,
          // Credentials issued before this process started are void: the
          // lease keeps occupying the resource, but its token cannot
          // renew or release it.
          legacy: true,
          heapSeq: 0,
          batch: event.batch ?? null,
        };
        leases.set(event.token, lease);
        addSlot(event.resource, event.token);
        replayBatchTokens.add(event.token);
        knownTokens.add(event.token);
        enqueue(event.token, lease);
        counters.granted += 1;
        break;
      }
      case 'renew': {
        const lease = leases.get(event.token);
        if (
          typeof event.token !== 'string' ||
          !Number.isFinite(event.expiresAt)
        ) {
          throw new LogFileError(
            `corrupt renew entry on line ${lineNumber} of ${logPath}`,
          );
        }
        if (lease !== undefined) {
          lease.expiresAt = event.expiresAt;
          lease.heapSeq += 1;
          enqueue(event.token, lease);
        }
        counters.renewed += 1;
        break;
      }
      case 'release': {
        if (typeof event.token !== 'string') {
          throw new LogFileError(
            `corrupt release entry on line ${lineNumber} of ${logPath}`,
          );
        }
        const lease = leases.get(event.token);
        if (lease !== undefined) {
          leases.delete(event.token);
          removeSlot(lease.resource, event.token);
        }
        counters.released += 1;
        break;
      }
      case 'reclaim': {
        if (!Array.isArray(event.items)) {
          throw new LogFileError(
            `corrupt reclaim entry on line ${lineNumber} of ${logPath}`,
          );
        }
        for (const item of event.items) {
          if (item === null || typeof item !== 'object' ||
              typeof item.token !== 'string') {
            throw new LogFileError(
              `corrupt reclaim entry on line ${lineNumber} of ${logPath}`,
            );
          }
          const token = item.token;
          const lease = leases.get(token);
          if (lease !== undefined) {
            leases.delete(token);
            removeSlot(lease.resource, token);
          }
          counters.reclaimed += 1;
        }
        break;
      }
      default: {
        throw new LogFileError(
          `unknown lease log event type on line ${lineNumber} of ${logPath}`,
        );
      }
    }
  }
}
