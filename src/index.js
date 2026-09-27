// In-process, time bounded lease registry with append-only log persistence
// and crash-safe snapshots. Time only moves when the injected clock moves;
// nothing expires on its own. Tokens are derived from the injected clock as
// well, never from wall time.
//
// A resource may declare a capacity before the first application: such a
// resource then hands out independent shares up to that capacity, one
// credential per share, instead of the single-occupancy rule resources get by
// default. Asking for N shares of one resource is exactly like listing that
// resource N times: every share gets its own credential and its own expiry,
// so shares renew, release and expire independently. Resources may attach to
// quota nodes arranged in a parent/child hierarchy; every ancestor level
// bounds the shares held by all its descendants at once.
//
// Persistence layout:
//   <logPath>           append-only event log, one JSON object per line
//   <logPath>.snapshot  last compacted state image (single JSON line)
// Startup loads the snapshot (if any) and replays only the events past the
// compacted boundary marker. Compaction publishes both files through a
// temp-file + fsync + rename sequence, so a failure at any step leaves the
// previous snapshot and the previous log usable. Every event carries a
// monotonic sequence number; a snapshot found next to an un-exchanged
// original log (the log swap died after the snapshot rename) simply skips
// the already-folded events by their sequence ids, so each event applies
// exactly once regardless of which rename made it to disk.

import fs from 'node:fs';
import path from 'node:path';

const LOG_VERSION = 1;
const SNAPSHOT_VERSION = 1;

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

// A request that fits every resource capacity but would push a quota node
// past its limit fails as one unit with this assertable code. It stays a
// LeaseTakenError, so callers handling the single-occupancy failure keep
// working, while `code` names 'QUOTA_EXCEEDED' and `quota` names the level
// that rejected the batch.
export class QuotaExceededError extends LeaseTakenError {
  constructor(quota) {
    super(null, null);
    this.name = 'QuotaExceededError';
    this.code = QuotaExceededError.code;
    this.message = `quota ${String(quota)} would be exceeded`;
    this.resource = null;
    this.quota = quota;
  }
}
QuotaExceededError.code = 'QUOTA_EXCEEDED';

export class LogFileError extends Error {
  constructor(message, cause) {
    super(message);
    this.name = 'LogFileError';
    this.code = LogFileError.code;
    this.cause = cause ?? null;
  }
}
LogFileError.code = 'LOG_FILE_ERROR';

// Binary min-heap parameterized by its ordering. It backs both the global
// expiry queue and the per-resource share heaps.
class Heap {
  constructor(before) {
    this.nodes = [];
    this.before = before;
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
      if (this.before(nodes[i], nodes[parent])) {
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
    const before = this.before;
    const top = nodes[0];
    const last = nodes.pop();
    if (nodes.length > 0) {
      nodes[0] = last;
      let i = 0;
      for (;;) {
        const left = i * 2 + 1;
        const right = left + 1;
        let next = i;
        if (left < nodes.length && before(nodes[left], nodes[next])) {
          next = left;
        }
        if (right < nodes.length && before(nodes[right], nodes[next])) {
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

function deadlineBefore(a, b) {
  if (a.deadline !== b.deadline) {
    return a.deadline < b.deadline;
  }
  return a.tie < b.tie;
}

let registrySequence = 0;
let atomicFileSequence = 0;

function asNameMap(value) {
  if (value === null || value === undefined) {
    return new Map();
  }
  if (value instanceof Map) {
    return new Map(value);
  }
  if (Array.isArray(value)) {
    return new Map(value);
  }
  if (typeof value === 'object') {
    return new Map(Object.entries(value));
  }
  throw new TypeError('capacities and quotas must be a Map or a plain object');
}

export function createRegistry({
  ttlMs,
  clock = Date.now,
  logPath = null,
  capacities = null,
  quotas = null,
} = {}) {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new TypeError('ttlMs must be a positive finite number');
  }
  if (typeof clock !== 'function') {
    throw new TypeError('clock must be a function');
  }

  const snapshotPath = logPath === null ? null : `${logPath}.snapshot`;
  const defaultTtlMs = ttlMs;
  // resource -> FIFO array of queue positions, one position per requested
  // share occurrence. A request listing one resource twice (or asking for two
  // shares of it) occupies two consecutive positions in that queue.
  const waitQueues = new Map();
  // requestId -> group, while the request is still queued.
  const pendingGroups = new Map();
  // quota node -> queued groups demanding that node, so freeing quota on one
  // branch reaches a waiter queued on another branch without scanning every
  // queue (or every outstanding share) in the registry.
  const quotaWaiters = new Map();
  // token -> { resource, holder, expiresAt, ttlMs, legacy, heapSeq, batch }.
  // A credential is always exactly one share: a request that takes several
  // shares is handed that many credentials, each with its own independent
  // expiry, so one of them can renew, release or expire on its own.
  const leases = new Map();
  // Every token this instance knows about, including replayed credentials
  // that were already released or reclaimed, so a freshly minted token can
  // never reuse a dead credential's string.
  const knownTokens = new Set();
  // resource -> set of tokens currently occupying it, one token per share.
  // An atomic group listing one resource several times holds one credential
  // per occurrence, all belonging to the same request.
  const slots = new Map();
  // resource -> expiry min-heap of its share nodes. Telling which shares of
  // one resource are past their expiry then only pops the due head of that
  // resource's heap instead of scanning every share the resource holds.
  const slotHeaps = new Map();
  // quota node -> shares currently charged against it (one per live or
  // expired-but-unswept credential descending from the node).
  const quotaUsed = new Map();
  // Declared resource capacities (a missing entry means single occupancy)
  // and the optional quota node a resource's shares charge to.
  const resourceCapacities = new Map();
  const resourceQuota = new Map();
  // quota node -> { limit, parent }.
  const quotaDefs = new Map();
  // Resources a credential has ever been granted on: their shape (capacity
  // or quota attachment) is fixed from then on.
  const grantedResources = new Set();
  // Global reclaim heap: one node per credential, ordered by expiry. Renewals
  // push a fresh node and retire the old one.
  const expiryQueue = new Heap(expiresBefore);
  // One node per queued wait request, ordered by wait deadline. Nodes outlive
  // their request only until the next lazy pop, flagged `dead`.
  const deadlineQueue = new Heap(deadlineBefore);

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
  let deadlineTie = 0;
  let requestSequence = 0;
  let batchSequence = 0;
  // Monotonic id stamped on every persisted event; a snapshot records the
  // last id it folded in, so a log still containing the folded prefix (the
  // log swap failed) replays those events as no-ops instead of twice.
  let eventSeq = 0;
  // Latest clock reading, used by the non-destructive feasibility probe.
  let clockNow = 0;

  function capacityFor(resource) {
    const declared = resourceCapacities.get(resource);
    return declared === undefined ? 1 : declared;
  }

  function isDeclared(resource) {
    return resourceCapacities.has(resource);
  }

  function validatePositiveInteger(value, label) {
    if (!Number.isInteger(value) || value <= 0) {
      throw new TypeError(`${label} must be a positive integer`);
    }
  }

  // The quota chain a share on `resource` charges: the resource's own node
  // first, then every ancestor up to the root. Empty when the resource is not
  // attached to a quota.
  function quotaChain(resource) {
    const chain = [];
    let cur = resourceQuota.get(resource);
    if (cur === undefined) {
      return chain;
    }
    const seen = new Set();
    while (cur !== null && cur !== undefined && !seen.has(cur)) {
      seen.add(cur);
      chain.push(cur);
      const def = quotaDefs.get(cur);
      cur = def === undefined ? null : def.parent;
    }
    return chain;
  }

  function quotaChainWouldCycle(node, parent) {
    let cur = parent;
    const seen = new Set([node]);
    while (cur !== null) {
      if (seen.has(cur)) {
        return true;
      }
      seen.add(cur);
      const def = quotaDefs.get(cur);
      cur = def === undefined ? null : def.parent;
    }
    return false;
  }

  function declareQuota(node, limit, parent = null) {
    validatePositiveInteger(limit, 'quota limit');
    if (parent !== null && !quotaDefs.has(parent)) {
      throw new TypeError(`unknown parent quota ${String(parent)}`);
    }
    if (quotaDefs.has(node)) {
      throw new TypeError(`quota ${String(node)} is already declared`);
    }
    if (quotaChainWouldCycle(node, parent)) {
      throw new TypeError(`quota chain under ${String(node)} would contain a cycle`);
    }
    // Runtime declarations ride the log (constructor seeds do not: they are
    // reconciled against the history during load). The map moves only after
    // the append lands, so a failed write rolls the declaration back too.
    if (declarationsLoaded && logPath !== null) {
      appendEvent({
        v: LOG_VERSION, type: 'declare-quota', at: clock(),
        node, limit, parent,
      });
    }
    quotaDefs.set(node, { limit, parent });
  }

  // Declarations only move before the first grant touches a resource:
  // changing the shape under live shares would rewrite the meaning of
  // outstanding credentials.
  function declareResource(resource, capacity, quota = null) {
    validatePositiveInteger(capacity, 'capacity');
    if (quota !== null && !quotaDefs.has(quota)) {
      throw new TypeError(`unknown quota node ${String(quota)}`);
    }
    const priorCapacity = resourceCapacities.get(resource);
    if (priorCapacity !== undefined && priorCapacity !== capacity) {
      throw new TypeError(`resource ${String(resource)} already declares another capacity`);
    }
    const priorQuota = resourceQuota.has(resource) ? resourceQuota.get(resource) : null;
    if (resourceQuota.has(resource) && priorQuota !== quota) {
      throw new TypeError(`resource ${String(resource)} already attaches to another quota`);
    }
    if (
      (grantedResources.has(resource) || slots.has(resource) || waitQueues.has(resource)) &&
      (priorCapacity === undefined || priorQuota !== quota)
    ) {
      throw new TypeError(`resource ${String(resource)} cannot be declared after it is in use`);
    }
    if (declarationsLoaded && logPath !== null && priorCapacity === undefined) {
      appendEvent({
        v: LOG_VERSION, type: 'declare-resource', at: clock(),
        resource, capacity, quota,
      });
    }
    resourceCapacities.set(resource, capacity);
    if (quota !== null) {
      resourceQuota.set(resource, quota);
    }
  }

  // Constructor seeds reconcile with the replayed history but are never
  // themselves appended; the flag flips once load() has finished, after which
  // runtime declare* calls persist.
  let declarationsLoaded = false;

  // Seed declarations handed in through the constructor options. Quotas come
  // first so capacity entries may attach to a node in the same options bag;
  // parents are wired in dependency order, so map insertion order never
  // matters.
  {
    const pending = [];
    for (const [node, spec] of asNameMap(quotas)) {
      if (spec !== null && typeof spec === 'object' && !Array.isArray(spec)) {
        pending.push([node, spec.limit, spec.parent ?? null]);
      } else {
        pending.push([node, spec, null]);
      }
    }
    let remaining = pending;
    while (remaining.length > 0) {
      const blocked = [];
      for (const [node, limit, parent] of remaining) {
        if (parent !== null && !quotaDefs.has(parent)) {
          blocked.push([node, limit, parent]);
        } else {
          declareQuota(node, limit, parent);
        }
      }
      if (blocked.length === remaining.length) {
        // Every leftover entry names an undeclared parent; surface it.
        declareQuota(blocked[0][0], blocked[0][1], blocked[0][2]);
      }
      remaining = blocked;
    }
  }
  for (const [resource, spec] of asNameMap(capacities)) {
    if (spec !== null && typeof spec === 'object' && !Array.isArray(spec)) {
      declareResource(resource, spec.capacity, spec.quota ?? null);
    } else {
      declareResource(resource, spec);
    }
  }

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

  function slotHeap(resource) {
    let heap = slotHeaps.get(resource);
    if (heap === undefined) {
      heap = new Heap(expiresBefore);
      slotHeaps.set(resource, heap);
    }
    return heap;
  }

  function getSlots(resource) {
    let set = slots.get(resource);
    if (set === undefined) {
      set = new Set();
      slots.set(resource, set);
    }
    return set;
  }

  // One credential occupies exactly one share on its resource and charges
  // exactly one share to every quota level in its resource's chain.
  function addOccupant(token, lease) {
    getSlots(lease.resource).add(token);
    slotHeap(lease.resource).push({
      token,
      expiresAt: lease.expiresAt,
      heapSeq: lease.heapSeq,
      order: queueOrder++,
    });
    for (const node of quotaChain(lease.resource)) {
      quotaUsed.set(node, (quotaUsed.get(node) ?? 0) + 1);
    }
  }

  function removeOccupant(token, lease) {
    const set = slots.get(lease.resource);
    if (set !== undefined) {
      set.delete(token);
      if (set.size === 0) {
        slots.delete(lease.resource);
        // No live or expired-unswept share remains on this resource, so its
        // heap now holds only retired nodes; drop the whole structure.
        slotHeaps.delete(lease.resource);
      }
    }
    for (const node of quotaChain(lease.resource)) {
      quotaUsed.set(node, (quotaUsed.get(node) ?? 0) - 1);
    }
  }

  // Non-destructive stale read for a resource about to be applied for: due
  // share nodes are popped only to be pushed straight back, so a rejected
  // application changes neither the lease table nor the future reclaim count.
  // Returns the number of expired-but-unswept shares and how many of them
  // charge each quota level.
  function inspectStale(resource, now) {
    const heap = slotHeaps.get(resource);
    const stale = [];
    let shares = 0;
    const quota = new Map();
    if (heap !== undefined) {
      while (heap.size > 0) {
        const node = heap.peek();
        const lease = leases.get(node.token);
        if (lease === undefined || lease.heapSeq !== node.heapSeq) {
          // Retired node: gone for good, it represents no share anymore.
          heap.pop();
          continue;
        }
        if (node.expiresAt > now) {
          break;
        }
        heap.pop();
        stale.push(node);
        shares += 1;
        for (const qn of quotaChain(resource)) {
          quota.set(qn, (quota.get(qn) ?? 0) + 1);
        }
      }
      for (const node of stale) {
        heap.push(node);
      }
    }
    return { shares, quota };
  }

  // Drop retired and expired-but-unswept shares from a resource whose new
  // shares are being committed: the shares are being taken over, and a retaken
  // share must never be reported as reclaimed later. Mirrors the
  // single-resource retake rule.
  function dropStale(resource, now) {
    const heap = slotHeaps.get(resource);
    if (heap === undefined) {
      return;
    }
    while (heap.size > 0) {
      const node = heap.peek();
      const lease = leases.get(node.token);
      if (lease === undefined || lease.heapSeq !== node.heapSeq) {
        heap.pop();
        continue;
      }
      if (node.expiresAt > now) {
        break;
      }
      heap.pop();
      leases.delete(node.token);
      getSlots(resource).delete(node.token);
      for (const qn of quotaChain(resource)) {
        quotaUsed.set(qn, (quotaUsed.get(qn) ?? 0) - 1);
      }
    }
    const set = slots.get(resource);
    if (set !== undefined && set.size === 0) {
      slots.delete(resource);
    }
  }

  // Earliest still-valid holder without scanning the resource's shares: the
  // first live node at the heap head is a holder; due nodes ahead of it are
  // set aside and put back verbatim because their shares still await sweep.
  function liveHolder(resource, now) {
    const heap = slotHeaps.get(resource);
    if (heap === undefined) {
      return null;
    }
    const restore = [];
    let holderName = null;
    while (heap.size > 0) {
      const node = heap.peek();
      const lease = leases.get(node.token);
      if (lease === undefined || lease.heapSeq !== node.heapSeq) {
        heap.pop();
        continue;
      }
      if (node.expiresAt <= now) {
        restore.push(heap.pop());
        continue;
      }
      holderName = lease.holder;
      break;
    }
    for (const node of restore) {
      heap.push(node);
    }
    return holderName;
  }

  function fsyncDirectory(file) {
    // Best-effort durability for the rename itself: on platforms where
    // fsyncing a directory handle is unsupported there is nothing to sync,
    // and the rename has already committed by the time this runs.
    try {
      const fd = fs.openSync(path.dirname(file), 'r');
      try {
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
    } catch {
      // Ignored: atomicity of the swap does not depend on it.
    }
  }

  // Publish a file via temp + fsync + rename so the target is either the old
  // complete content or the new complete content, never a mixture. Any failure
  // leaves the target untouched and surfaces as an assertable LogFileError.
  function writeAtomic(target, content) {
    atomicFileSequence += 1;
    const tmp = `${target}.tmp-${process.pid}-${atomicFileSequence}`;
    try {
      const fd = fs.openSync(tmp, 'wx');
      try {
        fs.writeFileSync(fd, content, 'utf8');
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      fs.renameSync(tmp, target);
      fsyncDirectory(target);
    } catch (cause) {
      try {
        fs.rmSync(tmp, { force: true });
      } catch {
        // The original error is the one the caller must act on.
      }
      throw new LogFileError(
        `failed to write lease file ${target}`,
        cause,
      );
    }
  }

  function appendEvents(events) {
    if (logPath === null) {
      return;
    }
    // One write for the whole batch: an operation that frees shares and
    // wakes requests either lands completely or not at all. Sequence ids are
    // numbered up front but only committed after the write lands, so a failed
    // append leaves both the log and the counter exactly as before.
    let text = '';
    let seq = eventSeq;
    for (const event of events) {
      seq += 1;
      event.e = seq;
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
    eventSeq = seq;
  }

  function appendEvent(event) {
    appendEvents([event]);
  }

  function registerQuotaWait(group) {
    for (const node of group.quotaNeeds) {
      let set = quotaWaiters.get(node);
      if (set === undefined) {
        set = new Set();
        quotaWaiters.set(node, set);
      }
      set.add(group);
    }
  }

  function unregisterQuotaWait(group) {
    for (const node of group.quotaNeeds) {
      const set = quotaWaiters.get(node);
      if (set === undefined) {
        continue;
      }
      set.delete(group);
      if (set.size === 0) {
        quotaWaiters.delete(node);
      }
    }
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
    unregisterQuotaWait(group);
    // Its deadline node retires lazily on the next purge.
    group.deadlineNode.dead = true;
  }

  // A queued request gives up once the clock reaches its deadline and is
  // never woken afterwards. Only the deadline heap is consulted: requests
  // that are still live are never even looked at, no matter how many wait.
  function purgeExpiredWaiters(now) {
    while (deadlineQueue.size > 0) {
      const node = deadlineQueue.peek();
      if (node.dead) {
        deadlineQueue.pop();
        continue;
      }
      if (node.deadline > now) {
        break;
      }
      deadlineQueue.pop();
      const group = pendingGroups.get(node.requestId);
      if (group === undefined || group.deadlineNode !== node) {
        continue;
      }
      detachGroup(group);
      group.ticket.status = 'expired';
    }
  }

  // One position per share occurrence: this group's demand at each resource
  // and at each quota level is just the number of positions that land there.
  function demandsFor(positions) {
    const resourceCount = new Map();
    const quotaCount = new Map();
    for (const position of positions) {
      resourceCount.set(
        position.resource,
        (resourceCount.get(position.resource) ?? 0) + 1,
      );
      for (const node of quotaChain(position.resource)) {
        quotaCount.set(node, (quotaCount.get(node) ?? 0) + 1);
      }
    }
    return { resourceCount, quotaCount };
  }

  function enqueueGroup(items, holder, now, waitMs, single) {
    requestSequence += 1;
    const requestId = `wait-${registryId}-${requestSequence}`;
    const resources = items.map((item) => item.resource);
    const ticket = single
      // Exact baseline shape for single-resource callers.
      ? { status: 'waiting', resource: resources[0], holder, waitId: requestId }
      : { status: 'waiting', resources: [...resources], holder, waitId: requestId };
    deadlineTie += 1;
    const deadlineNode = { requestId, deadline: now + waitMs, tie: deadlineTie, dead: false };
    const positions = items.map((item) => ({
      group: null,
      resource: item.resource,
      ttlMs: item.ttlMs,
      granted: false,
      token: null,
      expiresAt: null,
    }));
    const demands = demandsFor(positions);
    // Capacity room the batch needs per resource. Every occurrence counts on
    // a declared capacity resource; on an undeclared resource the request's
    // own repeated listings stack as one single-occupancy presence.
    const resourceNeeds = new Map();
    for (const position of positions) {
      if (isDeclared(position.resource)) {
        resourceNeeds.set(
          position.resource,
          (resourceNeeds.get(position.resource) ?? 0) + 1,
        );
      } else {
        resourceNeeds.set(position.resource, 1);
      }
    }
    const group = {
      requestId,
      arrival: requestSequence,
      holder,
      deadline: now + waitMs,
      deadlineNode,
      single,
      ticket,
      positions,
      resourceNeeds,
      // Per-quota demand of the whole batch, and the set of levels for the
      // quota-waiter index.
      quotaCount: demands.quotaCount,
      quotaNeeds: new Set(demands.quotaCount.keys()),
    };
    for (const position of positions) {
      position.group = group;
    }
    deadlineQueue.push(deadlineNode);
    // Positions of one request land consecutively, so a repeated resource or
    // a multi-share request lines up as many times as it asked.
    for (const position of positions) {
      let queue = waitQueues.get(position.resource);
      if (queue === undefined) {
        queue = [];
        waitQueues.set(position.resource, queue);
      }
      queue.push(position);
    }
    pendingGroups.set(requestId, group);
    registerQuotaWait(group);
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
      // A cascade grant retakes the resource's expired-but-unswept head just
      // like a direct grant (the planner already counted that room); a sweep
      // removed its due shares before reaching here, so this is a no-op then.
      dropStale(grant.resource, now);
      addOccupant(grant.token, lease);
      enqueue(grant.token, lease);
      counters.granted += 1;
      grantedResources.add(grant.resource);

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
      // One credential per share occurrence, each with its own expiry.
      ticket.leases = granted;
      // Aliases for callers that read the batch outcome as a flat list.
      ticket.results = granted;
    }
  }

  // Shares the triggering event commits, aggregated per resource and per
  // quota level. `live` says whether the freeing shares were live: a release
  // vacates a live share (it is subtracted directly), while a sweep's shares
  // were already expired and show up as stale heads instead, so they are
  // never counted twice. Plans already built in this cascade ride on top.
  function summarizeFrees(freedEntries, live) {
    const freedShares = new Map();
    const freedQuota = new Map();
    const touched = new Set();
    const touchedQuotas = new Set();
    for (const resource of freedEntries) {
      touched.add(resource);
      if (live) {
        freedShares.set(resource, (freedShares.get(resource) ?? 0) + 1);
        for (const node of quotaChain(resource)) {
          freedQuota.set(node, (freedQuota.get(node) ?? 0) + 1);
          touchedQuotas.add(node);
        }
      } else {
        // Expired shares reach waiters through the stale-head retake path;
        // the quota levels still have to be re-checked because those shares
        // stop charging them the moment this event commits.
        for (const node of quotaChain(resource)) {
          touchedQuotas.add(node);
        }
      }
    }
    return { freedShares, freedQuota, touched, touchedQuotas };
  }

  // A queued request is fillable exactly when every position reaches the
  // front of its resource's queue, every resource has enough committing and
  // remaining live capacity for the shares the request asks there, and every
  // quota level in its ancestry still has room once the triggering live frees,
  // the expired-but-unswept shares this grant (or an earlier grant in the
  // cascade) retakes, and the grants already planned in this cascade are
  // accounted for.
  function groupFillable(
    group, frees, plannedResourceShares, plannedQuotaShares, ctx,
  ) {
    const { freedShares, freedQuota } = frees;
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
        if (positionPlannedRef.has(ahead)) {
          continue;
        }
        return false;
      }
    }
    // Stale shares this group itself retakes: every resource it grants on
    // drops its expired head on commit, so those shares free room for the
    // group's own quota demand. Stale dropped by earlier plans in the cascade
    // is tracked in ctx.droppedStaleQuota.
    const ownStaleQuota = new Map();
    for (const [resource, need] of group.resourceNeeds) {
      const stale = ctx.staleFor(resource);
      // By commit time the resource's stale head is gone either way (this
      // grant drops it, or an earlier cascade grant already did).
      const liveCount = (slots.get(resource)?.size ?? 0) - stale.shares;
      const occupied = Math.max(
        0,
        liveCount - (freedShares.get(resource) ?? 0),
      ) + (plannedResourceShares.get(resource) ?? 0);
      if (occupied + need > capacityFor(resource)) {
        return false;
      }
      if (!ctx.droppedResources.has(resource)) {
        for (const node of quotaChain(resource)) {
          ownStaleQuota.set(node, (ownStaleQuota.get(node) ?? 0) + stale.shares);
        }
      }
    }
    for (const [node, need] of group.quotaCount) {
      const def = quotaDefs.get(node);
      const limit = def === undefined ? Infinity : def.limit;
      const guaranteedStale = (ownStaleQuota.get(node) ?? 0)
        + (ctx.droppedStaleQuota.get(node) ?? 0);
      const occupied = Math.max(
        0,
        (quotaUsed.get(node) ?? 0)
          - (freedQuota.get(node) ?? 0)
          - guaranteedStale,
      ) + (plannedQuotaShares.get(node) ?? 0);
      if (occupied + need > limit) {
        return false;
      }
    }
    return true;
  }

  // Set of positions already granted by plans built in the current cascade;
  // a position reference is stable for the request's life, so identity checks
  // stay O(1) while deciding whether a queue entry still blocks.
  const positionPlannedRef = new Set();

  // Grant every queued request whose shares become available through the
  // freeing event, cascading across resources, quotas and queues. A free on
  // one quota branch can wake a waiter queued on another branch. Everything
  // planned rides in the triggering log write.
  //
  // Only requests standing at the head of a queue touched by the event (plus
  // the requests exposed behind a batch planned in this cascade) can possibly
  // be fillable: a request was not fillable when it queued, and nothing else
  // changed. Wake planning therefore never inspects requests unrelated to the
  // frees, regardless of how many shares are outstanding in total.
  function planWakeups(freedResources, now, live) {
    const frees = summarizeFrees(freedResources, live);
    const plans = [];
    const planned = new Set();
    const candidates = new Set();
    // Shares already committed by plans built in this cascade, kept as
    // running totals so feasibility never replans earlier grants.
    const plannedResourceShares = new Map();
    const plannedQuotaShares = new Map();
    // Expired-but-unswept shares are dropped ("retaken") when a cascade grant
    // lands on their resource, mirroring the single-resource retake rule. A
    // sweep removes every due share unconditionally on commit, so all swept
    // resources start out dropped; a release instead drops a resource's stale
    // head only when a planned grant lands on it. The first inspection of a
    // resource is cached so planning never scans its heap twice.
    const staleCache = new Map();
    const ctx = {
      staleFor(resource) {
        let value = staleCache.get(resource);
        if (value === undefined) {
          value = inspectStale(resource, now);
          staleCache.set(resource, value);
        }
        return value;
      },
      droppedResources: new Set(),
      droppedStaleQuota: new Map(),
    };
    if (!live) {
      for (const resource of frees.touched) {
        ctx.droppedResources.add(resource);
        const stale = ctx.staleFor(resource);
        for (const node of quotaChain(resource)) {
          ctx.droppedStaleQuota.set(
            node,
            (ctx.droppedStaleQuota.get(node) ?? 0) + stale.shares,
          );
        }
      }
    }
    positionPlannedRef.clear();
    const seedHead = (resource) => {
      const queue = waitQueues.get(resource);
      if (queue !== undefined && queue.length > 0) {
        candidates.add(queue[0].group);
      }
    };
    const seedGroupHeads = (group) => {
      for (const position of group.positions) {
        seedHead(position.resource);
      }
    };
    for (const resource of frees.touched) {
      seedHead(resource);
    }
    for (const node of frees.touchedQuotas) {
      const set = quotaWaiters.get(node);
      if (set === undefined) {
        continue;
      }
      for (const group of set) {
        if (!planned.has(group)) {
          seedGroupHeads(group);
        }
      }
    }
    let seq = tokenSequence;
    for (;;) {
      // Candidates are picked strictly by arrival order: the earliest
      // pending request whose batch is now complete wins, so no queue can
      // jump ahead of an earlier request.
      let next = null;
      for (const group of candidates) {
        if (planned.has(group)) {
          continue;
        }
        if (!groupFillable(
          group, frees, plannedResourceShares, plannedQuotaShares, ctx,
        )) {
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
      planned.add(next);
      candidates.delete(next);
      // Committing this plan drops every expired-but-unswept share at the
      // head of each resource it takes (the retake rule); record that room
      // so later cascade candidates see it.
      const planResources = new Set();
      for (const grant of plan.grants) {
        positionPlannedRef.add(grant.position);
        plannedResourceShares.set(
          grant.resource,
          (plannedResourceShares.get(grant.resource) ?? 0) + 1,
        );
        for (const node of quotaChain(grant.resource)) {
          plannedQuotaShares.set(
            node,
            (plannedQuotaShares.get(node) ?? 0) + 1,
          );
        }
        planResources.add(grant.resource);
      }
      for (const resource of planResources) {
        if (ctx.droppedResources.has(resource)) {
          continue;
        }
        ctx.droppedResources.add(resource);
        const stale = ctx.staleFor(resource);
        for (const node of quotaChain(resource)) {
          ctx.droppedStaleQuota.set(
            node,
            (ctx.droppedStaleQuota.get(node) ?? 0) + stale.shares,
          );
        }
      }
      // The granted batch leaves its queues; expose the first request behind
      // it on each resource it took. Granting quota may also expose groups
      // waiting elsewhere under the same quota levels.
      const resources = new Set(plan.grants.map((grant) => grant.resource));
      for (const resource of resources) {
        const queue = waitQueues.get(resource);
        if (queue === undefined) {
          continue;
        }
        for (const position of queue) {
          if (!planned.has(position.group)) {
            candidates.add(position.group);
            break;
          }
        }
      }
      for (const grant of plan.grants) {
        for (const node of quotaChain(grant.resource)) {
          const set = quotaWaiters.get(node);
          if (set === undefined) {
            continue;
          }
          for (const group of set) {
            if (!planned.has(group)) {
              seedGroupHeads(group);
              candidates.add(group);
            }
          }
        }
      }
    }
    return plans;
  }

  function nextBatchId() {
    batchSequence += 1;
    return `batch-${registryId}-${batchSequence}`;
  }

  // Normalize one resource argument: a plain value means one occurrence; an
  // explicit { resource, shares } asks for that many occurrences.
  function normalizeEntry(raw) {
    if (raw !== null && typeof raw === 'object' && !Array.isArray(raw)) {
      if (!Number.isInteger(raw.shares) || raw.shares <= 0) {
        throw new TypeError('shares must be a positive integer');
      }
      return { resource: raw.resource, shares: raw.shares };
    }
    return { resource: raw, shares: 1 };
  }

  // Resolve the ttl argument into one positive finite ttl per occurrence.
  function resolveTtls(count, ttlMs) {
    if (ttlMs === undefined) {
      return Array.from({ length: count }, () => defaultTtlMs);
    }
    if (Array.isArray(ttlMs)) {
      if (ttlMs.length !== count) {
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
    return Array.from({ length: count }, () => ttlMs);
  }

  // Expand the listed resources into one item per share occurrence: an entry
  // asking for N shares becomes N items, each carrying that occurrence's ttl.
  // Multi-share requests are therefore exactly equivalent to listing the
  // resource that many times, and one credential is minted per item.
  function buildItems(rawResources, ttlMs) {
    const entries = rawResources.map(normalizeEntry);
    const occurrenceTtls = resolveTtls(entries.length, ttlMs);
    const items = [];
    entries.forEach((entry, index) => {
      for (let i = 0; i < entry.shares; i += 1) {
        items.push({ resource: entry.resource, ttlMs: occurrenceTtls[index] });
      }
    });
    return items;
  }

  // Capacity/quota feasibility without touching state. Stale shares are read
  // non-destructively, so a rejected application leaves the table exactly as
  // it was (the expired shares still await their sweep).
  function assessItems(items, demands) {
    const staleQuotaTotal = new Map();
    for (const [resource, count] of demands.resourceCount) {
      if (waitQueues.has(resource)) {
        continue;
      }
      const stale = inspectStale(resource, clockNow);
      const occupied = (slots.get(resource)?.size ?? 0) - stale.shares;
      // Every occurrence counts on a declared capacity resource; an
      // undeclared resource only needs its single-occupancy presence.
      const need = isDeclared(resource) ? count : 1;
      if (occupied + need > capacityFor(resource)) {
        return { kind: 'capacity', resource };
      }
      for (const [node, amount] of stale.quota) {
        staleQuotaTotal.set(node, (staleQuotaTotal.get(node) ?? 0) + amount);
      }
    }
    for (const [node, need] of demands.quotaCount) {
      const def = quotaDefs.get(node);
      const limit = def === undefined ? Infinity : def.limit;
      const occupied = (quotaUsed.get(node) ?? 0) - (staleQuotaTotal.get(node) ?? 0);
      if (occupied + need > limit) {
        return { kind: 'quota', quota: node };
      }
    }
    return null;
  }

  // Persist and commit one credential per share occurrence, all in a single
  // log write. A failed write leaves every resource exactly as before.
  function commitDirectGrants(items, holder, now) {
    const batchId = nextBatchId();
    const events = [];
    const grants = [];
    let seq = tokenSequence;
    for (const item of items) {
      const minted = mintToken(now, seq);
      seq = minted.seq;
      const expiresAt = now + item.ttlMs;
      events.push({
        v: LOG_VERSION,
        type: 'acquire',
        at: now,
        token: minted.token,
        resource: item.resource,
        holder,
        ttlMs: item.ttlMs,
        expiresAt,
        batch: batchId,
      });
      grants.push({
        resource: item.resource,
        ttlMs: item.ttlMs,
        token: minted.token,
        expiresAt,
      });
    }
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
      // The feasibility check ran before anything was removed; retake every
      // stale share this credential replaces now that the grant is on disk.
      dropStale(grant.resource, now);
      addOccupant(grant.token, lease);
      enqueue(grant.token, lease);
      counters.granted += 1;
      grantedResources.add(grant.resource);
      result.push({
        resource: grant.resource,
        holder,
        token: grant.token,
        expiresAt: grant.expiresAt,
      });
    }
    return result;
  }

  function validateWait(waitMs) {
    const wantsWait = waitMs !== undefined && waitMs !== null;
    if (wantsWait && (!Number.isFinite(waitMs) || waitMs <= 0)) {
      throw new TypeError('waitMs must be a positive finite number');
    }
    return wantsWait;
  }

  function failureError(reason, now) {
    if (reason.kind === 'quota') {
      return new QuotaExceededError(reason.quota);
    }
    return new LeaseTakenError(reason.resource, liveHolder(reason.resource, now));
  }

  // Atomic multi-resource acquisition. Every listed occurrence (and every
  // requested share) gets its own credential and independent expiry; either
  // all of them are held, or none are (the whole request queues instead).
  // ttlMs may be one number or one ttl per listed resource occurrence.
  function acquireAll(resources, holder, ttlMs = undefined, waitMs = undefined) {
    if (!Array.isArray(resources) || resources.length === 0) {
      throw new TypeError('resources must be a non-empty array');
    }
    const items = buildItems(resources, ttlMs);
    const wantsWait = validateWait(waitMs);
    const now = clock();
    clockNow = now;
    purgeExpiredWaiters(now);

    const demands = demandsFor(items);

    // Earlier waiters keep their place: every queued position reserves room,
    // even on a capacity resource with shares currently free.
    let reason = null;
    for (const item of items) {
      if (waitQueues.has(item.resource)) {
        reason = { kind: 'capacity', resource: item.resource };
        break;
      }
    }
    if (reason === null) {
      reason = assessItems(items, demands);
    }

    if (reason !== null) {
      if (!wantsWait) {
        throw failureError(reason, now);
      }
      return enqueueGroup(items, holder, now, waitMs, false).ticket;
    }

    return commitDirectGrants(items, holder, now);
  }

  function acquire(resourceOrResources, holder, ttlMs = undefined, waitMs = undefined) {
    // Array input is the multi-resource form: same entry point, atomic rules.
    if (Array.isArray(resourceOrResources)) {
      return acquireAll(resourceOrResources, holder, ttlMs, waitMs);
    }
    // An explicit descriptor asks for several shares of one resource. Taking
    // N shares is the same occurrence list as listing the resource N times;
    // N > 1 therefore settles through the atomic multi-occurrence path and
    // returns one credential per share.
    const descriptor = resourceOrResources !== null
      && typeof resourceOrResources === 'object'
      && !Array.isArray(resourceOrResources);
    if (descriptor) {
      if (!Number.isInteger(resourceOrResources.shares)
        || resourceOrResources.shares <= 0) {
        throw new TypeError('shares must be a positive integer');
      }
      if (resourceOrResources.shares === 1) {
        return acquireOne(resourceOrResources.resource, holder, ttlMs, waitMs);
      }
      const expanded = Array.from(
        { length: resourceOrResources.shares },
        () => resourceOrResources.resource,
      );
      return acquireAll(expanded, holder, ttlMs, waitMs);
    }
    return acquireOne(resourceOrResources, holder, ttlMs, waitMs);
  }

  function acquireOne(resource, holder, ttlMs = undefined, waitMs = undefined) {
    const effectiveTtl = ttlMs === undefined ? defaultTtlMs : ttlMs;
    if (!Number.isFinite(effectiveTtl) || effectiveTtl <= 0) {
      throw new TypeError('ttlMs must be a positive finite number');
    }
    const wantsWait = validateWait(waitMs);
    const now = clock();
    clockNow = now;
    purgeExpiredWaiters(now);

    const items = [{ resource, ttlMs: effectiveTtl }];
    const demands = demandsFor(items);

    let reason = null;
    if (waitQueues.has(resource)) {
      reason = { kind: 'capacity', resource };
    } else {
      reason = assessItems(items, demands);
    }

    if (reason !== null) {
      if (!wantsWait) {
        throw failureError(reason, now);
      }
      // Occupied or reserved: take a place in line instead of throwing. The
      // resource does not change hands just because a waiter showed up.
      return enqueueGroup(items, holder, now, waitMs, true).ticket;
    }

    const [granted] = commitDirectGrants(items, holder, now);
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
    // The global reclaim heap retires the old node; the resource's own share
    // heap gets the renewed node the same way. Other shares on the resource,
    // held by anyone, are untouched.
    enqueue(token, lease);
    slotHeap(lease.resource).push({
      token,
      expiresAt: lease.expiresAt,
      heapSeq: lease.heapSeq,
      order: queueOrder++,
    });
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

    // The freed share goes to the requests at the heads of the queue (and to
    // waiters the freed quota reaches on other branches); a group only wakes
    // when every share it needs is committable. All wakeup grants ride in the
    // same write as the release, so a failed log leaves the lease and every
    // queue untouched.
    const plans = planWakeups([lease.resource], now, true);
    const events = [{ v: LOG_VERSION, type: 'release', at: now, token }];
    for (const plan of plans) {
      for (const grant of plan.grants) {
        events.push(grant.event);
      }
    }
    appendEvents(events);

    // The record comes off immediately; its queued nodes retire lazily.
    leases.delete(token);
    removeOccupant(token, lease);
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
    // were filtered above) and a current lease always occupies its share.
    const reclaimable = due;

    // Reclaimed shares are handed to the head requests of their queues (and
    // to quota waiters on other branches) in the same expiry order the
    // reclaim happens; every wakeup grant rides along in the same log write,
    // so a failed append rolls everything back. Reclamation is per
    // credential: one share expiring never moves the other shares of the
    // resource, whatever holder owns them.
    const freedResources = reclaimable.map((node) => leases.get(node.token).resource);
    const events = [{
      v: LOG_VERSION,
      type: 'reclaim',
      at: now,
      items: reclaimable.map((node) => ({
        token: node.token,
        resource: leases.get(node.token).resource,
      })),
    }];
    const plans = planWakeups(freedResources, now, false);
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
      if (slots.get(lease.resource)?.has(node.token)) {
        removeOccupant(node.token, lease);
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
    const now = clock();
    purgeExpiredWaiters(now);
    return {
      granted: counters.granted,
      renewed: counters.renewed,
      released: counters.released,
      reclaimed: counters.reclaimed,
      live: liveShareCount(now),
    };
  }

  // Current valid share count, judged lazily from the expiry heap head: only
  // due nodes are inspected (retired ones dropped for good, live-but-expired
  // ones counted and put straight back), so the read never walks the whole
  // share population.
  function liveShareCount(now) {
    let expired = 0;
    const restore = [];
    while (expiryQueue.size > 0) {
      const node = expiryQueue.peek();
      const lease = leases.get(node.token);
      if (lease === undefined || lease.heapSeq !== node.heapSeq) {
        expiryQueue.pop();
        continue;
      }
      if (node.expiresAt > now) {
        break;
      }
      expiryQueue.pop();
      restore.push(node);
      expired += 1;
    }
    for (const node of restore) {
      expiryQueue.push(node);
    }
    // One credential per share: leases currently in the table minus the ones
    // already due but not yet swept.
    return leases.size - expired;
  }

  function quotaUsage(node) {
    return quotaUsed.get(node) ?? 0;
  }

  function resourceCapacity(resource) {
    return resourceCapacities.get(resource) ?? 1;
  }

  // Fold the whole history into one snapshot and restart the event log from
  // that point. The snapshot is published first (temp + fsync + rename) and
  // the trimmed log second; a failure at either step leaves the previously
  // usable files in place and throws LogFileError. Credentials captured by
  // the snapshot are void afterwards, exactly as across a restart.
  function compact(now = clock()) {
    if (logPath === null) {
      throw new LogFileError('cannot compact a registry without a logPath');
    }

    // Read the log up front: an unreadable log must fail compaction rather
    // than replacing it. Every complete line is already reflected in the
    // in-memory state; an unterminated fragment at the tail (an append that
    // died with the process) is folded nowhere and is dropped by the rewrite.
    try {
      fs.readFileSync(logPath, 'utf8');
    } catch (cause) {
      if (cause.code !== 'ENOENT') {
        throw new LogFileError(`failed to read lease log at ${logPath}`, cause);
      }
    }

    // The image carries every lease still in the table, including expired
    // ones nobody has swept yet, so a sweep after compaction frees and counts
    // them in exactly the same order as before. Only live credentials show in
    // ownership and liveness either way.
    //
    // Equal expiry times are tie-broken by expiry-heap insertion order
    // (renewals push a fresh node), so remember each lease's current node
    // order and serialize in heap-pop order: reclaim order is then identical
    // after a restart even when several credentials expire at the same
    // instant.
    const activeOrder = new Map();
    for (const node of expiryQueue.nodes) {
      const lease = leases.get(node.token);
      if (lease !== undefined && lease.heapSeq === node.heapSeq) {
        activeOrder.set(node.token, node.order);
      }
    }
    const leaseEntries = [];
    for (const [token, lease] of leases.entries()) {
      leaseEntries.push({
        token,
        resource: lease.resource,
        holder: lease.holder,
        expiresAt: lease.expiresAt,
        ttlMs: lease.ttlMs,
        batch: lease.batch ?? null,
        // Local tie-breaker for the sort below; stripped before writing.
        order: activeOrder.get(token) ?? 0,
      });
    }
    leaseEntries.sort((a, b) =>
      a.expiresAt !== b.expiresAt
        ? a.expiresAt - b.expiresAt
        : a.order - b.order);
    for (const entry of leaseEntries) {
      delete entry.order;
    }

    const capacityEntries = {};
    for (const [resource, capacity] of resourceCapacities) {
      const attached = resourceQuota.get(resource);
      capacityEntries[resource] = attached === undefined
        ? capacity
        : { capacity, quota: attached };
    }
    const quotaEntries = [];
    for (const [node, def] of quotaDefs) {
      quotaEntries.push({ node, limit: def.limit, parent: def.parent });
    }
    // Quota occupancy is derivable from the lease image; record it too so a
    // torn or hand-edited image that disagrees with its own leases is
    // rejected on load instead of silently accepted.
    const usageEntries = {};
    for (const [node, amount] of quotaUsed) {
      if (amount !== 0) {
        usageEntries[node] = amount;
      }
    }

    const snapshot = {
      v: SNAPSHOT_VERSION,
      type: 'snapshot',
      at: now,
      seq: eventSeq,
      tokenSeq: tokenSequence,
      batchSeq: batchSequence,
      counters: { ...counters },
      leases: leaseEntries,
      capacities: capacityEntries,
      quotas: quotaEntries,
      usage: usageEntries,
    };

    // 1. Publish the snapshot. If this fails, nothing else has moved and the
    //    original log stays the sole source of truth.
    writeAtomic(snapshotPath, `${JSON.stringify(snapshot)}\n`);
    // 2. Replace the log with a boundary marker naming the last folded event
    //    sequence. Every complete line was folded into the image, so the only
    //    thing dropped here is an unterminated fragment; later events append
    //    after the marker. If this rename never happens after a committed
    //    snapshot, startup finds no marker, folds the whole log and skips
    //    every event whose sequence is already in the image; either order
    //    yields each event exactly once.
    const marker = {
      v: LOG_VERSION,
      type: 'snapshot-start',
      seq: eventSeq,
    };
    writeAtomic(logPath, `${JSON.stringify(marker)}\n`);

    return { seq: eventSeq, leases: leaseEntries.length };
  }

  // Credentials of the atomic batch currently being replayed: a batch takes
  // over shares as one unit, so its own earlier credentials must not be
  // treated as the stale shares it replaces.
  let replayBatchId = null;
  let replayBatchTokens = new Set();
  // Highest batch/event number seen in the log, so ids minted after a restart
  // can never collide with an earlier process's.
  let replayMaxBatchSeq = 0;
  let replayMaxEventSeq = 0;
  let replayMaxTokenSeq = 0;

  load();
  declarationsLoaded = true;
  batchSequence = Math.max(batchSequence, replayMaxBatchSeq);
  eventSeq = Math.max(eventSeq, replayMaxEventSeq);
  // Folded events are skipped during replay, so their credentials (including
  // released or reclaimed ones absent from the image) can never come back
  // from the minting loop alone; the snapshot carries the high-water mark.
  tokenSequence = Math.max(tokenSequence, replayMaxTokenSeq);

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
    compact,
    declareResource,
    declareQuota,
    capacity: resourceCapacity,
    quotaUsage,
  };

  function load() {
    if (logPath === null) {
      return;
    }

    const snapshot = readSnapshot();
    if (snapshot === 'empty') {
      // A snapshot file with no content declares the old history gone:
      // start from a brand-new log and remove the stale files.
      removeIfPresent(snapshotPath);
      removeIfPresent(logPath);
      return;
    }

    let text;
    try {
      text = fs.readFileSync(logPath, 'utf8');
    } catch (cause) {
      if (cause.code === 'ENOENT') {
        text = '';
      } else {
        throw new LogFileError(`failed to read lease log at ${logPath}`, cause);
      }
    }

    // A write that died mid-line leaves bytes after the final newline.
    // Complete entries stay in force; the unterminated fragment never
    // happened, and is trimmed so later appends start on a fresh line.
    const lastNewline = text.lastIndexOf('\n');
    const validLength = lastNewline === -1 ? 0 : lastNewline + 1;
    if (validLength < text.length) {
      try {
        fs.truncateSync(logPath, validLength);
      } catch (cause) {
        throw new LogFileError(
          `failed to trim torn tail from lease log at ${logPath}`,
          cause,
        );
      }
    }

    const validText = text.slice(0, validLength);
    if (snapshot === null) {
      replayStream(validText, 0, false, 0);
      return;
    }

    // A compacted log is always exactly a marker line followed by the tail
    // events. No marker means the swap never committed and the log still
    // holds the whole folded history; the sequence ids make those events
    // apply exactly once.
    const lines = validText === '' ? [] : validText.split('\n');
    let startsAt = 0;
    if (lines.length > 0 && lines[0] !== '') {
      const marker = parseMarker(lines[0], 1);
      if (marker !== null) {
        replayMaxEventSeq = Math.max(replayMaxEventSeq, marker.seq);
        startsAt = 1;
      }
    }

    applySnapshot(snapshot);
    replayStream(lines.slice(startsAt).join('\n'), startsAt, true, snapshot.seq);
  }

  // Structural boundary line a compacted log begins with. It carries the
  // last folded event sequence, mirroring the snapshot.
  function parseMarker(line, lineNumber) {
    let value;
    try {
      value = JSON.parse(line);
    } catch (cause) {
      throw new LogFileError(
        `corrupt lease log entry on line ${lineNumber} of ${logPath}`,
        cause,
      );
    }
    if (
      value === null ||
      typeof value !== 'object' ||
      value.v !== LOG_VERSION ||
      value.type !== 'snapshot-start'
    ) {
      return null;
    }
    if (!Number.isInteger(value.seq) || value.seq < 0) {
      throw new LogFileError(
        `corrupt snapshot marker on line ${lineNumber} of ${logPath}`,
      );
    }
    return value;
  }

  function removeIfPresent(file) {
    try {
      fs.rmSync(file, { force: true });
    } catch (cause) {
      if (cause.code !== 'ENOENT') {
        throw new LogFileError(`failed to remove stale lease file ${file}`, cause);
      }
    }
  }

  // Read and classify the snapshot file:
  //   null      no snapshot file: replay the log from the beginning
  //   'empty'   file present but with no content: start a brand-new log
  //   object    a usable snapshot
  // A damaged image or an unrecognized snapshot version throws LogFileError;
  // the original log is never touched, so it stays a complete fallback.
  function readSnapshot() {
    let raw;
    try {
      raw = fs.readFileSync(snapshotPath, 'utf8');
    } catch (cause) {
      if (cause.code === 'ENOENT') {
        return null;
      }
      throw new LogFileError(`failed to read lease snapshot at ${snapshotPath}`, cause);
    }

    if (raw.trim() === '') {
      // Content missing: the declared behaviour is a fresh start.
      return 'empty';
    }

    let value;
    try {
      value = JSON.parse(raw);
    } catch (cause) {
      throw new LogFileError(
        `corrupt lease snapshot at ${snapshotPath}`,
        cause,
      );
    }

    if (value === null || typeof value !== 'object') {
      throw new LogFileError(`corrupt lease snapshot at ${snapshotPath}`);
    }
    if (value.v !== SNAPSHOT_VERSION || value.type !== 'snapshot') {
      // Unknown version or damaged envelope: refuse to guess.
      throw new LogFileError(
        `unrecognized lease snapshot version at ${snapshotPath}`,
      );
    }
    if (
      value.counters === undefined ||
      value.leases === undefined ||
      value.seq === undefined
    ) {
      // Recognizable envelope, no payload: content is missing.
      return 'empty';
    }
    if (
      typeof value.counters !== 'object' ||
      value.counters === null ||
      !Array.isArray(value.leases) ||
      !Number.isInteger(value.seq) ||
      value.seq < 0 ||
      (value.tokenSeq !== undefined &&
        (!Number.isInteger(value.tokenSeq) || value.tokenSeq < 0)) ||
      !Number.isFinite(value.at) ||
      (value.batchSeq !== undefined &&
        (!Number.isInteger(value.batchSeq) || value.batchSeq < 0)) ||
      (value.capacities !== undefined &&
        (typeof value.capacities !== 'object' || value.capacities === null)) ||
      (value.quotas !== undefined && !Array.isArray(value.quotas)) ||
      (value.usage !== undefined &&
        (typeof value.usage !== 'object' || value.usage === null))
    ) {
      throw new LogFileError(`corrupt lease snapshot at ${snapshotPath}`);
    }

    for (const key of ['granted', 'renewed', 'released', 'reclaimed']) {
      if (!Number.isFinite(value.counters[key])) {
        throw new LogFileError(`corrupt lease snapshot counters at ${snapshotPath}`);
      }
    }
    for (const entry of value.leases) {
      if (
        entry === null ||
        typeof entry !== 'object' ||
        typeof entry.token !== 'string' ||
        !Number.isFinite(entry.ttlMs) ||
        !Number.isFinite(entry.expiresAt)
      ) {
        throw new LogFileError(`corrupt lease snapshot lease at ${snapshotPath}`);
      }
    }

    return value;
  }

  function applyDeclarationSnapshot(snapshot) {
    if (Array.isArray(snapshot.quotas)) {
      // Parents first; iterate in dependency order, defensively against
      // hand-edited images.
      const pending = snapshot.quotas.slice();
      const applied = new Set();
      let progress = true;
      while (pending.length > 0 && progress) {
        progress = false;
        for (let i = 0; i < pending.length; ) {
          const entry = pending[i];
          if (
            entry === null || typeof entry !== 'object' ||
            typeof entry.node !== 'string' ||
            !Number.isInteger(entry.limit) || entry.limit <= 0 ||
            (entry.parent !== null && entry.parent !== undefined
              && typeof entry.parent !== 'string')
          ) {
            throw new LogFileError(`corrupt lease snapshot quota at ${snapshotPath}`);
          }
          const parent = entry.parent ?? null;
          if (parent === null || applied.has(parent) || quotaDefs.has(parent)) {
            const prior = quotaDefs.get(entry.node);
            if (prior !== undefined
              && (prior.limit !== entry.limit || prior.parent !== parent)) {
              throw new LogFileError(`corrupt lease snapshot quota at ${snapshotPath}`);
            }
            if (prior === undefined) {
              quotaDefs.set(entry.node, { limit: entry.limit, parent });
            }
            applied.add(entry.node);
            pending.splice(i, 1);
            progress = true;
          } else {
            i += 1;
          }
        }
      }
      if (pending.length > 0) {
        throw new LogFileError(`corrupt lease snapshot quota chain at ${snapshotPath}`);
      }
    }
    if (snapshot.capacities !== undefined && snapshot.capacities !== null) {
      for (const [resource, spec] of Object.entries(snapshot.capacities)) {
        if (spec !== null && typeof spec === 'object' && !Array.isArray(spec)) {
          if (!Number.isInteger(spec.capacity) || spec.capacity <= 0) {
            throw new LogFileError(`corrupt lease snapshot capacity at ${snapshotPath}`);
          }
          const attached = typeof spec.quota === 'string' ? spec.quota : null;
          if (attached !== null && !quotaDefs.has(attached)) {
            throw new LogFileError(`corrupt lease snapshot capacity at ${snapshotPath}`);
          }
          const priorCapacity = resourceCapacities.get(resource);
          const priorQuota = resourceQuota.get(resource);
          if (priorCapacity !== undefined
            && (priorCapacity !== spec.capacity || priorQuota !== attached)) {
            throw new LogFileError(`corrupt lease snapshot capacity at ${snapshotPath}`);
          }
          if (priorCapacity === undefined) {
            resourceCapacities.set(resource, spec.capacity);
            if (attached !== null) {
              resourceQuota.set(resource, attached);
            }
          }
        } else if (Number.isInteger(spec) && spec > 0) {
          if (resourceCapacities.has(resource)
            && resourceCapacities.get(resource) !== spec) {
            throw new LogFileError(`corrupt lease snapshot capacity at ${snapshotPath}`);
          }
          if (!resourceCapacities.has(resource)) {
            resourceCapacities.set(resource, spec);
          }
        } else {
          throw new LogFileError(`corrupt lease snapshot capacity at ${snapshotPath}`);
        }
      }
    }
  }

  function applySnapshot(snapshot) {
    applyDeclarationSnapshot(snapshot);
    counters.granted = snapshot.counters.granted;
    counters.renewed = snapshot.counters.renewed;
    counters.released = snapshot.counters.released;
    counters.reclaimed = snapshot.counters.reclaimed;
    replayMaxEventSeq = snapshot.seq;
    replayMaxTokenSeq = snapshot.tokenSeq ?? 0;
    replayMaxBatchSeq = snapshot.batchSeq ?? 0;
    for (const entry of snapshot.leases) {
      const lease = {
        resource: entry.resource,
        holder: entry.holder,
        expiresAt: entry.expiresAt,
        ttlMs: entry.ttlMs,
        // Credentials captured before this process started are void: the
        // share keeps occupying, but its token cannot renew or release it.
        legacy: true,
        heapSeq: 0,
        batch: entry.batch ?? null,
      };
      leases.set(entry.token, lease);
      addOccupant(entry.token, lease);
      knownTokens.add(entry.token);
      enqueue(entry.token, lease);
    }
    // Occupancy was just recomputed from the lease image; the recorded usage
    // vector must agree with it, otherwise the image is internally corrupt.
    if (snapshot.usage !== undefined && snapshot.usage !== null) {
      for (const [node, amount] of Object.entries(snapshot.usage)) {
        if (!Number.isInteger(amount) || amount < 0
          || (quotaUsed.get(node) ?? 0) !== amount) {
          throw new LogFileError(`corrupt lease snapshot usage at ${snapshotPath}`);
        }
      }
      for (const [node, amount] of quotaUsed) {
        if (amount !== 0 && (snapshot.usage[node] ?? 0) !== amount) {
          throw new LogFileError(`corrupt lease snapshot usage at ${snapshotPath}`);
        }
      }
    }
  }

  // Replay one chunk of complete log lines. In folded mode the snapshot is
  // already applied and events at or before foldedSeq are skipped; events
  // without a sequence id are folded history written by an older format and
  // are skipped as well. firstLineNumber only labels error messages.
  function replayStream(text, firstLineNumber, folded, foldedSeq) {
    if (text === '') {
      return;
    }
    const lines = text.split('\n');
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      if (line === '') {
        continue;
      }
      const lineNumber = firstLineNumber + i + 1;
      let event;
      try {
        event = JSON.parse(line);
      } catch (cause) {
        throw new LogFileError(
          `corrupt lease log entry on line ${lineNumber} of ${logPath}`,
          cause,
        );
      }
      if (folded) {
        if (
          event === null ||
          typeof event !== 'object' ||
          !Number.isInteger(event.e) ||
          event.e <= foldedSeq
        ) {
          // Already part of the snapshot image (or a legacy event folded in
          // before sequence ids existed).
          continue;
        }
      }
      replayEvent(event, lineNumber);
    }
  }

  function replayRemove(token) {
    const lease = leases.get(token);
    if (lease !== undefined) {
      leases.delete(token);
      removeOccupant(token, lease);
    }
  }

  function replayEvent(event, lineNumber) {
    if (event === null || typeof event !== 'object' || event.v !== LOG_VERSION) {
      throw new LogFileError(
        `unrecognized lease log version on line ${lineNumber} of ${logPath}`,
      );
    }
    if (Number.isInteger(event.e)) {
      replayMaxEventSeq = Math.max(replayMaxEventSeq, event.e);
    }

    switch (event.type) {
      case 'declare-quota': {
        if (
          typeof event.node !== 'string' ||
          !Number.isInteger(event.limit) || event.limit <= 0 ||
          (event.parent !== null && event.parent !== undefined
            && typeof event.parent !== 'string')
        ) {
          throw new LogFileError(
            `corrupt declare-quota entry on line ${lineNumber} of ${logPath}`,
          );
        }
        const parent = event.parent ?? null;
        if (parent !== null && !quotaDefs.has(parent)) {
          throw new LogFileError(
            `corrupt declare-quota chain on line ${lineNumber} of ${logPath}`,
          );
        }
        // Constructor seeds may already hold an identical declaration; only
        // a conflicting duplicate is corrupt.
        const prior = quotaDefs.get(event.node);
        if (prior === undefined) {
          quotaDefs.set(event.node, { limit: event.limit, parent });
        } else if (prior.limit !== event.limit || prior.parent !== parent) {
          throw new LogFileError(
            `corrupt declare-quota entry on line ${lineNumber} of ${logPath}`,
          );
        }
        break;
      }
      case 'declare-resource': {
        if (
          typeof event.resource !== 'string' ||
          !Number.isInteger(event.capacity) || event.capacity <= 0 ||
          (event.quota !== null && event.quota !== undefined
            && typeof event.quota !== 'string')
        ) {
          throw new LogFileError(
            `corrupt declare-resource entry on line ${lineNumber} of ${logPath}`,
          );
        }
        const attached = event.quota ?? null;
        if (attached !== null && !quotaDefs.has(attached)) {
          throw new LogFileError(
            `corrupt declare-resource entry on line ${lineNumber} of ${logPath}`,
          );
        }
        const priorCapacity = resourceCapacities.get(event.resource);
        const priorQuota = resourceQuota.get(event.resource);
        if (priorCapacity === undefined) {
          resourceCapacities.set(event.resource, event.capacity);
          if (attached !== null) {
            resourceQuota.set(event.resource, attached);
          }
        } else if (
          priorCapacity !== event.capacity
          || priorQuota !== attached
        ) {
          throw new LogFileError(
            `corrupt declare-resource entry on line ${lineNumber} of ${logPath}`,
          );
        }
        break;
      }
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
        // Credentials minted together for one atomic batch supersede any
        // expired, unswept shares on the resource as one unit: only slots
        // held by a different batch count as the stale shares retaken.
        if (event.batch !== undefined && event.batch !== null) {
          const logged = /^batch-[^-]+-(\d+)$/.exec(String(event.batch));
          if (logged !== null) {
            replayMaxBatchSeq = Math.max(replayMaxBatchSeq, Number(logged[1]));
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
          // Capacity resources may keep many live co-holders; the live path
          // only ever retakes shares that were already due at the grant
          // instant, so evict exactly the other-batch occupants that had
          // expired then. Own-batch credentials (a repeated listing) stay.
          for (const oldToken of [...occupantTokens]) {
            if (replayBatchTokens.has(oldToken)) {
              continue;
            }
            const oldLease = leases.get(oldToken);
            if (oldLease === undefined || oldLease.expiresAt <= event.at) {
              replayRemove(oldToken);
            }
          }
        }
        const lease = {
          resource: event.resource,
          holder: event.holder,
          expiresAt: event.expiresAt,
          ttlMs: event.ttlMs,
          // Credentials issued before this process started are void: the
          // share keeps occupying, but its token cannot renew or release it.
          legacy: true,
          heapSeq: 0,
          batch: event.batch ?? null,
        };
        leases.set(event.token, lease);
        addOccupant(event.token, lease);
        replayBatchTokens.add(event.token);
        knownTokens.add(event.token);
        enqueue(event.token, lease);
        counters.granted += 1;
        break;
      }
      case 'renew': {
        if (
          typeof event.token !== 'string' ||
          !Number.isFinite(event.expiresAt)
        ) {
          throw new LogFileError(
            `corrupt renew entry on line ${lineNumber} of ${logPath}`,
          );
        }
        const lease = leases.get(event.token);
        if (lease !== undefined) {
          lease.expiresAt = event.expiresAt;
          lease.heapSeq += 1;
          enqueue(event.token, lease);
          slotHeap(lease.resource).push({
            token: event.token,
            expiresAt: lease.expiresAt,
            heapSeq: lease.heapSeq,
            order: queueOrder++,
          });
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
        replayRemove(event.token);
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
          replayRemove(item.token);
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
