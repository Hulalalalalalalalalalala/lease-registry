// In-process, time bounded lease registry with append-only log persistence.
// Time only moves when the injected clock moves; nothing expires on its own.
// Tokens are derived from the injected clock as well, never from wall time.
//
// Contended acquisitions may wait in line: a queued request is handed the
// lease, first come first served, when the resource is released, reclaimed
// or found expired. Waiting is judged against the injected clock; a request
// whose wait deadline passes gives up and is never woken.

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
  // token -> { resource, holder, expiresAt, ttlMs, legacy, heapSeq }
  const leases = new Map();
  // Every token this instance knows about, including replayed credentials
  // that were already released or reclaimed, so a freshly minted token can
  // never reuse a dead credential's string.
  const knownTokens = new Set();
  // resource -> token of the lease currently occupying it
  const occupied = new Map();
  const expiryQueue = new ExpiryQueue();
  // requestId -> waiting request record. Requests live in memory only: they
  // are in-flight callers, so nothing about them is persisted or replayed.
  const requests = new Map();
  // resource -> FIFO line of waiting request records
  const waitLines = new Map();

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
  let requestSequence = 0;
  let queueOrder = 0;

  function mintToken(now, seq = tokenSequence, alsoTaken = undefined) {
    // Pure candidate: nothing is committed until the event is on disk, so a
    // failed append rolls all the way back. Pids and instance ids can coincide
    // after a restart, so skip any sequence that collides with a credential
    // reconstructed from the log. `alsoTaken` lets one call mint a batch of
    // tokens (a sweep waking several waiters) without committing any of them.
    let token;
    do {
      seq += 1;
      token = `lease-${process.pid}-${registryId}-${seq}-${now.toString(36)}`;
    } while (knownTokens.has(token) ||
        (alsoTaken !== undefined && alsoTaken.has(token)));
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

  function appendEvents(events) {
    if (logPath === null) {
      return;
    }
    // One write for the whole batch: a freeing operation and the handoffs it
    // triggers either all land on disk or none of them do.
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

  function normalizeWait(wait) {
    if (wait === undefined || wait === null) {
      return undefined;
    }
    const waitMs = typeof wait === 'object' ? wait.waitMs : wait;
    if (!Number.isFinite(waitMs) || waitMs <= 0) {
      throw new TypeError('waitMs must be a positive finite number');
    }
    return waitMs;
  }

  // Head of the resource's line that is still entitled to the lease, or
  // undefined. Requests past their wait deadline are marked as given up and
  // dropped; cancelled records retire here as well. The returned request is
  // NOT removed from the line: that only happens when the handoff commits,
  // so a failed log write leaves the line untouched.
  function nextWaitingRequest(resource, now) {
    const line = waitLines.get(resource);
    if (line === undefined) {
      return undefined;
    }
    while (line.length > 0) {
      const head = line[0];
      if (head.status !== 'waiting') {
        line.shift();
        continue;
      }
      if (head.waitDeadline <= now) {
        head.status = 'timeout';
        line.shift();
        continue;
      }
      return head;
    }
    waitLines.delete(resource);
    return undefined;
  }

  function refreshRequest(record) {
    if (record.status === 'waiting' && record.waitDeadline <= clock()) {
      record.status = 'timeout';
    }
  }

  // Live view of a queued request: the same object reads as waiting, granted,
  // timeout or cancelled as events unfold. The credential fields only appear
  // once the request has actually been woken.
  function requestView(record) {
    return {
      get status() {
        refreshRequest(record);
        return record.status;
      },
      requestId: record.requestId,
      resource: record.resource,
      holder: record.holder,
      waitDeadline: record.waitDeadline,
      get token() {
        return record.token;
      },
      get expiresAt() {
        return record.expiresAt;
      },
    };
  }

  function waitInLine(resource, holder, leaseTtlMs, waitMs, now) {
    requestSequence += 1;
    const requestId = `request-${process.pid}-${registryId}-${requestSequence}`;
    const record = {
      requestId,
      resource,
      holder,
      ttlMs: leaseTtlMs,
      waitDeadline: now + waitMs,
      status: 'waiting',
      token: undefined,
      expiresAt: undefined,
    };
    requests.set(requestId, record);
    let line = waitLines.get(resource);
    if (line === undefined) {
      line = [];
      waitLines.set(resource, line);
    }
    line.push(record);
    return requestView(record);
  }

  // The handoff is a real grant: it occupies the resource, carries its own
  // expiry and counts as granted. Only ever called after the events are on
  // disk, so a failed write never reaches here.
  function commitGrant(request, minted, now) {
    tokenSequence = minted.seq;
    knownTokens.add(minted.token);
    const line = waitLines.get(request.resource);
    if (line !== undefined && line[0] === request) {
      line.shift();
      if (line.length === 0) {
        waitLines.delete(request.resource);
      }
    }
    const expiresAt = now + request.ttlMs;
    request.status = 'granted';
    request.token = minted.token;
    request.expiresAt = expiresAt;
    const lease = {
      resource: request.resource,
      holder: request.holder,
      expiresAt,
      ttlMs: request.ttlMs,
      legacy: false,
      heapSeq: 0,
    };
    leases.set(minted.token, lease);
    occupied.set(request.resource, minted.token);
    enqueue(minted.token, lease);
    counters.granted += 1;
  }

  function acquire(resource, holder, ttlMs = defaultTtlMs, wait = undefined) {
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new TypeError('ttlMs must be a positive finite number');
    }
    const waitMs = normalizeWait(wait);
    const now = clock();
    const currentToken = occupied.get(resource);
    if (currentToken !== undefined) {
      const current = leases.get(currentToken);
      if (isLive(current, now)) {
        if (waitMs === undefined) {
          throw new LeaseTakenError(resource, current.holder);
        }
        // Occupied and willing to wait: take a number. The current holder is
        // not disturbed by the line forming behind it.
        return waitInLine(resource, holder, ttlMs, waitMs, now);
      }
      // Expired but not yet swept: the resource is free. The stale record is
      // dropped together with the next grant below (no event of its own).
    }

    // The resource is free, but a queued request arrived earlier than this
    // call, so the head of the line is served first and this call is treated
    // as arriving behind it.
    const head = nextWaitingRequest(resource, now);
    if (head !== undefined) {
      const minted = mintToken(now);
      appendEvent({
        v: LOG_VERSION,
        type: 'acquire',
        at: now,
        token: minted.token,
        resource,
        holder: head.holder,
        ttlMs: head.ttlMs,
        expiresAt: now + head.ttlMs,
      });
      if (currentToken !== undefined) {
        occupied.delete(resource);
        leases.delete(currentToken);
      }
      commitGrant(head, minted, now);
      if (waitMs === undefined) {
        throw new LeaseTakenError(resource, head.holder);
      }
      return waitInLine(resource, holder, ttlMs, waitMs, now);
    }

    const { token, seq } = mintToken(now);
    const expiresAt = now + ttlMs;
    // Persist before touching any state: a failed write leaves the registry
    // exactly as it was before the call.
    appendEvent({
      v: LOG_VERSION,
      type: 'acquire',
      at: now,
      token,
      resource,
      holder,
      ttlMs,
      expiresAt,
    });

    tokenSequence = seq;
    knownTokens.add(token);
    if (currentToken !== undefined) {
      occupied.delete(resource);
      leases.delete(currentToken);
    }
    const lease = {
      resource,
      holder,
      expiresAt,
      ttlMs,
      legacy: false,
      heapSeq: 0,
    };
    leases.set(token, lease);
    occupied.set(resource, token);
    enqueue(token, lease);
    counters.granted += 1;

    return { resource, holder, token, expiresAt };
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

    // The freed resource goes straight to the head of its line; the release
    // and the handoff are persisted as one atomic append.
    const head = nextWaitingRequest(lease.resource, now);
    const events = [{ v: LOG_VERSION, type: 'release', at: now, token }];
    let minted;
    if (head !== undefined && occupied.get(lease.resource) === token) {
      minted = mintToken(now);
      events.push({
        v: LOG_VERSION,
        type: 'acquire',
        at: now,
        token: minted.token,
        resource: lease.resource,
        holder: head.holder,
        ttlMs: head.ttlMs,
        expiresAt: now + head.ttlMs,
      });
    }
    appendEvents(events);

    // The record comes off immediately; its queued node retires lazily.
    leases.delete(token);
    if (occupied.get(lease.resource) === token) {
      occupied.delete(lease.resource);
    }
    counters.released += 1;
    if (minted !== undefined) {
      commitGrant(head, minted, now);
    }
    return true;
  }

  function sweep(now = clock()) {
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

    // Each reclaimed resource is handed to the head of its wait line in the
    // same pass; reclaim and handoffs are persisted as one atomic append.
    const events = [{
      v: LOG_VERSION,
      type: 'reclaim',
      at: now,
      items: due.map((node) => ({
        token: node.token,
        resource: leases.get(node.token).resource,
      })),
    }];
    const handoffs = [];
    let seqCursor = tokenSequence;
    const pendingTokens = new Set();
    for (const node of due) {
      const lease = leases.get(node.token);
      if (occupied.get(lease.resource) !== node.token) {
        continue;
      }
      const head = nextWaitingRequest(lease.resource, now);
      if (head === undefined) {
        continue;
      }
      const minted = mintToken(now, seqCursor, pendingTokens);
      seqCursor = minted.seq;
      pendingTokens.add(minted.token);
      events.push({
        v: LOG_VERSION,
        type: 'acquire',
        at: now,
        token: minted.token,
        resource: lease.resource,
        holder: head.holder,
        ttlMs: head.ttlMs,
        expiresAt: now + head.ttlMs,
      });
      handoffs.push({ request: head, minted });
    }

    try {
      appendEvents(events);
    } catch (error) {
      // The write never happened: put the popped nodes back so the queue is
      // byte-for-byte the same as before the call. Waiting requests were
      // never dequeued, so the lines are untouched as well.
      for (const node of due) {
        expiryQueue.push(node);
      }
      throw error;
    }

    const freed = [];
    for (const node of due) {
      const lease = leases.get(node.token);
      leases.delete(node.token);
      if (occupied.get(lease.resource) === node.token) {
        occupied.delete(lease.resource);
        counters.reclaimed += 1;
        freed.push(lease.resource);
      }
    }
    for (const { request, minted } of handoffs) {
      commitGrant(request, minted, now);
    }
    return freed;
  }

  function poll(requestId) {
    const id = requestId !== null && typeof requestId === 'object'
      ? requestId.requestId
      : requestId;
    const record = requests.get(id);
    if (record === undefined) {
      return null;
    }
    refreshRequest(record);
    const snapshot = {
      status: record.status,
      requestId: record.requestId,
      resource: record.resource,
      holder: record.holder,
    };
    if (record.status === 'waiting') {
      snapshot.waitDeadline = record.waitDeadline;
    }
    if (record.status === 'granted') {
      snapshot.token = record.token;
      snapshot.expiresAt = record.expiresAt;
    }
    return snapshot;
  }

  function cancel(requestId) {
    const id = requestId !== null && typeof requestId === 'object'
      ? requestId.requestId
      : requestId;
    const record = requests.get(id);
    if (record === undefined || record.status !== 'waiting') {
      // Unknown, already woken, already given up or already cancelled: the
      // cancel does not land.
      return false;
    }
    if (record.waitDeadline <= clock()) {
      record.status = 'timeout';
      return false;
    }
    record.status = 'cancelled';
    return true;
  }

  function holder(resource) {
    const token = occupied.get(resource);
    if (token === undefined) {
      return null;
    }
    const lease = leases.get(token);
    if (!isLive(lease, clock())) {
      return null;
    }
    return lease.holder;
  }

  function stats() {
    let live = 0;
    const now = clock();
    for (const token of occupied.values()) {
      const lease = leases.get(token);
      if (isLive(lease, now)) {
        live += 1;
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

  replay();

  return { acquire, renew, release, sweep, holder, stats, poll, cancel };

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
        const previousToken = occupied.get(event.resource);
        if (previousToken !== undefined && previousToken !== event.token) {
          // A later grant retook an expired, unswept resource.
          leases.delete(previousToken);
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
        };
        leases.set(event.token, lease);
        occupied.set(event.resource, event.token);
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
          if (occupied.get(lease.resource) === event.token) {
            occupied.delete(lease.resource);
          }
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
            if (occupied.get(lease.resource) === token) {
              occupied.delete(lease.resource);
            }
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
