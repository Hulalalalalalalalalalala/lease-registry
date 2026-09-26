// In-process, time bounded lease registry with append-only persistence.
// Time only moves when the injected clock moves; nothing expires on its own.

import { appendFileSync, readFileSync } from 'node:fs';

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
  constructor(message, { cause, path } = {}) {
    super(message, cause !== undefined ? { cause } : undefined);
    this.name = 'LogFileError';
    this.code = LogFileError.code;
    if (path !== undefined) {
      this.path = path;
    }
  }
}
LogFileError.code = 'LOG_FILE_ERROR';

// Min-heap of { token, expiresAt, seq } ordered by expiry, then arrival.
// Renewals push a fresh entry; stale entries are discarded lazily when they
// reach the top, so sweeps only touch what is due.
function createExpiryQueue() {
  const items = [];
  const before = (a, b) =>
    a.expiresAt < b.expiresAt ||
    (a.expiresAt === b.expiresAt && a.seq < b.seq);

  function push(item) {
    items.push(item);
    let i = items.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (!before(items[i], items[parent])) {
        break;
      }
      [items[i], items[parent]] = [items[parent], items[i]];
      i = parent;
    }
  }

  function peek() {
    return items[0];
  }

  function pop() {
    const top = items[0];
    const last = items.pop();
    if (items.length > 0) {
      items[0] = last;
      let i = 0;
      for (;;) {
        const left = 2 * i + 1;
        const right = 2 * i + 2;
        let smallest = i;
        if (left < items.length && before(items[left], items[smallest])) {
          smallest = left;
        }
        if (right < items.length && before(items[right], items[smallest])) {
          smallest = right;
        }
        if (smallest === i) {
          break;
        }
        [items[i], items[smallest]] = [items[smallest], items[i]];
        i = smallest;
      }
    }
    return top;
  }

  return {
    push,
    peek,
    pop,
    get size() {
      return items.length;
    },
  };
}

// Distinguishes tokens minted by different registries in this process.
let instanceSequence = 0;

export function createRegistry({ ttlMs, clock = Date.now, logPath } = {}) {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new TypeError('ttlMs must be a positive finite number');
  }
  if (typeof clock !== 'function') {
    throw new TypeError('clock must be a function');
  }
  if (logPath !== undefined && typeof logPath !== 'string') {
    throw new TypeError('logPath must be a string');
  }

  const defaultTtlMs = ttlMs;
  // token -> { resource, holder, expiresAt, ttlMs }
  const leases = new Map();
  // resource -> token of the lease currently occupying it
  const occupied = new Map();
  // Tokens minted by this instance. Replayed leases stay valid until they
  // expire, but their pre-restart credentials are never honored again.
  const ownTokens = new Set();
  const queue = createExpiryQueue();

  const counters = {
    granted: 0,
    renewed: 0,
    released: 0,
    reclaimed: 0,
  };

  instanceSequence += 1;
  const instanceId = instanceSequence;
  let tokenSequence = 0;
  let entrySequence = 0;

  function makeToken() {
    tokenSequence += 1;
    return `lease-${process.pid}-${instanceId}-${tokenSequence}-${clock().toString(36)}`;
  }

  function pushExpiry(token, expiresAt) {
    entrySequence += 1;
    queue.push({ token, expiresAt, seq: entrySequence });
  }

  function formatEvent(event) {
    return `${JSON.stringify({ v: LOG_VERSION, ...event })}\n`;
  }

  // Persistence is write-first: a failed append raises LogFileError before
  // any in-memory state changes, so the registry stays as the caller left it.
  function appendEvents(events) {
    if (logPath === undefined || events.length === 0) {
      return;
    }
    try {
      appendFileSync(logPath, events.map(formatEvent).join(''), 'utf8');
    } catch (error) {
      throw new LogFileError(
        `could not append to lease log ${logPath}: ${error.message}`,
        { cause: error, path: logPath },
      );
    }
  }

  function parseEvent(line, lineNumber) {
    const corrupt = (reason, cause) =>
      new LogFileError(`lease log ${logPath} line ${lineNumber}: ${reason}`, {
        cause,
        path: logPath,
      });
    let event;
    try {
      event = JSON.parse(line);
    } catch (error) {
      throw corrupt('is not valid JSON', error);
    }
    if (event === null || typeof event !== 'object' || Array.isArray(event)) {
      throw corrupt('is not an event object');
    }
    if (event.v !== LOG_VERSION) {
      throw corrupt(`has unrecognized version ${String(event.v)}`);
    }
    const hasToken = typeof event.token === 'string' && event.token.length > 0;
    switch (event.type) {
      case 'grant':
        if (!hasToken || !Number.isFinite(event.expiresAt) || !Number.isFinite(event.ttlMs)) {
          throw corrupt('is a malformed grant event');
        }
        break;
      case 'renew':
        if (!hasToken || !Number.isFinite(event.expiresAt)) {
          throw corrupt('is a malformed renew event');
        }
        break;
      case 'release':
      case 'reclaim':
        if (!hasToken) {
          throw corrupt(`is a malformed ${event.type} event`);
        }
        break;
      default:
        throw corrupt(`has unknown event type ${String(event.type)}`);
    }
    return event;
  }

  // Replays one logged event. Counters move exactly as they did when the
  // event was first written, so a restart rebuilds the same totals without
  // counting anything twice.
  function replayEvent(event) {
    switch (event.type) {
      case 'grant': {
        const previousToken = occupied.get(event.resource);
        if (previousToken !== undefined) {
          leases.delete(previousToken);
        }
        leases.set(event.token, {
          resource: event.resource,
          holder: event.holder,
          expiresAt: event.expiresAt,
          ttlMs: event.ttlMs,
        });
        occupied.set(event.resource, event.token);
        pushExpiry(event.token, event.expiresAt);
        counters.granted += 1;
        break;
      }
      case 'renew': {
        const lease = leases.get(event.token);
        if (lease !== undefined) {
          lease.expiresAt = event.expiresAt;
          pushExpiry(event.token, event.expiresAt);
          counters.renewed += 1;
        }
        break;
      }
      case 'release':
      case 'reclaim': {
        const lease = leases.get(event.token);
        if (lease !== undefined) {
          leases.delete(event.token);
          if (occupied.get(lease.resource) === event.token) {
            occupied.delete(lease.resource);
          }
          if (event.type === 'release') {
            counters.released += 1;
          } else {
            counters.reclaimed += 1;
          }
        }
        break;
      }
    }
  }

  function replayLog() {
    if (logPath === undefined) {
      return;
    }
    let text;
    try {
      text = readFileSync(logPath, 'utf8');
    } catch (error) {
      if (error !== null && typeof error === 'object' && error.code === 'ENOENT') {
        return; // No log yet: start as a fresh registry.
      }
      throw new LogFileError(
        `could not read lease log ${logPath}: ${error.message}`,
        { cause: error, path: logPath },
      );
    }
    const lines = text.split('\n');
    if (lines.length > 0 && lines[lines.length - 1] === '') {
      lines.pop(); // trailing newline after the last event
    }
    lines.forEach((line, index) => replayEvent(parseEvent(line, index + 1)));
  }

  function dropRecord(token) {
    leases.delete(token);
    ownTokens.delete(token);
  }

  replayLog();

  function isLive(lease, now) {
    return lease !== undefined && lease.expiresAt > now;
  }

  function acquire(resource, holder, ttlMs = defaultTtlMs) {
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new TypeError('ttlMs must be a positive finite number');
    }
    const now = clock();
    const currentToken = occupied.get(resource);
    if (currentToken !== undefined) {
      const current = leases.get(currentToken);
      if (isLive(current, now)) {
        throw new LeaseTakenError(resource, current.holder);
      }
    }

    const token = makeToken();
    const lease = {
      resource,
      holder,
      expiresAt: now + ttlMs,
      ttlMs,
    };
    appendEvents([
      { type: 'grant', token, resource, holder, expiresAt: lease.expiresAt, ttlMs },
    ]);

    if (currentToken !== undefined) {
      // Expired but not yet swept: the resource is free. Drop the stale
      // record (sweep never reclaims a resource that was already retaken).
      dropRecord(currentToken);
    }
    leases.set(token, lease);
    occupied.set(resource, token);
    ownTokens.add(token);
    pushExpiry(token, lease.expiresAt);
    counters.granted += 1;

    return {
      resource,
      holder,
      token,
      expiresAt: lease.expiresAt,
    };
  }

  function renew(token) {
    if (!ownTokens.has(token)) {
      return false;
    }
    const lease = leases.get(token);
    if (lease === undefined) {
      return false;
    }
    const now = clock();
    if (lease.expiresAt <= now) {
      return false;
    }
    const expiresAt = now + lease.ttlMs;
    appendEvents([{ type: 'renew', token, expiresAt }]);
    lease.expiresAt = expiresAt;
    pushExpiry(token, expiresAt);
    counters.renewed += 1;
    return true;
  }

  function release(token) {
    if (!ownTokens.has(token)) {
      return false;
    }
    const lease = leases.get(token);
    if (lease === undefined) {
      return false;
    }
    const now = clock();
    if (lease.expiresAt <= now) {
      return false;
    }
    appendEvents([{ type: 'release', token }]);
    dropRecord(token);
    if (occupied.get(lease.resource) === token) {
      occupied.delete(lease.resource);
    }
    counters.released += 1;
    return true;
  }

  function sweep(now = clock()) {
    // Pop due entries in expiry order; entries whose lease was renewed,
    // released or retaken are stale and discarded silently.
    const due = [];
    while (queue.size > 0) {
      const top = queue.peek();
      if (top.expiresAt > now) {
        break;
      }
      queue.pop();
      const lease = leases.get(top.token);
      if (lease === undefined || lease.expiresAt !== top.expiresAt) {
        continue;
      }
      due.push(top);
    }
    if (due.length === 0) {
      return [];
    }

    const events = due.map((entry) => ({
      type: 'reclaim',
      token: entry.token,
      resource: leases.get(entry.token).resource,
    }));
    try {
      appendEvents(events);
    } catch (error) {
      // Nothing was applied; put the due entries back so a later sweep
      // can reclaim them again.
      for (const entry of due) {
        queue.push(entry);
      }
      throw error;
    }

    const freed = [];
    for (const entry of due) {
      const lease = leases.get(entry.token);
      dropRecord(entry.token);
      if (occupied.get(lease.resource) === entry.token) {
        occupied.delete(lease.resource);
      }
      counters.reclaimed += 1;
      freed.push(lease.resource);
    }
    return freed;
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

  return { acquire, renew, release, sweep, holder, stats };
}
