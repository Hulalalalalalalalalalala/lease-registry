import { randomUUID } from 'node:crypto';

export class LeaseTakenError extends Error {
  constructor(resource) {
    super(`lease for resource already held: ${resource}`);
    this.name = 'LeaseTakenError';
    this.code = 'LEASE_TAKEN';
    this.resource = resource;
  }
}

const isPositiveSafeInteger = (value) =>
  Number.isSafeInteger(value) && value > 0;

const isNonEmptyString = (value) =>
  typeof value === 'string' && value.length > 0;

export function createRegistry({ ttlMs, clock = Date.now } = {}) {
  if (!isPositiveSafeInteger(ttlMs)) {
    throw new RangeError('ttlMs must be a positive safe integer');
  }
  if (typeof clock !== 'function') {
    throw new TypeError('clock must be a function');
  }

  const defaultTtlMs = ttlMs;

  /** @type {Map<string, {holder: string, token: string, expiresAt: number, ttlMs: number}>} */
  const leases = new Map();
  /** @type {Map<string, string>} token -> resource */
  const tokens = new Map();

  const counters = {
    granted: 0,
    renewed: 0,
    released: 0,
    reclaimed: 0,
  };

  const isLive = (lease, now = clock()) => now < lease.expiresAt;

  function removeLease(resource) {
    const lease = leases.get(resource);
    if (lease !== undefined) {
      tokens.delete(lease.token);
    }
    leases.delete(resource);
  }

  function acquire(resource, holder, ttlMs = defaultTtlMs) {
    if (!isNonEmptyString(resource)) {
      throw new TypeError('resource must be a non-empty string');
    }
    if (!isNonEmptyString(holder)) {
      throw new TypeError('holder must be a non-empty string');
    }
    if (!isPositiveSafeInteger(ttlMs)) {
      throw new TypeError('ttlMs must be a positive safe integer');
    }

    const now = clock();
    const existing = leases.get(resource);
    if (existing !== undefined && isLive(existing, now)) {
      throw new LeaseTakenError(resource);
    }

    if (existing !== undefined) {
      // Expired but not yet swept: its token is dead, the resource changes hands.
      tokens.delete(existing.token);
    }

    let token;
    do {
      token = randomUUID();
    } while (tokens.has(token));
    const lease = {
      holder,
      token,
      expiresAt: now + ttlMs,
      ttlMs,
    };
    leases.set(resource, lease);
    tokens.set(token, resource);
    counters.granted += 1;

    return { resource, holder, token, expiresAt: lease.expiresAt };
  }

  function resolveLiveLease(token) {
    if (!isNonEmptyString(token)) {
      return undefined;
    }
    const resource = tokens.get(token);
    if (resource === undefined) {
      return undefined;
    }
    const lease = leases.get(resource);
    if (lease === undefined || lease.token !== token || !isLive(lease)) {
      return undefined;
    }
    return { resource, lease };
  }

  function renew(token) {
    const resolved = resolveLiveLease(token);
    if (resolved === undefined) {
      return false;
    }
    const { lease } = resolved;
    lease.expiresAt = clock() + lease.ttlMs;
    counters.renewed += 1;
    return true;
  }

  function release(token) {
    const resolved = resolveLiveLease(token);
    if (resolved === undefined) {
      return false;
    }
    removeLease(resolved.resource);
    counters.released += 1;
    return true;
  }

  function holder(resource) {
    const lease = leases.get(resource);
    if (lease === undefined || !isLive(lease)) {
      return null;
    }
    return lease.holder;
  }

  function sweep(now) {
    if (!Number.isSafeInteger(now)) {
      throw new TypeError('now must be a finite safe integer');
    }

    const freed = [];
    for (const [resource, lease] of leases) {
      if (now >= lease.expiresAt) {
        freed.push(resource);
      }
    }
    for (const resource of freed) {
      removeLease(resource);
    }
    freed.sort();
    counters.reclaimed += freed.length;
    return freed;
  }

  function stats() {
    const now = clock();
    let live = 0;
    for (const lease of leases.values()) {
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

export default { createRegistry, LeaseTakenError };
