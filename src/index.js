// In-process, time bounded lease registry.
// Time only moves when the injected clock moves; nothing expires on its own.

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

export function createRegistry({ ttlMs, clock = Date.now } = {}) {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new TypeError('ttlMs must be a positive finite number');
  }
  if (typeof clock !== 'function') {
    throw new TypeError('clock must be a function');
  }

  const defaultTtlMs = ttlMs;
  // token -> { resource, holder, expiresAt, ttlMs, released }
  const leases = new Map();
  // resource -> token of the lease currently occupying it
  const occupied = new Map();

  const counters = {
    granted: 0,
    renewed: 0,
    released: 0,
    reclaimed: 0,
  };

  function isLive(lease, now) {
    return lease !== undefined && !lease.released && lease.expiresAt > now;
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
      // Expired but not yet swept: the resource is free. Drop the stale
      // record (sweep never reclaims a resource that was already retaken).
      occupied.delete(resource);
      leases.delete(currentToken);
    }

    const token = makeToken();
    const lease = {
      resource,
      holder,
      expiresAt: now + ttlMs,
      ttlMs,
      released: false,
    };
    leases.set(token, lease);
    occupied.set(resource, token);
    counters.granted += 1;

    return {
      resource,
      holder,
      token,
      expiresAt: lease.expiresAt,
    };
  }

  function renew(token) {
    const lease = leases.get(token);
    if (lease === undefined || lease.released) {
      return false;
    }
    const now = clock();
    if (lease.expiresAt <= now) {
      return false;
    }
    lease.expiresAt = now + lease.ttlMs;
    counters.renewed += 1;
    return true;
  }

  function release(token) {
    const lease = leases.get(token);
    if (lease === undefined || lease.released) {
      return false;
    }
    const now = clock();
    if (lease.expiresAt <= now) {
      return false;
    }
    lease.released = true;
    if (occupied.get(lease.resource) === token) {
      occupied.delete(lease.resource);
    }
    counters.released += 1;
    return true;
  }

  function sweep(now = clock()) {
    const expired = [];
    for (const [token, lease] of leases) {
      if (lease.released) {
        // Credential is already dead; nothing to report or count.
        leases.delete(token);
        continue;
      }
      if (lease.expiresAt <= now) {
        expired.push([token, lease]);
      }
    }
    // Reclaim in expiry order (acquisition order breaks ties).
    expired.sort((a, b) => a[1].expiresAt - b[1].expiresAt);

    const freed = [];
    for (const [token, lease] of expired) {
      leases.delete(token);
      if (occupied.get(lease.resource) === token) {
        occupied.delete(lease.resource);
        counters.reclaimed += 1;
        freed.push(lease.resource);
      }
      // Otherwise the resource was already retaken by a new holder; this
      // stale credential is discarded silently.
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

let tokenSequence = 0;
function makeToken() {
  tokenSequence += 1;
  return `lease-${process.pid}-${tokenSequence}-${Date.now().toString(36)}`;
}
