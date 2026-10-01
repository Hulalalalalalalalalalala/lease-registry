// In-process, time-bounded lease registry.
// Time only advances through the injected clock.

import { randomUUID } from 'node:crypto';

export class LeaseTakenError extends Error {
  constructor(message = 'Lease is already held') {
    super(message);
    this.name = 'LeaseTakenError';
    this.code = 'LEASE_TAKEN';
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

  // resource -> { holder, token, expiresAt, ttlMs }
  const leases = new Map();
  // token -> resource
  const tokens = new Map();

  const stats = {
    granted: 0,
    renewed: 0,
    released: 0,
    reclaimed: 0,
  };

  const isLive = (lease, now = clock()) => now < lease.expiresAt;

  const mintToken = () => {
    let token = randomUUID();
    while (tokens.has(token)) {
      token = randomUUID();
    }
    return token;
  };

  function acquire(resource, holder, ttlMs) {
    if (!isNonEmptyString(resource)) {
      throw new TypeError('resource must be a non-empty string');
    }
    if (!isNonEmptyString(holder)) {
      throw new TypeError('holder must be a non-empty string');
    }
    if (!isPositiveSafeInteger(ttlMs)) {
      throw new TypeError('ttlMs must be a positive safe integer');
    }

    const existing = leases.get(resource);
    if (existing !== undefined && isLive(existing)) {
      throw new LeaseTakenError(`Resource "${resource}" is already leased`);
    }

    // Expired lease (swept or not): its token is dead and the resource
    // is available for takeover.
    if (existing !== undefined) {
      tokens.delete(existing.token);
    }

    const now = clock();
    const token = mintToken();
    const lease = {
      holder,
      token,
      expiresAt: now + ttlMs,
      ttlMs,
    };
    leases.set(resource, lease);
    tokens.set(token, resource);
    stats.granted += 1;

    return {
      resource,
      holder,
      token,
      expiresAt: lease.expiresAt,
    };
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
    // Unknown, released, expired, or taken over by a newer token.
    if (lease === undefined || lease.token !== token || !isLive(lease)) {
      return undefined;
    }
    return { resource, lease };
  }

  function renew(token) {
    const live = resolveLiveLease(token);
    if (live === undefined) {
      return false;
    }
    live.lease.expiresAt = clock() + live.lease.ttlMs;
    stats.renewed += 1;
    return true;
  }

  function release(token) {
    const live = resolveLiveLease(token);
    if (live === undefined) {
      return false;
    }
    leases.delete(live.resource);
    tokens.delete(token);
    stats.released += 1;
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
      // Live means strictly before expiry, so equality counts as expired.
      if (now >= lease.expiresAt) {
        leases.delete(resource);
        tokens.delete(lease.token);
        freed.push(resource);
      }
    }
    if (freed.length > 0) {
      freed.sort();
      stats.reclaimed += freed.length;
    }
    return freed;
  }

  function liveCount() {
    const now = clock();
    let count = 0;
    for (const lease of leases.values()) {
      if (now < lease.expiresAt) {
        count += 1;
      }
    }
    return count;
  }

  function registryStats() {
    return {
      granted: stats.granted,
      renewed: stats.renewed,
      released: stats.released,
      reclaimed: stats.reclaimed,
      live: liveCount(),
    };
  }

  return {
    acquire,
    renew,
    release,
    sweep,
    holder,
    stats: registryStats,
  };
}
