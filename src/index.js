export class LeaseTakenError extends Error {
  constructor(resource, holder) {
    super(`resource "${resource}" is already leased by "${holder}"`);
    this.name = 'LeaseTakenError';
    this.code = 'LEASE_TAKEN';
    this.resource = resource;
    this.holder = holder;
  }
}

export function createRegistry({ ttlMs, clock = Date.now } = {}) {
  const defaultTtlMs = ttlMs;
  // resource name -> live lease record
  const byResource = new Map();
  // token -> lease record, only while the lease can still be renewed/released
  const byToken = new Map();
  let nextTokenId = 0;
  let granted = 0;
  let renewed = 0;
  let released = 0;
  let reclaimed = 0;

  const isLive = (lease, now) => !lease.released && lease.expiresAt > now;

  function acquire(resource, holder, leaseTtlMs = defaultTtlMs) {
    const now = clock();
    const existing = byResource.get(resource);
    if (existing && isLive(existing, now)) {
      throw new LeaseTakenError(resource, existing.holder);
    }
    if (existing) {
      // Expired but never swept: the old token is void from here on.
      byToken.delete(existing.token);
    }
    const lease = {
      resource,
      holder,
      token: `lease-${++nextTokenId}`,
      ttlMs: leaseTtlMs,
      expiresAt: now + leaseTtlMs,
      released: false,
    };
    byResource.set(resource, lease);
    byToken.set(lease.token, lease);
    granted += 1;
    return {
      resource: lease.resource,
      holder: lease.holder,
      token: lease.token,
      expiresAt: lease.expiresAt,
    };
  }

  function renew(token) {
    const lease = byToken.get(token);
    if (!lease || !isLive(lease, clock())) {
      return false;
    }
    lease.expiresAt = clock() + lease.ttlMs;
    renewed += 1;
    return true;
  }

  function release(token) {
    const lease = byToken.get(token);
    if (!lease || !isLive(lease, clock())) {
      return false;
    }
    lease.released = true;
    byResource.delete(lease.resource);
    byToken.delete(token);
    released += 1;
    return true;
  }

  function sweep(now) {
    const expired = [];
    for (const lease of byResource.values()) {
      if (!lease.released && lease.expiresAt <= now) {
        expired.push(lease);
      }
    }
    expired.sort((a, b) => a.expiresAt - b.expiresAt);
    for (const lease of expired) {
      byResource.delete(lease.resource);
      byToken.delete(lease.token);
    }
    reclaimed += expired.length;
    return expired.map((lease) => lease.resource);
  }

  function holder(resource) {
    const lease = byResource.get(resource);
    if (!lease || !isLive(lease, clock())) {
      return null;
    }
    return lease.holder;
  }

  function stats() {
    const now = clock();
    let live = 0;
    for (const lease of byResource.values()) {
      if (isLive(lease, now)) {
        live += 1;
      }
    }
    return { granted, renewed, released, reclaimed, live };
  }

  return { acquire, renew, release, sweep, holder, stats };
}
