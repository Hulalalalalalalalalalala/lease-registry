import test from 'node:test';
import assert from 'node:assert/strict';

import {
  createRegistry,
  LeaseTakenError,
  QuotaExceededError,
} from '../src/index.js';

function fakeClock(start = 0) {
  let now = start;
  const clock = () => now;
  clock.set = (value) => {
    now = value;
  };
  clock.advance = (delta) => {
    now += delta;
  };
  return clock;
}

const hierarchy = () => ({
  quotas: {
    root: 6,
    mid: { limit: 3, parent: 'root' },
    leaf: { limit: 2, parent: 'mid' },
  },
  capacities: {
    leafRes: { capacity: 100, quota: 'leaf' },
    midRes: { capacity: 100, quota: 'mid' },
    rootRes: { capacity: 100, quota: 'root' },
    plain: 100,
  },
});

test('every hierarchy level is enforced at the same time', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock, ...hierarchy() });

  registry.acquire({ resource: 'leafRes', shares: 2 }, 'a'); // leaf2 mid2 root2
  registry.acquire({ resource: 'midRes', shares: 1 }, 'b');  // mid3 root3

  // leaf would go to 3 > 2
  assert.throws(
    () => registry.acquire('leafRes', 'c'),
    (e) => e instanceof QuotaExceededError && e.code === 'QUOTA_EXCEEDED' && e.quota === 'leaf',
  );
  // mid is already at its own limit of 3
  assert.throws(
    () => registry.acquire('midRes', 'c'),
    (e) => e.code === 'QUOTA_EXCEEDED' && e.quota === 'mid',
  );
  // root has room (3/6) for a root-only resource
  registry.acquire({ resource: 'rootRes', shares: 3 }, 'c'); // root6
  assert.throws(
    () => registry.acquire('rootRes', 'd'),
    (e) => e.code === 'QUOTA_EXCEEDED' && e.quota === 'root',
  );

  assert.equal(registry.quotaUsage('leaf'), 2);
  assert.equal(registry.quotaUsage('mid'), 3);
  assert.equal(registry.quotaUsage('root'), 6);
  assert.deepEqual(registry.stats(), {
    granted: 6, renewed: 0, released: 0, reclaimed: 0, live: 6,
  });
});

test('QuotaExceededError is a LeaseTakenError with an assertable code', () => {
  const registry = createRegistry({
    ttlMs: 100, clock: fakeClock(0),
    quotas: { q: 1 }, capacities: { r: { capacity: 10, quota: 'q' } },
  });
  registry.acquire('r', 'h');
  let caught;
  assert.throws(
    () => registry.acquire('r', 'g'),
    (error) => {
      caught = error;
      return error instanceof LeaseTakenError;
    },
  );
  assert.equal(caught.code, 'QUOTA_EXCEEDED');
  assert.equal(caught.quota, 'q');
  assert.equal(caught.resource, null);
  assert.equal(caught instanceof QuotaExceededError, true);
});

test('a group over any quota level fails as one unit and leaves nothing', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock,
    quotas: { q: 3 },
    capacities: {
      x: { capacity: 10, quota: 'q' },
      y: { capacity: 10, quota: 'q' },
    },
  });
  registry.acquire({ resource: 'x', shares: 1 }, 'occ');

  assert.throws(
    () => registry.acquireAll([
      { resource: 'x', shares: 1 },
      { resource: 'y', shares: 2 },
    ], 'G'),
    (e) => e.code === 'QUOTA_EXCEEDED' && e.quota === 'q',
  );

  // No share landed anywhere.
  assert.equal(registry.holder('y'), null);
  assert.equal(registry.quotaUsage('q'), 1);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });

  // The same shape succeeds once it fits both capacity and quota.
  const leases = registry.acquireAll([
    { resource: 'x', shares: 1 },
    { resource: 'y', shares: 1 },
  ], 'G2');
  assert.equal(leases.length, 2);
  assert.equal(registry.quotaUsage('q'), 3);
});

test('capacity and quota constrain together: the tighter bound wins', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock,
    quotas: { q: 10 },
    capacities: { small: { capacity: 2, quota: 'q' } },
  });
  registry.acquire({ resource: 'small', shares: 2 }, 'h');
  // Capacity is the tight bound here (quota would allow 8 more).
  assert.throws(
    () => registry.acquire('small', 'g'),
    (e) => e.code === 'LEASE_TAKEN' && e.resource === 'small',
  );

  const registry2 = createRegistry({
    ttlMs: 100, clock,
    quotas: { q: 2 },
    capacities: { wide: { capacity: 10, quota: 'q' } },
  });
  registry2.acquire({ resource: 'wide', shares: 2 }, 'h');
  // Quota is the tight bound here.
  assert.throws(
    () => registry2.acquire('wide', 'g'),
    (e) => e.code === 'QUOTA_EXCEEDED' && e.quota === 'q',
  );
});

test('release and expiry return quota at every ancestor level', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock, ...hierarchy() });
  const a = registry.acquire({ resource: 'leafRes', shares: 2 }, 'a', 40);
  const b = registry.acquire('rootRes', 'b', 30);
  assert.deepEqual(
    ['leaf', 'mid', 'root'].map((n) => registry.quotaUsage(n)),
    [2, 2, 3],
  );

  clock.set(20);
  registry.release(b.token);
  assert.deepEqual(
    ['leaf', 'mid', 'root'].map((n) => registry.quotaUsage(n)),
    [2, 2, 2],
  );

  clock.set(40);
  registry.sweep();
  assert.deepEqual(
    ['leaf', 'mid', 'root'].map((n) => registry.quotaUsage(n)),
    [0, 0, 0],
  );
  assert.equal(registry.holder('leafRes'), null);
  void a;
});

test('quota freed on one branch wakes a waiter queued on another branch', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock,
    quotas: { root: 5, mid: { limit: 5, parent: 'root' } },
    capacities: {
      a: { capacity: 5, quota: 'mid' },
      b: { capacity: 5, quota: 'root' },
    },
  });
  const held = registry.acquire({ resource: 'a', shares: 4 }, 'A', 100);
  // root is at 4; this waiter needs 3 shares on b (root only) and blocks.
  const waiter = registry.acquire({ resource: 'b', shares: 3 }, 'B', 50, 1000);
  assert.equal(waiter.status, 'waiting');
  assert.equal(registry.holder('b'), null);

  // Freeing a's quota (both mid and root) reaches the waiter on branch b,
  // even though no share of b itself was ever occupied.
  clock.set(10);
  registry.release(held[0].token); // root 4 -> 3, only 2 room: still waiting
  assert.equal(waiter.status, 'waiting');
  registry.release(held[1].token); // root 3 -> 2: room for all 3 now
  assert.equal(waiter.status, 'granted');
  assert.equal(waiter.leases.length, 3);
  assert.equal(registry.holder('b'), 'B');
  // Branch a still holds its two other shares; root is now 2 (a) + 3 (b).
  assert.equal(registry.quotaUsage('root'), 5);
  assert.equal(registry.quotaUsage('mid'), 2);
});

test('a branch quota waking cannot grant more than the shared root allows', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock,
    quotas: { root: 3, m1: { limit: 3, parent: 'root' }, m2: { limit: 3, parent: 'root' } },
    capacities: {
      r1: { capacity: 3, quota: 'm1' },
      r2: { capacity: 3, quota: 'm2' },
    },
  });
  const held = registry.acquire({ resource: 'r1', shares: 2 }, 'A', 100);
  // root at 2; r2 wants 2 (m2 free but root only has 1 room)
  const waiter = registry.acquire({ resource: 'r2', shares: 2 }, 'B', 50, 1000);
  assert.equal(waiter.status, 'waiting');

  clock.set(5);
  registry.release(held[0].token); // root 2 -> 1: B's 2 now fit exactly
  assert.equal(waiter.status, 'granted');
  assert.equal(waiter.leases.length, 2);
  assert.equal(registry.quotaUsage('m2'), 2);
  // r1 still holds its unreleased share, so root = 1 (r1) + 2 (r2).
  assert.equal(registry.quotaUsage('root'), 3);
});

test('mixed single and group waiters obey arrival order under quota freeing', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock,
    quotas: { root: 4, m1: { limit: 4, parent: 'root' }, m2: { limit: 4, parent: 'root' } },
    capacities: {
      a: { capacity: 4, quota: 'm1' },
      b: { capacity: 4, quota: 'm2' },
    },
  });
  const held = registry.acquire({ resource: 'a', shares: 4 }, 'H', 100);

  const single = registry.acquire('a', 'S', 50, 1000);
  const group = registry.acquireAll(
    [{ resource: 'a', shares: 1 }, { resource: 'b', shares: 2 }],
    'G', 50, 1000,
  );
  const other = registry.acquire({ resource: 'b', shares: 2 }, 'O', 50, 1000);

  clock.set(10);
  registry.release(held[0].token);
  // Earliest waiter is single (1 share); it wins.
  assert.equal(single.status, 'granted');
  assert.equal(group.status, 'waiting');
  assert.equal(other.status, 'waiting');

  // Next the group needs one a-share and two b-shares; the shared root caps
  // the whole cascade: H still holds three, then two, then one share.
  registry.release(held[1].token);
  assert.equal(group.status, 'waiting'); // root 3 + group 3 > 4
  registry.release(held[2].token);
  assert.equal(group.status, 'waiting'); // root 2 + group 3 > 4
  registry.release(held[3].token);
  assert.equal(group.status, 'granted'); // root 1 + group 3 = 4
  // O arrived after G and so follows it; root: S1 + G3 = 4 -> O still waits.
  assert.equal(other.status, 'waiting');
  assert.equal(registry.quotaUsage('root'), 4);
});

test('declaration rules reject cycles, duplicates, unknown parents and late changes', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });

  registry.declareQuota('a', 5);
  registry.declareQuota('b', 5, 'a');
  assert.throws(() => registry.declareQuota('a', 9), TypeError);
  assert.throws(() => registry.declareQuota('c', 5, 'unknown'), TypeError);
  // A cycle: b -> a, making a a child of b would loop.
  assert.throws(() => registry.declareQuota('a2', 5, 'a2'), TypeError);

  assert.throws(
    () => registry.declareResource('r', 2, 'nope'),
    TypeError,
  );
  registry.declareResource('r', 2, 'b');
  assert.throws(() => registry.declareResource('r', 3, 'b'), TypeError);

  // Once a resource is in use its shape cannot change.
  registry.acquire('r', 'h');
  assert.throws(() => registry.declareResource('r', 5, 'b'), TypeError);
});

test('constructor declarations accept quotas in any map order', () => {
  const registry = createRegistry({
    ttlMs: 100, clock: fakeClock(0),
    quotas: new Map([
      ['child', { limit: 1, parent: 'parent' }],
      ['parent', { limit: 2, parent: null }],
    ]),
    capacities: { r: { capacity: 5, quota: 'child' } },
  });
  registry.acquire('r', 'h');
  assert.throws(
    () => registry.acquire('r', 'g'),
    (e) => e.code === 'QUOTA_EXCEEDED' && e.quota === 'child',
  );
});

test('a release cascade retakes expired shares on another resource as a grant', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock,
    quotas: { q: 5 },
    capacities: {
      x: { capacity: 5, quota: 'q' },
      y: { capacity: 5, quota: 'q' },
    },
  });
  const live = registry.acquire({ resource: 'x', shares: 3 }, 'X', 100);
  registry.acquire({ resource: 'y', shares: 2 }, 'Y', 10); // stale by t=11

  clock.set(11);
  // Retaking y's two stale shares frees two units, but the three live x shares
  // plus three new y shares would be 6 > 5, so the request queues.
  const waiter = registry.acquire({ resource: 'y', shares: 3 }, 'W', 50, 1000);
  assert.equal(waiter.status, 'waiting');

  // Releasing one live x share frees a quota unit; the waiter's grant on y
  // retakes y's two expired shares, so the whole request fits at once.
  registry.release(live[0].token);
  assert.equal(waiter.status, 'granted');
  assert.equal(waiter.leases.length, 3);
  assert.equal(registry.holder('y'), 'W');
  assert.equal(registry.quotaUsage('q'), 5); // x holds two plus y's three
  // Retaken shares are grants, never reclaims.
  assert.equal(registry.stats().reclaimed, 0);
  assert.deepEqual(registry.sweep(11), []);
});
