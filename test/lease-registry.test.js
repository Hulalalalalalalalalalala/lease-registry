import test from 'node:test';
import assert from 'node:assert/strict';

import { createRegistry, LeaseTakenError } from '../src/index.js';

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

test('acquire returns a credential with an absolute expiry', () => {
  const clock = fakeClock(1000);
  const registry = createRegistry({ ttlMs: 50, clock });

  const lease = registry.acquire('db', 'node-1', 30);

  assert.deepEqual(
    { resource: lease.resource, holder: lease.holder, expiresAt: lease.expiresAt },
    { resource: 'db', holder: 'node-1', expiresAt: 1030 },
  );
  assert.equal(typeof lease.token, 'string');
  assert.deepEqual(registry.stats(), {
    granted: 1,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 1,
  });
});

test('acquire falls back to the registry default ttl', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 25, clock });

  const lease = registry.acquire('r', 'h');
  assert.equal(lease.expiresAt, 25);
});

test('acquire on a live resource throws LeaseTakenError with a code', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 50, clock });
  registry.acquire('r', 'a');

  let caught;
  assert.throws(
    () => registry.acquire('r', 'b'),
    (error) => {
      caught = error;
      return error instanceof LeaseTakenError;
    },
  );
  assert.equal(caught.code, 'LEASE_TAKEN');
  assert.equal(caught.resource, 'r');

  // Failed acquire changes nothing.
  assert.equal(registry.holder('r'), 'a');
  assert.equal(registry.stats().granted, 1);
});

test('time does not advance on its own', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  registry.acquire('r', 'a');

  assert.equal(registry.holder('r'), 'a');
  assert.equal(registry.stats().live, 1);
});

test('renew extends a live lease from the current clock reading', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 40);
  assert.equal(lease.expiresAt, 40);

  clock.set(30);
  assert.equal(registry.renew(lease.token), true);
  assert.equal(registry.stats().renewed, 1);

  clock.set(60);
  assert.equal(registry.holder('r'), 'a');
  clock.set(71);
  assert.equal(registry.holder('r'), null);
});

test('renew after expiry returns false and changes nothing', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  const lease = registry.acquire('r', 'a');

  clock.set(10); // expiresAt <= now means expired
  assert.equal(registry.renew(lease.token), false);
  assert.equal(registry.stats().renewed, 0);
  assert.equal(registry.stats().granted, 1);
});

test('operations on unknown, released and swept tokens return false', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  const lease = registry.acquire('r', 'a');

  assert.equal(registry.renew('does-not-exist'), false);
  assert.equal(registry.release('does-not-exist'), false);

  assert.equal(registry.release(lease.token), true);
  assert.equal(registry.release(lease.token), false);
  assert.equal(registry.renew(lease.token), false);
  assert.deepEqual(registry.stats(), {
    granted: 1,
    renewed: 0,
    released: 1,
    reclaimed: 0,
    live: 0,
  });

  clock.set(20);
  assert.deepEqual(registry.sweep(), []);
});

test('release frees the resource immediately', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  const lease = registry.acquire('r', 'a');
  registry.release(lease.token);

  assert.equal(registry.holder('r'), null);
  const next = registry.acquire('r', 'b');
  assert.equal(next.holder, 'b');
  assert.equal(registry.renew(lease.token), false);
  assert.equal(registry.release(lease.token), false);
});

test('sweep reclaims expired leases in resource order and reports them', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('c', 'h', 10);
  registry.acquire('a', 'h', 20);
  registry.acquire('b', 'h', 30);
  registry.acquire('d', 'h', 100);

  clock.set(25);
  assert.deepEqual(registry.sweep(), ['c', 'a']);
  assert.deepEqual(registry.stats(), {
    granted: 4,
    renewed: 0,
    released: 0,
    reclaimed: 2,
    live: 2, // b (expires 30) and d (expires 100)
  });

  // Idle sweep returns an empty list and does not count again.
  assert.deepEqual(registry.sweep(), []);
  assert.equal(registry.stats().reclaimed, 2);

  clock.set(30);
  assert.deepEqual(registry.sweep(), ['b']);
  assert.equal(registry.stats().reclaimed, 3);
});

test('sweep reports resources in expiry order regardless of acquisition order', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('later', 'h', 50);
  registry.acquire('earlier', 'h', 10);
  registry.acquire('middle', 'h', 30);

  clock.set(100);
  assert.deepEqual(registry.sweep(), ['earlier', 'middle', 'later']);
});

test('sweep uses the passed-in timestamp even when it disagrees with the clock', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('r', 'a', 10);

  // Clock still says 0, but the caller says time has passed.
  assert.deepEqual(registry.sweep(50), ['r']);
  assert.equal(registry.stats().reclaimed, 1);
});

test('expired but unswept resources can be taken over; old token dies', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  const old = registry.acquire('r', 'a', 10);

  clock.set(11);
  assert.equal(registry.holder('r'), null);

  const fresh = registry.acquire('r', 'b', 10);
  assert.notEqual(old.token, fresh.token);
  assert.equal(registry.renew(old.token), false);
  assert.equal(registry.release(old.token), false);
  assert.equal(registry.holder('r'), 'b');

  // The expired lease was retaken, never reclaimed by a sweep.
  clock.set(11);
  assert.deepEqual(registry.sweep(), []);
  assert.equal(registry.stats().reclaimed, 0);
  assert.equal(registry.stats().granted, 2);
});

test('an expired lease drops out of live count at the expiry instant', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  registry.acquire('r', 'a', 10);

  clock.set(9);
  assert.equal(registry.stats().live, 1);
  clock.set(10);
  assert.equal(registry.stats().live, 0);
  // Still counted as reclaimed only once swept.
  assert.equal(registry.stats().reclaimed, 0);
});

test('released leases are never reported by sweep', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  const lease = registry.acquire('r', 'a', 10);
  registry.release(lease.token);

  clock.set(100);
  assert.deepEqual(registry.sweep(), []);
  assert.equal(registry.stats().reclaimed, 0);
  assert.equal(registry.stats().released, 1);
});

test('holder returns null for free, released and expired resources', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  registry.acquire('live', 'a', 100);
  const released = registry.acquire('freed', 'a', 100);
  registry.release(released.token);
  registry.acquire('gone', 'a', 5);

  assert.equal(registry.holder('never'), null);
  assert.equal(registry.holder('freed'), null);
  clock.set(6);
  assert.equal(registry.holder('gone'), null);
  assert.equal(registry.holder('live'), 'a');
});
