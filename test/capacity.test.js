import test from 'node:test';
import assert from 'node:assert/strict';

import { createRegistry, LeaseTakenError, QuotaExceededError } from '../src/index.js';

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

test('a capacity resource hands out shares up to its capacity', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock, capacities: { c: 3 },
  });

  const a = registry.acquire('c', 'a');
  const b = registry.acquire('c', 'b');
  const third = registry.acquire('c', 'c');
  assert.equal(typeof a.token, 'string');
  assert.notEqual(a.token, b.token);
  assert.notEqual(b.token, third.token);
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 0, live: 3,
  });

  assert.throws(
    () => registry.acquire('c', 'd'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN' && e.resource === 'c',
  );
  // A rejected application changes nothing.
  assert.equal(registry.stats().granted, 3);
  assert.equal(registry.holder('c'), 'a');
});

test('asking for several shares returns one credential per share', () => {
  const clock = fakeClock(1000);
  const registry = createRegistry({
    ttlMs: 100, clock, capacities: { c: 4 },
  });

  const leases = registry.acquire({ resource: 'c', shares: 3 }, 'h', [40, 50, 60]);
  assert.equal(Array.isArray(leases), true);
  assert.equal(leases.length, 3);
  assert.ok(leases.every((l) => l.resource === 'c' && l.holder === 'h'));
  assert.deepEqual(leases.map((l) => l.expiresAt), [1040, 1050, 1060]);
  assert.equal(new Set(leases.map((l) => l.token)).size, 3);
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 0, live: 3,
  });
  assert.equal(registry.capacity('c'), 4);

  // Only one share is left.
  assert.ok(registry.acquire('c', 'g'));
  assert.throws(
    () => registry.acquire('c', 'x'),
    (e) => e.code === 'LEASE_TAKEN',
  );
});

test('shares validate their count', () => {
  const registry = createRegistry({
    ttlMs: 100, clock: fakeClock(0), capacities: { c: 4 },
  });
  for (const bad of [0, -1, 1.5, NaN, '2']) {
    assert.throws(
      () => registry.acquire({ resource: 'c', shares: bad }, 'h'),
      TypeError,
    );
  }
  assert.throws(
    () => registry.acquireAll([{ resource: 'c', shares: 0 }], 'h'),
    TypeError,
  );
  assert.equal(registry.stats().granted, 0);
});

test('an undeclared resource keeps single occupancy and capacity() reports 1', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.acquire('r', 'a');
  assert.equal(registry.capacity('r'), 1);
  assert.throws(
    () => registry.acquire('r', 'b'),
    (e) => e.code === 'LEASE_TAKEN',
  );
});

test('expiry reclamation is per share and never touches the other shares', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock, capacities: { c: 3 },
  });
  registry.acquire('c', 'a', 10);
  registry.acquire('c', 'b', 30);
  registry.acquire('c', 'c', 50);

  clock.set(10);
  assert.deepEqual(registry.sweep(), ['c']);
  assert.equal(registry.holder('c'), 'b');
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 1, live: 2,
  });

  clock.set(30);
  assert.deepEqual(registry.sweep(), ['c']);
  assert.equal(registry.holder('c'), 'c');

  clock.set(50);
  assert.deepEqual(registry.sweep(), ['c']);
  assert.equal(registry.holder('c'), null);
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 3, live: 0,
  });
});

test('released shares are immediately grantable again', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock, capacities: { c: 2 },
  });
  const a = registry.acquire('c', 'a');
  registry.acquire('c', 'b');

  assert.throws(
    () => registry.acquire('c', 'd'),
    (e) => e.code === 'LEASE_TAKEN',
  );
  registry.release(a.token);
  const fresh = registry.acquire('c', 'd', 40);
  assert.equal(fresh.holder, 'd');
  assert.equal(fresh.expiresAt, 40);
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 1, reclaimed: 0, live: 2,
  });
});

test('renewing one share leaves every other share on the resource untouched', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock, capacities: { c: 3 },
  });
  const mine = registry.acquire('c', 'a', 20);
  const other = registry.acquire('c', 'b', 50);
  const third = registry.acquire('c', 'a', 50);

  clock.set(15);
  assert.equal(registry.renew(mine.token), true); // now expires 35
  // No other share's expiry moved.
  assert.equal(registry.stats().renewed, 1);

  clock.set(35);
  assert.deepEqual(registry.sweep(), ['c']); // only the renewed share is due
  assert.equal(registry.holder('c'), 'b');
  assert.equal(registry.renew(mine.token), false);
  clock.set(50);
  assert.deepEqual(registry.sweep(), ['c', 'c']);
  assert.equal(registry.holder('c'), null);
  void third;
});

test('a multi-share request that fits nowhere yet queues and wakes atomically', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock, capacities: { c: 3 },
  });
  const held = registry.acquire({ resource: 'c', shares: 3 }, 'h', 100);
  assert.equal(held.length, 3);

  const ticket = registry.acquire({ resource: 'c', shares: 2 }, 'G', 40, 1000);
  assert.equal(ticket.status, 'waiting');
  assert.deepEqual(ticket.resources, ['c', 'c']);
  assert.equal('leases' in ticket, false);

  clock.set(10);
  registry.release(held[0].token);
  assert.equal(ticket.status, 'waiting', 'two shares are needed, one freed');
  registry.release(held[1].token);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.leases.length, 2);
  assert.deepEqual(ticket.leases.map((l) => l.expiresAt), [50, 50]);
  assert.deepEqual(registry.stats(), {
    granted: 5, renewed: 0, released: 2, reclaimed: 0, live: 3,
  });
});

test('freed shares stay with the head request; a later request cannot skip it', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock, capacities: { c: 3 },
  });
  const held = registry.acquire({ resource: 'c', shares: 3 }, 'h', 100);
  const wants2 = registry.acquire({ resource: 'c', shares: 2 }, 'big', 50, 1000);
  const wants1 = registry.acquire('c', 'small', 50, 1000);

  clock.set(5);
  registry.release(held[0].token);
  // The earlier request needs two shares, so the single freed share is not
  // handed to the later single-share request either.
  assert.equal(wants2.status, 'waiting');
  assert.equal(wants1.status, 'waiting');

  registry.release(held[1].token);
  assert.equal(wants2.status, 'granted');
  assert.equal(wants1.status, 'waiting');

  registry.release(held[2].token);
  assert.equal(wants1.status, 'granted');
});

test('a request that cannot get every share leaves no credential and no count', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock, capacities: { c: 2 },
  });
  registry.acquire('c', 'a');

  assert.throws(
    () => registry.acquire({ resource: 'c', shares: 2 }, 'b'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN',
  );
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });

  // A group spread over resources fails as a unit on capacity too.
  const registry2 = createRegistry({
    ttlMs: 100, clock, capacities: { x: 2, y: 2 },
  });
  registry2.acquire({ resource: 'x', shares: 2 }, 'occ');
  assert.throws(
    () => registry2.acquireAll([
      { resource: 'x', shares: 1 },
      { resource: 'y', shares: 1 },
    ], 'G'),
    (e) => e.code === 'LEASE_TAKEN',
  );
  assert.equal(registry2.holder('y'), null);
  assert.equal(registry2.stats().granted, 2);
});

test('a timed-out multi-share waiter removes every queue position', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 100, clock, capacities: { c: 2 },
  });
  const held = registry.acquire({ resource: 'c', shares: 2 }, 'h', 100);
  const big = registry.acquire({ resource: 'c', shares: 2 }, 'big', 50, 5);
  const small = registry.acquire('c', 'small', 50, 1000);

  clock.set(5);
  registry.stats();
  assert.equal(big.status, 'expired');
  assert.equal(small.status, 'waiting');

  // With big's two positions gone, the first freed share reaches the head
  // waiter small at once; the expired big request is never woken.
  clock.set(10);
  registry.release(held[0].token);
  assert.equal(big.status, 'expired');
  assert.equal(small.status, 'granted');
  assert.equal(registry.holder('c'), 'small');
  assert.equal(registry.stats().granted, 3);
});

test('many shares expiring reclaim only the due ones', () => {
  const clock = fakeClock(0);
  const n = 1000;
  const registry = createRegistry({
    ttlMs: 10_000_000, clock, capacities: { c: n + 1 },
  });
  const bulk = registry.acquire({ resource: 'c', shares: n }, 'bulk', 10_000_000);
  void bulk;
  registry.acquire('c', 'short', 10);

  clock.set(10);
  const freed = registry.sweep();
  assert.deepEqual(freed, ['c']);
  assert.equal(registry.stats().live, n);
  assert.equal(registry.stats().reclaimed, 1);
});
