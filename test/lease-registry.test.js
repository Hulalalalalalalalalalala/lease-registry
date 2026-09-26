import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRegistry, LeaseTakenError } from '../src/index.js';

function makeClock(start = 1000) {
  let now = start;
  return {
    fn: () => now,
    set(value) { now = value; },
    advance(ms) { now += ms; },
  };
}

test('acquire returns lease descriptor with expiry from the injected clock', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  const lease = registry.acquire('db', 'alice', 1000);
  assert.equal(lease.resource, 'db');
  assert.equal(lease.holder, 'alice');
  assert.equal(typeof lease.token, 'string');
  assert.equal(lease.expiresAt, 2000);
  assert.equal(registry.holder('db'), 'alice');
});

test('acquire uses the registry default ttl when none is given', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  const lease = registry.acquire('db', 'alice');
  assert.equal(lease.expiresAt, 6000);
});

test('acquiring a held resource throws LeaseTakenError with a code', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  registry.acquire('db', 'alice', 1000);
  assert.throws(
    () => registry.acquire('db', 'bob', 1000),
    (err) => err instanceof LeaseTakenError && err.code === 'LEASE_TAKEN',
  );
  // Failed acquire changes nothing.
  assert.equal(registry.holder('db'), 'alice');
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
});

test('renew extends a live lease and reports success', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  const lease = registry.acquire('db', 'alice', 1000);
  clock.advance(600);
  assert.equal(registry.renew(lease.token), true);
  clock.advance(900); // 2500 total: past the original expiry, still live
  assert.equal(registry.holder('db'), 'alice');
  assert.equal(registry.stats().renewed, 1);
});

test('renew on expired, released, or unknown tokens returns false', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  const lease = registry.acquire('db', 'alice', 1000);
  clock.advance(1500); // expired
  assert.equal(registry.renew(lease.token), false);

  const second = registry.acquire('cache', 'alice', 1000);
  assert.equal(registry.release(second.token), true);
  assert.equal(registry.renew(second.token), false);

  assert.equal(registry.renew('no-such-token'), false);
  assert.equal(registry.stats().renewed, 0);
});

test('release frees the resource immediately and is idempotent-ish', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  const lease = registry.acquire('db', 'alice', 1000);
  assert.equal(registry.release(lease.token), true);
  assert.equal(registry.holder('db'), null);
  // Same token again: no state change, no error.
  assert.equal(registry.release(lease.token), false);
  assert.equal(registry.renew(lease.token), false);
  assert.equal(registry.stats().released, 1);
  // Resource can be re-acquired right away.
  const next = registry.acquire('db', 'bob', 1000);
  assert.equal(next.holder, 'bob');
});

test('sweep reclaims expired leases in expiry order and counts them', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  registry.acquire('a', 'alice', 3000); // expires 4000
  registry.acquire('b', 'alice', 1000); // expires 2000
  registry.acquire('c', 'alice', 2000); // expires 3000
  const freed = registry.sweep(3500);
  assert.deepEqual(freed, ['b', 'c']);
  assert.equal(registry.holder('b'), null);
  assert.equal(registry.holder('a'), 'alice');
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 2, live: 1,
  });
  // Sweeping with nothing expired yields an empty list and no count change.
  assert.deepEqual(registry.sweep(3500), []);
  assert.equal(registry.stats().reclaimed, 2);
});

test('sweep judges by the passed timestamp, not the injected clock', () => {
  const clock = makeClock(1000);
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  registry.acquire('db', 'alice', 1000); // expires 2000 per the clock
  // Passed "now" is ahead of the injected clock: still decisive.
  assert.deepEqual(registry.sweep(5000), ['db']);
  assert.equal(registry.stats().reclaimed, 1);
});

test('expired-but-unswept resource can be acquired and the old token is void', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  const old = registry.acquire('db', 'alice', 1000);
  clock.advance(1500); // expired, not swept
  const fresh = registry.acquire('db', 'bob', 1000);
  assert.equal(fresh.holder, 'bob');
  assert.equal(registry.holder('db'), 'bob');
  assert.equal(registry.renew(old.token), false);
  assert.equal(registry.release(old.token), false);
  // Takeover is not a reclaim.
  assert.equal(registry.stats().reclaimed, 0);
  assert.equal(registry.stats().granted, 2);
});

test('live count excludes expired and released leases', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  registry.acquire('a', 'alice', 1000);
  const b = registry.acquire('b', 'alice', 5000);
  registry.acquire('c', 'alice', 9000);
  assert.equal(registry.stats().live, 3);
  clock.advance(2000); // 'a' expired but unswept
  assert.equal(registry.stats().live, 2);
  registry.release(b.token);
  assert.equal(registry.stats().live, 1);
  registry.sweep(clock.fn());
  assert.equal(registry.stats().live, 1);
});

test('holder returns null for unknown, released, and expired resources', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  assert.equal(registry.holder('nope'), null);
  const lease = registry.acquire('db', 'alice', 1000);
  clock.advance(1500);
  assert.equal(registry.holder('db'), null);
  const other = registry.acquire('cache', 'alice', 1000);
  registry.release(other.token);
  assert.equal(registry.holder('cache'), null);
});

test('time does not advance on its own', () => {
  const clock = makeClock();
  const registry = createRegistry({ ttlMs: 5000, clock: clock.fn });
  registry.acquire('db', 'alice', 1000);
  assert.equal(registry.holder('db'), 'alice');
  assert.equal(registry.stats().live, 1);
});
