import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  createRegistry,
  LeaseTakenError,
} from '../src/index.js';

function controllableClock(start = 1000) {
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

test('createRegistry rejects invalid ttlMs with RangeError', () => {
  for (const ttlMs of [0, -1, 1.5, NaN, Infinity, -Infinity, '100', null, undefined, 2 ** 53]) {
    assert.throws(() => createRegistry({ ttlMs }), RangeError);
  }
  assert.throws(() => createRegistry(), RangeError);
  assert.doesNotThrow(() => createRegistry({ ttlMs: 1 }));
});

test('acquire validates resource, holder and ttlMs with TypeError', () => {
  const registry = createRegistry({ ttlMs: 100, clock: controllableClock() });

  for (const bad of ['', 42, {}, [], null, undefined]) {
    assert.throws(() => registry.acquire(bad, 'h'), TypeError);
    assert.throws(() => registry.acquire('r', bad), TypeError);
  }
  for (const badTtl of [0, -5, 1.5, NaN, Infinity, '100']) {
    assert.throws(() => registry.acquire('r', 'h', badTtl), TypeError);
  }
  assert.deepEqual(registry.stats(), {
    granted: 0,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 0,
  });
});

test('successful acquire returns the documented fields', () => {
  const clock = controllableClock(1000);
  const registry = createRegistry({ ttlMs: 100, clock });

  const lease = registry.acquire('resource-a', 'holder-1');
  assert.deepEqual(Object.keys(lease).sort(), [
    'expiresAt',
    'holder',
    'resource',
    'token',
  ]);
  assert.equal(lease.resource, 'resource-a');
  assert.equal(lease.holder, 'holder-1');
  assert.equal(lease.expiresAt, 1100);
  assert.equal(typeof lease.token, 'string');
  assert.ok(lease.token.length > 0);
  assert.equal(registry.holder('resource-a'), 'holder-1');
});

test('each acquire mints a unique non-empty token', () => {
  const clock = controllableClock();
  const registry = createRegistry({ ttlMs: 100, clock });

  const first = registry.acquire('a', 'h');
  registry.release(first.token);
  const second = registry.acquire('a', 'h');
  assert.notEqual(first.token, second.token);
});

test('acquiring a live resource throws LeaseTakenError with code LEASE_TAKEN', () => {
  const clock = controllableClock(1000);
  const registry = createRegistry({ ttlMs: 100, clock });

  registry.acquire('r', 'holder-1');
  let caught;
  try {
    registry.acquire('r', 'holder-2');
  } catch (error) {
    caught = error;
  }
  assert.ok(caught instanceof LeaseTakenError);
  assert.equal(caught.code, 'LEASE_TAKEN');
  assert.ok(caught instanceof Error);
  assert.equal(registry.holder('r'), 'holder-1');

  // One grant, one failed attempt.
  assert.equal(registry.stats().granted, 1);
  assert.equal(registry.stats().live, 1);

  // Still held right up to the millisecond before expiry.
  clock.set(1099);
  assert.throws(() => registry.acquire('r', 'holder-2'), LeaseTakenError);
});

test('a lease is expired when now equals expiresAt', () => {
  const clock = controllableClock(1000);
  const registry = createRegistry({ ttlMs: 100, clock });

  const lease = registry.acquire('r', 'holder-1');
  clock.set(1100);

  assert.equal(registry.holder('r'), null);
  assert.equal(registry.renew(lease.token), false);
  assert.equal(registry.release(lease.token), false);
  assert.equal(registry.stats().live, 0);

  const taken = registry.acquire('r', 'holder-2');
  assert.equal(taken.expiresAt, 1200);
  assert.equal(registry.holder('r'), 'holder-2');
});

test('an expired lease can be taken over and the old token is dead', () => {
  const clock = controllableClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });

  const old = registry.acquire('r', 'old-holder');
  clock.advance(11);

  const next = registry.acquire('r', 'new-holder');
  assert.notEqual(next.token, old.token);
  assert.equal(registry.holder('r'), 'new-holder');
  assert.equal(registry.renew(old.token), false);
  assert.equal(registry.release(old.token), false);
  assert.equal(registry.renew(next.token), true);
});

test('renew extends a live lease by its original ttl and reports success', () => {
  const clock = controllableClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });

  const lease = registry.acquire('r', 'h', 50);
  assert.equal(lease.expiresAt, 50);

  clock.set(40);
  assert.equal(registry.renew(lease.token), true);
  assert.equal(registry.stats().renewed, 1);

  // Without renewal it would have expired at 50; it now survives to 90.
  clock.set(50);
  assert.equal(registry.holder('r'), 'h');
  clock.set(90);
  assert.equal(registry.holder('r'), null);
  assert.equal(registry.renew(lease.token), false);
});

test('renew fails for unknown, released, expired and superseded tokens', () => {
  const clock = controllableClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });

  assert.equal(registry.renew('no-such-token'), false);
  assert.equal(registry.renew(''), false);

  const lease = registry.acquire('r', 'h');
  assert.equal(registry.release(lease.token), true);
  assert.equal(registry.renew(lease.token), false);

  const short = registry.acquire('s', 'h', 10);
  clock.set(10);
  assert.equal(registry.renew(short.token), false);

  const taken = registry.acquire('s', 'h2', 10);
  assert.equal(registry.renew(short.token), false);
  assert.equal(registry.renew(taken.token), true);

  assert.equal(registry.stats().renewed, 1);
});

test('release ends only a live lease and is idempotent', () => {
  const clock = controllableClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });

  assert.equal(registry.release('no-such-token'), false);

  const lease = registry.acquire('r', 'h');
  assert.equal(registry.release(lease.token), true);
  assert.equal(registry.release(lease.token), false);
  assert.equal(registry.holder('r'), null);
  assert.equal(registry.stats().released, 1);

  const expiring = registry.acquire('e', 'h', 10);
  clock.set(10);
  assert.equal(registry.release(expiring.token), false);
  assert.equal(registry.stats().released, 1);
});

test('holder reports the current live holder or null', () => {
  const clock = controllableClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });

  assert.equal(registry.holder('missing'), null);
  registry.acquire('r', 'h');
  assert.equal(registry.holder('r'), 'h');
  clock.set(10);
  assert.equal(registry.holder('r'), null);
});

test('sweep frees expired leases in ascending resource order', () => {
  const clock = controllableClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });

  registry.acquire('charlie', 'h', 10);
  registry.acquire('alpha', 'h', 20);
  registry.acquire('bravo', 'h', 10);
  registry.acquire('delta', 'h', 100);

  const freed = registry.sweep(15);
  assert.deepEqual(freed, ['bravo', 'charlie']);
  assert.equal(registry.holder('charlie'), null);
  assert.equal(registry.holder('bravo'), null);
  assert.equal(registry.holder('alpha'), 'h');
  assert.equal(registry.holder('delta'), 'h');
  assert.equal(registry.stats().reclaimed, 2);
  assert.equal(registry.stats().live, 2);

  // Repeating the sweep frees nothing new and does not accumulate.
  assert.deepEqual(registry.sweep(15), []);
  assert.equal(registry.stats().reclaimed, 2);

  // A later sweep catches leases that expire in the meantime.
  assert.deepEqual(registry.sweep(1000), ['alpha', 'delta']);
  assert.equal(registry.stats().reclaimed, 4);
  assert.equal(registry.stats().live, 0);
});

test('sweep rejects non-finite, non-safe-integer arguments', () => {
  const registry = createRegistry({ ttlMs: 100, clock: controllableClock() });
  registry.acquire('r', 'h');

  for (const bad of [undefined, null, '10', 1.5, NaN, Infinity, -Infinity, 2 ** 53]) {
    assert.throws(() => registry.sweep(bad), TypeError);
  }
  assert.deepEqual(registry.sweep(0), []);
});

test('sweep invalidates tokens so released resources can be re-granted', () => {
  const clock = controllableClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });

  const old = registry.acquire('r', 'old');
  clock.set(10);
  assert.deepEqual(registry.sweep(10), ['r']);
  assert.equal(registry.renew(old.token), false);
  assert.equal(registry.release(old.token), false);

  const fresh = registry.acquire('r', 'new');
  assert.notEqual(fresh.token, old.token);
  assert.equal(registry.holder('r'), 'new');
});

test('stats only count successful operations and liveness follows the clock', () => {
  const clock = controllableClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });

  const a = registry.acquire('a', 'h', 10);
  registry.acquire('b', 'h', 20);

  // Failed operations move no counters.
  assert.throws(() => registry.acquire('a', 'intruder'), LeaseTakenError);
  registry.renew('unknown');
  registry.release('unknown');

  assert.deepEqual(registry.stats(), {
    granted: 2,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 2,
  });

  clock.set(5);
  assert.equal(registry.renew(a.token), true);
  clock.set(10); // a would have expired at 10; renewed at t=5 it now survives to 15.
  assert.equal(registry.stats().live, 2);

  clock.set(15);
  assert.equal(registry.stats().live, 1); // a expired but not yet swept; b lives to 20

  clock.set(20);
  assert.equal(registry.stats().live, 0); // expired but not yet swept
  registry.sweep(20);
  const stats = registry.stats();
  assert.equal(stats.granted, 2);
  assert.equal(stats.renewed, 1);
  assert.equal(stats.released, 0);
  assert.equal(stats.reclaimed, 2);
  assert.equal(stats.live, 0);
});

test('works with the default Date.now clock', () => {
  const registry = createRegistry({ ttlMs: 60_000 });
  const lease = registry.acquire('r', 'h');
  assert.ok(lease.expiresAt > Date.now());
  assert.equal(registry.holder('r'), 'h');
  assert.equal(registry.renew(lease.token), true);
  assert.equal(registry.release(lease.token), true);
});
