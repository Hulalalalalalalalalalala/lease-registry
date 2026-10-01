import { test } from 'node:test';
import assert from 'node:assert/strict';

import { createRegistry, LeaseTakenError } from '../src/index.js';

function fakeClock(start = 1000) {
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
  for (const ttlMs of [0, -1, 1.5, NaN, Infinity, '100', null, undefined]) {
    assert.throws(() => createRegistry({ ttlMs }), RangeError);
  }
});

test('createRegistry defaults clock to Date.now', () => {
  const registry = createRegistry({ ttlMs: 10 });
  const lease = registry.acquire('r', 'h', 10);
  assert.equal(lease.resource, 'r');
  assert.equal(lease.holder, 'h');
  assert.ok(typeof lease.token === 'string' && lease.token.length > 0);
  assert.ok(lease.expiresAt >= Date.now());
});

test('acquire validates resource, holder and ttlMs with TypeError', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock() });
  assert.throws(() => registry.acquire('', 'h', 100), TypeError);
  assert.throws(() => registry.acquire(1, 'h', 100), TypeError);
  assert.throws(() => registry.acquire(null, 'h', 100), TypeError);
  assert.throws(() => registry.acquire('r', '', 100), TypeError);
  assert.throws(() => registry.acquire('r', 2, 100), TypeError);
  for (const ttlMs of [0, -5, 1.5, NaN, Infinity, '100']) {
    assert.throws(() => registry.acquire('r', 'h', ttlMs), TypeError);
  }
});

test('successful acquire returns README fields with expiresAt = clock + ttlMs', () => {
  const clock = fakeClock(1000);
  const registry = createRegistry({ ttlMs: 50, clock });
  const lease = registry.acquire('res', 'owner', 250);
  assert.deepEqual(Object.keys(lease).sort(), [
    'expiresAt',
    'holder',
    'resource',
    'token',
  ]);
  assert.equal(lease.resource, 'res');
  assert.equal(lease.holder, 'owner');
  assert.equal(lease.expiresAt, 1250);
  assert.ok(lease.token.length > 0);
});

test('tokens are unique non-empty strings', () => {
  const clock = fakeClock();
  const registry = createRegistry({ ttlMs: 100, clock });
  const tokens = new Set();
  for (let i = 0; i < 100; i += 1) {
    const lease = registry.acquire(`r${i}`, 'h', 100);
    assert.ok(typeof lease.token === 'string' && lease.token.length > 0);
    assert.equal(tokens.has(lease.token), false);
    tokens.add(lease.token);
  }
});

test('acquiring a live resource throws LeaseTakenError with code LEASE_TAKEN', () => {
  const clock = fakeClock(1000);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('r', 'a', 100);
  try {
    registry.acquire('r', 'b', 100);
    assert.fail('expected LeaseTakenError');
  } catch (error) {
    assert.ok(error instanceof LeaseTakenError);
    assert.equal(error.code, 'LEASE_TAKEN');
  }

  // Still live one millisecond before expiry.
  clock.set(1099);
  assert.throws(() => registry.acquire('r', 'b', 100), LeaseTakenError);
});

test('a lease expiring exactly at expiresAt is expired and can be taken over', () => {
  const clock = fakeClock(1000);
  const registry = createRegistry({ ttlMs: 100, clock });
  const first = registry.acquire('r', 'a', 100); // expiresAt 1100

  clock.set(1100); // equal => expired
  assert.equal(registry.holder('r'), null);
  assert.equal(registry.renew(first.token), false);
  assert.equal(registry.release(first.token), false);

  const second = registry.acquire('r', 'b', 100);
  assert.notEqual(second.token, first.token);
  assert.equal(registry.holder('r'), 'b');
  // The old token is permanently dead even though the name is live again.
  assert.equal(registry.renew(first.token), false);
  assert.equal(registry.release(first.token), false);
});

test('expired lease without sweep is dead but does not block a new holder', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  const first = registry.acquire('r', 'a', 10);
  clock.set(11);
  const second = registry.acquire('r', 'b', 10);
  assert.equal(registry.holder('r'), 'b');
  assert.equal(registry.renew(first.token), false);
  assert.equal(second.expiresAt, 21);
});

test('renew extends a live lease by its original TTL from the current clock', () => {
  const clock = fakeClock(1000);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 100); // expiresAt 1100

  clock.set(1050);
  assert.equal(registry.renew(lease.token), true);
  assert.equal(registry.holder('r'), 'a');

  // Original expiry would have been 1100; renewal pushes it to 1150.
  clock.set(1120);
  assert.equal(registry.holder('r'), 'a');
  clock.set(1150);
  assert.equal(registry.holder('r'), null);
});

test('renew fails for unknown, released, expired and taken-over tokens', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });

  assert.equal(registry.renew('no-such-token'), false);
  assert.equal(registry.renew(''), false);

  const lease = registry.acquire('r', 'a', 10);
  assert.equal(registry.release(lease.token), true);
  assert.equal(registry.renew(lease.token), false); // released

  const lease2 = registry.acquire('r', 'a', 10);
  clock.set(10);
  assert.equal(registry.renew(lease2.token), false); // expired
  const lease3 = registry.acquire('r', 'b', 10);
  assert.equal(registry.renew(lease2.token), false); // taken over
  assert.equal(registry.renew(lease3.token), true);
});

test('release ends only a live lease and is idempotent', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });

  assert.equal(registry.release('unknown'), false);

  const lease = registry.acquire('r', 'a', 10);
  assert.equal(registry.release(lease.token), true);
  assert.equal(registry.release(lease.token), false);
  assert.equal(registry.holder('r'), null);
  // The resource is immediately releasable by someone else.
  const next = registry.acquire('r', 'b', 10);
  assert.equal(next.holder, 'b');

  clock.set(10);
  assert.equal(registry.release(next.token), false); // expired
});

test('holder reports the current live holder or null', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  assert.equal(registry.holder('never'), null);

  registry.acquire('r', 'a', 10);
  assert.equal(registry.holder('r'), 'a');
  clock.set(10);
  assert.equal(registry.holder('r'), null);
});

test('sweep rejects non finite safe-integer now with TypeError', () => {
  const registry = createRegistry({ ttlMs: 10, clock: fakeClock() });
  for (const now of [NaN, Infinity, -Infinity, 1.5, '10', null, undefined]) {
    assert.throws(() => registry.sweep(now), TypeError);
  }
  // Negative integers are accepted (nothing can have expired before 0).
  assert.deepEqual(registry.sweep(-1), []);
});

test('sweep frees expired leases in ascending resource-name order', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  registry.acquire('charlie', 'h', 10); // expires 10
  registry.acquire('alpha', 'h', 5); // expires 5
  registry.acquire('bravo', 'h', 20); // expires 20, stays live at 10

  assert.deepEqual(registry.sweep(10), ['alpha', 'charlie']);
  assert.equal(registry.holder('alpha'), null);
  assert.equal(registry.holder('charlie'), null);
  assert.equal(registry.holder('bravo'), 'h');

  // Repeating the sweep returns nothing and frees nothing new.
  assert.deepEqual(registry.sweep(10), []);
  assert.deepEqual(registry.sweep(100), ['bravo']);
  assert.deepEqual(registry.sweep(100), []);
});

test('sweep reclaimed tokens are dead and resources reusable', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  const lease = registry.acquire('r', 'a', 10);
  assert.deepEqual(registry.sweep(10), ['r']);
  assert.equal(registry.renew(lease.token), false);
  assert.equal(registry.release(lease.token), false);
  assert.doesNotThrow(() => registry.acquire('r', 'b', 10));
});

test('stats count only successful operations and current live leases', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });

  const a = registry.acquire('a', 'h', 10);
  registry.acquire('b', 'h', 10);
  assert.deepEqual(registry.stats(), {
    granted: 2,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 2,
  });

  // Failures do not move any counter.
  assert.throws(() => registry.acquire('a', 'other', 10), LeaseTakenError);
  assert.equal(registry.renew('unknown'), false);
  assert.equal(registry.release('unknown'), false);
  assert.deepEqual(registry.stats(), {
    granted: 2,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 2,
  });

  // Successful renew/release count; repeating release does not.
  assert.equal(registry.renew(a.token), true);
  assert.equal(registry.release(a.token), true);
  assert.equal(registry.release(a.token), false);
  assert.deepEqual(registry.stats(), {
    granted: 2,
    renewed: 1,
    released: 1,
    reclaimed: 0,
    live: 1,
  });

  // Expiry takeover is a grant, not a release or reclaim.
  clock.set(10);
  assert.equal(registry.stats().live, 0);
  registry.acquire('b', 'takeover', 10);
  assert.deepEqual(registry.stats(), {
    granted: 3,
    renewed: 1,
    released: 1,
    reclaimed: 0,
    live: 1,
  });

  // Sweep reclaims count once and only once.
  clock.set(20);
  assert.deepEqual(registry.sweep(20), ['b']);
  assert.deepEqual(registry.sweep(20), []);
  assert.deepEqual(registry.stats(), {
    granted: 3,
    renewed: 1,
    released: 1,
    reclaimed: 1,
    live: 0,
  });
});

test('stats.live is computed against the current clock', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock });
  registry.acquire('a', 'h', 5);
  registry.acquire('b', 'h', 10);

  clock.set(5);
  assert.equal(registry.stats().live, 1); // equality expires 'a'
  clock.set(4);
  assert.equal(registry.stats().live, 2);
});
