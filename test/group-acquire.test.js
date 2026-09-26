import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { createRegistry, LeaseTakenError, LogFileError } from '../src/index.js';

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

let dir;
test.beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-group-'));
});
test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});
function logPath(name) {
  return path.join(dir, `${name}.log`);
}
function sabotage(p) {
  fs.renameSync(p, p + '.bak');
  fs.mkdirSync(p);
}
function restore(p) {
  fs.rmdirSync(p);
  fs.renameSync(p + '.bak', p);
}

test('acquireAll holds every resource atomically with independent credentials', () => {
  const clock = fakeClock(1000);
  const registry = createRegistry({ ttlMs: 100, clock });

  const leases = registry.acquireAll(['a', 'b', 'c'], 'h', [30, 40, 50]);

  assert.equal(Array.isArray(leases), true);
  assert.deepEqual(leases.map((l) => l.resource), ['a', 'b', 'c']);
  assert.ok(leases.every((l) => l.holder === 'h'));
  assert.deepEqual(leases.map((l) => l.expiresAt), [1030, 1040, 1050]);
  const tokens = new Set(leases.map((l) => l.token));
  assert.equal(tokens.size, 3);
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 0, live: 3,
  });
  assert.equal(registry.holder('a'), 'h');
  assert.equal(registry.holder('b'), 'h');
  assert.equal(registry.holder('c'), 'h');
});

test('acquireAll falls back to the default ttl and accepts an array through acquire', () => {
  const registry = createRegistry({ ttlMs: 25, clock: fakeClock(0) });
  const leases = registry.acquire(['r1', 'r2'], 'h');
  assert.deepEqual(leases.map((l) => l.expiresAt), [25, 25]);
});

test('acquireAll validates the resource list and ttl values', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  assert.throws(() => registry.acquireAll([], 'h'), TypeError);
  assert.throws(() => registry.acquireAll('a', 'h'), TypeError);
  for (const bad of [0, -1, NaN, Infinity, '10']) {
    assert.throws(() => registry.acquireAll(['a'], 'h', bad), TypeError);
  }
  assert.throws(
    () => registry.acquireAll(['a', 'b'], 'h', [10]),
    TypeError,
    'per-resource ttl list length must match',
  );
  assert.throws(
    () => registry.acquireAll(['a', 'b'], 'h', [10, 0]),
    TypeError,
  );
  assert.equal(registry.stats().granted, 0);
});

test('one unavailable resource fails the whole acquireAll without leaving anything', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('b', 'owner');

  let caught;
  assert.throws(
    () => registry.acquireAll(['a', 'b', 'c'], 'h'),
    (error) => {
      caught = error;
      return error instanceof LeaseTakenError;
    },
  );
  assert.equal(caught.code, 'LEASE_TAKEN');
  assert.equal(caught.resource, 'b');

  // Nothing landed on any resource.
  assert.equal(registry.holder('a'), null);
  assert.equal(registry.holder('b'), 'owner');
  assert.equal(registry.holder('c'), null);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });

  // The untouched resources remain freely acquirable.
  const leases = registry.acquireAll(['a', 'c'], 'h2', 10);
  assert.equal(leases.length, 2);
  assert.equal(registry.stats().granted, 3);
});

test('a contended group queues on every resource and holds none', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('a', 'owner');

  const ticket = registry.acquireAll(['a', 'b'], 'h', 50, 1000);
  assert.equal(ticket.status, 'waiting');
  assert.deepEqual(ticket.resources, ['a', 'b']);
  assert.equal(ticket.holder, 'h');
  assert.equal(typeof ticket.waitId, 'string');
  assert.equal('token' in ticket, false);
  assert.equal('leases' in ticket, false);

  assert.equal(registry.holder('a'), 'owner');
  assert.equal(registry.holder('b'), null);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });

  // A queued position reserves the free resource too: a later non-waiting
  // request cannot take it.
  assert.throws(
    () => registry.acquire('b', 'late'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN',
  );
});

test('freeing only some resources does not wake the group', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h1');
  const b = registry.acquire('b', 'h2');
  const group = registry.acquireAll(['a', 'b'], 'G', [10, 20], 1000);

  clock.set(10);
  assert.equal(registry.release(b.token), true);
  assert.equal(group.status, 'waiting');
  assert.equal('leases' in group, false);
  assert.equal(registry.holder('b'), null);

  assert.equal(registry.release(a.token), true);
  assert.equal(group.status, 'granted');
  assert.deepEqual(group.leases.map((l) => l.resource), ['a', 'b']);
  assert.equal(group.leases[0].expiresAt, 20); // wake time 10 + own ttl 10
  assert.equal(group.leases[1].expiresAt, 30); // wake time 10 + own ttl 20
  assert.equal(registry.holder('a'), 'G');
  assert.equal(registry.holder('b'), 'G');
  assert.deepEqual(registry.stats(), {
    granted: 4, renewed: 0, released: 2, reclaimed: 0, live: 2,
  });
});

test('singles and groups are served in strict arrival order per resource', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h0');
  registry.acquire('b', 'hb');

  const single = registry.acquire('a', 'S', 30, 1000);
  const group = registry.acquireAll(['a', 'b'], 'G', 30, 1000);

  clock.set(10);
  registry.release(a.token);
  assert.equal(single.status, 'granted', 'the earlier single request wins a');
  assert.equal(group.status, 'waiting');
  assert.equal(registry.holder('a'), 'S');

  registry.release(single.token);
  assert.equal(group.status, 'waiting', 'the group still needs b');
});

test('a group queued first wins all its resources ahead of a later single', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h0');
  const b = registry.acquire('b', 'h0');
  const group = registry.acquireAll(['a', 'b'], 'G', 20, 1000);
  const single = registry.acquire('a', 'S', 20, 1000);

  clock.set(10);
  registry.release(a.token);
  registry.release(b.token);
  assert.equal(group.status, 'granted');
  assert.equal(single.status, 'waiting');
  assert.equal(registry.holder('a'), 'G');
  assert.equal(registry.holder('b'), 'G');

  const groupA = group.leases.find((l) => l.resource === 'a');
  registry.release(groupA.token);
  assert.equal(single.status, 'granted');
  assert.equal(registry.holder('a'), 'S');
});

test('a resource listed twice is issued twice, one credential per occurrence', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const leases = registry.acquireAll(['x', 'x'], 'h', [40, 60]);

  assert.equal(leases.length, 2);
  assert.notEqual(leases[0].token, leases[1].token);
  assert.deepEqual(leases.map((l) => l.expiresAt), [40, 60]);
  assert.deepEqual(registry.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });

  assert.throws(
    () => registry.acquire('x', 'other'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN',
  );

  // The two credentials renew and release independently.
  clock.set(30);
  assert.equal(registry.renew(leases[0].token), true);
  assert.equal(registry.release(leases[1].token), true);
  assert.equal(registry.holder('x'), 'h'); // first credential still live
  assert.equal(registry.release(leases[0].token), true);
  assert.equal(registry.holder('x'), null);
});

test('a waiting group for a repeated resource stays queued until all copies are gone', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const held = registry.acquireAll(['x', 'x'], 'h', 40);
  const group = registry.acquireAll(['x', 'x'], 'G', 10, 1000);

  clock.set(10);
  registry.release(held[0].token);
  assert.equal(group.status, 'waiting', 'one surviving copy still blocks both positions');
  registry.release(held[1].token);
  assert.equal(group.status, 'granted');
  assert.equal(group.leases.length, 2);
  assert.notEqual(group.leases[0].token, group.leases[1].token);
  assert.equal(registry.stats().granted, 4);
});

test('a group whose wait budget runs out abandons every queue position', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'owner');
  const group = registry.acquireAll(['a', 'b'], 'G', 10, 5);

  clock.set(5);
  registry.stats(); // observe the deadline
  assert.equal(group.status, 'expired');
  assert.equal('leases' in group, false);

  registry.release(a.token);
  assert.equal(group.status, 'expired', 'an expired group is never woken');
  assert.equal(registry.holder('a'), null);
  assert.equal(registry.holder('b'), null);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 1, reclaimed: 0, live: 0,
  });

  // Both queue positions were removed: the resources can be taken normally.
  const next = registry.acquireAll(['a', 'b'], 'next', 10);
  assert.equal(next.length, 2);
  assert.equal(registry.stats().granted, 3);
});

test('a timed-out group unblocks a later single request behind it', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'owner');
  const group = registry.acquireAll(['a', 'b'], 'G', 10, 5);
  const single = registry.acquire('a', 'S', 30, 1000);

  clock.set(6);
  registry.stats();
  assert.equal(group.status, 'expired');
  registry.release(a.token);
  assert.equal(single.status, 'granted');
  assert.equal(registry.holder('a'), 'S');
});

test('cancel voids the whole group exactly once', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'owner');
  const group = registry.acquireAll(['a', 'b'], 'G', 10, 1000);

  assert.equal(registry.cancel(group.waitId), true);
  assert.equal(group.status, 'cancelled');
  assert.equal(registry.cancel(group.waitId), false);
  assert.equal(registry.cancel('wait-unknown'), false);

  // Releasing frees the resources to nobody: every position was removed.
  registry.release(a.token);
  assert.equal(group.status, 'cancelled');
  assert.equal(registry.holder('a'), null);
  assert.equal(registry.holder('b'), null);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 1, reclaimed: 0, live: 0,
  });

  // A granted group can no longer be cancelled.
  const a2 = registry.acquire('a', 'owner2');
  const g2 = registry.acquireAll(['a', 'b'], 'G2', 10, 1000);
  registry.release(a2.token); // b is free, so this wakes g2 as a whole
  assert.equal(g2.status, 'granted');
  assert.equal(registry.cancel(g2.waitId), false);
  assert.equal(registry.holder('a'), 'G2');
  assert.equal(registry.holder('b'), 'G2');
});

test('sweep grants a whole group in one reclaim and reports expiry order', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('a', 'h1', 10);
  registry.acquire('b', 'h2', 20);
  const group = registry.acquireAll(['a', 'b'], 'G', 50, 1000);

  clock.set(15);
  assert.deepEqual(registry.sweep(), ['a']);
  assert.equal(group.status, 'waiting');
  clock.set(25);
  assert.deepEqual(registry.sweep(), ['b']);
  assert.equal(group.status, 'granted');
  assert.deepEqual(group.leases.map((l) => l.expiresAt), [75, 75]);
  assert.deepEqual(registry.stats(), {
    granted: 4, renewed: 0, released: 0, reclaimed: 2, live: 2,
  });
});

test('one sweep waking several groups grants them in arrival order', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('a', 'h', 10);
  registry.acquire('b', 'h', 10);
  registry.acquire('c', 'h', 10);
  registry.acquire('d', 'h', 10);
  const g1 = registry.acquireAll(['a', 'b'], 'G1', 30, 1000);
  const g2 = registry.acquireAll(['c', 'd'], 'G2', 40, 1000);

  clock.set(15);
  assert.deepEqual(registry.sweep().sort(), ['a', 'b', 'c', 'd']);
  assert.equal(g1.status, 'granted');
  assert.equal(g2.status, 'granted');
  assert.equal(g1.leases[0].expiresAt, 45);
  assert.equal(g2.leases[0].expiresAt, 55);
  assert.equal(registry.stats().granted, 8);
});

test('group grants are persisted one credential per acquire and replay as leases', () => {
  const p = logPath('direct-replay');
  const clock = fakeClock(0);
  let registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  const leases = registry.acquireAll(['a', 'b'], 'h', [40, 60]);
  assert.deepEqual(
    fs.readFileSync(p, 'utf8').trim().split('\n').map((l) => JSON.parse(l).type),
    ['acquire', 'acquire'],
  );

  registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(registry.holder('a'), 'h');
  assert.equal(registry.holder('b'), 'h');
  assert.deepEqual(registry.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
  assert.equal(registry.renew(leases[0].token), false);
  assert.equal(registry.release(leases[1].token), false);
  assert.deepEqual(registry.sweep(39), []);
  assert.deepEqual(registry.sweep(40), ['a']);
  assert.deepEqual(registry.sweep(60), ['b']);
});

test('a woken group is persisted as an atomic batch and replays together', () => {
  const p = logPath('wake-replay');
  const clock = fakeClock(0);
  let registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = registry.acquire('a', 'h1');
  const b = registry.acquire('b', 'h2');
  const group = registry.acquireAll(['a', 'b'], 'G', [40, 60], 1000);

  clock.set(10);
  registry.release(a.token);
  registry.release(b.token);
  assert.equal(group.status, 'granted');

  registry = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assert.equal(registry.holder('a'), 'G');
  assert.equal(registry.holder('b'), 'G');
  assert.deepEqual(registry.stats(), {
    granted: 4, renewed: 0, released: 2, reclaimed: 0, live: 2,
  });
  for (const lease of group.leases) {
    assert.equal(registry.release(lease.token), false);
  }
  assert.deepEqual(registry.sweep(49), []);
  assert.deepEqual(registry.sweep(50), ['a']);
  assert.deepEqual(registry.sweep(70), ['b']);
});

test('a group retaking an expired unswept resource replays without a reclaim count', () => {
  const p = logPath('retake-replay');
  const clock = fakeClock(0);
  let registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  registry.acquire('a', 'old', 10);
  clock.set(11);
  const fresh = registry.acquireAll(['a', 'b'], 'new', 50);
  assert.equal(fresh.length, 2);
  assert.deepEqual(registry.sweep(), []);
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });

  registry = createRegistry({ ttlMs: 100, clock: fakeClock(11), logPath: p });
  assert.equal(registry.holder('a'), 'new');
  assert.equal(registry.holder('b'), 'new');
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
});

test('a failed log write on a direct group rolls every resource back', () => {
  const p = logPath('direct-rollback');
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  registry.acquire('x', 'h');

  sabotage(p);
  try {
    assert.throws(
      () => registry.acquireAll(['a', 'b'], 'h'),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }
  assert.equal(registry.holder('a'), null);
  assert.equal(registry.holder('b'), null);
  assert.equal(registry.stats().granted, 1);

  const retry = registry.acquireAll(['a', 'b'], 'h', 10);
  assert.equal(retry.length, 2);
  assert.equal(registry.stats().granted, 3);
});

test('a failed log write on a waking group rolls the lease and all queues back', () => {
  const p = logPath('wake-rollback');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = registry.acquire('a', 'h1');
  registry.acquire('b', 'h2');
  const group = registry.acquireAll(['a', 'b'], 'G', 50, 1000);

  sabotage(p);
  try {
    assert.throws(
      () => registry.release(a.token),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }
  assert.equal(group.status, 'waiting');
  assert.equal(registry.holder('a'), 'h1');
  assert.deepEqual(registry.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });

  // Retry lands and wakes nothing until b is freed as well.
  assert.equal(registry.release(a.token), true);
  assert.equal(group.status, 'waiting');
  assert.equal(registry.holder('a'), null);
  assert.equal(registry.stats().released, 1);
});
