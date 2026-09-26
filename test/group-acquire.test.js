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

test('acquireAll holds every resource with independent credentials and expiries', () => {
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

test('group ttl falls back to the registry default and a scalar ttl applies to every resource', () => {
  const registry = createRegistry({ ttlMs: 25, clock: fakeClock(0) });
  assert.deepEqual(
    registry.acquireAll(['r1', 'r2'], 'h').map((l) => l.expiresAt),
    [25, 25],
  );
  const other = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  assert.deepEqual(
    other.acquireAll(['x', 'y'], 'h', 10).map((l) => l.expiresAt),
    [10, 10],
  );
});

test('the array form of acquire is the same atomic entry point under its aliases', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const leases = registry.acquire(['r1', 'r2'], 'h');
  assert.deepEqual(leases.map((l) => l.resource), ['r1', 'r2']);

  const aliased = registry.acquireGroup(['s1', 's2'], 'g');
  assert.deepEqual(aliased.map((l) => l.resource), ['s1', 's2']);
  const many = registry.acquireMany(['t1', 't2'], 'g');
  assert.deepEqual(many.map((l) => l.resource), ['t1', 't2']);
  assert.equal(registry.stats().granted, 6);
});

test('group calls validate the resource list, ttl values and wait budget', () => {
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
  assert.throws(() => registry.acquireAll(['a', 'b'], 'h', [10, 0]), TypeError);
  for (const bad of [0, -1, NaN, Infinity]) {
    assert.throws(() => registry.acquireAll(['a', 'b'], 'h', 10, bad), TypeError);
  }
  assert.equal(registry.stats().granted, 0);
});

test('one unavailable resource fails the whole group and leaves nothing behind', () => {
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
  assert.equal(caught.holder, 'owner');

  assert.equal(registry.holder('a'), null);
  assert.equal(registry.holder('b'), 'owner');
  assert.equal(registry.holder('c'), null);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });

  // The untouched resources remain freely acquirable, with fresh counters.
  const leases = registry.acquireAll(['a', 'c'], 'h2', 10);
  assert.equal(leases.length, 2);
  assert.equal(registry.stats().granted, 3);
});

test('a queued group lines up on every resource and holds none', () => {
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

  // The queued position reserves the still-free resource too.
  assert.throws(
    () => registry.acquire('b', 'late'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN',
  );
  // A later waiting request joins behind it instead of jumping ahead.
  const behind = registry.acquire('b', 'late', 10, 1000);
  assert.equal(behind.status, 'waiting');
  assert.equal(registry.stats().granted, 1);
});

test('single-resource waiting tickets keep the exact baseline shape', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.acquire('r', 'a');
  const ticket = registry.acquire('r', 'b', 50, 1000);
  assert.deepEqual(Object.keys(ticket).sort(), ['holder', 'resource', 'status', 'waitId']);
  assert.equal('resources' in ticket, false);
  assert.equal(ticket.resource, 'r');
});

test('freeing only some resources never grants the group', () => {
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
  assert.deepEqual(group.results, group.leases);
  assert.deepEqual(registry.stats(), {
    granted: 4, renewed: 0, released: 2, reclaimed: 0, live: 2,
  });
});

test('singles and groups are served in strict arrival order per resource', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h0');
  registry.acquire('b', 'hb', 15);

  const single = registry.acquire('a', 'S', 30, 1000);
  const group = registry.acquireAll(['a', 'b'], 'G', 30, 1000);

  clock.set(10);
  registry.release(a.token);
  assert.equal(single.status, 'granted', 'the earlier single request wins a');
  assert.equal(group.status, 'waiting');
  assert.equal(registry.holder('a'), 'S');

  registry.release(single.token);
  assert.equal(group.status, 'waiting', 'the group still needs b');

  // A later waiting request on b joins behind the group.
  const bLease = registry.acquire('b', 'take', 10, 1000);
  assert.equal(bLease.status, 'waiting');
  clock.set(20);
  registry.sweep(); // hb's lease expires and the whole group lands together
  assert.equal(group.status, 'granted');
  assert.equal(bLease.status, 'waiting', 'the later request stays behind the group');
  assert.equal(registry.holder('a'), 'G');
  assert.equal(registry.holder('b'), 'G');

  const groupA = group.leases.find((l) => l.resource === 'a');
  registry.release(groupA.token);
  assert.equal(bLease.status, 'waiting', 'releasing a cannot hand out b');
  registry.release(group.leases.find((l) => l.resource === 'b').token);
  assert.equal(bLease.status, 'granted');
  assert.equal(registry.holder('b'), 'take');
});

test('a group queued first wins all its resources ahead of a later single', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h0', 10);
  const b = registry.acquire('b', 'h0', 10);
  const group = registry.acquireAll(['a', 'b'], 'G', 20, 1000);
  const single = registry.acquire('a', 'S', 20, 1000);

  clock.set(15);
  registry.sweep(); // both holders expire in one reclaim event
  assert.equal(group.status, 'granted');
  assert.equal(single.status, 'waiting');
  assert.equal(registry.holder('a'), 'G');
  assert.equal(registry.holder('b'), 'G');

  registry.release(group.leases[0].token);
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
  assert.equal(registry.stats().live, 1);
  assert.equal(registry.release(leases[0].token), true);
  assert.equal(registry.holder('x'), null);
});

test('each expired twin counts as live until its own expiry instant and is reclaimed separately', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquireAll(['x', 'x'], 'h', [20, 10]);

  clock.set(10);
  assert.equal(registry.stats().live, 1, 'live drops per credential at the expiry instant');
  assert.equal(registry.stats().reclaimed, 0);
  assert.deepEqual(registry.sweep(), ['x']);
  assert.equal(registry.stats().reclaimed, 1);
  assert.equal(registry.holder('x'), 'h', 'one twin still holds the resource');

  clock.set(20);
  assert.deepEqual(registry.sweep(), ['x']);
  assert.equal(registry.stats().reclaimed, 2);
  assert.equal(registry.holder('x'), null);
});

test('a waiting group for a repeated resource stays queued until every copy is gone', () => {
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

  clock.set(5); // deadline reached
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

test('a timed-out group unblocks requests queued behind it on every resource', () => {
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

test('cancel voids the whole group exactly once and never lands twice', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'owner');
  const group = registry.acquireAll(['a', 'b'], 'G', 10, 1000);

  assert.equal(registry.cancel(group.waitId), true);
  assert.equal(group.status, 'cancelled');
  assert.equal(registry.cancel(group.waitId), false, 'the repeat cancel lands nothing');
  assert.equal(registry.cancel('wait-unknown'), false);

  registry.release(a.token);
  assert.equal(group.status, 'cancelled');
  assert.equal(registry.holder('a'), null);
  assert.equal(registry.holder('b'), null);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 1, reclaimed: 0, live: 0,
  });

  // An already granted group can no longer be cancelled.
  const a2 = registry.acquire('a', 'owner2');
  const g2 = registry.acquireAll(['a', 'b'], 'G2', 10, 1000);
  registry.release(a2.token); // b is free, so this frees the whole group at once
  assert.equal(g2.status, 'granted');
  assert.equal(registry.cancel(g2.waitId), false);
  assert.equal(registry.holder('a'), 'G2');
  assert.equal(registry.holder('b'), 'G2');
});

test('a group expired at the deadline cannot be cancelled afterwards', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('a', 'owner');
  const group = registry.acquireAll(['a', 'b'], 'G', 10, 5);

  clock.set(5);
  assert.equal(registry.cancel(group.waitId), false);
  assert.equal(group.status, 'expired');
});

test('sweep grants a group only once every resource has been reclaimed', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('a', 'h1', 10);
  registry.acquire('b', 'h2', 20);
  const group = registry.acquireAll(['a', 'b'], 'G', 50, 1000);

  clock.set(15);
  assert.deepEqual(registry.sweep(), ['a']);
  assert.equal(group.status, 'waiting');
  assert.equal(registry.holder('a'), null);
  clock.set(25);
  assert.deepEqual(registry.sweep(), ['b']);
  assert.equal(group.status, 'granted');
  assert.deepEqual(group.leases.map((l) => l.expiresAt), [75, 75]);
  assert.deepEqual(registry.stats(), {
    granted: 4, renewed: 0, released: 0, reclaimed: 2, live: 2,
  });
});

test('one reclaim event grants independent groups in arrival order, never overbooking a shared resource', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('a', 'h', 10);
  registry.acquire('b', 'h', 10);
  registry.acquire('c', 'h', 10);
  // g1 and g2 both line up for b; only the earlier one can win it.
  const g1 = registry.acquireAll(['a', 'b'], 'G1', 30, 1000);
  const g2 = registry.acquireAll(['b', 'c'], 'G2', 40, 1000);

  clock.set(15);
  assert.deepEqual(registry.sweep().sort(), ['a', 'b', 'c']);
  assert.equal(g1.status, 'granted');
  assert.equal(g2.status, 'waiting', 'g2 arrived later and loses b to g1');
  assert.equal(registry.holder('a'), 'G1');
  assert.equal(registry.holder('b'), 'G1');
  assert.equal(registry.holder('c'), null);

  registry.release(g1.leases[1].token);
  assert.equal(g2.status, 'granted');
  assert.equal(g2.leases[0].expiresAt, 15 + 40);
  assert.equal(registry.holder('b'), 'G2');
  assert.equal(registry.holder('c'), 'G2');
});

test('disjoint groups freed by one sweep are both granted, each with its own expiry', () => {
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

test('a group retaking an expired-but-unswept resource is granted with no reclaim count', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('a', 'old', 10);
  clock.set(11);
  assert.equal(registry.holder('a'), null);

  const fresh = registry.acquireAll(['a', 'b'], 'new', 50);
  assert.equal(fresh.length, 2);
  clock.set(11);
  assert.deepEqual(registry.sweep(), []);
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
});

test('group grants are persisted one credential per acquire in a single write', () => {
  const p = logPath('direct-replay');
  const clock = fakeClock(0);
  let registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  const leases = registry.acquireAll(['a', 'b'], 'h', [40, 60]);
  const lines = fs.readFileSync(p, 'utf8').trim().split('\n').map((l) => JSON.parse(l));
  assert.deepEqual(lines.map((l) => l.type), ['acquire', 'acquire']);
  assert.equal(lines[0].batch, lines[1].batch);
  assert.equal(lines[0].requestId, undefined);

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

test('a woken group is persisted as one atomic batch and replays together', () => {
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
  const types = fs.readFileSync(p, 'utf8').trim().split('\n').map((l) => JSON.parse(l).type);
  assert.deepEqual(types, ['acquire', 'acquire', 'release', 'release', 'acquire', 'acquire']);

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

  // The registry keeps working; the retry grants both credentials.
  const retry = registry.acquireAll(['a', 'b'], 'h', 10);
  assert.equal(retry.length, 2);
  assert.equal(registry.stats().granted, 3);
});

test('a failed log write on a waking group rolls the lease and every queue back', () => {
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

test('a failed log write on a waking sweep rolls the reclaim and the group back', () => {
  const p = logPath('sweep-rollback');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  registry.acquire('a', 'h1', 10);
  registry.acquire('b', 'h2', 10);
  const group = registry.acquireAll(['a', 'b'], 'G', 50, 1000);

  clock.set(15);
  sabotage(p);
  try {
    assert.throws(() => registry.sweep(), (e) => e instanceof LogFileError);
  } finally {
    restore(p);
  }
  assert.equal(group.status, 'waiting');
  assert.equal(registry.stats().reclaimed, 0);
  assert.equal(registry.stats().granted, 2);

  assert.deepEqual(registry.sweep().sort(), ['a', 'b']);
  assert.equal(group.status, 'granted');
  assert.deepEqual(group.leases.map((l) => l.expiresAt), [65, 65]);
  assert.deepEqual(registry.stats(), {
    granted: 4, renewed: 0, released: 0, reclaimed: 2, live: 2,
  });
});

test('queued groups live in memory only and do not survive a restart', () => {
  const p = logPath('restart');
  let registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  registry.acquire('a', 'h1');
  const group = registry.acquireAll(['a', 'b'], 'G', 50, 1000);

  registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(registry.cancel(group.waitId), false);
  // b was never granted on disk and the queue reservation lived in memory
  // only, so a fresh caller can take it immediately.
  const late = registry.acquire('b', 'late');
  assert.equal(late.holder, 'late');
  assert.equal(registry.holder('a'), 'h1');
});

test('clock readings going backwards are taken as given for groups too', () => {
  const clock = fakeClock(100);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h1');
  const group = registry.acquireAll(['a', 'b'], 'G', 50, 1000);

  clock.set(10);
  assert.equal(group.status, 'waiting');
  assert.equal(registry.holder('a'), 'h1');
  registry.release(a.token);
  assert.equal(group.status, 'granted', 'grants use the reading the clock gives, even backwards');
  assert.deepEqual(group.leases.map((l) => l.expiresAt), [60, 60]);
});
