import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  createRegistry,
  LeaseTakenError,
  QuotaExceededError,
  LogFileError,
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

let dir;
test.beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-shares-'));
});
test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});
function logPath(name) {
  return path.join(dir, `${name}.log`);
}

// ---- Capacity: several holders share one resource -------------------------

test('a declared capacity lets several holders occupy one resource at once', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('pool', 3);

  const l1 = registry.acquire('pool', 'a', 100);
  const l2 = registry.acquire('pool', 'b', 100);
  const l3 = registry.acquire('pool', 'c', 100);
  assert.equal(new Set([l1.token, l2.token, l3.token]).size, 3);
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 0, live: 3,
  });

  let caught;
  assert.throws(
    () => registry.acquire('pool', 'd'),
    (error) => {
      caught = error;
      return error instanceof LeaseTakenError;
    },
  );
  assert.equal(caught.code, 'LEASE_TAKEN');
  assert.equal(caught.resource, 'pool');
  assert.equal(registry.stats().granted, 3);
});

test('a released share is immediately re-grantable while other holders stay', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('pool', 2);
  const a = registry.acquire('pool', 'a', 100);
  const b = registry.acquire('pool', 'b', 100);

  clock.set(10);
  assert.equal(registry.release(a.token), true);
  const d = registry.acquire('pool', 'd', 100);
  assert.equal(d.holder, 'd');
  assert.equal(registry.holder('pool'), 'b'); // b still occupies it
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 1, reclaimed: 0, live: 2,
  });

  // b's credential is untouched.
  clock.set(20);
  assert.equal(registry.renew(b.token), true);
  assert.equal(registry.release(b.token), true);
  assert.equal(registry.release(a.token), false);
});

test('each share carries its own credential and expires independently', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('pool', 3);
  const a = registry.acquire('pool', 'a', 10);
  const b = registry.acquire('pool', 'b', 20);
  const c = registry.acquire('pool', 'c', 30);

  clock.set(10);
  assert.deepEqual(registry.sweep(), ['pool']);
  assert.equal(registry.holder('pool'), 'b'); // b and c still live
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 1, live: 2,
  });
  // One expired credential cannot be renewed; the others can.
  assert.equal(registry.renew(a.token), false);
  clock.set(15);
  assert.equal(registry.renew(b.token), true); // b -> 35
  assert.equal(registry.renew(c.token), true); // c -> 45

  // The freed share was handed out again at t=10.
  const d = registry.acquire('pool', 'd', 50);
  assert.equal(d.holder, 'd');

  clock.set(30);
  assert.deepEqual(registry.sweep(), []); // c's renewal moved it to 45
  clock.set(35);
  assert.deepEqual(registry.sweep(), ['pool']); // b
  clock.set(45);
  assert.deepEqual(registry.sweep(), ['pool']); // c
  clock.set(64);
  assert.deepEqual(registry.sweep(), []);
  clock.set(65);
  assert.deepEqual(registry.sweep(), ['pool']); // d (granted at 15 + 50)
  assert.equal(registry.holder('pool'), null);
  assert.equal(registry.stats().reclaimed, 4);
});

test('renewing one share leaves the other holders on the same resource alone', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('pool', 2);
  const a = registry.acquire('pool', 'a', 10);
  const b = registry.acquire('pool', 'b', 100);

  clock.set(5);
  assert.equal(registry.renew(a.token), true); // a -> 15
  clock.set(10);
  assert.deepEqual(registry.sweep(), []); // a survived, b untouched
  clock.set(15);
  assert.deepEqual(registry.sweep(), ['pool']); // only a
  assert.equal(registry.holder('pool'), 'b');
  assert.equal(registry.stats().reclaimed, 1);
});

test('an undeclared resource still behaves as single occupancy', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.acquire('legacy', 'a');
  assert.throws(
    () => registry.acquire('legacy', 'b'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN',
  );
});

// ---- Multi-share atomic demands -------------------------------------------

test('a group can demand several shares of one resource, one credential each', () => {
  const clock = fakeClock(1000);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('pool', 5);

  const leases = registry.acquireAll(
    [{ resource: 'pool', count: 3 }], 'h', [40],
  );
  assert.equal(leases.length, 3);
  assert.ok(leases.every((l) => l.resource === 'pool' && l.holder === 'h'));
  assert.deepEqual(leases.map((l) => l.expiresAt), [1040, 1040, 1040]);
  assert.equal(new Set(leases.map((l) => l.token)).size, 3);
  assert.equal(registry.stats().live, 3);
});

test('a multi-share demand that cannot fully fit fails whole and leaves nothing', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('pool', 3);
  registry.acquire('pool', 'owner', 100);
  registry.acquire('pool', 'owner2', 100); // 2/3 used

  let caught;
  assert.throws(
    () => registry.acquireAll([{ resource: 'pool', count: 2 }], 'g'),
    (error) => {
      caught = error;
      return error instanceof LeaseTakenError;
    },
  );
  assert.equal(caught.code, 'LEASE_TAKEN');
  assert.equal(caught.resource, 'pool');
  assert.equal(caught.requested, 2);
  assert.equal(caught.available, 1);

  // No partial shares, no counters, existing owners untouched.
  assert.deepEqual(registry.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
  // The one free share is still directly grantable.
  assert.equal(registry.acquire('pool', 'x', 100).holder, 'x');
});

test('a multi-share group queues until enough shares accumulate, then wakes whole', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('pool', 3);
  const held = [
    registry.acquire('pool', 'h0', 100),
    registry.acquire('pool', 'h1', 100),
    registry.acquire('pool', 'h2', 100),
  ];
  const group = registry.acquireAll(
    [{ resource: 'pool', count: 2 }], 'G', [50], 1000,
  );
  assert.equal(group.status, 'waiting');

  clock.set(10);
  registry.release(held[0].token); // only one share free: group still waits
  assert.equal(group.status, 'waiting');
  registry.release(held[1].token); // two shares free: whole group wakes
  assert.equal(group.status, 'granted');
  assert.equal(group.leases.length, 2);
  assert.equal(new Set(group.leases.map((l) => l.token)).size, 2);
  assert.deepEqual(group.leases.map((l) => l.expiresAt), [60, 60]);
  assert.equal(registry.stats().granted, 5);
  assert.equal(registry.stats().live, 3); // h2 + two group shares

  // The queued group reserved its turn: while it waited, a one-share direct
  // request could not jump ahead.
  const group2 = registry.acquireAll(
    [{ resource: 'pool', count: 1 }], 'G2', 50, 1000,
  );
  assert.throws(
    () => registry.acquire('pool', 'late'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN',
  );
  registry.release(held[2].token);
  assert.equal(group2.status, 'granted');
});

// ---- Hierarchical quotas ---------------------------------------------------

test('every quota level limits descendant share occupation at once', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('root', 5);
  registry.setQuota('east', 3, 'root');
  registry.setQuota('east-db', 1, 'east');
  registry.declareResource('db1', 10, 'east-db');
  registry.declareResource('db2', 10, 'east');
  registry.declareResource('db3', 10, 'root');

  registry.acquire('db1', 'h1'); // east-db 1/1, east 1/3, root 1/4
  registry.acquire('db2', 'h2'); // east 2/3
  registry.acquire('db3', 'h3'); // root 2/4

  // db1's tight leaf quota is 1/1 although east and the resource have room.
  assertQuotaError(() => registry.acquire('db1', 'h4'), 'east-db');
  // Fill east to its own limit.
  registry.acquire('db2', 'h5'); // east 3/3
  assertQuotaError(() => registry.acquire('db2', 'h6'), 'east');
  // Root still has room: one more fits outside the full east subtree.
  assert.equal(registry.acquire('db3', 'h7').holder, 'h7'); // root 5/5
  assertQuotaError(() => registry.acquire('db3', 'h8'), 'root');

  assert.deepEqual(registry.stats(), {
    granted: 5, renewed: 0, released: 0, reclaimed: 0, live: 5,
  });
});

function assertQuotaError(fn, quotaId) {
  let caught;
  assert.throws(
    () => fn(),
    (error) => {
      caught = error;
      return error instanceof QuotaExceededError;
    },
  );
  assert.equal(caught.code, 'QUOTA_EXCEEDED');
  assert.equal(caught.quota, quotaId);
}

test('a parent quota bounds the share sum across several child resources', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('team', 3);
  registry.declareResource('cpu', 8, 'team');
  registry.declareResource('gpu', 8, 'team');

  registry.acquire('cpu', 'a');
  registry.acquire('cpu', 'b');
  registry.acquire('gpu', 'c'); // team 3/3

  assertQuotaError(() => registry.acquire('cpu', 'd'), 'team');
  assertQuotaError(() => registry.acquire('gpu', 'e'), 'team');
});

test('a quota-blocked group fails atomically with an assertable code', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('q', 2);
  registry.declareResource('a', 10, 'q');
  registry.declareResource('b', 10, 'q');
  registry.acquire('a', 'x');
  registry.acquire('b', 'y'); // q 2/2, both resources otherwise empty of room

  let caught;
  assert.throws(
    () => registry.acquireAll(['a', 'b'], 'g'),
    (error) => {
      caught = error;
      return error instanceof QuotaExceededError;
    },
  );
  assert.equal(caught.code, 'QUOTA_EXCEEDED');
  assert.equal(caught.quota, 'q');
  assert.equal(caught.requested, 2);
  assert.equal(caught.available, 0);

  // Not a single share left behind anywhere.
  assert.equal(registry.holder('a'), 'x');
  assert.equal(registry.holder('b'), 'y');
  assert.deepEqual(registry.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
});

test('a quota-blocked group queues and wakes only when a share frees anywhere under the quota', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('q', 3);
  registry.declareResource('a', 10, 'q');
  registry.declareResource('b', 10, 'q');
  const l1 = registry.acquire('a', 'x', 100);
  const l2 = registry.acquire('b', 'y', 100);
  registry.acquire('b', 'z', 100); // q 3/3

  const group = registry.acquireAll(['a', 'b'], 'G', [50, 60], 1000);
  assert.equal(group.status, 'waiting');

  clock.set(10);
  registry.release(l1.token); // q 3 -> 2; group needs 2 shares: 2+2 > 3
  assert.equal(group.status, 'waiting');
  registry.release(l2.token); // q 2 -> 1: 1+2 === 3
  assert.equal(group.status, 'granted');
  assert.deepEqual(group.leases.map((l) => l.resource), ['a', 'b']);
  assert.deepEqual(group.leases.map((l) => l.expiresAt), [60, 70]);
  assert.equal(registry.holder('a'), 'G');
  assert.equal(registry.stats().granted, 5);
});

test('a share expiring on one quota resource funds a waiter on another', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('q', 2);
  registry.declareResource('a', 10, 'q');
  registry.declareResource('b', 10, 'q');
  registry.acquire('a', 'old', 10);
  registry.acquire('b', 'filler', 100); // q 2/2

  const ticket = registry.acquire('b', 'new', 100, 1000);
  clock.set(10);
  assert.deepEqual(registry.sweep(), ['a']);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.expiresAt, 110);
  assert.equal(registry.stats().granted, 3);
  assert.equal(registry.stats().reclaimed, 1);
  assert.equal(registry.stats().live, 2);
});

test('expired shares free quota room lazily on the next observation', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('q', 1);
  registry.declareResource('x', 10, 'q');
  registry.declareResource('y', 10, 'q');
  registry.acquire('x', 'h', 10);

  clock.set(20);
  assert.equal(registry.stats().live, 0); // observes the expiry
  // The expired share already returned its quota room, even unswept.
  const granted = registry.acquire('y', 'h2', 10);
  assert.equal(granted.resource, 'y');
  // And a sweep afterwards reclaims the expired credential exactly once.
  assert.deepEqual(registry.sweep(), ['x']);
  assert.equal(registry.stats().reclaimed, 1);
});

test('quota usage counts per share and recovers as shares leave any way', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('q', 2);
  registry.declareResource('r', 10, 'q');
  const a = registry.acquire('r', 'a', 10);
  const b = registry.acquire('r', 'b', 100);
  assertQuotaError(() => registry.acquire('r', 'c'), 'q');

  clock.set(10);
  registry.sweep(); // a reclaimed: room returns
  assert.equal(registry.acquire('r', 'c', 100).holder, 'c');

  registry.release(b.token); // early release: room returns
  assert.equal(registry.acquire('r', 'd', 100).holder, 'd');
});

test('raising a quota limit takes effect and lowering it only blocks new grants', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('q', 1);
  registry.declareResource('r', 10, 'q');
  registry.acquire('r', 'a');
  assertQuotaError(() => registry.acquire('r', 'b'), 'q');

  registry.setQuota('q', 2);
  assert.equal(registry.acquire('r', 'b').holder, 'b');
  registry.setQuota('q', 1); // live shares above the new limit stay
  assert.equal(registry.holder('r'), 'a');
  assertQuotaError(() => registry.acquire('r', 'c'), 'q');
});

// ---- Mixed FIFO across shares ----------------------------------------------

test('single and group waiters on a share resource keep strict arrival order', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('r', 2);
  const h1 = registry.acquire('r', 'h1', 100);
  const h2 = registry.acquire('r', 'h2', 100);

  // First in line wants 2 shares; second wants 1.
  const group = registry.acquireAll(
    [{ resource: 'r', count: 2 }], 'G', 30, 1000,
  );
  const single = registry.acquire('r', 'S', 30, 1000);

  clock.set(10);
  registry.release(h1.token); // one share free: the single could fit, but
  assert.equal(group.status, 'waiting'); // the earlier group must not be jumped
  assert.equal(single.status, 'waiting');
  assert.equal(registry.stats().live, 1); // only h2; the free share stays vacant

  registry.release(h2.token); // two shares free: group wakes whole
  assert.equal(group.status, 'granted');
  assert.equal(group.leases.length, 2);
  assert.equal(single.status, 'waiting');

  registry.release(group.leases[0].token);
  // One share freed funds the head behind the group: it asked for exactly
  // one, so it wakes without waiting for the group to leave entirely.
  assert.equal(single.status, 'granted');
  assert.equal(registry.holder('r'), 'G'); // the group's other share stays
  registry.release(group.leases[1].token);
});

test('several groups free in one cascade in arrival order under a shared quota', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('q', 2);
  registry.declareResource('a', 10, 'q');
  registry.declareResource('b', 10, 'q');
  const la = registry.acquire('a', 'oldA', 10);
  const lb = registry.acquire('b', 'oldB', 10);
  const g1 = registry.acquireAll(['a'], 'G1', 50, 1000);
  const g2 = registry.acquireAll(['b'], 'G2', 50, 1000);

  clock.set(10);
  assert.deepEqual(registry.sweep().sort(), ['a', 'b']);
  assert.equal(g1.status, 'granted');
  assert.equal(g2.status, 'granted');
  assert.equal(g1.leases[0].expiresAt, 60);
  assert.equal(g2.leases[0].expiresAt, 60);
});

// ---- Declaration validation ------------------------------------------------

test('resource and quota declarations validate their arguments', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  for (const bad of [0, -1, 1.5, NaN, Infinity, '3']) {
    assert.throws(() => registry.declareResource('r', bad), TypeError);
  }
  assert.throws(() => registry.declareResource('r', 1, 'nope'), TypeError);
  assert.throws(() => registry.setQuota('q', 0), TypeError);
  assert.throws(() => registry.setQuota('q', 2, 'ghost'), TypeError);
  assert.throws(() => registry.setQuota(null, 1), TypeError);

  registry.setQuota('q', 2);
  assert.throws(() => registry.setQuota('q', 2, 'q'), TypeError); // cycles/re-parenting
  registry.declareResource('r', 1);
  assert.throws(() => registry.declareResource('r', 1), TypeError); // redeclared
  assert.throws(() => registry.declareResource('r', 2, 'q'), TypeError);

  // Multi-share entries validate counts.
  registry.declareResource('s', 5, 'q');
  assert.throws(
    () => registry.acquireAll([{ resource: 's', count: 0 }], 'h'),
    TypeError,
  );
  assert.throws(
    () => registry.acquireAll([{ resource: 's', count: 2.5 }], 'h'),
    TypeError,
  );
});

// ---- Persistence ------------------------------------------------------------

test('capacity and quota declarations and usage survive a plain restart', () => {
  const p = logPath('restart');
  const clock = fakeClock(0);
  let registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  registry.setQuota('root', 4);
  registry.setQuota('east', 2, 'root');
  registry.declareResource('a', 10, 'east');
  registry.declareResource('b', 10, 'root');
  const keep = registry.acquire('a', 'h1', 100);
  registry.acquire('a', 'h2', 20); // east 2/2
  registry.acquire('b', 'h3', 100);

  clock.set(20);
  assert.deepEqual(registry.sweep(), ['a']); // h2 reclaimed, east back to 1
  const after = registry.acquire('a', 'h4', 50);
  assert.equal(after.holder, 'h4');

  registry = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  assert.equal(registry.holder('a'), 'h4');
  assert.equal(registry.holder('b'), 'h3');
  assert.deepEqual(registry.stats(), {
    granted: 4, renewed: 0, released: 0, reclaimed: 1, live: 3,
  });
  // Hierarchy is intact: east is 2/2 again (h1 + h4).
  assertQuotaError(() => registry.acquire('a', 'h5'), 'east');
  // root is 3/4 (east's two shares + h3): one more fits on b.
  assert.equal(registry.acquire('b', 'h6', 100).holder, 'h6');
  assertQuotaError(() => registry.acquire('b', 'h7'), 'root');

  // Pre-restart credentials stay void.
  assert.equal(registry.renew(keep.token), false);
});

test('a quota declaration out of order in the log is a corrupt log error', () => {
  const p = logPath('bad-order');
  fs.writeFileSync(
    p,
    `${JSON.stringify({
      v: 1, type: 'quota', at: 0, quota: 'child', parent: 'ghost', limit: 1,
    })}\n`,
  );
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
});

function buildShareHistory(p, clock, { rootLimit = 8 } = {}) {
  const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  registry.setQuota('root', rootLimit);
  registry.setQuota('east', 4, 'root');
  registry.setQuota('west', 3, 'root');
  registry.declareResource('a', 10, 'east');
  registry.declareResource('b', 10, 'east');
  registry.declareResource('c', 10, 'west');
  registry.declareResource('d', 10, 'west');
  registry.declareResource('solo', 1);
  const keep = registry.acquire('a', 'h1', 200);
  const renewed = registry.acquire('a', 'h2', 40);
  const freed = registry.acquire('b', 'h3', 200);
  registry.acquire('b', 'gone1', 10);
  registry.acquire('d', 'gone2', 20);
  const group = registry.acquireAll(['c', 'solo'], 'h5', [60, 70]);
  clock.set(30);
  registry.renew(renewed.token); // -> 70
  registry.release(freed.token);
  clock.set(50);
  return { registry, keep, renewed, freed, group };
}

const SHARE_STATS = {
  // Live at t=50: keep(200), renewed(->70), group c(60), group solo(70).
  // gone1(10) and gone2(20) expired unswept; freed was released.
  granted: 7, renewed: 1, released: 1, reclaimed: 0, live: 4,
};

test('compaction preserves shares, quota occupation and counters item for item', () => {
  const pFull = logPath('full');
  const pFold = logPath('fold');
  const c1 = fakeClock(0);
  const c2 = fakeClock(0);
  buildShareHistory(pFull, c1);
  const folded = buildShareHistory(pFold, c2);
  c1.set(50);
  c2.set(50);
  folded.registry.compact();

  const full = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: pFull });
  const fold = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: pFold });

  assert.deepEqual(fold.stats(), SHARE_STATS);
  assert.deepEqual(fold.stats(), full.stats());
  for (const resource of ['a', 'b', 'c', 'd', 'solo']) {
    assert.equal(fold.holder(resource), full.holder(resource), resource);
  }

  // Reclaim sequences match, including equal-expiry share reclaims:
  // gone1(b)@10 and gone2(d)@20 due at 50; c@60; solo and renewed@70;
  // keep@200.
  assert.deepEqual(fold.sweep(50), full.sweep(50));
  assert.deepEqual(fold.sweep(59), full.sweep(59));
  assert.deepEqual(fold.sweep(60), full.sweep(60));
  assert.deepEqual(fold.sweep(70), full.sweep(70));
  assert.deepEqual(fold.sweep(200), full.sweep(200));
  assert.deepEqual(fold.stats(), full.stats());
});

test('every quota level keeps its room after a compacted restart', () => {
  const p = logPath('rooms');
  const clock = fakeClock(0);
  buildShareHistory(p, clock);
  clock.set(50);
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });

  // At t=50 (solo sits outside the quota forest): east 2/4 (keep, renewed),
  // west 1/3 (group c), root 3/8; the two expired shares leave lazily as the
  // first observations touch their heaps.
  assert.equal(r.acquire('c', 'w2', 100).holder, 'w2'); // west 2, root 4
  assert.equal(r.acquire('d', 'w3', 100).holder, 'w3'); // west 3, root 5
  assertQuotaError(() => r.acquire('c', 'w4', 100), 'west');
  assert.equal(r.acquire('a', 'e2', 100).holder, 'e2'); // east 3, root 6
  assert.equal(r.acquire('a', 'e3', 100).holder, 'e3'); // east 4, root 7
  assertQuotaError(() => r.acquire('b', 'rb', 100), 'east');
  // Root still shows one spare, but west is full: the tightest level wins.
  assertQuotaError(() => r.acquire('d', 'rd', 100), 'west');

  // The same usage survives another restart; live counts solo (no quota).
  const reopened = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.deepEqual(reopened.stats(), {
    granted: 11, renewed: 1, released: 1, reclaimed: 0, live: 8,
  });
  assertQuotaError(() => reopened.acquire('d', 'rd', 100), 'west');
});

test('post-compaction increments replay on top of recovered quota usage', () => {
  const p = logPath('increment');
  const clock = fakeClock(0);
  const { registry } = buildShareHistory(p, clock);
  clock.set(50);
  registry.compact();

  const extra = registry.acquire('c', 'h6', 100);
  assert.equal(extra.holder, 'h6'); // west 2/3
  assert.deepEqual(registry.stats(), {
    granted: 8, renewed: 1, released: 1, reclaimed: 0, live: 5,
  });

  const reopened = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.deepEqual(reopened.stats(), {
    granted: 8, renewed: 1, released: 1, reclaimed: 0, live: 5,
  });
  assert.equal(reopened.acquire('d', 'h7', 100).holder, 'h7'); // west 3/3
  assertQuotaError(() => reopened.acquire('c', 'h8', 100), 'west');
});

test('a corrupted quota section of a snapshot throws and the full log recovers', () => {
  const p = logPath('snap-corrupt');
  const clock = fakeClock(0);
  const { registry } = buildShareHistory(p, clock);
  clock.set(50);
  const fullLog = fs.readFileSync(p, 'utf8');
  registry.compact();
  fs.writeFileSync(p, fullLog);

  const image = JSON.parse(fs.readFileSync(`${p}.snapshot`, 'utf8'));
  image.quotas.push({ quota: 'broken', parent: 'ghost', limit: 1 });
  fs.writeFileSync(`${p}.snapshot`, `${JSON.stringify(image)}\n`);
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  fs.rmSync(`${p}.snapshot`);
  const recovered = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.deepEqual(recovered.stats(), SHARE_STATS);
});

test('queued share requests do not survive a compacted restart', () => {
  const p = logPath('waiters');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  registry.declareResource('r', 2, null);
  registry.acquire('r', 'a', 100);
  registry.acquire('r', 'b', 100);
  const ticket = registry.acquireAll(
    [{ resource: 'r', count: 2 }], 'w', 50, 1000,
  );
  registry.compact();

  const reopened = createRegistry({ ttlMs: 100, clock: fakeClock(100), logPath: p });
  assert.deepEqual(reopened.sweep(), ['r', 'r']);
  assert.equal(ticket.status, 'waiting');
  // Both shares are simply free afterwards; fresh queueing works normally.
  const next = reopened.acquire('r', 'c', 50);
  assert.equal(next.holder, 'c');
});

// ---- Non-linear scaling -----------------------------------------------------

test('hot operations with many coexisting shares do not scan them all', () => {
  const n = 10000;
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('big', n);
  const tokens = [];
  for (let i = 0; i < n; i += 1) {
    tokens.push(registry.acquire('big', 'h', 100).token);
  }

  // Every share live: availability and holder checks stop at heap heads.
  let start = process.hrtime.bigint();
  assert.equal(registry.stats().live, n);
  assert.ok(registry.holder('big') !== null);
  let elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
  assert.ok(elapsedMs < 100, `hot observations took ${elapsedMs}ms`);

  // Release one share: wake planning with no waiters stays constant.
  clock.set(10);
  start = process.hrtime.bigint();
  registry.release(tokens[0]);
  elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
  assert.ok(elapsedMs < 100, `release took ${elapsedMs}ms`);

  // All shares expire; observing stats pops each due heap node once
  // (amortized), and a second observation is then heap-head cheap.
  clock.set(200);
  registry.stats();
  start = process.hrtime.bigint();
  assert.equal(registry.stats().live, 0);
  elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
  assert.ok(elapsedMs < 100, `second observation took ${elapsedMs}ms`);
});

test('a quota freeing on one resource plans only the heads it can unblock', () => {
  const m = 5000;
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('root', 2 * m);
  let releaseToken = null;
  for (let i = 0; i < m; i += 1) {
    registry.declareResource(`r${i}`, 2, 'root');
    const first = registry.acquire(`r${i}`, 'a', 100);
    registry.acquire(`r${i}`, 'b', 100); // each resource packed at 2/2
    if (i === 0) {
      releaseToken = first;
    }
  }
  const tickets = [];
  for (let i = 0; i < m; i += 1) {
    tickets.push(registry.acquire(`r${i}`, `w${i}`, 100, 100000));
  }

  clock.set(10);
  const start = process.hrtime.bigint();
  registry.release(releaseToken.token);
  const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
  assert.ok(elapsedMs < 300, `wake planning scanned too much: ${elapsedMs}ms`);

  // Exactly the earliest head (r0) wakes; nobody else had local room.
  assert.equal(tickets[0].status, 'granted');
  for (let i = 1; i < m; i += 1) {
    assert.equal(tickets[i].status, 'waiting');
  }
});
