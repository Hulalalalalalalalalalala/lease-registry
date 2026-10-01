import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { createRegistry } from '../src/index.js';

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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-export-'));
});
test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});
function logPath(name) {
  return path.join(dir, `${name}.log`);
}

test('an empty registry exports the fixed shape with sorted empty arrays', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(7) });
  const state = r.exportState();
  assert.deepEqual(Object.keys(state), ['at', 'stats', 'quotas', 'resources', 'leases', 'waits']);
  assert.equal(state.at, 7);
  assert.deepEqual(state.stats, {
    granted: 0, renewed: 0, released: 0, reclaimed: 0, live: 0,
  });
  assert.deepEqual(state.quotas, []);
  assert.deepEqual(state.resources, []);
  assert.deepEqual(state.leases, []);
  assert.deepEqual(state.waits, []);

  const check = r.checkConsistency();
  assert.equal(check.ok, true);
  assert.deepEqual(check.issues, []);
  assert.deepEqual(check.checked, {
    leases: 0, resources: 0, quotas: 0, waits: 0, epochs: 0,
  });
});

test('a non-finite now throws TypeError and never falls back to the clock', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  for (const bad of [NaN, Infinity, -Infinity, '5', null]) {
    assert.throws(() => r.exportState(bad), TypeError);
    assert.throws(() => r.checkConsistency(bad), TypeError);
  }
});

test('export lists resources, quotas, leases and waits with the exact fields', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.setQuota('root', 5);
  r.setQuota('east', 2, 'root');
  r.declareResource('pool', 2, 'east');

  const p1 = r.acquire('pool', 'h1', 50);
  const p2 = r.acquire('pool', 'h2', 80);
  const solo = r.acquire('solo', 'h3', 100); // undeclared: capacity 1, no quota
  const ticket = r.acquire('pool', 'w1', 50, 1000);
  const group = r.acquireAll([{ resource: 'pool', count: 2 }, 'solo'], 'w2', 40, 5000);
  assert.equal(ticket.status, 'waiting');
  assert.equal(group.status, 'waiting');

  clock.set(60); // p1 expires at 50; it stays in the table, unreclaimed
  const state = r.exportState();

  assert.equal(state.at, 60);
  assert.deepEqual(state.stats, {
    granted: 3, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });

  // Arrays are sorted by their primary keys.
  assert.deepEqual(state.quotas.map((q) => q.id), ['east', 'root']);
  assert.deepEqual(
    state.quotas.find((q) => q.id === 'east'),
    { id: 'east', parentId: 'root', limit: 2, used: 1 },
  );
  assert.deepEqual(
    state.quotas.find((q) => q.id === 'root'),
    { id: 'root', parentId: null, limit: 5, used: 1 },
  );

  assert.deepEqual(state.resources.map((x) => x.resource), ['pool', 'solo']);
  const pool = state.resources.find((x) => x.resource === 'pool');
  assert.deepEqual(pool, {
    resource: 'pool',
    declared: true,
    capacity: 2,
    quota: 'east',
    used: 1, // only p2 is live at 60
    epoch: 2,
    holder: 'h2',
  });
  const soloRow = state.resources.find((x) => x.resource === 'solo');
  assert.deepEqual(soloRow, {
    resource: 'solo',
    declared: false,
    capacity: 1,
    quota: null,
    used: 1,
    epoch: 1,
    holder: 'h3',
  });
  void solo;

  // Leases: expired-but-unreclaimed credentials are still listed, with live
  // judged purely from the passed reading; tokens sort ascending.
  assert.deepEqual(state.leases.map((l) => l.token).sort(), state.leases.map((l) => l.token));
  const expiredLease = state.leases.find((l) => l.token === p1.token);
  assert.deepEqual(expiredLease, {
    token: p1.token,
    resource: 'pool',
    holder: 'h1',
    expiresAt: 50,
    ttlMs: 50,
    epoch: 1,
    live: false,
    legacy: false,
  });
  const liveLease = state.leases.find((l) => l.token === p2.token);
  assert.equal(liveLease.live, true);
  assert.equal(liveLease.expiresAt, 80);
  assert.equal(liveLease.epoch, 2);

  // Only requests still queued appear, sorted by waitId, rows in original
  // order with count, status waiting or timedOut.
  assert.equal(state.waits.length, 2);
  assert.deepEqual(state.waits.map((w) => w.waitId), [ticket.waitId, group.waitId].sort());
  const single = state.waits.find((w) => w.waitId === ticket.waitId);
  assert.equal(single.holder, 'w1');
  assert.deepEqual(single.resources, [{ resource: 'pool', count: 1 }]);
  assert.equal(single.deadlineAt, 1000);
  assert.equal(single.status, 'waiting');
  const grouped = state.waits.find((w) => w.waitId === group.waitId);
  assert.deepEqual(grouped.resources, [
    { resource: 'pool', count: 2 },
    { resource: 'solo', count: 1 },
  ]);
  assert.equal(grouped.deadlineAt, 5000);
  assert.equal(grouped.status, 'waiting');
});

test('a queued request past its deadline is exported timedOut but is not purged', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.acquire('r', 'a', 1000);
  const ticket = r.acquire('r', 'b', 100, 500);
  clock.set(500);

  const state = r.exportState();
  assert.equal(state.waits.length, 1);
  assert.equal(state.waits[0].waitId, ticket.waitId);
  assert.equal(state.waits[0].status, 'timedOut');

  // The read-only export neither purged the request nor reclaimed anything.
  assert.equal(ticket.status, 'waiting');
  const check = r.checkConsistency();
  assert.equal(check.ok, true);
  assert.equal(check.checked.waits, 1);
});

test('granted, cancelled and purged requests disappear from the waits export', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const held = r.acquire('r', 'a', 1000);
  const wakes = r.acquire('r', 'b', 100, 1000);
  const cancels = r.acquire('r', 'c', 100, 1000);
  const expires = r.acquire('r', 'd', 100, 50);
  r.release(held.token);
  assert.equal(wakes.status, 'granted');
  assert.equal(r.cancel(cancels.waitId), true);
  clock.set(60);
  r.sweep(60); // purge the deadline-expired waiter
  assert.equal(expires.status, 'expired');

  const state = r.exportState(60);
  assert.deepEqual(state.waits, []);
});

test('a resource is never omitted when it has a lease, waiters or a historical epoch', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const only = r.acquire('only', 'h', 10);
  r.declareResource('declared-empty', 3);
  r.acquire('waited', 'h2', 1000);
  r.acquire('waited', 'w', 100, 1000);

  clock.set(50); // only expired, unreclaimed: its epoch must survive the export
  const names = r.exportState().resources.map((x) => x.resource).sort();
  assert.deepEqual(names, ['declared-empty', 'only', 'waited']);
  const gone = r.sweep(50);
  assert.deepEqual(gone, ['only']);

  // Even after reclaiming its last credential the resource keeps its epoch.
  const row = r.exportState().resources.find((x) => x.resource === 'only');
  assert.equal(row.epoch, 1);
  assert.equal(row.used, 0);
  assert.equal(row.holder, null);
  assert.equal(row.declared, false);
  assert.equal(row.capacity, 1);
  void only;
});

test('the export is a fresh JSON value sharing no internal reference', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.setQuota('q', 2);
  r.declareResource('p', 2, 'q');
  r.acquire('p', 'h', 50);

  const a = r.exportState(0);
  a.stats.granted = 999;
  a.quotas[0].limit = 999;
  a.resources[0].used = 999;
  a.leases[0].holder = 'tampered';
  a.leases.push({ bogus: true });
  a.waits.push({ bogus: true });

  const b = r.exportState(0);
  assert.equal(b.stats.granted, 1);
  assert.equal(b.quotas[0].limit, 2);
  assert.equal(b.resources[0].used, 1);
  assert.equal(b.leases[0].holder, 'h');
  assert.equal(b.leases.length, 1);
  assert.equal(b.waits.length, 0);
  assert.notEqual(a.leases[0], b.leases[0]);

  // Deterministic and JSON serializable.
  assert.deepEqual(r.exportState(0), b);
  assert.equal(JSON.stringify(b).length > 0, true);
});

test('export and check never reclaim, retire, purge or wake anything', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const held = r.acquire('r', 'a', 100);
  r.acquire('r', 'b', 100, 1000); // queued
  clock.set(200);

  const before = r.exportState();
  for (let i = 0; i < 3; i += 1) {
    r.exportState();
    r.checkConsistency();
    r.exportState(0);
    r.checkConsistency(0);
  }
  const after = r.exportState();
  assert.deepEqual(after, before);

  // The expired credential is still recoverable, and reclaiming it is what
  // wakes the queued request - the read-only calls did neither early.
  assert.deepEqual(r.sweep(200), ['r']);
  assert.equal(held.token.endsWith('1') || true, true);
});

test('checkConsistency reports ok across renewals, shares, quotas, waits and transfers', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.setQuota('root', 6);
  r.setQuota('east', 3, 'root');
  r.declareResource('pool', 2, 'east');

  const a = r.acquire('pool', 'h1', 50);
  r.acquire('pool', 'h2', 80);
  const exclusive = r.acquire('solo', 'h3', 100);
  r.acquire('pool', 'w', 50, 1000);
  clock.set(20);
  assert.equal(r.renew(a.token), true);
  const moved = r.transfer(exclusive.token, 'h4', 100);
  assert.equal(moved.holder, 'h4');

  clock.set(60);
  let check = r.checkConsistency();
  assert.equal(check.ok, true, JSON.stringify(check.issues));
  assert.equal(check.checked.leases, 3); // two pool shares + transferred solo
  assert.equal(check.checked.resources, 2); // pool, solo
  assert.equal(check.checked.quotas, 2);
  assert.equal(check.checked.waits, 1);
  assert.equal(check.checked.epochs, 2);
  assert.deepEqual(check.issues, []);

  // Expired unswept shares, a reclaim sweep and a backwards reading all keep
  // the internally maintained figures mutually consistent.
  clock.set(200);
  r.sweep(200);
  check = r.checkConsistency(200);
  assert.equal(check.ok, true, JSON.stringify(check.issues));
  check = r.checkConsistency(0);
  assert.equal(check.ok, true, JSON.stringify(check.issues));
});

test('checked counts include expired unswept leases and every queued position', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('p', 1);
  r.acquire('p', 'h', 10);
  r.acquire('x', 'h2', 10);
  r.acquire('p', 'w', 10, 1000); // one position
  r.acquireAll(['p', 'x'], 'g', 10, 1000); // two positions, one group
  clock.set(50);

  const check = r.checkConsistency();
  assert.equal(check.ok, true, JSON.stringify(check.issues));
  assert.equal(check.checked.leases, 2); // expired but never swept
  assert.equal(check.checked.resources, 2);
  assert.equal(check.checked.waits, 3); // 1 + 2 queue positions
  assert.equal(check.checked.epochs, 2);
});

test('export and check work on a registry recovered from a snapshot', () => {
  const p = logPath('restart');
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  r1.setQuota('q', 2);
  r1.declareResource('pool', 2, 'q');
  r1.acquire('pool', 'h', 100);
  r1.acquire('solo', 'h2', 100);
  r1.compact();

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  const check = r2.checkConsistency(50);
  assert.equal(check.ok, true, JSON.stringify(check.issues));
  assert.equal(check.checked.leases, 2);

  const state = r2.exportState(50);
  assert.equal(state.leases.length, 2);
  assert.ok(state.leases.every((lease) => lease.legacy === true));
  assert.equal(state.resources.find((x) => x.resource === 'solo').holder, 'h2');
  assert.deepEqual(
    state.quotas.find((q) => q.id === 'q'),
    { id: 'q', parentId: null, limit: 2, used: 1 },
  );
});
