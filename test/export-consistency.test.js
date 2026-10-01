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
function logPath(name) {
  return path.join(dir, `${name}.log`);
}

test.beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-export-'));
});

test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

test('exportState on an empty registry returns the fixed shape', () => {
  const clock = fakeClock(7);
  const registry = createRegistry({ ttlMs: 100, clock });

  const state = registry.exportState();

  assert.deepEqual(state, {
    at: 7,
    stats: { granted: 0, renewed: 0, released: 0, reclaimed: 0, live: 0 },
    quotas: [],
    resources: [],
    leases: [],
    waits: [],
  });
});

test('exportState rejects a non-finite now', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock() });
  for (const bad of [Number.NaN, Number.POSITIVE_INFINITY, -Number.POSITIVE_INFINITY, '10']) {
    assert.throws(() => registry.exportState(bad), TypeError);
    assert.throws(() => registry.checkConsistency(bad), TypeError);
  }
});

test('exportState reports leases, including expired-but-unswept ones', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const keep = registry.acquire('keep', 'h1', 100);
  const gone = registry.acquire('gone', 'h2', 10);
  clock.set(20);

  const state = registry.exportState();

  assert.equal(state.at, 20);
  assert.deepEqual(state.stats, {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
  assert.equal(state.leases.length, 2);
  const byToken = new Map(state.leases.map((lease) => [lease.token, lease]));
  assert.deepEqual(byToken.get(keep.token), {
    token: keep.token,
    resource: 'keep',
    holder: 'h1',
    expiresAt: 100,
    ttlMs: 100,
    epoch: 1,
    live: true,
    legacy: false,
  });
  assert.deepEqual(byToken.get(gone.token), {
    token: gone.token,
    resource: 'gone',
    holder: 'h2',
    expiresAt: 10,
    ttlMs: 10,
    epoch: 1,
    live: false,
    legacy: false,
  });
  // The export is read-only: the expired credential is still there and a
  // sweep still reclaims it afterwards.
  assert.deepEqual(registry.sweep(), ['gone']);
});

test('exportState judges liveness by the passed now, not the clock', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'h', 50);

  const future = registry.exportState(60);
  assert.equal(future.at, 60);
  assert.equal(future.leases[0].live, false);
  assert.equal(future.stats.live, 0);
  assert.equal(future.resources[0].used, 0);
  assert.equal(future.resources[0].holder, null);

  const present = registry.exportState();
  assert.equal(present.leases[0].live, true);
  assert.equal(present.stats.live, 1);
  assert.equal(present.resources[0].used, 1);
  assert.equal(present.resources[0].holder, 'h');
  assert.equal(registry.holder('r'), 'h');
  assert.equal(lease.token, present.leases[0].token);
});

test('exportState presents undeclared resources as single-capacity', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('pool', 4);
  registry.acquire('plain', 'h');

  const state = registry.exportState();
  const byName = new Map(state.resources.map((r) => [r.resource, r]));

  assert.deepEqual(byName.get('plain'), {
    resource: 'plain',
    declared: false,
    capacity: 1,
    quota: null,
    used: 1,
    epoch: 1,
    holder: 'h',
  });
  assert.deepEqual(byName.get('pool'), {
    resource: 'pool',
    declared: true,
    capacity: 4,
    quota: null,
    used: 0,
    epoch: 0,
    holder: null,
  });
});

test('exportState keeps resources with only a historical epoch or a wait', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('retaken', 'h', 10);
  registry.release(lease.token);
  registry.acquire('blocked', 'a', 100);
  registry.acquire('blocked', 'b', 50, 1000);

  const state = registry.exportState();
  const names = state.resources.map((r) => r.resource);

  assert.deepEqual(names, ['blocked', 'retaken']);
  const retaken = state.resources[1];
  assert.equal(retaken.used, 0);
  assert.equal(retaken.holder, null);
  assert.equal(retaken.epoch, 1);
});

test('exportState reports quotas and hierarchical usage', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('root', 6);
  registry.setQuota('east', 3, 'root');
  registry.declareResource('zone/a', 10, 'east');
  registry.declareResource('zone/b', 10, 'root');
  registry.acquireAll([{ resource: 'zone/a', count: 2 }], 'h', 50);
  registry.acquire('zone/b', 'h', 50);

  const state = registry.exportState();

  assert.deepEqual(state.quotas, [
    { id: 'east', parentId: 'root', limit: 3, used: 2 },
    { id: 'root', parentId: null, limit: 6, used: 3 },
  ]);
  const byName = new Map(state.resources.map((r) => [r.resource, r]));
  assert.equal(byName.get('zone/a').quota, 'east');
  assert.equal(byName.get('zone/a').used, 2);
  assert.equal(byName.get('zone/b').quota, 'root');
  assert.equal(byName.get('zone/b').used, 1);
});

test('exportState lists queued waits in order and flags timed-out ones', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('a', 'x', 100);
  registry.acquire('b', 'x', 100);
  const first = registry.acquireAll(['a', { resource: 'b', count: 1 }], 'g', 50, 500);
  const second = registry.acquire('a', 's', 50, 40);

  clock.set(50);
  const state = registry.exportState();

  assert.equal(state.waits.length, 2);
  const byId = new Map(state.waits.map((w) => [w.waitId, w]));
  assert.deepEqual(byId.get(first.waitId), {
    waitId: first.waitId,
    holder: 'g',
    resources: [
      { resource: 'a', count: 1 },
      { resource: 'b', count: 1 },
    ],
    deadlineAt: 500,
    status: 'waiting',
  });
  assert.deepEqual(byId.get(second.waitId), {
    waitId: second.waitId,
    holder: 's',
    resources: [{ resource: 'a', count: 1 }],
    deadlineAt: 40,
    status: 'timedOut',
  });
  // Read-only: the timed-out request is still queued, not purged.
  assert.equal(registry.exportState().waits.length, 2);
  assert.equal(first.status, 'waiting');
  assert.equal(second.status, 'waiting');
});

test('exportState does not wake waiters, reclaim, or move any counter', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 10);
  const ticket = registry.acquire('r', 'b', 50, 1000);
  clock.set(20);

  const before = registry.exportState();
  registry.exportState();
  registry.checkConsistency();
  const after = registry.exportState();

  assert.deepEqual(after, before);
  assert.equal(ticket.status, 'waiting');
  assert.equal(registry.exportState().leases.length, 1);
  // The expired lease was never reclaimed by the exports.
  assert.deepEqual(registry.sweep(), ['r']);
  assert.equal(registry.release(lease.token), false);
});

test('exportState sorts every array by its primary key', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('q-b', 5);
  registry.setQuota('q-a', 5);
  registry.declareResource('r-b', 2);
  registry.declareResource('r-a', 2);
  registry.acquire('r-a', 'h', 100);
  registry.acquire('r-b', 'h', 100);
  registry.acquire('r-a', 'w', 50, 1000);
  registry.acquire('r-b', 'w', 50, 1000);

  const state = registry.exportState();

  assert.deepEqual(state.quotas.map((q) => q.id), ['q-a', 'q-b']);
  assert.deepEqual(state.resources.map((r) => r.resource), ['r-a', 'r-b']);
  const tokens = state.leases.map((l) => l.token);
  assert.deepEqual(tokens, [...tokens].sort());
  const waitIds = state.waits.map((w) => w.waitId);
  assert.deepEqual(waitIds, [...waitIds].sort());
});

test('exportState shares no mutable structure with the registry', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('q', 5);
  registry.declareResource('pool', 1, 'q');
  registry.acquire('pool', 'h', 100);
  registry.acquire('pool', 'w', 50, 1000);

  const state = registry.exportState();
  state.stats.granted = 999;
  state.quotas[0].used = 999;
  state.resources[0].capacity = 999;
  state.leases[0].holder = 'tampered';
  state.waits[0].resources.push({ resource: 'x', count: 1 });
  state.waits[0].status = 'timedOut';

  const fresh = registry.exportState();
  assert.equal(fresh.stats.granted, 1);
  assert.equal(fresh.quotas[0].used, 1);
  assert.equal(fresh.resources[0].capacity, 1);
  assert.equal(fresh.leases[0].holder, 'h');
  assert.equal(fresh.waits[0].resources.length, 1);
  assert.equal(fresh.waits[0].status, 'waiting');
  assert.equal(registry.stats().granted, 1);
});

test('exportState marks recovered credentials as legacy', () => {
  const clock = fakeClock(0);
  const p = logPath('legacy');
  const first = createRegistry({ ttlMs: 100, clock, logPath: p });
  first.acquire('r', 'h', 100);
  first.setQuota('q', 3);
  first.declareResource('pool', 2, 'q');
  first.acquire('pool', 'h', 100);

  const second = createRegistry({ ttlMs: 100, clock, logPath: p });
  const state = second.exportState();

  assert.equal(state.leases.length, 2);
  assert.ok(state.leases.every((lease) => lease.legacy));
  assert.deepEqual(state.stats, {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
  assert.deepEqual(state.quotas, [{ id: 'q', parentId: null, limit: 3, used: 1 }]);
  const pool = state.resources.find((r) => r.resource === 'pool');
  assert.equal(pool.declared, true);
  assert.equal(pool.quota, 'q');
  assert.equal(pool.used, 1);
});

test('checkConsistency reports ok with per-area counts on a live registry', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('root', 6);
  registry.setQuota('east', 3, 'root');
  registry.declareResource('zone/a', 4, 'east');
  registry.declareResource('zone/b', 1);
  registry.acquireAll([{ resource: 'zone/a', count: 2 }], 'h1', 50);
  const short = registry.acquire('zone/b', 'h2', 10);
  registry.acquire('zone/b', 'h3', 50, 1000);
  registry.acquire('plain', 'h4', 100);
  const moved = registry.acquire('handover', 'h5', 100);
  registry.transfer(moved.token, 'h6');
  clock.set(20);
  registry.renew(short.token);
  registry.exportState();

  const result = registry.checkConsistency();

  assert.equal(result.ok, true);
  assert.deepEqual(result.issues, []);
  assert.deepEqual(result.checked, {
    leases: 5,
    resources: 4,
    quotas: 2,
    waits: 1,
    epochs: 4,
  });
});

test('checkConsistency is read-only and stays ok after sweeps and restarts', () => {
  const clock = fakeClock(0);
  const p = logPath('check');
  const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  registry.setQuota('q', 4);
  registry.declareResource('pool', 3, 'q');
  registry.acquireAll([{ resource: 'pool', count: 2 }], 'h1', 10);
  const keep = registry.acquire('pool', 'h2', 100);
  registry.acquire('other', 'h3', 10);
  registry.acquire('other', 'w', 50, 1000);
  clock.set(20);

  const before = registry.exportState();
  const check = registry.checkConsistency();
  assert.equal(check.ok, true);
  assert.deepEqual(registry.exportState(), before);

  registry.sweep();
  assert.equal(registry.checkConsistency().ok, true);
  registry.release(keep.token);
  assert.equal(registry.checkConsistency().ok, true);
  registry.compact();
  assert.equal(registry.checkConsistency().ok, true);

  const recovered = createRegistry({ ttlMs: 100, clock, logPath: p });
  const again = recovered.checkConsistency();
  assert.equal(again.ok, true);
  assert.deepEqual(again.issues, []);
  // Only the woken waiter's credential on 'other' survived the sweep, the
  // release and the compaction.
  assert.equal(again.checked.leases, 1);
  assert.equal(again.checked.quotas, 1);
  assert.equal(again.checked.resources, 2);
  assert.equal(again.checked.epochs, 2);
});

test('checkConsistency on an empty registry is ok with zero counts', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock() });
  assert.deepEqual(registry.checkConsistency(), {
    ok: true,
    checked: { leases: 0, resources: 0, quotas: 0, waits: 0, epochs: 0 },
    issues: [],
  });
});

test('checkConsistency stays ok while expired shares wait for a sweep', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.setQuota('q', 2);
  registry.declareResource('pool', 2, 'q');
  registry.acquireAll([{ resource: 'pool', count: 2 }], 'h', 10);
  clock.set(20);
  // Force the lazy owner bookkeeping to observe the reading.
  assert.equal(registry.stats().live, 0);

  const result = registry.checkConsistency();
  assert.equal(result.ok, true);
  assert.deepEqual(result.issues, []);
  assert.equal(result.checked.leases, 2);

  // A backwards reading revives the shares; the books must still balance.
  clock.set(5);
  assert.equal(registry.checkConsistency().ok, true);
  assert.equal(registry.exportState().stats.live, 2);
});
