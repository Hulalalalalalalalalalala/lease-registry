import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { createRegistry, LogFileError } from '../src/index.js';

function fakeClock(start = 0) {
  let now = start;
  const clock = () => now;
  clock.set = (value) => {
    now = value;
  };
  return clock;
}

let dir;
test.beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-share-persist-'));
});
test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});
function logPath(name) {
  return path.join(dir, `${name}.log`);
}
const CONFIG = {
  quotas: { root: 6, mid: { limit: 3, parent: 'root' } },
  capacities: {
    r1: { capacity: 10, quota: 'mid' },
    r2: { capacity: 10, quota: 'root' },
    c: 3,
  },
};
function open(p, clock) {
  return createRegistry({ ttlMs: 100, clock, logPath: p, ...CONFIG });
}

test('replay restores share ownership, per-share expiries, quota usage and counters', () => {
  const p = logPath('replay');
  const clock = fakeClock(0);
  let r = open(p, clock);
  const keep = r.acquire({ resource: 'r1', shares: 2 }, 'h1', 80);
  const short = r.acquire('c', 'h2', 100);
  r.acquire({ resource: 'r2', shares: 2 }, 'h3', 90);
  clock.set(10);
  r.renew(short.token); // ttl 100 from t=10 -> c lives until 110
  clock.set(30);
  assert.deepEqual(r.stats(), {
    granted: 5, renewed: 1, released: 0, reclaimed: 0, live: 5,
  });

  r = open(p, fakeClock(30));
  assert.deepEqual(r.stats(), {
    granted: 5, renewed: 1, released: 0, reclaimed: 0, live: 5,
  });
  assert.equal(r.holder('r1'), 'h1');
  assert.equal(r.holder('r2'), 'h3');
  assert.equal(r.holder('c'), 'h2');
  assert.equal(r.quotaUsage('mid'), 2);
  assert.equal(r.quotaUsage('root'), 4);

  // Pre-restart credentials are void.
  assert.equal(r.renew(keep[0].token), false);
  assert.equal(r.release(keep[1].token), false);
  assert.equal(r.renew(short.token), false);

  // Per-share reclaim order is identical: r1 shares expire 80, r2 at 90,
  // the renewed c share at 110.
  assert.deepEqual(r.sweep(79), []);
  assert.deepEqual(r.sweep(80), ['r1', 'r1']);
  assert.equal(r.quotaUsage('mid'), 0);
  assert.equal(r.quotaUsage('root'), 2);
  assert.deepEqual(r.sweep(90), ['r2', 'r2']);
  assert.equal(r.quotaUsage('root'), 0);
  assert.deepEqual(r.sweep(110), ['c']);
  assert.equal(r.holder('c'), null);
});

test('quota enforcement survives a restart', () => {
  const p = logPath('enforce');
  let r = open(p, fakeClock(0));
  r.acquire({ resource: 'r1', shares: 3 }, 'h'); // mid at its limit 3

  r = open(p, fakeClock(0));
  assert.throws(
    () => r.acquire('r1', 'g'),
    (e) => e.code === 'QUOTA_EXCEEDED' && e.quota === 'mid',
  );
  // A root-only resource with room is still grantable (root 3/6).
  const granted = r.acquire({ resource: 'r2', shares: 2 }, 'g');
  assert.equal(granted.length, 2);
  assert.equal(r.quotaUsage('root'), 5);
});

test('runtime declarations replay from the log without constructor config', () => {
  const p = logPath('runtime-decl');
  let r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  r.declareQuota('root', 5);
  r.declareQuota('mid', 3, 'root');
  r.declareResource('c', 4, 'mid');
  r.acquire({ resource: 'c', shares: 2 }, 'h');

  // Reopen with no capacities/quotas options at all.
  r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(r.capacity('c'), 4);
  assert.equal(r.quotaUsage('mid'), 2);
  assert.throws(
    () => r.acquire({ resource: 'c', shares: 2 }, 'g'),
    (e) => e.code === 'QUOTA_EXCEEDED' && e.quota === 'mid',
  );
});

test('compaction preserves shares, per-share expiries and per-level quota usage', () => {
  const p = logPath('compact');
  const clock = fakeClock(0);
  const r = open(p, clock);
  const a = r.acquire({ resource: 'r1', shares: 2 }, 'h1', 60);
  r.acquire('c', 'h2', 40);
  const gone = r.acquire({ resource: 'r2', shares: 2 }, 'h3', 50);
  clock.set(20);
  r.release(gone[0].token);
  r.release(gone[1].token);
  const before = { stats: r.stats(), mid: r.quotaUsage('mid'), root: r.quotaUsage('root') };
  r.compact();
  void a;

  const r2 = open(p, fakeClock(20));
  assert.deepEqual(r2.stats(), before.stats);
  assert.equal(r2.quotaUsage('mid'), before.mid);
  assert.equal(r2.quotaUsage('root'), before.root);
  assert.equal(r2.holder('r1'), 'h1');
  assert.equal(r2.holder('c'), 'h2');
  assert.equal(r2.holder('r2'), null);

  // Per-share reclaim order is preserved across compaction: c expires 40,
  // the two r1 shares at 60.
  assert.deepEqual(r2.sweep(40), ['c']);
  assert.deepEqual(r2.sweep(60), ['r1', 'r1']);
  assert.equal(r2.quotaUsage('mid'), 0);
  assert.equal(r2.quotaUsage('root'), 0);
});

test('a compacted restart matches a full-log restart item by item', () => {
  const p1 = logPath('full');
  const p2 = logPath('folded');
  const shared = {
    quotas: { q: 4 },
    capacities: {
      a: { capacity: 4, quota: 'q' },
      b: { capacity: 4, quota: 'q' },
    },
  };
  function build(logP) {
    const c = fakeClock(0);
    const r = createRegistry({ ttlMs: 100, clock: c, logPath: logP, ...shared });
    r.acquire({ resource: 'a', shares: 2 }, 'h', 60);
    r.acquire('b', 'h', 30);
    c.set(40);
    r.sweep(); // the b share expires at 30
    return r;
  }
  build(p1);
  const folded = build(p2);
  folded.compact();

  const full = createRegistry({ ttlMs: 100, clock: fakeClock(40), logPath: p1, ...shared });
  const compacted = createRegistry({ ttlMs: 100, clock: fakeClock(40), logPath: p2, ...shared });
  assert.deepEqual(compacted.stats(), full.stats());
  assert.equal(compacted.quotaUsage('q'), full.quotaUsage('q'));
  assert.equal(compacted.holder('a'), full.holder('a'));
  assert.equal(compacted.holder('b'), full.holder('b'));
  assert.deepEqual(compacted.sweep(200), full.sweep(200));
});

test('a snapshot whose usage disagrees with its leases is rejected', () => {
  const p = logPath('usage-corrupt');
  const r = open(p, fakeClock(0));
  r.acquire({ resource: 'r1', shares: 2 }, 'h', 100);
  r.compact();
  const snapshotPath = `${p}.snapshot`;
  const image = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));
  image.usage.mid = 99;
  fs.writeFileSync(snapshotPath, `${JSON.stringify(image)}\n`);

  assert.throws(
    () => open(p, fakeClock(0)),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
});

test('a bad snapshot still falls back to the original log for share state', () => {
  const p = logPath('fallback');
  const r = open(p, fakeClock(0));
  r.acquire({ resource: 'r1', shares: 2 }, 'h', 100);
  const fullLog = fs.readFileSync(p, 'utf8');
  r.compact();
  fs.writeFileSync(p, fullLog);
  fs.writeFileSync(`${p}.snapshot`, '{broken\n');
  assert.throws(() => open(p, fakeClock(0)), LogFileError);

  // Removing the bad image replays the original log completely.
  fs.rmSync(`${p}.snapshot`);
  const r2 = open(p, fakeClock(0));
  assert.equal(r2.holder('r1'), 'h');
  assert.equal(r2.quotaUsage('mid'), 2);
  assert.equal(r2.stats().granted, 2);
});

test('a failed multi-share grant write rolls every share back', () => {
  const p = logPath('rollback');
  const r = open(p, fakeClock(0));
  r.acquire('c', 'h');
  fs.renameSync(p, `${p}.bak`);
  fs.mkdirSync(p);
  try {
    assert.throws(
      () => r.acquire({ resource: 'c', shares: 2 }, 'g'),
      (e) => e instanceof LogFileError,
    );
  } finally {
    fs.rmdirSync(p);
    fs.renameSync(`${p}.bak`, p);
  }
  assert.equal(r.holder('c'), 'h');
  assert.equal(r.stats().granted, 1);
  // The registry keeps working: the two remaining shares grant normally.
  const granted = r.acquire({ resource: 'c', shares: 2 }, 'g');
  assert.equal(granted.length, 2);
  assert.equal(r.stats().granted, 3);
});

test('queued share requests do not survive a compacted restart', () => {
  const p = logPath('waiters-gone');
  const clock = fakeClock(0);
  const r = open(p, clock);
  const held = r.acquire({ resource: 'c', shares: 3 }, 'h', 100);
  const ticket = r.acquire({ resource: 'c', shares: 2 }, 'w', 50, 1000);
  r.compact();

  const r2 = open(p, fakeClock(0));
  r2.sweep(100);
  assert.equal(ticket.status, 'waiting'); // the old ticket object is not touched
  assert.equal(r2.holder('c'), null); // nobody was woken after restart

  // After a restart the resource is free, so a fresh application grants
  // directly rather than queueing.
  const next = r2.acquire({ resource: 'c', shares: 2 }, 'w2', 50);
  assert.equal(Array.isArray(next), true);
  assert.equal(next.length, 2);
  void held;
});
