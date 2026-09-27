import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

import { createRegistry, LogFileError } from '../src/index.js';

const REGISTRY_SRC = path.resolve('src/index.js');

// Run one registry operation in a fresh OS process, so its per-process
// registry identity restarts exactly as in a real crash recovery.
function childRun(mode, p) {
  const body = `
    import { createRegistry } from ${JSON.stringify(`file://${REGISTRY_SRC}`)};
    const [mode, p] = process.argv.slice(1);
    const r = createRegistry({ ttlMs: 100, clock: () => 0, logPath: p });
    if (mode === 'setup') {
      const tokens = [r.acquire('a', 'h').token];
      const dead = r.acquire('freed', 'h');
      tokens.push(dead.token);
      r.release(dead.token);
      for (const l of r.acquireAll(['x', 'x'], 'h', 10)) tokens.push(l.token);
      r.compact();
      process.stdout.write(JSON.stringify(tokens));
    } else {
      process.stdout.write(r.acquire('new', 'h').token);
    }
  `;
  return execFileSync(
    process.execPath,
    ['--input-type=module', '-e', body, mode, p],
    { encoding: 'utf8' },
  );
}

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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-snap-'));
});
test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

function snapshotOf(p) {
  return `${p}.snapshot`;
}

// Build a history that exercises every counter and leaves a mix of live,
// renewed and expired-but-unswept credentials behind.
function buildHistory(p, clock) {
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const keep = r.acquire('keep', 'h1', 100);
  const renewed = r.acquire('renewed', 'h2', 40);
  const freed = r.acquire('freed', 'h3', 100);
  r.acquire('expired', 'h4', 10);
  const group = r.acquireAll(['g1', 'g2'], 'h5', [30, 80]);
  clock.set(30);
  r.renew(renewed.token);
  r.release(freed.token);
  clock.set(50);
  return { r, keep, renewed, freed, group };
}

const EXPECTED_STATS = {
  // At t=50: keep(100), renewed(->130), g2(80) live; expired(10) and
  // g1(30) are expired but unswept; freed was released.
  granted: 6, renewed: 1, released: 1, reclaimed: 0, live: 3,
};

test('compaction publishes a snapshot and a marker, then folds the history', () => {
  const p = logPath('basic');
  const c = fakeClock(0);
  const { r } = buildHistory(p, c);
  assert.deepEqual(r.stats(), EXPECTED_STATS);

  const result = r.compact();
  assert.equal(result.leases, 5);
  assert.equal(fs.existsSync(snapshotOf(p)), true);
  const logText = fs.readFileSync(p, 'utf8');
  assert.equal(logText.trim().split('\n').length, 1);
  const marker = JSON.parse(logText);
  assert.equal(marker.type, 'snapshot-start');
  assert.equal(marker.seq, result.seq);

  // Restart from the snapshot alone: ownership and counters come back item
  // by item, with no extra counts from the rebuild itself.
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r2.holder('keep'), 'h1');
  assert.equal(r2.holder('renewed'), 'h2');
  assert.equal(r2.holder('freed'), null);
  assert.equal(r2.holder('expired'), null);
  assert.equal(r2.holder('g1'), null); // expired at 30, unswept
  assert.equal(r2.holder('g2'), 'h5');
  assert.deepEqual(r2.stats(), EXPECTED_STATS);

  // renewed carries its own ttl of 40: renewed at t=30 moves it from 40 to
  // 70. g1 expires 30, g2 80, keep 100.
  assert.deepEqual(r2.sweep(50), ['expired', 'g1']);
  assert.deepEqual(r2.sweep(69), []);
  assert.deepEqual(r2.sweep(70), ['renewed']);
  assert.deepEqual(r2.sweep(79), []);
  assert.deepEqual(r2.sweep(80), ['g2']);
  assert.deepEqual(r2.sweep(99), []);
  assert.deepEqual(r2.sweep(100), ['keep']);
});

test('credentials captured by the snapshot are void after the restart', () => {
  const p = logPath('void');
  const c = fakeClock(0);
  const { keep, renewed, group } = buildHistory(p, c);
  c.set(50);
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  r1.compact();

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  for (const token of [keep.token, renewed.token, ...group.map((l) => l.token)]) {
    assert.equal(r2.renew(token), false);
    assert.equal(r2.release(token), false);
  }
  // The leases still occupy their resources until they expire.
  assert.equal(r2.holder('keep'), 'h1');
  assert.deepEqual(r2.stats(), EXPECTED_STATS);
});

test('incremental events appended after the snapshot are replayed on top of it', () => {
  const p = logPath('increment');
  const c = fakeClock(0);
  const { r } = buildHistory(p, c);
  c.set(50);
  r.compact();

  // Post-compact work rides the same log, right after the marker.
  const extra = r.acquire('after', 'h6', 100);
  c.set(60);
  assert.equal(r.release(extra.token), true);
  assert.deepEqual(r.sweep(80), ['expired', 'g1', 'renewed', 'g2']);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(80), logPath: p });
  assert.equal(r2.holder('after'), null);
  assert.equal(r2.holder('keep'), 'h1');
  assert.deepEqual(r2.stats(), {
    granted: 7, renewed: 1, released: 2, reclaimed: 4, live: 1,
  });
  assert.equal(r2.renew(extra.token), false);
});

test('repeated compactions keep state identical across restarts', () => {
  const p = logPath('twice');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 100);
  c.set(10);
  r.compact();
  const b = r.acquire('b', 'h', 100);
  c.set(20);
  r.compact();
  c.set(30);
  r.renew(a.token);
  r.compact();

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(30), logPath: p });
  assert.equal(r2.holder('a'), 'h');
  assert.equal(r2.holder('b'), 'h');
  assert.deepEqual(r2.stats(), {
    granted: 2, renewed: 1, released: 0, reclaimed: 0, live: 2,
  });
  for (const token of [a.token, b.token]) {
    assert.equal(r2.renew(token), false);
  }
  // b was acquired at 10 + 100 -> 110; a was renewed at 30 for 100 -> 130.
  assert.deepEqual(r2.sweep(110), ['b']);
  assert.deepEqual(r2.sweep(130), ['a']);
});

test('a compacted restart matches a plain full-log restart item by item', () => {
  const p1 = logPath('full');
  const p2 = logPath('compact');
  const c1 = fakeClock(0);
  const c2 = fakeClock(0);
  const h1 = buildHistory(p1, c1);
  c1.set(50);
  h1.r.sweep(80);
  const h2 = buildHistory(p2, c2);
  c2.set(50);
  h2.r.compact();
  h2.r.sweep(80);

  const full = createRegistry({ ttlMs: 100, clock: fakeClock(80), logPath: p1 });
  const folded = createRegistry({ ttlMs: 100, clock: fakeClock(80), logPath: p2 });
  assert.deepEqual(folded.stats(), full.stats());
  for (const resource of ['keep', 'renewed', 'freed', 'expired', 'g1', 'g2']) {
    assert.equal(folded.holder(resource), full.holder(resource), resource);
  }
  // And the next sweep reclaims in the same order.
  assert.deepEqual(folded.sweep(200), full.sweep(200));
});

test('failure while publishing the snapshot leaves the original log usable', () => {
  const p = logPath('snap-fail');
  const c = fakeClock(0);
  const { r } = buildHistory(p, c);
  c.set(50);

  // A directory at the snapshot target makes the snapshot rename fail; the
  // log rename never runs, so the original log must be byte-intact.
  fs.mkdirSync(snapshotOf(p));
  assert.throws(
    () => r.compact(),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
  fs.rmdirSync(snapshotOf(p));
  assert.equal(fs.existsSync(snapshotOf(p)), false);

  // The registry never stopped working...
  assert.equal(r.holder('keep'), 'h1');
  // ...and a fresh process replays the complete original log.
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.deepEqual(r2.stats(), EXPECTED_STATS);
});

test('a committed snapshot next to the unswapped full log still folds exactly once', () => {
  // Simulate a crash between the snapshot rename and the log swap: snapshot
  // is on disk, but the log still contains the whole folded history.
  const p = logPath('swap-crash');
  const c = fakeClock(0);
  const { r } = buildHistory(p, c);
  c.set(50);
  const fullLog = fs.readFileSync(p, 'utf8');
  r.compact();
  fs.writeFileSync(p, fullLog);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r2.holder('keep'), 'h1');
  assert.equal(r2.holder('g2'), 'h5');
  assert.deepEqual(r2.stats(), EXPECTED_STATS);

  // The log stays usable for more events; nothing is applied twice.
  r2.acquire('more', 'h9', 100);
  const r3 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.deepEqual(r3.stats(), {
    granted: 7, renewed: 1, released: 1, reclaimed: 0, live: 4,
  });
});

test('corrupt snapshot throws LogFileError and the original log still recovers everything', () => {
  const p = logPath('snap-corrupt');
  const c = fakeClock(0);
  const { r } = buildHistory(p, c);
  c.set(50);
  const fullLog = fs.readFileSync(p, 'utf8');
  r.compact();
  fs.writeFileSync(p, fullLog);

  fs.writeFileSync(snapshotOf(p), '{not json\n');
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  const badVersion = { v: 99, type: 'snapshot', counters: {}, leases: [], seq: 0 };
  fs.writeFileSync(snapshotOf(p), `${JSON.stringify(badVersion)}\n`);
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  // The log was never modified: remove the broken image and recover fully.
  fs.rmSync(snapshotOf(p));
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.deepEqual(r2.stats(), EXPECTED_STATS);
  assert.equal(r2.holder('renewed'), 'h2');
});

test('a snapshot with missing content starts a brand new log', () => {
  const p = logPath('snap-empty');
  const c = fakeClock(0);
  const { r } = buildHistory(p, c);
  r.compact();
  // Image present but emptied out, as if the snapshot write never finished.
  fs.writeFileSync(snapshotOf(p), '');

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.deepEqual(r2.stats(), {
    granted: 0, renewed: 0, released: 0, reclaimed: 0, live: 0,
  });
  assert.equal(r2.holder('keep'), null);
  // Fresh history accumulates normally.
  r2.acquire('new', 'h');
  const r3 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r3.holder('new'), 'h');
  assert.equal(r3.stats().granted, 1);
});

test('compact without a logPath throws LogFileError', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  r.acquire('r', 'h');
  assert.throws(
    () => r.compact(),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
});

test('a torn half-line after complete entries is ignored and trimmed', () => {
  const p = logPath('halftail');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.acquire('one', 'h1', 100);
  r.acquire('two', 'h2', 100);

  // Simulate an append that died mid-line: no terminating newline.
  fs.appendFileSync(
    p,
    JSON.stringify({ v: 1, type: 'acquire', at: 0, token: 'half', resource: 'three', holder: 'h3', ttlMs: 100, expiresAt: 100 }),
  );
  assert.ok(fs.readFileSync(p, 'utf8').endsWith('expiresAt":100}'));

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(r2.holder('one'), 'h1');
  assert.equal(r2.holder('two'), 'h2');
  assert.equal(r2.holder('three'), null); // the half line never happened
  assert.equal(r2.stats().granted, 2);

  // The fragment was trimmed, so the next complete line starts clean.
  assert.ok(fs.readFileSync(p, 'utf8').endsWith('\n'));
  r2.acquire('four', 'h4', 100);
  const r3 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(r3.holder('three'), null);
  assert.equal(r3.holder('four'), 'h4');
  assert.equal(r3.stats().granted, 3);
});

test('a lone half-line with no complete entry is treated as an empty log', () => {
  const p = logPath('onlyhalf');
  fs.writeFileSync(p, '{broken');
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.deepEqual(r.stats(), {
    granted: 0, renewed: 0, released: 0, reclaimed: 0, live: 0,
  });
  assert.equal(fs.readFileSync(p, 'utf8'), '');
});

test('a complete corrupt line still throws even when followed by a half-line', () => {
  const p = logPath('complete-then-half');
  fs.writeFileSync(p, '{"v":1,"type":"acquire","token":"t","ttlMs":10,"expiresAt":10}\n{not json\n{"half":');
  assert.throws(
    () => createRegistry({ ttlMs: 10, clock: fakeClock(0), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
});

test('a torn half-line after compaction is dropped on the next restart', () => {
  const p = logPath('snap-half');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.acquire('keep', 'h', 100);
  r.compact();
  fs.appendFileSync(p, '{"v":1,"type":"acquire","half"');

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(r2.holder('keep'), 'h');
  assert.equal(r2.stats().granted, 1);
});

test('each credential of a repeated-resource group survives compaction independently', () => {
  const p = logPath('repeat');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const leases = r.acquireAll(['x', 'x'], 'h', [40, 60]);
  r.compact();

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.deepEqual(r2.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
  assert.equal(r2.holder('x'), 'h');
  assert.deepEqual(r2.sweep(50), ['x']); // first credential expires alone
  assert.equal(r2.holder('x'), 'h');     // the twin still holds it
  assert.deepEqual(r2.sweep(60), ['x']);
  assert.equal(r2.holder('x'), null);
  assert.equal(r2.stats().reclaimed, 2);
  void leases;
});

test('queued requests do not survive a compacted restart', () => {
  const p = logPath('waiters');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.acquire('r', 'a', 100);
  const ticket = r.acquire('r', 'b', 50, 1000);
  r.compact();

  // After the restart the old queue is empty: reclaiming the resource wakes
  // nobody and hands it to no one.
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(100), logPath: p });
  assert.deepEqual(r2.sweep(), ['r']);
  assert.equal(ticket.status, 'waiting');
  assert.equal(r2.holder('r'), null);

  // Fresh queueing works exactly as on an empty registry.
  const fresh = r2.acquire('r', 'd', 50);
  assert.equal(fresh.holder, 'd');
  const next = r2.acquire('r', 'c', 50, 1000);
  assert.equal(next.status, 'waiting');
  r2.release(fresh.token);
  assert.equal(next.status, 'granted');
});

test('fresh OS processes never mint a folded credential over again', () => {
  const p = logPath('proc-restart');
  const before = JSON.parse(childRun('setup', p));
  // A genuinely new process (new pid, reset registry sequence) mints after
  // the compacted high-water mark and cannot collide with a folded token,
  // including the released credential absent from the snapshot.
  const after = childRun('mint', p);
  assert.ok(!before.includes(after), `${after} reused a folded token`);
});

test('equal-expiry reclaim order, including renewals, survives compaction', () => {
  const p1 = logPath('tie-full');
  const p2 = logPath('tie-compact');
  // Same history in both logs; only the second is compacted. Renewals push
  // fresh expiry-heap nodes and retire old ones, which is exactly the tie
  // ordering the snapshot must preserve.
  function seed(p) {
    const c = fakeClock(0);
    const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
    r.acquire('u', 'h', 100);
    const v = r.acquire('v', 'h', 20);
    const w = r.acquire('w', 'h', 30);
    c.set(80);
    r.renew(v.token); // v -> 100 with a later heap node, tying u
    c.set(90);
    r.renew(w.token); // w -> 120
    return r;
  }

  seed(p1);
  const seeded = seed(p2);
  seeded.compact();

  const full = createRegistry({ ttlMs: 100, clock: fakeClock(90), logPath: p1 });
  const folded = createRegistry({ ttlMs: 100, clock: fakeClock(90), logPath: p2 });

  // u and v both expire at 100; their order is fixed by heap insertion and
  // must be identical after compaction.
  assert.deepEqual(folded.sweep(100), full.sweep(100));
  assert.deepEqual(folded.sweep(120), full.sweep(120));
});

test('the snapshot alone recovers state when the log file is absent', () => {
  const p = logPath('snap-only');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.acquire('keep', 'h1', 100);
  r.acquire('gone', 'h2', 10);
  c.set(40);
  r.compact();
  fs.rmSync(p);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(40), logPath: p });
  assert.equal(r2.holder('keep'), 'h1');
  assert.equal(r2.holder('gone'), null);
  assert.deepEqual(r2.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
  // New events append cleanly to the fresh log and survive another restart.
  r2.acquire('extra', 'h3', 100);
  const r3 = createRegistry({ ttlMs: 100, clock: fakeClock(40), logPath: p });
  assert.equal(r3.holder('extra'), 'h3');
  assert.equal(r3.stats().granted, 3);
});
