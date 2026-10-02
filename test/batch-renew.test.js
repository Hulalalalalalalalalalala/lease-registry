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
  clock.advance = (delta) => {
    now += delta;
  };
  return clock;
}

let dir;
test.beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-renewall-'));
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

test('renewAll renews every lease from one reading using each own ttl', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h1', 100);
  const b = r.acquire('b', 'h2', 40);
  r.declareResource('p', 2);
  const pool = r.acquireAll(['p', 'p'], 'h3', [70, 90]);
  const before = new Map(
    r.exportState().leases.map((l) => [l.token, l]),
  );

  clock.set(30);
  assert.equal(r.renewAll([a.token, b.token, pool[0].token, pool[1].token]), true);

  const after = new Map(r.exportState(30).leases.map((l) => [l.token, l]));
  assert.equal(after.get(a.token).expiresAt, 130);
  assert.equal(after.get(b.token).expiresAt, 70);
  assert.equal(after.get(pool[0].token).expiresAt, 100);
  assert.equal(after.get(pool[1].token).expiresAt, 120);
  for (const [token, l] of after) {
    assert.equal(l.holder, before.get(token).holder);
    assert.equal(l.ttlMs, before.get(token).ttlMs);
    assert.equal(l.epoch, before.get(token).epoch);
    assert.equal(l.token, token);
  }
  assert.deepEqual(r.stats(), {
    granted: 4, renewed: 4, released: 0, reclaimed: 0, live: 4,
  });
});

test('renewAll reads the clock exactly once for the whole batch', () => {
  let reads = 0;
  let now = 0;
  const clock = () => {
    reads += 1;
    return now;
  };
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 250);
  reads = 0;
  now = 20;
  assert.equal(r.renewAll([a.token, b.token]), true);
  assert.equal(reads, 1);
  const leases = Object.fromEntries(
    r.exportState(20).leases.map((l) => [l.resource, l]),
  );
  // Same base reading; deadlines differ only by the per-lease ttl.
  assert.equal(leases.b.expiresAt - leases.a.expiresAt, 150);
});

test('renewAll validates input before any existence judgment', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const l = r.acquire('a', 'h');
  for (const bad of [null, undefined, 'nope', 42, {}]) {
    assert.throws(() => r.renewAll(bad), TypeError);
  }
  assert.throws(() => r.renewAll([]), TypeError);
  assert.throws(() => r.renewAll([null]), TypeError);
  assert.throws(() => r.renewAll([undefined]), TypeError);
  assert.throws(() => r.renewAll([1]), TypeError);
  assert.throws(() => r.renewAll(['']), TypeError);
  // A malformed element beats the unknown/known lookup.
  assert.throws(() => r.renewAll(['unknown', null]), TypeError);
  assert.throws(() => r.renewAll([l.token, '']), TypeError);
  // Duplicates are rejected even when no token exists.
  assert.throws(() => r.renewAll(['x', 'x']), TypeError);
  assert.throws(() => r.renewAll([l.token, l.token]), TypeError);
  assert.equal(r.stats().renewed, 0);
});

test('renewAll throws TypeError on a non-finite clock reading', () => {
  const r = createRegistry({ ttlMs: 100, clock: () => NaN });
  const l = r.acquire('a', 'h');
  assert.throws(() => r.renewAll([l.token]), TypeError);
});

test('renewAll throws TypeError if one computed deadline is not finite', () => {
  // The reading itself is finite, but reading + ttl overflows to Infinity.
  const r = createRegistry({ ttlMs: 100, clock: () => Number.MAX_VALUE });
  const l = r.acquire('a', 'h', 1e308);
  assert.throws(() => r.renewAll([l.token]), TypeError);
  // Nothing appended, nothing counted.
  assert.equal(r.stats().renewed, 0);
});

test('renewAll returns false without changes when any token is not renewable', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const live = r.acquire('live', 'h', 100);
  const short = r.acquire('short', 'h', 10);
  const gone = r.acquire('gone', 'h', 100);
  assert.equal(r.release(gone.token), true);
  const transferred = r.acquire('x', 'h', 100);
  const replacement = r.transfer(transferred.token, 'k', 100);
  const reclaimed = r.acquire('y', 'h', 5);

  clock.set(5);
  assert.deepEqual(r.sweep(), ['y']);

  const liveExpiry = r.exportState(5).leases.find((l) => l.token === live.token).expiresAt;
  for (const bad of ['unknown', gone.token, transferred.token, reclaimed.token]) {
    assert.equal(r.renewAll([live.token, bad]), false, `batch with ${bad}`);
  }
  // An expired-but-unswept credential fails the whole batch too.
  clock.set(10);
  assert.equal(r.renewAll([live.token, short.token]), false);

  // Nothing moved: live keeps its original deadline, no renewal counted.
  const state = r.exportState(10);
  assert.equal(state.leases.find((l) => l.token === live.token).expiresAt, liveExpiry);
  assert.equal(r.stats().renewed, 0);
  // The transferred replacement credential itself is perfectly renewable.
  assert.equal(r.renewAll([live.token, replacement.token]), true);
  // Sweeping afterwards reclaims the earlier failures only.
  assert.deepEqual(r.sweep(10), ['short']);
});

test('renewAll accepts coexisting shares with stale epochs and mixed holders', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 4);
  const first = r.acquire('pool', 'a', 100);
  const second = r.acquire('pool', 'b', 100);
  // second carries the resource's last epoch; first does not, yet renews.
  assert.notEqual(first.epoch, r.epoch('pool'));
  assert.equal(second.epoch, r.epoch('pool'));
  clock.set(20);
  assert.equal(r.renewAll([first.token, second.token]), true);
  const leases = Object.fromEntries(
    r.exportState(20).leases.map((l) => [l.token, l]),
  );
  assert.equal(leases[first.token].expiresAt, 120);
  assert.equal(leases[second.token].expiresAt, 120);
  assert.equal(leases[first.token].epoch, first.epoch);
});

test('renewAll rejects a credential another instance owns', () => {
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const l1 = r1.acquire('a', 'h');
  const l2 = r2.acquire('b', 'h');
  assert.equal(r1.renewAll([l1.token, l2.token]), false);
  assert.equal(r2.renewAll([l1.token]), false);
  assert.equal(r1.stats().renewed, 0);
});

test('renewAll judges a backwards reading by the actual reading', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  clock.set(90);
  // Renew once, then move the clock backwards: both revive-style and renew.
  assert.equal(r.renewAll([a.token, b.token]), true);
  clock.set(10);
  assert.equal(r.renewAll([a.token, b.token]), true);
  const leases = Object.fromEntries(
    r.exportState(10).leases.map((l) => [l.resource, l]),
  );
  assert.equal(leases.a.expiresAt, 110);
  assert.equal(leases.b.expiresAt, 110);
});

test('renewAll never touches the wait queue, on success or failure', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const l = r.acquire('r', 'h', 100);
  const waiting = r.acquire('r', 'w1', 100, 50);

  // A heartbeat past the waiter's deadline neither expires nor detaches it:
  // the ticket object is unchanged and read-only export merely reports it.
  clock.set(60);
  assert.equal(r.renewAll([l.token]), true);
  assert.equal(waiting.status, 'waiting');
  assert.equal(r.exportState(60).waits[0].status, 'timedOut');
  // Roll the clock back: the same untouched position is live-looking again.
  clock.set(20);
  assert.equal(r.exportState(20).waits[0].status, 'waiting');
  assert.equal(r.cancel(waiting.waitId), true);

  // A failing batch is equally inert: a fresh waiter keeps its place.
  const waiting2 = r.acquire('r', 'w2', 100, 50);
  assert.equal(r.renewAll([l.token, 'unknown']), false);
  assert.equal(waiting2.status, 'waiting');
  assert.equal(r.exportState(20).waits.length, 1);
  assert.equal(r.cancel(waiting2.waitId), true);
  assert.equal(r.exportState(20).waits.length, 0);
});

test('after renewAll sweep reclaims by the new deadlines and queue rules still apply', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 20);
  const ticket = r.acquire('b', 'w', 100, 1000);
  clock.set(15);
  assert.equal(r.renewAll([a.token, b.token]), true);
  clock.set(20);
  // b would have expired at 20; the batch extended it to 35, so nothing frees.
  assert.deepEqual(r.sweep(), []);
  assert.equal(ticket.status, 'waiting');
  clock.set(35);
  assert.deepEqual(r.sweep(), ['b']);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.holder, 'w');
  clock.set(115);
  assert.deepEqual(r.sweep(), ['a']);
});

test('a failed batch appends no log line', () => {
  const p = logPath('no-line');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h', 100);
  clock.set(10);
  assert.equal(r.renewAll(['unknown']), false);
  assert.equal(r.renewAll([a.token, 'unknown']), false);
  const lines = fs.readFileSync(p, 'utf8').trim().split('\n');
  assert.equal(lines.length, 1);
  assert.equal(JSON.parse(lines[0]).type, 'acquire');
});

test('batch renewal is one indivisible log commit', () => {
  const p = logPath('commit');
  const clock = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h1', 100);
  const b = r.acquire('b', 'h2', 40);
  clock.set(30);
  assert.equal(r.renewAll([a.token, b.token]), true);
  const lines = fs.readFileSync(p, 'utf8').trim().split('\n');
  assert.equal(lines.length, 3);
  const event = JSON.parse(lines[2]);
  assert.equal(event.type, 'renew-all');
  assert.deepEqual(event.items.map((i) => i.token).sort(), [a.token, b.token].sort());
  assert.equal(event.items.length, 2);

  r = createRegistry({ ttlMs: 100, clock: fakeClock(30), logPath: p });
  const leases = Object.fromEntries(
    r.exportState(30).leases.map((l) => [l.resource, l]),
  );
  assert.equal(leases.a.expiresAt, 130);
  assert.equal(leases.b.expiresAt, 70);
  assert.equal(leases.a.epoch, 1);
  assert.equal(leases.b.epoch, 1);
  assert.deepEqual(r.stats(), {
    granted: 2, renewed: 2, released: 0, reclaimed: 0, live: 2,
  });
  // Credentials from before the restart stay void, also in a batch.
  assert.equal(r.renewAll([a.token, b.token]), false);
  assert.equal(r.stats().renewed, 2);
});

test('a failed append on renewAll leaves every old deadline and counter intact', () => {
  const p = logPath('rollback');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);

  clock.set(20);
  sabotage(p);
  try {
    assert.throws(() => r.renewAll([a.token, b.token]), (e) =>
      e instanceof LogFileError && e.code === 'LOG_FILE_ERROR');
  } finally {
    restore(p);
  }
  assert.equal(r.stats().renewed, 0);
  const leases = Object.fromEntries(
    r.exportState(20).leases.map((l) => [l.resource, l]),
  );
  assert.equal(leases.a.expiresAt, 100);
  assert.equal(leases.b.expiresAt, 100);
  // Retryable: the retry lands and renews exactly once.
  assert.equal(r.renewAll([a.token, b.token]), true);
  assert.equal(r.stats().renewed, 2);
});

test('restart after compaction keeps every batch deadline, count and epoch', () => {
  const p = logPath('compact');
  const clock = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h1', 100);
  const b = r.acquire('b', 'h2', 50);
  clock.set(30);
  r.renewAll([a.token, b.token]);
  r.compact();

  r = createRegistry({ ttlMs: 100, clock: fakeClock(30), logPath: p });
  assert.deepEqual(r.stats(), {
    granted: 2, renewed: 2, released: 0, reclaimed: 0, live: 2,
  });
  const leases = Object.fromEntries(
    r.exportState(30).leases.map((l) => [l.resource, l]),
  );
  assert.equal(leases.a.expiresAt, 130);
  assert.equal(leases.b.expiresAt, 80);
  assert.equal(leases.a.epoch, 1);
  assert.equal(r.checkConsistency(30).ok, true);
  assert.deepEqual(r.sweep(79), []);
  assert.deepEqual(r.sweep(129), ['b']);
  assert.deepEqual(r.sweep(130), ['a']);
});

test('an unfinished batch record at the tail is ignored wholesale', () => {
  const p = logPath('torn');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  clock.set(20);
  r.renewAll([a.token, b.token]);
  // A second batch dies mid-line: no terminating newline, so the loader
  // treats it as an interrupted append and trims it away.
  fs.appendFileSync(
    p,
    JSON.stringify({ v: 1, type: 'renew-all', at: 40, items: [{ token: a.token, expiresAt: 140 }] }).slice(0, 20),
  );

  const reopened = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  assert.deepEqual(reopened.stats(), {
    granted: 2, renewed: 2, released: 0, reclaimed: 0, live: 2,
  });
  const leases = Object.fromEntries(
    reopened.exportState(20).leases.map((l) => [l.resource, l]),
  );
  assert.equal(leases.a.expiresAt, 120);
  assert.equal(leases.b.expiresAt, 120);
  // The fragment was trimmed: a later append starts a clean line.
  clock.set(50);
  assert.equal(reopened.renewAll([a.token, b.token]), false); // legacy now
});

test('a complete but damaged batch record throws LogFileError', () => {
  const p = logPath('corrupt-batch');
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  r.acquire('a', 'h', 100);
  for (const damaged of [
    { v: 1, type: 'renew-all', at: 10, items: [] },
    { v: 1, type: 'renew-all', at: 10, items: [{ token: 123, expiresAt: 110 }] },
    { v: 1, type: 'renew-all', at: 10, items: [{ token: 'x', expiresAt: NaN }] },
    { v: 1, type: 'renew-all', at: 10, items: [{ token: 'x', expiresAt: 110 }, { token: 'x', expiresAt: 120 }] },
  ]) {
    const file = logPath(`corrupt-${Math.random().toString(36).slice(2)}`);
    fs.copyFileSync(p, file);
    fs.appendFileSync(file, `${JSON.stringify(damaged)}\n`);
    assert.throws(
      () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: file }),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
    );
  }
});

test('renewAll keeps checkConsistency happy', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 3, null);
  const leases = r.acquireAll(['c', 'pool', 'pool'], 'g', [100, 30, 60]);
  clock.set(20);
  assert.equal(r.renewAll(leases.map((l) => l.token)), true);
  assert.equal(r.checkConsistency(20).ok, true);
  clock.set(40);
  // Even after one renewed share has expired, the structure is consistent.
  assert.equal(r.checkConsistency(40).ok, true);
});
