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

function countingClock(start = 0) {
  let now = start;
  let reads = 0;
  const clock = () => {
    reads += 1;
    return now;
  };
  clock.set = (value) => {
    now = value;
  };
  clock.reads = () => reads;
  return clock;
}

test('renewAll extends every lease at one reading, each with its own ttl', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h1', 40);
  const b = r.acquire('b', 'h2', 70);

  clock.set(30);
  assert.equal(r.renewAll([a.token, b.token]), true);

  const exported = r.exportState(30);
  const byToken = new Map(exported.leases.map((l) => [l.token, l]));
  assert.equal(byToken.get(a.token).expiresAt, 70); // 30 + 40
  assert.equal(byToken.get(b.token).expiresAt, 100); // 30 + 70
  // token, holder, ttlMs and epoch stay exactly as granted.
  assert.equal(byToken.get(a.token).holder, 'h1');
  assert.equal(byToken.get(a.token).ttlMs, 40);
  assert.equal(byToken.get(a.token).epoch, a.epoch);
  assert.equal(byToken.get(b.token).holder, 'h2');
  assert.equal(byToken.get(b.token).ttlMs, 70);
  assert.equal(byToken.get(b.token).epoch, b.epoch);
  assert.equal(r.epoch('a'), a.epoch);
  assert.equal(r.epoch('b'), b.epoch);

  assert.deepEqual(r.stats(), {
    granted: 2,
    renewed: 2,
    released: 0,
    reclaimed: 0,
    live: 2,
  });
  assert.equal(r.checkConsistency().ok, true);
});

test('renewAll accepts tokens from different batches, holders and shares of one resource', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 4);
  const single = r.acquire('exclusive', 'h0', 100);
  const groupA = r.acquireAll(['x', 'y'], 'h1', [40, 60]);
  const groupB = r.acquireAll([{ resource: 'pool', count: 2 }], 'h2', 50);

  clock.set(20);
  const tokens = [
    single.token,
    groupA[0].token,
    groupA[1].token,
    ...groupB.map((l) => l.token),
  ];
  assert.equal(r.renewAll(tokens), true);

  const exported = r.exportState(20);
  const byToken = new Map(exported.leases.map((l) => [l.token, l]));
  assert.equal(byToken.get(single.token).expiresAt, 120);
  assert.equal(byToken.get(groupA[0].token).expiresAt, 60);
  assert.equal(byToken.get(groupA[1].token).expiresAt, 80);
  for (const l of groupB) {
    assert.equal(byToken.get(l.token).expiresAt, 70);
  }
  assert.equal(exported.stats.renewed, 5);
  assert.equal(r.checkConsistency().ok, true);
});

test('renewAll reads the clock exactly once on success and on failure', () => {
  const clock = countingClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 50);
  const b = r.acquire('b', 'h', 50);

  clock.set(10);
  const before = clock.reads();
  assert.equal(r.renewAll([a.token, b.token]), true);
  assert.equal(clock.reads() - before, 1);

  const beforeFailed = clock.reads();
  assert.equal(r.renewAll([a.token, 'unknown-token']), false);
  assert.equal(clock.reads() - beforeFailed, 1);
});

test('renewAll shape errors are TypeErrors, raised before any existence check', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h');

  const badInputs = [
    undefined,
    null,
    'not-an-array',
    {},
    42,
    [],
    [a.token, null],
    [a.token, undefined],
    [a.token, 42],
    [a.token, {}],
    [a.token, ''],
    [a.token, a.token],
    ['', a.token],
  ];
  for (const input of badInputs) {
    assert.throws(
      () => r.renewAll(input),
      (e) => e instanceof TypeError,
      `expected TypeError for ${JSON.stringify(input)}`,
    );
  }

  // A malformed call throws even though a listed token is also unknown:
  // validation strictly precedes existence judgment.
  assert.throws(() => r.renewAll(['unknown', 'unknown']), TypeError);

  // Nothing moved.
  assert.equal(r.stats().renewed, 0);
});

test('renewAll shape validation precedes the clock read', () => {
  let calls = 0;
  const r = createRegistry({
    ttlMs: 100,
    clock: () => {
      calls += 1;
      return NaN;
    },
  });
  assert.throws(() => r.renewAll([]), TypeError);
  assert.equal(calls, 0);
});

test('renewAll with a non-finite clock reading throws TypeError and changes nothing', () => {
  let now = 0;
  const clock = () => now;
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);

  now = NaN;
  assert.throws(() => r.renewAll([a.token, b.token]), TypeError);
  assert.equal(r.stats().renewed, 0);

  now = 20;
  assert.equal(r.renewAll([a.token, b.token]), true);
  assert.equal(r.stats().renewed, 2);
});

test('renewAll returns false when any token is unknown, released, reclaimed or replaced', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h1', 100);
  const released = r.acquire('released', 'h2', 100);
  const reclaimed = r.acquire('reclaimed', 'h3', 10);
  const transferred = r.acquire('exclusive', 'h4', 100);

  assert.equal(r.release(released.token), true);
  clock.set(10);
  assert.deepEqual(r.sweep(), ['reclaimed']);
  const moved = r.transfer(transferred.token, 'h5', 100);

  clock.set(20);
  assert.equal(r.renewAll([a.token, 'never-minted']), false);
  assert.equal(r.renewAll([a.token, released.token]), false);
  assert.equal(r.renewAll([a.token, reclaimed.token]), false);
  assert.equal(r.renewAll([a.token, transferred.token]), false);

  // The good credential in every failed batch is untouched.
  const exported = r.exportState(20);
  const leaseA = exported.leases.find((l) => l.token === a.token);
  assert.equal(leaseA.expiresAt, 100);
  assert.equal(r.stats().renewed, 0);
  assert.equal(r.renewAll([moved.token]), true);
  assert.equal(r.stats().renewed, 1);
});

test('renewAll fails as a whole at the expiry boundary and changes nothing', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 40);
  const b = r.acquire('b', 'h', 60);

  // expiresAt > now is required; at equality the lease is already gone.
  clock.set(40);
  assert.equal(r.renewAll([a.token, b.token]), false);

  const exported = r.exportState(40);
  const byToken = new Map(exported.leases.map((l) => [l.token, l]));
  assert.equal(byToken.get(a.token).expiresAt, 40);
  assert.equal(byToken.get(b.token).expiresAt, 60);
  assert.equal(r.stats().renewed, 0);
  assert.equal(r.checkConsistency().ok, true);

  // The still-live credential renews on its own afterwards.
  assert.equal(r.renew(b.token), true);
});

test('renewAll with a non-finite resulting deadline throws TypeError and renews nothing', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  // Both leases are live at the later reading, but the first lease's new
  // deadline overflows to Infinity while the second's stays finite.
  const huge = r.acquire('huge', 'h', 1.5e308); // expires 1.5e308
  const other = r.acquire('other', 'h', 9e307); // expires 9e307
  clock.set(8e307);

  assert.throws(
    () => r.renewAll([huge.token, other.token]),
    (e) => e instanceof TypeError,
  );
  const exported = r.exportState(8e307);
  const byToken = new Map(exported.leases.map((l) => [l.token, l]));
  assert.equal(byToken.get(huge.token).expiresAt, 1.5e308);
  assert.equal(byToken.get(other.token).expiresAt, 9e307);
  assert.equal(r.stats().renewed, 0);

  // The finite lease renews on its own once the overflowing one is gone.
  assert.equal(r.renewAll([other.token]), true);
  const after = r.exportState(8e307);
  const renewedOther = after.leases.find((l) => l.token === other.token);
  assert.equal(renewedOther.expiresAt, 1.7e308);
});

test('renewAll on a backwards clock reading judges liveness at the earlier reading', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 50); // expires 50

  clock.set(60);
  r.stats(); // observe the expiry at 60 (owners retire the share)
  clock.set(40); // clock moves backwards: the share is live again
  assert.equal(r.renewAll([a.token]), true);
  assert.equal(r.exportState(40).leases[0].expiresAt, 90);
  assert.equal(r.checkConsistency().ok, true);
});

test('renewAll does not purge waiters that are already past their deadline', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 1);
  r.acquire('pool', 'a', 100);
  const other = r.acquire('other', 'a', 100);

  // A waiter whose wait budget runs out at t=10.
  const ticket = r.acquire('pool', 'b', 50, 10);
  clock.set(50);
  // Renewing an unrelated lease must not purge the overdue waiter: its
  // ticket is untouched and it still occupies the queue (reported timed
  // out by the read-only export).
  assert.equal(r.renewAll([other.token]), true);
  assert.equal(ticket.status, 'waiting');
  const waits = r.exportState(50).waits;
  assert.equal(waits.length, 1);
  assert.equal(waits[0].status, 'timedOut');
  assert.equal(r.stats().renewed, 1);
});

test('renewAll never wakes a live waiter because occupancy never changes', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 1);
  const held = r.acquire('pool', 'a', 100);
  const liveTicket = r.acquire('pool', 'c', 50, 1000);

  clock.set(50);
  assert.equal(r.renewAll([held.token]), true);
  assert.equal(liveTicket.status, 'waiting');
  assert.equal(r.stats().granted, 1);
});

test('after renewAll sweep reclaims at the new deadlines and grants freed shares to waiters', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 1);
  const held = r.acquire('pool', 'a', 100);
  const ticket = r.acquire('pool', 'b', 100, 1000);

  clock.set(90);
  assert.equal(r.renewAll([held.token]), true); // now expires 190
  clock.set(100);
  assert.deepEqual(r.sweep(), []); // old deadline would have reclaimed it
  assert.equal(ticket.status, 'waiting');
  clock.set(190);
  assert.deepEqual(r.sweep(), ['pool']);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.expiresAt, 290); // woken at 190 with its own ttl of 100
});

test('renewAll works without a logPath and writes nothing', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 50);
  clock.set(10);
  assert.equal(r.renewAll([a.token]), true);
  assert.equal(r.stats().renewed, 1);
});

// --- Persistence -----------------------------------------------------------

let dir;
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

test.beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-renewall-'));
});

test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a batch heartbeat appends as one indivisible renew-all record', () => {
  const p = logPath('one-record');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h1', 40);
  const b = r.acquire('b', 'h2', 60);
  c.set(30);
  assert.equal(r.renewAll([b.token, a.token]), true);

  const lines = fs.readFileSync(p, 'utf8').trim().split('\n').map(JSON.parse);
  const batch = lines.filter((e) => e.type === 'renew-all');
  assert.equal(batch.length, 1);
  assert.deepEqual(
    batch[0].items,
    [
      { token: b.token, expiresAt: 90 },
      { token: a.token, expiresAt: 70 },
    ],
  );
  assert.equal(batch[0].at, 30);
});

test('restart restores every new expiry, the renewal total and the epochs', () => {
  const p = logPath('restart');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h1', 40);
  const b = r.acquire('b', 'h2', 60);
  const keep = r.acquire('keep', 'h3', 200);
  c.set(30);
  assert.equal(r.renewAll([a.token, b.token]), true); // +2 renewed
  assert.equal(r.renew(keep.token), true); // +1 renewed
  c.set(50);

  r = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.deepEqual(r.stats(), {
    granted: 3,
    renewed: 3,
    released: 0,
    reclaimed: 0,
    live: 3,
  });
  assert.equal(r.epoch('a'), a.epoch);
  assert.equal(r.epoch('b'), b.epoch);

  assert.deepEqual(r.sweep(69), []);
  assert.deepEqual(r.sweep(70), ['a']);
  assert.deepEqual(r.sweep(89), []);
  assert.deepEqual(r.sweep(90), ['b']);
  assert.deepEqual(r.sweep(250), ['keep']);

  // Credentials from before the restart cannot be batch renewed.
  const fresh = r.acquire('fresh', 'h4', 100);
  assert.equal(r.renewAll([a.token, fresh.token]), false);
  assert.equal(r.renewAll([fresh.token]), true);
});

test('a failed append on renewAll leaves old credentials and expiries retryable', () => {
  const p = logPath('rollback');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);

  c.set(20);
  sabotage(p);
  try {
    assert.throws(
      () => r.renewAll([a.token, b.token]),
      (e) => e instanceof LogFileError && e.code === LogFileError.code,
    );
  } finally {
    restore(p);
  }

  const exported = r.exportState(20);
  const byToken = new Map(exported.leases.map((l) => [l.token, l]));
  assert.equal(byToken.get(a.token).expiresAt, 100);
  assert.equal(byToken.get(b.token).expiresAt, 100);
  assert.equal(r.stats().renewed, 0);

  // The same heartbeat retries successfully once the log is back.
  assert.equal(r.renewAll([a.token, b.token]), true);
  assert.equal(r.stats().renewed, 2);
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  assert.equal(r2.stats().renewed, 2);
});

test('an unfinished batch record at the log tail is ignored in its entirety', () => {
  const p = logPath('torn');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 40);
  const b = r.acquire('b', 'h', 60);
  c.set(30);
  assert.equal(r.renewAll([a.token, b.token]), true);

  // A later batch append dies mid-line, without a terminating newline:
  // the whole unfinished record (both would-be renewals) never happened.
  const partial = JSON.stringify({
    v: 1,
    type: 'renew-all',
    at: 50,
    items: [
      { token: a.token, expiresAt: 90 },
      { token: b.token, expiresAt: 110 },
    ],
  }).slice(0, 60);
  fs.appendFileSync(p, partial);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r2.stats().renewed, 2); // only the committed batch counts
  assert.deepEqual(r2.sweep(69), []);
  assert.deepEqual(r2.sweep(70), ['a']); // expiry 70 from the first batch
  assert.deepEqual(r2.sweep(90), ['b']); // expiry 90 from the first batch
});

test('a complete but corrupt batch record throws LogFileError', () => {
  const c = fakeClock(0);

  const seed = (name, line) => {
    const p = logPath(name);
    const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
    const a = r.acquire('a', 'h', 100);
    const b = r.acquire('b', 'h', 100);
    fs.appendFileSync(p, `${JSON.stringify(line(a, b))}\n`);
    assert.throws(
      () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
      name,
    );
  };

  // items missing
  seed('no-items', () => ({ v: 1, type: 'renew-all', at: 0 }));
  // items empty
  seed('empty-items', () => ({ v: 1, type: 'renew-all', at: 0, items: [] }));
  // item not an object
  seed('bad-item', (a) => ({ v: 1, type: 'renew-all', at: 0, items: [a.token] }));
  // non-string token
  seed('bad-token', () => ({
    v: 1, type: 'renew-all', at: 0, items: [{ token: 7, expiresAt: 50 }],
  }));
  // empty token
  seed('empty-token', () => ({
    v: 1, type: 'renew-all', at: 0, items: [{ token: '', expiresAt: 50 }],
  }));
  // non-finite expiry encoded as a string
  seed('bad-expiry', (a) => ({
    v: 1, type: 'renew-all', at: 0, items: [{ token: a.token, expiresAt: 'soon' }],
  }));
  // duplicate token inside the batch
  seed('duplicate-token', (a) => ({
    v: 1, type: 'renew-all', at: 0,
    items: [
      { token: a.token, expiresAt: 90 },
      { token: a.token, expiresAt: 90 },
    ],
  }));
  // token the reconstructed log never granted
  seed('unknown-token', (a) => ({
    v: 1, type: 'renew-all', at: 0,
    items: [
      { token: a.token, expiresAt: 90 },
      { token: 'lease-9-9-9-zzz', expiresAt: 90 },
    ],
  }));
});

test('compaction preserves batch expiries, counts and epochs exactly once', () => {
  const p = logPath('compact');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h1', 40);
  const b = r.acquire('b', 'h2', 60);
  c.set(30);
  assert.equal(r.renewAll([a.token, b.token]), true);
  r.compact();

  // Another batch after compaction rides the tail and replays on top.
  const d = r.acquire('d', 'h3', 50);
  c.set(40);
  assert.equal(r.renewAll([d.token]), true);

  r = createRegistry({ ttlMs: 100, clock: fakeClock(40), logPath: p });
  assert.deepEqual(r.stats(), {
    granted: 3,
    renewed: 3,
    released: 0,
    reclaimed: 0,
    live: 3,
  });
  assert.equal(r.epoch('a'), a.epoch);
  assert.equal(r.epoch('b'), b.epoch);
  assert.deepEqual(r.sweep(69), []);
  assert.deepEqual(r.sweep(70), ['a']);
  // b (batch-renewed before compaction) and d (renewed after) both expire
  // at 90 and are reclaimed together in heap order.
  assert.deepEqual(r.sweep(89), []);
  assert.deepEqual(r.sweep(90), ['b', 'd']);
  assert.deepEqual(r.sweep(200), []);
});

test('a folded batch next to the unswapped full log is never counted twice', () => {
  const p = logPath('swap-crash');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 40);
  const b = r.acquire('b', 'h', 60);
  c.set(30);
  r.renewAll([a.token, b.token]);
  const fullLog = fs.readFileSync(p, 'utf8');
  r.compact();
  // The log swap died after the snapshot rename: the folded batch line is
  // back in the log, but its sequence id marks it as already folded.
  fs.writeFileSync(p, fullLog);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(30), logPath: p });
  assert.equal(r2.stats().renewed, 2);
  assert.deepEqual(r2.sweep(70), ['a']);
  assert.deepEqual(r2.sweep(90), ['b']);
});

test('old logs without batch records keep replaying unchanged', () => {
  const p = logPath('legacy-log');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 40);
  c.set(10);
  r.renew(a.token); // plain single renew line

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assert.equal(r2.stats().renewed, 1);
  assert.deepEqual(r2.sweep(50), ['a']);
});
