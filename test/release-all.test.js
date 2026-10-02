import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  createRegistry,
  LogFileError,
  LeaseFencedError,
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

// ---- happy path ------------------------------------------------------------

test('releaseAll removes every named credential at one reading', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h1', 40);
  const b = r.acquire('b', 'h2', 70);

  clock.set(30);
  assert.equal(r.releaseAll([a.token, b.token]), true);

  assert.equal(r.release(a.token), false);
  assert.equal(r.release(b.token), false);
  assert.equal(r.renewAll([a.token]), false);
  assert.throws(() => r.assertLease('a', a.token, a.epoch), LeaseFencedError);
  assert.deepEqual(r.stats(), {
    granted: 2,
    renewed: 0,
    released: 2,
    reclaimed: 0,
    live: 0,
  });
  assert.equal(r.exportState(30).leases.length, 0);
  assert.equal(r.checkConsistency(30).ok, true);
});

test('releaseAll mixes holders, batches, exclusive and shared credentials', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 4);
  const single = r.acquire('exclusive', 'h0', 100);
  const group = r.acquireAll(['x', 'y'], 'h1', 50);
  const shares = r.acquireAll([{ resource: 'pool', count: 3 }], 'h2', 80);
  const keep = r.acquire('keep', 'h3', 200);

  clock.set(20);
  const tokens = [single.token, ...group.map((l) => l.token), shares[0].token, shares[2].token];
  assert.equal(r.releaseAll(tokens), true);

  assert.deepEqual(r.stats(), {
    granted: 7,
    renewed: 0,
    released: 5,
    reclaimed: 0,
    live: 2, // one share of pool plus keep
  });
  // The un-named credentials are untouched and keep working.
  assert.equal(r.renewAll([shares[1].token, keep.token]), true);
  assert.equal(r.epoch('exclusive'), single.epoch); // releases never roll an epoch back
  assert.equal(r.epoch('pool'), shares[2].epoch);
  assert.equal(r.checkConsistency(20).ok, true);
});

test('releaseAll does not advance any epoch itself', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 3);
  const leases = r.acquireAll([{ resource: 'pool', count: 3 }], 'h', 100);
  const before = leases.map((l) => l.epoch);

  clock.set(10);
  assert.equal(r.releaseAll(leases.map((l) => l.token)), true);
  assert.deepEqual(before.map((e, i) => r.epoch('pool') >= before[i]).every(Boolean), true);
  // No new credential: the high-water epoch is exactly the one the last
  // share carried.
  assert.equal(r.epoch('pool'), before[before.length - 1]);
});

test('releaseAll reads the clock exactly once on success and on failure', () => {
  const clock = countingClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 50);
  const b = r.acquire('b', 'h', 50);

  clock.set(10);
  const before = clock.reads();
  assert.equal(r.releaseAll([a.token, b.token]), true);
  assert.equal(clock.reads() - before, 1);

  const c = r.acquire('c', 'h', 50);
  const beforeFailed = clock.reads();
  assert.equal(r.releaseAll([c.token, 'unknown-token']), false);
  assert.equal(clock.reads() - beforeFailed, 1);
});

// ---- validation ------------------------------------------------------------

test('releaseAll shape errors are TypeErrors raised before any lookup', () => {
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
      () => r.releaseAll(input),
      (e) => e instanceof TypeError,
      `expected TypeError for ${JSON.stringify(input)}`,
    );
  }
  assert.throws(() => r.releaseAll(['unknown', 'unknown']), TypeError);
  assert.equal(r.stats().released, 0);
});

test('releaseAll shape validation precedes the clock read', () => {
  let calls = 0;
  const r = createRegistry({
    ttlMs: 100,
    clock: () => {
      calls += 1;
      return NaN;
    },
  });
  assert.throws(() => r.releaseAll([]), TypeError);
  assert.equal(calls, 0);
});

test('releaseAll with a non-finite clock reading throws TypeError, nothing moves', () => {
  let now = 0;
  const clock = () => now;
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);

  now = NaN;
  assert.throws(() => r.releaseAll([a.token, b.token]), TypeError);
  assert.equal(r.stats().released, 0);

  now = 20;
  assert.equal(r.releaseAll([a.token, b.token]), true);
  assert.equal(r.stats().released, 2);
});

// ---- whole-batch failure ---------------------------------------------------

test('releaseAll returns false when any token is unknown, foreign, replaced, reclaimed or expired', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h1', 100);
  const released = r.acquire('released', 'h2', 100);
  const reclaimed = r.acquire('reclaimed', 'h3', 10);
  const transferred = r.acquire('exclusive', 'h4', 100);
  const other = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const foreign = other.acquire('a', 'h', 100);

  assert.equal(r.release(released.token), true);
  clock.set(10);
  assert.deepEqual(r.sweep(), ['reclaimed']);
  const moved = r.transfer(transferred.token, 'h5', 100);

  clock.set(20);
  assert.equal(r.releaseAll([a.token, 'never-minted']), false);
  assert.equal(r.releaseAll([a.token, foreign.token]), false);
  assert.equal(r.releaseAll([a.token, released.token]), false);
  assert.equal(r.releaseAll([a.token, reclaimed.token]), false);
  assert.equal(r.releaseAll([a.token, transferred.token]), false);

  // The good credential survives every failed batch untouched.
  const leaseA = r.exportState(20).leases.find((l) => l.token === a.token);
  assert.equal(leaseA.expiresAt, 100);
  assert.equal(r.stats().released, 1); // only the earlier single release
  assert.equal(r.releaseAll([a.token, moved.token]), true);
  assert.equal(r.stats().released, 3);
});

test('releaseAll fails as a whole at the expiry boundary and changes nothing', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 40);
  const b = r.acquire('b', 'h', 60);

  // expiresAt > now is required; equality already means expired.
  clock.set(40);
  assert.equal(r.releaseAll([a.token, b.token]), false);

  const byToken = new Map(r.exportState(40).leases.map((l) => [l.token, l]));
  assert.equal(byToken.get(a.token).expiresAt, 40);
  assert.equal(byToken.get(b.token).expiresAt, 60);
  assert.equal(r.stats().released, 0);
  assert.equal(r.checkConsistency(40).ok, true);

  clock.set(30);
  assert.equal(r.releaseAll([b.token]), true);
});

test('releaseAll on a backwards reading judges liveness then, and removed credentials never revive', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 50); // expires 50
  const b = r.acquire('b', 'h', 50);

  clock.set(60);
  r.stats(); // observe both shares as expired
  clock.set(40); // backwards: live again at the earlier reading
  assert.equal(r.releaseAll([a.token]), true);

  // A removed credential does not come back when the clock keeps moving.
  clock.set(30);
  assert.equal(r.releaseAll([a.token]), false);
  assert.equal(r.exportState(30).leases.some((l) => l.token === a.token), false);
  assert.equal(r.releaseAll([b.token]), true);
  assert.equal(r.checkConsistency(30).ok, true);
});

test('a failed releaseAll leaves counters, leases and tickets untouched and is retryable', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 40);
  const b = r.acquire('b', 'h', 60);

  clock.set(30);
  assert.equal(r.releaseAll([a.token, 'unknown']), false);
  assert.equal(r.stats().released, 0);
  assert.equal(r.releaseAll([a.token, b.token]), true);
  assert.equal(r.stats().released, 2);
});

test('a failed releaseAll neither purges nor expires waiters and changes no state', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 1);
  const held = r.acquire('pool', 'a', 100);
  const other = r.acquire('other', 'h', 100);
  // A waiter whose budget runs out at t=5; a failed batch must not purge it.
  const overdue = r.acquire('pool', 'b', 100, 5);

  clock.set(10);
  assert.equal(r.releaseAll([other.token, 'unknown']), false);
  assert.equal(overdue.status, 'waiting'); // not expired
  assert.equal(r.exportState(10).waits.length, 1);
  assert.deepEqual(r.stats(), {
    granted: 2,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 2,
  });
  assert.equal(r.checkConsistency(10).ok, true);

  // A successful release at the same reading then expires and removes it as
  // part of the wake commit.
  assert.equal(r.releaseAll([held.token]), true);
  assert.equal(overdue.status, 'expired');
});

test('releaseAll treats sparse-array holes as empty slots', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h');
  // eslint-disable-next-line no-sparse-arrays
  assert.throws(() => r.releaseAll([a.token, , 'x']), TypeError);
});

// ---- wake cascade ----------------------------------------------------------

test('releaseAll freed shares together wake single waiters in arrival order', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 2);
  r.acquireAll([{ resource: 'pool', count: 2 }], 'holder', 100);
  const t1 = r.acquire('pool', 'w1', 50, 1000);
  const t2 = r.acquire('pool', 'w2', 70, 1000);
  const held = r.exportState(0).leases.filter((l) => l.holder === 'holder');

  clock.set(10);
  assert.equal(r.releaseAll(held.map((l) => l.token)), true);
  assert.equal(t1.status, 'granted');
  assert.equal(t2.status, 'granted');
  assert.equal(t1.expiresAt, 60); // 10 + 50
  assert.equal(t2.expiresAt, 80); // 10 + 70
  assert.equal(t1.epoch < t2.epoch, true);
  assert.equal(r.checkConsistency(10).ok, true);
});

test('releaseAll result is independent of the input token order', () => {
  function build() {
    const clock = fakeClock(0);
    const r = createRegistry({ ttlMs: 100, clock });
    r.declareResource('pool', 2);
    const held = r.acquireAll([{ resource: 'pool', count: 2 }], 'holder', 100);
    const t1 = r.acquire('pool', 'w1', 50, 1000);
    const t2 = r.acquire('pool', 'w2', 50, 1000);
    return { clock, r, held, t1, t2 };
  }
  const forward = build();
  forward.clock.set(10);
  forward.r.releaseAll(forward.held.map((l) => l.token));
  const backward = build();
  backward.clock.set(10);
  backward.r.releaseAll(backward.held.map((l) => l.token).reverse());

  assert.deepEqual(
    { s1: forward.t1.status, s2: forward.t2.status, e1: forward.t1.expiresAt, e2: forward.t2.expiresAt },
    { s1: backward.t1.status, s2: backward.t2.status, e1: backward.t1.expiresAt, e2: backward.t2.expiresAt },
  );
  // Woken tickets keep the single-request shape in both orderings.
  for (const ticket of [forward.t1, forward.t2, backward.t1, backward.t2]) {
    assert.equal(typeof ticket.token, 'string');
    assert.equal(Number.isInteger(ticket.epoch), true);
    assert.equal(ticket.resource, 'pool');
  }
  assert.deepEqual(forward.r.stats(), backward.r.stats());
});

test('releaseAll wakes a group only once every demanded share is free', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const g1 = r.acquire('p1', 'a', 100);
  const g2 = r.acquire('p2', 'b', 100);
  const g3 = r.acquire('p3', 'c', 100);
  // A group wanting p1 and p2: freeing just p1 keeps it waiting.
  const ticketBoth = r.acquireAll(['p1', 'p2'], 'wg', 50, 1000);
  // A group wanting p1, p2 and p3: even freeing p1 and p2 in one batch keeps
  // it waiting.
  const ticketThree = r.acquireAll(['p1', 'p2', 'p3'], 'w3', 60, 1000);

  clock.set(10);
  assert.equal(r.releaseAll([g1.token]), true);
  assert.equal(ticketBoth.status, 'waiting');
  assert.equal(ticketThree.status, 'waiting');

  assert.equal(r.releaseAll([g2.token]), true);
  assert.equal(ticketBoth.status, 'granted');
  assert.deepEqual(ticketBoth.leases.map((l) => l.resource), ['p1', 'p2']); // original order
  assert.deepEqual(ticketBoth.leases.map((l) => l.expiresAt), [60, 60]);
  assert.equal(ticketThree.status, 'waiting');

  // The earlier group now holds p1 and p2, so the three-resource group still
  // waits; releasing the group's fresh credentials together with p3 frees
  // everything in one batch and wakes it.
  assert.equal(
    r.releaseAll([g3.token, ...ticketBoth.leases.map((l) => l.token)]),
    true,
  );
  assert.equal(ticketThree.status, 'granted');
  assert.deepEqual(ticketThree.leases.map((l) => l.resource), ['p1', 'p2', 'p3']);
  assert.equal(r.checkConsistency(10).ok, true);
});

test('releaseAll frees several resources in one batch and wakes the group at once', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const g1 = r.acquire('p1', 'a', 100);
  const g2 = r.acquire('p2', 'b', 100);
  const ticket = r.acquireAll(['p1', 'p2'], 'wg', [40, 60], 1000);

  clock.set(10);
  assert.equal(r.releaseAll([g2.token, g1.token]), true);
  assert.equal(ticket.status, 'granted');
  assert.deepEqual(ticket.leases.map((l) => l.expiresAt), [50, 70]);
  assert.equal(r.stats().granted, 4);
});

test('releaseAll obeys per-resource FIFO with single and group waiters mixed', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const p1 = r.acquire('p1', 'h', 100);
  const p2 = r.acquire('p2', 'h', 100);
  // w1 arrives first and only needs p1; the group behind it needs both.
  const w1 = r.acquire('p1', 'w1', 100, 1000);
  const wg = r.acquireAll(['p1', 'p2'], 'wg', 100, 1000);

  clock.set(10);
  assert.equal(r.releaseAll([p1.token, p2.token]), true);
  assert.equal(w1.status, 'granted');
  assert.equal(wg.status, 'waiting'); // p1 went to the earlier single request
  assert.equal(r.exportState(10).waits.length, 1);

  // Releasing w1's fresh credential then lets the group complete.
  assert.equal(r.releaseAll([w1.token]), true);
  assert.equal(wg.status, 'granted');
});

test('releaseAll quota room across resources wakes in strict arrival order', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.setQuota('root', 4);
  r.declareResource('p1', 2, 'root');
  r.declareResource('p2', 2, 'root');
  const first = r.acquireAll(
    [{ resource: 'p1', count: 1 }, { resource: 'p2', count: 1 }], 'h', 100,
  );
  const second = r.acquireAll(
    [{ resource: 'p1', count: 1 }, { resource: 'p2', count: 1 }], 'h2', 100,
  );
  const w1 = r.acquire('p1', 'w1', 100, 1000);
  const wg = r.acquireAll(['p1', 'p2'], 'wg', 100, 1000);

  clock.set(10);
  // Freeing only the second group: w1 takes the single p1 share, the group
  // still lacks room and stays waiting regardless of input order.
  assert.equal(
    r.releaseAll([second[1].token, second[0].token]), true,
  );
  assert.equal(w1.status, 'granted');
  assert.equal(wg.status, 'waiting');

  // Freeing the first group as well funds the group request.
  assert.equal(r.releaseAll(first.map((l) => l.token)), true);
  assert.equal(wg.status, 'granted');
  assert.equal(r.checkConsistency(10).ok, true);
});

test('releaseAll expires due waiters and grants live ones in the same commit', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 1);
  const held = r.acquire('pool', 'holder', 100);
  // First waiter gives up at t=5; a later one waits long enough.
  const overdue = r.acquire('pool', 'b', 50, 5);
  const live = r.acquire('pool', 'c', 70, 1000);

  clock.set(10);
  assert.equal(r.releaseAll([held.token]), true);
  assert.equal(overdue.status, 'expired');
  assert.equal(live.status, 'granted');
  assert.equal(live.expiresAt, 80);
  const exported = r.exportState(10);
  assert.equal(exported.waits.length, 0);
  assert.deepEqual(exported.leases.map((l) => l.holder), ['c']);
  assert.equal(r.stats().released, 1);
  assert.equal(r.stats().granted, 2); // the original holder and the one live woken request
  assert.equal(r.checkConsistency(10).ok, true);
});

test('releaseAll with a non-finite woken deadline throws TypeError and grants nothing', () => {
  let now = 0;
  const clock = () => now;
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 1);
  const held = r.acquire('pool', 'a', 1e308);
  const ticket = r.acquire('pool', 'b', 1e308, 1.7e308); // 8e307 + 1e308 = Infinity

  now = 8e307;
  assert.throws(() => r.releaseAll([held.token]), TypeError);
  assert.equal(ticket.status, 'waiting');
  assert.equal(r.exportState(now).leases.length, 1);
  assert.equal(r.stats().released, 0);
  assert.equal(r.stats().granted, 1);
  assert.equal(r.checkConsistency(now).ok, true);
});

test('releaseAll works without a logPath', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 50);
  clock.set(10);
  assert.equal(r.releaseAll([a.token]), true);
  assert.equal(r.stats().released, 1);
});

// --- Persistence ------------------------------------------------------------

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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-release-all-'));
});

test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a batch release appends as one indivisible release-all record', () => {
  const p = logPath('one-record');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.declareResource('pool', 4);
  const a = r.acquire('a', 'h1', 100);
  const shares = r.acquireAll([{ resource: 'pool', count: 2 }], 'h2', 100);

  c.set(30);
  assert.equal(r.releaseAll([shares[1].token, a.token, shares[0].token]), true);

  const lines = fs.readFileSync(p, 'utf8').trim().split('\n').map(JSON.parse);
  const batch = lines.filter((e) => e.type === 'release-all');
  assert.equal(batch.length, 1);
  assert.equal(batch[0].at, 30);
  assert.deepEqual(
    batch[0].items,
    [
      { token: shares[1].token, resource: 'pool' },
      { token: a.token, resource: 'a' },
      { token: shares[0].token, resource: 'pool' },
    ],
  );
});

test('restart restores the final holders, counters and epochs', () => {
  const p = logPath('restart');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.declareResource('pool', 3);
  const a = r.acquire('a', 'h1', 100);
  const gone = r.acquireAll([{ resource: 'pool', count: 2 }], 'h2', 60);
  const keep = r.acquire('keep', 'h3', 200);
  c.set(20);
  assert.equal(r.releaseAll([a.token, ...gone.map((l) => l.token)]), true);

  r = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  assert.deepEqual(r.stats(), {
    granted: 4,
    renewed: 0,
    released: 3,
    reclaimed: 0,
    live: 1,
  });
  assert.equal(r.epoch('a'), a.epoch);
  assert.equal(r.epoch('pool'), gone[1].epoch);
  const exported = r.exportState(20);
  assert.deepEqual(exported.leases.map((l) => l.token), [keep.token]);

  // Credentials from before the restart cannot be batch released.
  const fresh = r.acquire('fresh', 'h4', 100);
  assert.equal(r.releaseAll([a.token, fresh.token]), false);
  assert.equal(r.releaseAll([fresh.token]), true);
});

test('woken grants ride in the same write as the release-all and replay once', () => {
  const p = logPath('wakes');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.declareResource('pool', 1);
  const held = r.acquire('pool', 'a', 100);
  const ticket = r.acquire('pool', 'b', 100, 1000);
  c.set(10);
  assert.equal(r.releaseAll([held.token]), true);
  assert.equal(ticket.status, 'granted');

  r = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assert.deepEqual(r.stats(), {
    granted: 2,
    renewed: 0,
    released: 1,
    reclaimed: 0,
    live: 1,
  });
  // The woken credential carries its granted epoch and holds the pool.
  assert.equal(r.holder('pool'), 'b');
  assert.equal(r.epoch('pool'), ticket.epoch);
});

test('a failed append on releaseAll restores leases, queues and tickets, then retries', () => {
  const p = logPath('rollback');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.declareResource('p1', 1);
  r.declareResource('p2', 1);
  const p1 = r.acquire('p1', 'h', 100);
  const p2 = r.acquire('p2', 'h', 100);
  const w1 = r.acquire('p1', 'w', 100, 1000);
  const wg = r.acquireAll(['p1', 'p2'], 'wg', 100, 1000);
  c.set(10);

  sabotage(p);
  try {
    assert.throws(
      () => r.releaseAll([p1.token, p2.token]),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }

  assert.equal(w1.status, 'waiting');
  assert.equal(wg.status, 'waiting');
  assert.equal(r.stats().released, 0);
  assert.equal(r.stats().granted, 2);
  const exported = r.exportState(10);
  assert.equal(exported.leases.length, 2);
  assert.equal(exported.waits.length, 2);
  assert.equal(r.checkConsistency(10).ok, true);

  // The same call retries successfully once the log is back.
  assert.equal(r.releaseAll([p1.token, p2.token]), true);
  assert.equal(w1.status, 'granted');
  assert.equal(wg.status, 'waiting'); // FIFO: w1 took p1
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assert.equal(r2.stats().released, 2);
  assert.equal(r2.stats().granted, 3);
});

test('a failed append restores a waiter that expired at the reading', () => {
  const p = logPath('rollback-expired');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.declareResource('pool', 1);
  const held = r.acquire('pool', 'a', 100);
  const overdue = r.acquire('pool', 'b', 100, 5);

  c.set(10);
  sabotage(p);
  try {
    assert.throws(() => r.releaseAll([held.token]), LogFileError);
  } finally {
    restore(p);
  }
  assert.equal(overdue.status, 'waiting');
  assert.equal(r.exportState(10).waits[0].status, 'timedOut'); // still queued, reported timed out
  assert.equal(r.stats().released, 0);
  assert.equal(r.checkConsistency(10).ok, true);

  // Retry commits: the overdue request expires and no live waiter remains.
  assert.equal(r.releaseAll([held.token]), true);
  assert.equal(overdue.status, 'expired');
  assert.equal(r.exportState(10).waits.length, 0);
});

test('an unfinished release-all record at the tail is ignored entirely', () => {
  const p = logPath('torn');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 40);
  const b = r.acquire('b', 'h', 60);
  c.set(30);
  assert.equal(r.releaseAll([a.token, b.token]), true);

  const partial = JSON.stringify({
    v: 1,
    type: 'release-all',
    at: 50,
    items: [{ token: a.token, resource: 'a' }],
  }).slice(0, 30);
  fs.appendFileSync(p, partial);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r2.stats().released, 2); // only the committed batch
  assert.equal(r2.stats().live, 0);
});

test('a complete but corrupt release-all record throws LogFileError', () => {
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

  seed('no-items', () => ({ v: 1, type: 'release-all', at: 0 }));
  seed('empty-items', () => ({ v: 1, type: 'release-all', at: 0, items: [] }));
  seed('bad-item', (a) => ({ v: 1, type: 'release-all', at: 0, items: [a.token] }));
  seed('bad-token', () => ({
    v: 1, type: 'release-all', at: 0, items: [{ token: 7, resource: 'a' }],
  }));
  seed('empty-token', (a) => ({
    v: 1, type: 'release-all', at: 0, items: [{ token: '', resource: 'a' }],
  }));
  seed('missing-resource', (a) => ({
    v: 1, type: 'release-all', at: 0, items: [{ token: a.token }],
  }));
  seed('duplicate-token', (a) => ({
    v: 1, type: 'release-all', at: 0,
    items: [
      { token: a.token, resource: 'a' },
      { token: a.token, resource: 'a' },
    ],
  }));
  seed('unknown-token', (a) => ({
    v: 1, type: 'release-all', at: 0,
    items: [
      { token: a.token, resource: 'a' },
      { token: 'lease-9-9-9-zzz', resource: 'a' },
    ],
  }));
  seed('resource-mismatch', (a) => ({
    v: 1, type: 'release-all', at: 0,
    items: [{ token: a.token, resource: 'b' }],
  }));
});

test('compaction preserves the released total and epochs exactly once', () => {
  const p = logPath('compact');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.declareResource('pool', 3);
  const a = r.acquire('a', 'h1', 100);
  const shares = r.acquireAll([{ resource: 'pool', count: 2 }], 'h2', 80);
  const keep = r.acquire('keep', 'h3', 200);
  c.set(20);
  assert.equal(r.releaseAll([a.token, shares[0].token]), true);
  r.compact();

  // Another release after compaction rides the tail and replays on top.
  const d = r.acquire('d', 'h4', 50);
  c.set(30);
  assert.equal(r.releaseAll([d.token]), true);

  r = createRegistry({ ttlMs: 100, clock: fakeClock(30), logPath: p });
  assert.deepEqual(r.stats(), {
    granted: 5,
    renewed: 0,
    released: 3,
    reclaimed: 0,
    live: 2, // shares[1] and keep
  });
  assert.equal(r.epoch('a'), a.epoch);
  assert.equal(r.epoch('pool'), shares[1].epoch);
  assert.deepEqual(r.sweep(110), ['pool']); // shares[1] expired at 100
  assert.deepEqual(r.sweep(200), ['keep']); // keep expires at 200
});

test('a folded release-all next to the unswapped full log is never counted twice', () => {
  const p = logPath('swap-crash');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 40);
  const b = r.acquire('b', 'h', 60);
  c.set(30);
  r.releaseAll([a.token, b.token]);
  const fullLog = fs.readFileSync(p, 'utf8');
  r.compact();
  // The log swap died after the snapshot rename: the folded record is back
  // in the log, but its sequence id marks it already folded.
  fs.writeFileSync(p, fullLog);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(30), logPath: p });
  assert.equal(r2.stats().released, 2);
  assert.equal(r2.stats().live, 0);
});

test('old logs without release-all records keep replaying unchanged', () => {
  const p = logPath('legacy-log');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 40);
  c.set(10);
  r.release(a.token); // plain single release line

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assert.equal(r2.stats().released, 1);
  assert.equal(r2.stats().live, 0);
});
