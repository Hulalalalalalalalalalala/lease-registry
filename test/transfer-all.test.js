import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  createRegistry,
  LeaseTakenError,
  LeaseFencedError,
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

let dir;
test.beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-transfer-all-'));
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

function assertFenced(fn, resource) {
  let caught;
  assert.throws(
    () => {
      try {
        fn();
      } catch (error) {
        caught = error;
        throw error;
      }
    },
    (error) => error instanceof LeaseFencedError
      && error.code === 'LEASE_FENCED',
  );
  if (resource !== undefined) {
    assert.equal(caught.resource, resource);
  }
}

// ---- success shape ----------------------------------------------------------

test('transferAll hands every resource to one holder with fresh credentials in input order', () => {
  const clock = fakeClock(10);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h1', 40);
  const [b, c] = r.acquireAll(['b', 'c'], 'h2', [60, 80]);

  clock.set(30);
  const result = r.transferAll([c.token, a.token, b.token], 'new', 70);

  assert.ok(Array.isArray(result));
  assert.equal(result.length, 3);
  for (const entry of result) {
    assert.deepEqual(Object.keys(entry).sort(), [
      'epoch', 'expiresAt', 'holder', 'resource', 'token',
    ]);
    assert.equal(entry.holder, 'new');
    assert.equal(entry.expiresAt, 100); // shared reading + ttl
  }
  assert.deepEqual(result.map((x) => x.resource), ['c', 'a', 'b']);
  assert.deepEqual(result.map((x) => x.epoch), [2, 2, 2]);
  assert.equal(r.holder('a'), 'new');
  assert.equal(r.holder('b'), 'new');
  assert.equal(r.holder('c'), 'new');
  assert.equal(r.epoch('a'), 2);
  assert.equal(r.epoch('b'), 2);
  assert.equal(r.epoch('c'), 2);

  // Tokens are brand new, pairwise distinct and different from every old one.
  const oldTokens = new Set([a.token, b.token, c.token]);
  const newTokens = new Set();
  for (const entry of result) {
    assert.equal(typeof entry.token, 'string');
    assert.equal(entry.token === '', false);
    assert.equal(oldTokens.has(entry.token), false);
    assert.equal(newTokens.has(entry.token), false);
    newTokens.add(entry.token);
  }
});

test('tokens may come from different holders and batches, and may keep their holder', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h1', 100);
  const b = r.acquire('b', 'h2', 100);
  const [c, d] = r.acquireAll(['c', 'd'], 'h3', [100, 100]);

  // Hand a back to h1 and the rest to h1 too; same-holder is allowed.
  const result = r.transferAll([b.token, a.token, c.token, d.token], 'h1', 100);
  assert.deepEqual(result.map((x) => x.holder), ['h1', 'h1', 'h1', 'h1']);
  assert.equal(r.holder('b'), 'h1');
  assert.equal(r.checkConsistency().ok, true);
});

test('an omitted ttlMs applies the registry default to every new credential', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 40);
  const b = r.acquire('b', 'h', 70);
  clock.set(25);
  const result = r.transferAll([a.token, b.token], 'g');
  for (const entry of result) {
    assert.equal(entry.expiresAt, 125);
  }
});

test('the old credentials die together and the new ones work', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h1', 60);
  const b = r.acquire('b', 'h2', 60);

  const next = r.transferAll([a.token, b.token], 'g', 60);
  for (const old of [a, b]) {
    assert.equal(r.renew(old.token), false);
    assert.equal(r.release(old.token), false);
  }
  assertFenced(() => r.assertLease('a', a.token, 1), 'a');
  assertFenced(() => r.assertLease('b', b.token, 1), 'b');
  assert.equal(r.assertLease('a', next[0].token, 2), true);
  assert.equal(r.assertLease('b', next[1].token, 2), true);

  clock.set(10);
  assert.equal(r.renew(next[0].token), true);
  assert.equal(r.renew(next[1].token), true);
  const again = r.transferAll(next.map((x) => x.token), 'k', 60);
  assert.deepEqual(again.map((x) => x.epoch), [3, 3]);
  assert.equal(r.release(again[0].token), true);
  assert.equal(r.release(again[1].token), true);
});

test('a group handover chains with single transfers and keeps strict epochs', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  const group = r.transferAll([a.token, b.token], 'g', 100);
  assert.deepEqual(group.map((x) => x.epoch), [2, 2]);

  const single = r.transfer(group[0].token, 's', 100);
  assert.equal(single.epoch, 3);
  const merged = r.transferAll([single.token, group[1].token], 'm', 100);
  assert.deepEqual(merged.map((x) => x.epoch), [4, 3]);
  assert.equal(r.assertLease('a', merged[0].token, 4), true);
  assert.equal(r.assertLease('b', merged[1].token, 3), true);
});

test('the handover leaves no occupancy gap', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  r.transferAll([a.token, b.token], 'g', 100);
  assert.throws(() => r.acquire('a', 'z'), LeaseTakenError);
  assert.throws(() => r.acquire('b', 'z'), LeaseTakenError);
});

// ---- counters ---------------------------------------------------------------

test('transferAll moves none of the four counters and keeps live constant', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  assert.deepEqual(r.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
  const next = r.transferAll([a.token, b.token], 'g', 100);
  assert.deepEqual(r.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
  r.transferAll(next.map((x) => x.token), 'k', 100);
  assert.equal(r.stats().live, 2);
});

// ---- wait queues ------------------------------------------------------------

test('transferAll does not wake or disturb a live waiting request', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const held = r.acquire('r', 'a', 100);
  const other = r.acquire('o', 'a', 100);
  const ticket = r.acquire('r', 'w', 50, 1000);
  assert.equal(ticket.status, 'waiting');

  const moved = r.transferAll([held.token, other.token], 'c', 100);
  assert.equal(ticket.status, 'waiting');
  assert.equal(r.holder('r'), 'c');
  assert.throws(() => r.acquire('r', 'd'), LeaseTakenError);

  // Only releasing the new credential frees the resource; FIFO is intact and
  // the epoch continues past the handover.
  assert.equal(r.release(moved[0].token), true);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.holder, 'w');
  assert.equal(ticket.epoch, 3);
});

test('transferAll neither purges nor wakes an already timed-out ticket', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const held = r.acquire('r', 'a', 100);
  const ticket = r.acquire('r', 'w', 50, 10); // deadline t=10
  clock.set(50);

  r.transferAll([held.token], 'c', 100);
  // The overdue ticket is still queued, untouched; the read-only export
  // reports it as timed out.
  assert.equal(ticket.status, 'waiting');
  const waits = r.exportState(50).waits;
  assert.equal(waits.length, 1);
  assert.equal(waits[0].status, 'timedOut');
  assert.equal(r.holder('r'), 'c');
});

// ---- fencing ----------------------------------------------------------------

test('any fenced member fails the whole group and leaves the rest untouched', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const good = r.acquire('good', 'h', 100);
  const expired = r.acquire('expired', 'h', 15);
  const released = r.acquire('released', 'h', 100);
  const reclaimed = r.acquire('reclaimed', 'h', 5);
  const moved = r.acquire('moved', 'h', 100);

  r.release(released.token);
  clock.set(11);
  r.sweep(); // reclaims only `reclaimed` (expires 5); `expired` runs to 15
  const movedNext = r.transfer(moved.token, 'k', 100);
  clock.set(20);

  assertFenced(() => r.transferAll([good.token, 'never-minted'], 'z'), null);
  assertFenced(() => r.transferAll([good.token, expired.token], 'z'), 'expired');
  assertFenced(() => r.transferAll([good.token, released.token], 'z'), null);
  assertFenced(() => r.transferAll([good.token, reclaimed.token], 'z'), null);
  assertFenced(() => r.transferAll([good.token, moved.token], 'z'));

  // The good credential and every other surviving credential are intact:
  // same token, same epoch, still renewable and fencing under epoch 1.
  assert.equal(r.assertLease('good', good.token, 1), true);
  assert.equal(r.renew(good.token), true);
  assert.equal(r.epoch('good'), 1);
  assert.equal(r.assertLease('moved', movedNext.token, 2), true);
  assert.equal(r.epoch('moved'), 2);
  assert.deepEqual(r.stats(), {
    granted: 5, renewed: 1, released: 1, reclaimed: 1, live: 2,
  });
});

test('a credential from another instance or minted before a restart is fenced', () => {
  const p = logPath('legacy');
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const lease = r1.acquire('r', 'a', 200);
  const other = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const foreign = other.acquire('q', 'a', 100);

  assertFenced(() => other.transferAll([lease.token, foreign.token], 'b'));
  assert.equal(other.assertLease('q', foreign.token, 1), true);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assertFenced(() => r2.transferAll([lease.token], 'b'), 'r');
});

test('a stale epoch (retake, replacement) fences the whole group', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 10);
  const b = r.acquire('b', 'h', 100);
  clock.set(11);
  const retake = r.acquire('a', 'k', 100); // expired, then retaken at epoch 2
  assert.equal(retake.epoch, 2);
  // The old credential was structurally removed by the retake, so the fence
  // can no longer name its resource.
  assertFenced(() => r.transferAll([a.token, b.token], 'z'));
  assert.equal(r.assertLease('b', b.token, 1), true);
  assert.equal(r.assertLease('a', retake.token, 2), true);
});

test('credentials of declared resources always fence, even at capacity one', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  r.declareResource('pool', 4);
  r.declareResource('single', 1);
  const p = r.acquire('pool', 'a');
  const newest = r.acquire('pool', 'b');
  const s = r.acquire('single', 'a');
  const exclusive = r.acquire('e', 'a');

  assertFenced(() => r.transferAll([exclusive.token, p.token], 'z'), 'pool');
  assertFenced(() => r.transferAll([newest.token], 'z'), 'pool');
  assertFenced(() => r.transferAll([s.token], 'z'), 'single');
  // The exclusive member of the failed mixed group is untouched.
  assert.equal(r.assertLease('e', exclusive.token, 1), true);
});

test('a removed credential can never come back through a later group', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = r.acquire('a', 'h', 100);
  const next = r.transfer(a.token, 'k', 100);
  assertFenced(() => r.transferAll([a.token], 'z'));
  // The current credential still hands over normally.
  assert.equal(r.transferAll([next.token], 'z')[0].epoch, 3);
});

test('a backwards clock reading judges liveness at the earlier instant', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 40);
  const b = r.acquire('b', 'h', 60);
  clock.set(50);
  r.stats(); // observe `a` expire
  assertFenced(() => r.transferAll([a.token, b.token], 'z'), 'a');
  clock.set(30); // backwards: a is live again
  const moved = r.transferAll([a.token, b.token], 'z', 100);
  assert.deepEqual(moved.map((x) => x.epoch), [2, 2]);
  assert.equal(moved[0].expiresAt, 130);
  assert.equal(r.checkConsistency().ok, true);
});

// ---- shape and clock validation ---------------------------------------------

test('transferAll validates the token list shape', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = r.acquire('a', 'h');
  const bad = [
    undefined,
    null,
    'not-an-array',
    {},
    42,
    [],
    [, a.token], // eslint-disable-line no-sparse-arrays
    [a.token, null],
    [a.token, undefined],
    [a.token, 42],
    [a.token, {}],
    [a.token, ''],
    [a.token, a.token],
    ['', a.token],
  ];
  for (const input of bad) {
    assert.throws(
      () => r.transferAll(input, 'b', 100),
      (e) => e instanceof TypeError,
      `expected TypeError for ${JSON.stringify(input)}`,
    );
  }
});

test('transferAll validates holder and ttl types', () => {
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = r.acquire('a', 'h');
  for (const badHolder of [1, null, undefined, {}, [], true, '']) {
    assert.throws(() => r.transferAll([a.token], badHolder, 100), TypeError);
  }
  for (const badTtl of [0, -1, NaN, Infinity, -Infinity, '100', null, true]) {
    assert.throws(() => r.transferAll([a.token], 'b', badTtl), TypeError);
  }
});

test('shape validation precedes credential lookup and the clock read', () => {
  let reads = 0;
  const r = createRegistry({ ttlMs: 100, clock: () => { reads += 1; return 0; } });
  assert.throws(() => r.transferAll([], 'b', 100), TypeError);
  assert.throws(() => r.transferAll(['unknown'], '', 100), TypeError);
  assert.throws(() => r.transferAll(['unknown'], 'b', 0), TypeError);
  assert.equal(reads, 0);
  // Nothing moved even though the only token was unknown.
  const a = r.acquire('a', 'h');
  assert.equal(r.epoch('a'), 1);
  assert.equal(r.assertLease('a', a.token, 1), true);
});

test('the clock is read exactly once on success and on a fence failure', () => {
  const clock = countingClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 50);
  const b = r.acquire('b', 'h', 50);

  clock.set(10);
  const before = clock.reads();
  r.transferAll([a.token, b.token], 'g', 100);
  assert.equal(clock.reads() - before, 1);

  const failed = clock.reads();
  assertFenced(() => r.transferAll([a.token, 'unknown'], 'g'));
  assert.equal(clock.reads() - failed, 1);
});

test('a non-finite clock reading throws TypeError and changes nothing', () => {
  let now = 0;
  const r = createRegistry({ ttlMs: 100, clock: () => now });
  const a = r.acquire('a', 'h', 100);
  now = NaN;
  assert.throws(() => r.transferAll([a.token], 'g', 100), TypeError);
  assert.equal(r.epoch('a'), 1);
  now = 20;
  assert.equal(r.assertLease('a', a.token, 1), true);
});

test('a non-finite resulting expiry throws TypeError and changes nothing', () => {
  const clock = fakeClock(1.7e308);
  const r = createRegistry({ ttlMs: 100, clock });
  const big = r.acquire('big', 'h', 1.5e308); // live; 1.7e308 + 1.5e308 = Infinity
  assert.throws(
    () => r.transferAll([big.token], 'g', 1.5e308),
    (e) => e instanceof TypeError,
  );
  assert.equal(r.assertLease('big', big.token, 1), true);
  assert.equal(r.epoch('big'), 1);
});

// ---- export and consistency -------------------------------------------------

test('exportState shows the whole group result and checkConsistency agrees', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h1', 50);
  const b = r.acquire('b', 'h2', 70);
  clock.set(20);
  const moved = r.transferAll([a.token, b.token], 'g', 30);

  const state = r.exportState(20);
  const byToken = new Map(state.leases.map((l) => [l.token, l]));
  for (const old of [a.token, b.token]) {
    assert.equal(byToken.has(old), false);
  }
  for (const entry of moved) {
    const lease = byToken.get(entry.token);
    assert.equal(lease.resource, entry.resource);
    assert.equal(lease.holder, 'g');
    assert.equal(lease.expiresAt, 50);
    assert.equal(lease.ttlMs, 30);
    assert.equal(lease.epoch, 2);
    assert.equal(lease.live, true);
    assert.equal(lease.legacy, false);
  }
  const resources = new Map(state.resources.map((x) => [x.resource, x]));
  assert.equal(resources.get('a').holder, 'g');
  assert.equal(resources.get('a').epoch, 2);
  assert.equal(resources.get('a').used, 1);
  assert.equal(resources.get('b').holder, 'g');
  assert.equal(resources.get('b').epoch, 2);
  assert.equal(state.stats.live, 2);
  assert.equal(r.checkConsistency().ok, true);
});

// ---- persistence ------------------------------------------------------------

test('a group handover appends exactly one indivisible record', () => {
  const p = logPath('one-record');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h1', 200);
  const b = r.acquire('b', 'h2', 200);
  clock.set(30);
  const moved = r.transferAll([b.token, a.token], 'g', 70);

  const lines = fs.readFileSync(p, 'utf8').trim().split('\n').map(JSON.parse);
  const groups = lines.filter((e) => e.type === 'transfer-all');
  assert.equal(groups.length, 1);
  assert.deepEqual(groups[0], {
    v: 1,
    e: 3,
    type: 'transfer-all',
    at: 30,
    holder: 'g',
    ttlMs: 70,
    expiresAt: 100,
    items: [
      { from: b.token, token: moved[0].token, resource: 'b', epoch: 2 },
      { from: a.token, token: moved[1].token, resource: 'a', epoch: 2 },
    ],
  });
});

test('a failed log write rolls the whole handover back', () => {
  const p = logPath('rollback');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h1', 100);
  const b = r.acquire('b', 'h2', 100);
  const held = r.acquire('r', 'h3', 100);
  const ticket = r.acquire('r', 'w', 50, 1000);

  sabotage(p);
  try {
    assert.throws(
      () => r.transferAll([a.token, b.token, held.token], 'g', 100),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }

  // Every old credential is still fully usable with its original epoch.
  for (const lease of [a, b, held]) {
    assert.equal(r.renew(lease.token), true);
    assert.equal(
      r.assertLease(lease.resource, lease.token, 1), true,
    );
  }
  assert.equal(r.holder('a'), 'h1');
  assert.equal(r.holder('b'), 'h2');
  assert.equal(r.holder('r'), 'h3');
  assert.equal(ticket.status, 'waiting');
  assert.deepEqual(r.stats(), {
    granted: 3, renewed: 3, released: 0, reclaimed: 0, live: 3,
  });

  // The log never received a transfer-all line.
  const types = fs.readFileSync(p, 'utf8').trim().split('\n')
    .map((line) => JSON.parse(line).type);
  assert.deepEqual(types, ['acquire', 'acquire', 'acquire', 'renew', 'renew', 'renew']);

  // A restart sees no handover at all.
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(r2.epoch('a'), 1);
  assert.equal(r2.epoch('b'), 1);
  assert.equal(r2.holder('r'), 'h3');

  // The live registry retries the same group and now takes epochs 2.
  const again = r.transferAll([a.token, b.token], 'g', 100);
  assert.deepEqual(again.map((x) => x.epoch), [2, 2]);
  assert.equal(r.holder('a'), 'g');
});

test('restart restores every new holder, shared expiry and epoch', () => {
  const p = logPath('restart');
  const clock = fakeClock(0);
  const r1 = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r1.acquire('a', 'h1', 200);
  const b = r1.acquire('b', 'h2', 200);
  clock.set(30);
  const moved = r1.transferAll([a.token, b.token], 'g', 70); // expire 100

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(40), logPath: p });
  assert.equal(r2.holder('a'), 'g');
  assert.equal(r2.holder('b'), 'g');
  assert.equal(r2.epoch('a'), 2);
  assert.equal(r2.epoch('b'), 2);
  assert.deepEqual(r2.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
  // Old and new credentials are both void across a restart.
  for (const old of [a.token, b.token]) {
    assert.equal(r2.renew(old), false);
    assertFenced(() => r2.assertLease('a', old, 1));
  }
  assertFenced(() => r2.assertLease('a', moved[0].token, 2), 'a');
  assert.equal(r2.renew(moved[0].token), false);
  assert.throws(() => r2.acquire('a', 'z'), LeaseTakenError);

  // Both leave together at the shared deadline; the next grants use epoch 3.
  assert.deepEqual(r2.sweep(99), []);
  const freed = r2.sweep(100).sort();
  assert.deepEqual(freed, ['a', 'b']);
  assert.equal(r2.acquire('a', 'z', 100).epoch, 3);
  assert.equal(r2.acquire('b', 'z', 100).epoch, 3);
});

test('compaction preserves the group result and never applies it twice', () => {
  const p = logPath('compact');
  const clock = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h1', 300);
  const b = r.acquire('b', 'h2', 300);
  const moved = r.transferAll([a.token, b.token], 'g', 300);
  r.compact();

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r2.holder('a'), 'g');
  assert.equal(r2.holder('b'), 'g');
  assert.equal(r2.epoch('a'), 2);
  assert.equal(r2.epoch('b'), 2);
  assertFenced(() => r2.assertLease('a', moved[0].token, 2), 'a');
  assert.equal(r2.checkConsistency().ok, true);
  assert.deepEqual(r2.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });

  // A group handover in the tail after compaction replays on top.
  const c = r.acquire('c', 'h3', 300);
  clock.set(60);
  r.transferAll([c.token], 'g', 300);
  const r3 = createRegistry({ ttlMs: 100, clock: fakeClock(60), logPath: p });
  assert.equal(r3.epoch('c'), 2);
  assert.equal(r3.holder('c'), 'g');
  assert.equal(r3.checkConsistency().ok, true);
});

test('a folded group next to the unswapped full log is never applied twice', () => {
  const p = logPath('swap-crash');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  clock.set(10);
  r.transferAll([a.token, b.token], 'n', 90);
  const fullLog = fs.readFileSync(p, 'utf8');
  r.compact();
  // The log swap died after the snapshot rename: the group line is back,
  // but its sequence id marks it already folded.
  fs.writeFileSync(p, fullLog);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assert.equal(r2.holder('a'), 'n');
  assert.equal(r2.holder('b'), 'n');
  assert.equal(r2.epoch('a'), 2);
  assert.equal(r2.epoch('b'), 2);
  assert.equal(r2.stats().granted, 2);
  assert.equal(r2.checkConsistency().ok, true);
});

test('an unfinished group record at the log tail is ignored in its entirety', () => {
  const p = logPath('torn');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  clock.set(20);
  const moved = r.transferAll([a.token, b.token], 'g', 80); // expires 100

  // A later group append dies mid-line, without a terminating newline.
  const partial = JSON.stringify({
    v: 1,
    type: 'transfer-all',
    at: 50,
    holder: 'z',
    ttlMs: 10,
    expiresAt: 60,
    items: moved.map((m) => ({
      from: m.token, token: 'would-be-new', resource: m.resource, epoch: 3,
    })),
  }).slice(0, 40);
  fs.appendFileSync(p, partial);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r2.holder('a'), 'g');
  assert.equal(r2.epoch('a'), 2);
  const freed = r2.sweep(100).sort();
  assert.deepEqual(freed, ['a', 'b']);
});

test('a complete but corrupt group record throws LogFileError', () => {
  const seed = (name, line) => {
    const p = logPath(name);
    fs.writeFileSync(p, [
      JSON.stringify({
        v: 1, e: 1, type: 'acquire', at: 0, token: 't1', resource: 'r',
        holder: 'a', ttlMs: 100, expiresAt: 100, epoch: 1,
      }),
      JSON.stringify({
        v: 1, e: 2, type: 'acquire', at: 0, token: 'u1', resource: 'u',
        holder: 'a', ttlMs: 100, expiresAt: 100, epoch: 1,
      }),
      JSON.stringify(line),
    ].join('\n') + '\n');
    assert.throws(
      () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
      name,
    );
  };
  const goodItem = { from: 't1', token: 't2', resource: 'r', epoch: 2 };

  seed('no-items', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
  });
  seed('empty-items', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105, items: [],
  });
  seed('bad-holder', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: '',
    ttlMs: 100, expiresAt: 105, items: [goodItem],
  });
  seed('bad-ttl', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 'soon', expiresAt: 105, items: [goodItem],
  });
  seed('bad-expiry', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: NaN, items: [goodItem],
  });
  seed('item-not-object', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105, items: ['t1'],
  });
  seed('bad-from', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [{ from: 7, token: 't2', resource: 'r', epoch: 2 }],
  });
  seed('empty-from', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [{ from: '', token: 't2', resource: 'r', epoch: 2 }],
  });
  seed('bad-token', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [{ from: 't1', token: 7, resource: 'r', epoch: 2 }],
  });
  seed('missing-resource', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [{ from: 't1', token: 't2', epoch: 2 }],
  });
  seed('bad-epoch', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [{ from: 't1', token: 't2', resource: 'r', epoch: 2.5 }],
  });
  seed('non-monotonic-epoch', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [{ from: 't1', token: 't2', resource: 'r', epoch: 9 }],
  });
  seed('unknown-from', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [{ from: 'never-minted', token: 't2', resource: 'r', epoch: 2 }],
  });
  seed('duplicate-from', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [
      { from: 't1', token: 't2', resource: 'r', epoch: 2 },
      { from: 't1', token: 't3', resource: 'u', epoch: 2 },
    ],
  });
  seed('duplicate-new-token', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [
      { from: 't1', token: 't2', resource: 'r', epoch: 2 },
      { from: 'u1', token: 't2', resource: 'u', epoch: 2 },
    ],
  });
  seed('duplicate-resource', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [
      { from: 't1', token: 't2', resource: 'r', epoch: 2 },
      { from: 'u1', token: 't3', resource: 'r', epoch: 3 },
    ],
  });
  seed('new-token-is-another-from', {
    v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'b',
    ttlMs: 100, expiresAt: 105,
    items: [
      { from: 't1', token: 'u1', resource: 'r', epoch: 2 },
      { from: 'u1', token: 't3', resource: 'u', epoch: 2 },
    ],
  });
});

test('older logs without group records keep replaying unchanged', () => {
  const p = logPath('legacy-log');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h', 40);
  clock.set(10);
  r.renew(a.token);
  const moved = r.transfer(a.token, 'g', 40); // plain single transfer line

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assert.equal(r2.holder('a'), 'g');
  assert.equal(r2.epoch('a'), 2);
  assertFenced(() => r2.assertLease('a', moved.token, 2), 'a');
  assert.equal(r2.stats().renewed, 1);
});

test('transferAll works without a logPath and writes nothing', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 50);
  clock.set(10);
  const moved = r.transferAll([a.token], 'g', 50);
  assert.equal(moved[0].expiresAt, 60);
  assert.equal(r.stats().granted, 1);
  assert.equal(r.checkConsistency().ok, true);
});
