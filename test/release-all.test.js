import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  createRegistry,
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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-releaseall-'));
});

test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

test('releaseAll ends every lease at once and counts each credential', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h1', 40);
  const b = r.acquire('b', 'h2', 70);
  const c = r.acquire('c', 'h3', 90);

  clock.set(20);
  assert.equal(r.releaseAll([a.token, c.token]), true);

  assert.equal(r.holder('a'), null);
  assert.equal(r.holder('c'), null);
  assert.equal(r.holder('b'), 'h2');
  assert.equal(r.renew(a.token), false);
  assert.equal(r.release(c.token), false);
  assert.equal(r.renew(b.token), true);

  // A release advances no epoch.
  assert.equal(r.epoch('a'), a.epoch);
  assert.equal(r.epoch('c'), c.epoch);
  assert.equal(r.epoch('b'), b.epoch);

  assert.deepEqual(r.stats(), {
    granted: 3,
    renewed: 1,
    released: 2,
    reclaimed: 0,
    live: 1,
  });
  assert.equal(r.checkConsistency().ok, true);

  const exported = r.exportState(20);
  const tokens = exported.leases.map((l) => l.token);
  assert.deepEqual(tokens.sort(), [b.token]);
});

test('releaseAll mixes exclusive leases with several shares of one pool', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 4);
  const solo = r.acquire('solo', 'h0', 100);
  const shares = r.acquireAll([{ resource: 'pool', count: 3 }], 'h1', 50);
  const other = r.acquireAll([{ resource: 'pool', count: 1 }], 'h2', 50)[0];

  clock.set(10);
  assert.equal(
    r.releaseAll([solo.token, shares[0].token, shares[2].token]),
    true,
  );

  assert.equal(r.holder('solo'), null);
  const st = r.exportState(10);
  const poolLive = st.resources.find((x) => x.resource === 'pool');
  assert.equal(poolLive.used, 2);
  const holders = st.leases
    .filter((l) => l.resource === 'pool')
    .map((l) => l.holder)
    .sort();
  assert.deepEqual(holders, ['h1', 'h2']);
  // Releasing shares advances no epoch; the pool remembers the last grant.
  assert.equal(r.epoch('pool'), 4);
  assert.equal(r.epoch('solo'), solo.epoch);
  assert.deepEqual(
    { ...r.stats(), live: r.stats().live },
    { granted: 5, renewed: 0, released: 3, reclaimed: 0, live: 2 },
  );
  assert.equal(r.checkConsistency().ok, true);
  // The unreleased share still works.
  assert.equal(r.renew(shares[1].token), true);
  assert.equal(r.renew(other.token), true);
});

test('releaseAll validates the shape before looking anything up or reading the clock', () => {
  const clock = countingClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 100);
  const baseline = clock.reads();

  for (const bad of [
    undefined,
    null,
    'token',
    {},
    [],
    ['x', , 'y'], // eslint-disable-line no-sparse-arrays
    ['x', 42],
    ['x', ''],
    [a.token, a.token],
  ]) {
    assert.throws(() => r.releaseAll(bad), TypeError);
  }
  assert.equal(clock.reads() - baseline, 0);
  // A well-formed list of unknown tokens reads the clock once and returns
  // false rather than throwing.
  assert.equal(r.releaseAll(['does-not-exist']), false);
  assert.equal(clock.reads() - baseline, 1);
});

test('releaseAll reads the injected clock exactly once', () => {
  const clock = countingClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  const before = clock.reads();
  assert.equal(r.releaseAll([a.token, b.token]), true);
  assert.equal(clock.reads() - before, 1);
});

test('releaseAll throws TypeError on a non-finite clock reading', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 100);

  clock.set(NaN);
  assert.throws(() => r.releaseAll([a.token]), TypeError);
  clock.set(Infinity);
  assert.throws(() => r.releaseAll([a.token]), TypeError);
  // Nothing changed.
  clock.set(10);
  assert.equal(r.renew(a.token), true);
  assert.equal(r.stats().released, 0);
});

test('releaseAll returns false and changes nothing when one credential is unusable', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const good = r.acquire('good', 'h', 100);
  const released = r.acquire('released', 'h', 100);
  assert.equal(r.release(released.token), true);

  const replaced = r.acquire('moved', 'h', 100);
  const replacement = r.transfer(replaced.token, 'h2', 100);

  const short = r.acquire('short', 'h', 10);
  const reclaimed = r.acquire('reclaimedx', 'h', 5);
  clock.set(5);
  assert.deepEqual(r.sweep(), ['reclaimedx']);
  clock.set(10);
  // `short` is expired at this reading but not swept.

  const cases = [
    ['unknown token', ['no-such-token', good.token]],
    ['already released', [released.token, good.token]],
    ['transfer-replaced', [replaced.token, good.token]],
    ['reclaimed', [reclaimed.token, good.token]],
    ['expired at the reading', [short.token, good.token]],
  ];
  const snapshotBefore = JSON.stringify(r.exportState(10));
  for (const [name, tokens] of cases) {
    assert.equal(r.releaseAll(tokens), false, name);
    assert.equal(JSON.stringify(r.exportState(10)), snapshotBefore, name);
  }
  // The good credential and the transferred one are fully intact.
  assert.equal(r.renew(good.token), true);
  assert.equal(r.renew(replacement.token), true);
  assert.equal(r.stats().released, 1);
  assert.equal(r.stats().reclaimed, 1);
});

test('releaseAll judges a backwards clock reading by expiresAt > now', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const a = r.acquire('a', 'h', 100);
  clock.set(150);
  // At this reading the lease is expired but unswept.
  assert.equal(r.exportState(150).leases[0].live, false);
  // An earlier reading revives it for the batch judgment.
  clock.set(50);
  assert.equal(r.releaseAll([a.token]), true);
  assert.equal(r.stats().released, 1);
  assert.equal(r.holder('a'), null);
});

test('a credential from another instance cannot be batch released', () => {
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const l1 = r1.acquire('res', 'a');
  const l2 = r2.acquire('res', 'b');
  assert.equal(r1.releaseAll([l1.token, l2.token]), false);
  assert.equal(r1.holder('res'), 'a');
  assert.equal(r2.holder('res'), 'b');
  assert.equal(r1.stats().released, 0);
});

test('freed shares together fund singles and groups in arrival order', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.declareResource('pool', 3);

  const solo = r.acquire('solo', 'h', 100);
  const pool = r.acquireAll([{ resource: 'pool', count: 3 }], 'h', 100);

  // Arrival 1: group needs solo + one pool share.
  const g1 = r.acquireAll(['solo', 'pool'], 'g1', [60, 70], 1000);
  assert.equal(g1.status, 'waiting');
  // Arrival 2: single wants one pool share.
  const s2 = r.acquire('pool', 'g2', 80, 1000);
  assert.equal(s2.status, 'waiting');
  // Arrival 3: group wants two pool shares - cannot fit behind the others.
  const g3 = r.acquireAll([{ resource: 'pool', count: 2 }], 'g3', 90, 1000);
  assert.equal(g3.status, 'waiting');

  clock.set(20);
  // Free the exclusive resource and two pool shares at once.
  assert.equal(r.releaseAll([solo.token, pool[0].token, pool[1].token]), true);

  assert.equal(g1.status, 'granted');
  assert.equal(Array.isArray(g1.leases), true);
  assert.deepEqual(g1.leases.map((l) => l.resource), ['solo', 'pool']);
  assert.equal(g1.leases[0].expiresAt, 80); // 20 + 60
  assert.equal(g1.leases[1].expiresAt, 90); // 20 + 70
  assert.equal(s2.status, 'granted');
  assert.equal(s2.resource, 'pool');
  assert.equal(s2.expiresAt, 100); // 20 + 80
  // Only one pool share is left; the two-share group stays waiting.
  assert.equal(g3.status, 'waiting');

  assert.equal(r.holder('solo'), 'g1');
  // 4 initial credentials (solo + 3 pool), 3 cascade grants; live shares
  // are the one untouched pool share plus the three granted credentials.
  assert.deepEqual(
    r.stats(),
    { granted: 7, renewed: 0, released: 3, reclaimed: 0, live: 4 },
  );
  assert.equal(r.checkConsistency(20).ok, true);

  // Freeing the last share still cannot satisfy the two-share group.
  assert.equal(r.releaseAll([pool[2].token]), true);
  assert.equal(g3.status, 'waiting');
});

test('the wake result does not depend on the input order', () => {
  function build() {
    const r = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
    const solo = r.acquire('solo', 'h', 100);
    const other = r.acquire('other', 'h', 100);
    const group = r.acquireAll(['solo', 'other'], 'g', 40, 1000);
    const single = r.acquire('other', 's', 50, 1000);
    return { r, solo, other, group, single };
  }

  const a = build();
  const b = build();
  a.r.releaseAll([a.solo.token, a.other.token]);
  b.r.releaseAll([b.other.token, b.solo.token]);

  for (const side of [a, b]) {
    assert.equal(side.group.status, 'granted');
    assert.equal(side.single.status, 'waiting');
    assert.deepEqual(
      side.group.leases.map((l) => l.resource),
      ['solo', 'other'],
    );
  }
  assert.equal(a.r.holder('solo'), 'g');
  assert.equal(b.r.holder('solo'), 'g');
  assert.equal(a.r.holder('other'), 'g');
  assert.equal(b.r.holder('other'), 'g');
});

test('past-deadline tickets expire and leave every queue during releaseAll', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  const x = r.acquire('x', 'h', 100);
  const y = r.acquire('y', 'h', 100);
  const stale = r.acquire('x', 'late', 100, 10);
  const fresh = r.acquire('y', 'on-time', 100, 1000);
  assert.equal(stale.status, 'waiting');
  assert.equal(fresh.status, 'waiting');

  clock.set(50);
  assert.equal(r.releaseAll([x.token, y.token]), true);
  assert.equal(stale.status, 'expired');
  assert.equal(fresh.status, 'granted');
  assert.equal(r.checkConsistency(50).ok, true);
});

test('freed quota room on one resource wakes a head request on another', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  r.setQuota('root', 2);
  r.declareResource('pa', 2, 'root');
  r.declareResource('pb', 2, 'root');

  // Quota full: one share on pa, one on pb.
  const a = r.acquireAll([{ resource: 'pa', count: 1 }], 'h', 100)[0];
  r.acquireAll([{ resource: 'pb', count: 1 }], 'h', 100);
  // pb has local room but the root quota is full; the waiter queues.
  const ticket = r.acquireAll(['pb'], 'w', 30, 1000);
  assert.equal(ticket.status, 'waiting');

  clock.set(10);
  assert.equal(r.releaseAll([a.token]), true);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.leases[0].expiresAt, 40);
  assert.equal(r.checkConsistency(10).ok, true);
});

test('a non-finite granted expiry throws TypeError with no state change', () => {
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock });
  // Targets stay live at the huge reading; the queued grant would overflow.
  const q = r.acquire('q', 'h', 1.5e308);
  const ticket = r.acquire('q', 'w', 1e308, 1.5e308);
  assert.equal(ticket.status, 'waiting');

  clock.set(1e308);
  assert.throws(() => r.releaseAll([q.token]), TypeError);
  assert.equal(ticket.status, 'waiting');
  assert.equal(r.stats().released, 0);
  assert.equal(r.stats().granted, 1);
  const exported = r.exportState(1e308);
  assert.deepEqual(exported.leases.map((l) => l.token), [q.token]);
});

test('releaseAll and its cascade are persisted and replayed exactly once', () => {
  const p = logPath('replay');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  const waiter = r.acquire('b', 'w', 60, 1000);

  c.set(20);
  assert.equal(r.releaseAll([a.token, b.token]), true);
  assert.equal(waiter.status, 'granted');
  const grantedToken = waiter.token;
  assert.deepEqual(r.stats(), {
    granted: 3, renewed: 0, released: 2, reclaimed: 0, live: 1,
  });
  assert.ok(fs.readFileSync(p, 'utf8').includes('"type":"release-all"'));

  r = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  assert.equal(r.holder('a'), null);
  assert.equal(r.holder('b'), 'w');
  assert.deepEqual(r.stats(), {
    granted: 3, renewed: 0, released: 2, reclaimed: 0, live: 1,
  });
  // Queued requests are not recovered, and pre-restart credentials are void.
  assert.equal(r.releaseAll([a.token, grantedToken]), false);
  assert.equal(r.checkConsistency(20).ok, true);
});

test('releaseAll survives compaction with holders, epochs and counters', () => {
  const p = logPath('compact');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  const w = r.acquire('b', 'w', 80, 1000);
  c.set(10);
  r.releaseAll([a.token, b.token]);
  assert.equal(w.status, 'granted');
  const epochB = r.epoch('b');
  r.compact(10);

  r = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assert.equal(r.holder('a'), null);
  assert.equal(r.holder('b'), 'w');
  assert.equal(r.epoch('a'), 1);
  assert.equal(r.epoch('b'), epochB);
  assert.deepEqual(r.stats(), {
    granted: 3, renewed: 0, released: 2, reclaimed: 0, live: 1,
  });
  assert.equal(r.checkConsistency(10).ok, true);
});

test('a failed append rolls leases, counters, queues and tickets back', () => {
  const p = logPath('rollback');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  const waiter = r.acquire('b', 'w', 60, 1000);

  c.set(20);
  sabotage(p);
  try {
    assert.throws(
      () => r.releaseAll([a.token, b.token]),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }

  assert.equal(waiter.status, 'waiting');
  assert.equal(r.holder('a'), 'h');
  assert.equal(r.holder('b'), 'h');
  assert.deepEqual(r.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
  // The identical call retries cleanly.
  assert.equal(r.releaseAll([a.token, b.token]), true);
  assert.equal(waiter.status, 'granted');
  assert.equal(waiter.expiresAt, 80);
  assert.deepEqual(r.stats(), {
    granted: 3, renewed: 0, released: 2, reclaimed: 0, live: 1,
  });
});

test('an unfinished release-all tail record is ignored; a damaged one throws', () => {
  const torn = logPath('torn');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: torn });
  const a = r.acquire('a', 'h', 100);
  const b = r.acquire('b', 'h', 100);
  // Simulate an append that died mid-line: no terminating newline.
  fs.appendFileSync(
    torn,
    JSON.stringify({
      v: 1, type: 'release-all', at: 20, items: [{ token: a.token, resource: 'a' }],
    }).slice(0, 30),
  );

  r = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: torn });
  assert.equal(r.holder('a'), 'h');
  assert.equal(r.holder('b'), 'h');
  assert.equal(r.stats().released, 0);
  // The fragment was trimmed; the log accepts new complete lines.
  const again = r.acquire('c', 'h', 100);
  assert.equal(again.holder, 'h');

  const damaged = logPath('damaged');
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: damaged });
  const d = r2.acquire('d', 'h', 100);
  fs.appendFileSync(
    damaged,
    `${JSON.stringify({ v: 1, type: 'release-all', at: 10, items: [{ token: d.token }] })}\n`,
  );
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: damaged }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
});
