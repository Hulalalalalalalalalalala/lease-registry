import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  createRegistry,
  LeaseTakenError,
  QuotaExceededError,
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

let dir;
test.beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-epoch-'));
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

// ---- epoch() basics --------------------------------------------------------

test('a never-granted resource reports epoch 0; the first grant opens epoch 1', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  assert.equal(registry.epoch('r'), 0);
  const lease = registry.acquire('r', 'h');
  assert.equal(lease.epoch, 1);
  assert.equal(registry.epoch('r'), 1);
});

test('every successful credential grants a strictly larger epoch', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });

  const first = registry.acquire('r', 'a');
  assert.equal(first.epoch, 1);

  // Retake after an explicit release.
  assert.equal(registry.release(first.token), true);
  assert.equal(registry.epoch('r'), 1, 'release never rolls the epoch back');
  const second = registry.acquire('r', 'b');
  assert.equal(second.epoch, 2);

  // Retake after a sweep.
  clock.set(150);
  assert.deepEqual(registry.sweep(), ['r']);
  assert.equal(registry.epoch('r'), 2);
  const third = registry.acquire('r', 'c', 100);
  assert.equal(third.epoch, 3);

  // Retake by the very same holder after expiry.
  clock.set(300);
  const fourth = registry.acquire('r', 'c', 100);
  assert.equal(fourth.epoch, 4);
  assert.equal(registry.epoch('r'), 4);
});

test('a waiter woken by sweep, then by release, receives successive epochs', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('r', 'a', 10);
  const ticket = registry.acquire('r', 'b', 50, 1000);

  clock.set(10);
  assert.deepEqual(registry.sweep(), ['r']);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.epoch, 2);
  assert.equal(registry.epoch('r'), 2);

  const w2 = registry.acquire('r', 'c', 50, 1000);
  assert.equal(registry.release(ticket.token), true);
  assert.equal(w2.status, 'granted');
  assert.equal(w2.epoch, 3);
});

test('renew extends expiresAt but never moves the epoch', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'h', 40);
  clock.set(10);
  assert.equal(registry.renew(lease.token), true);
  assert.equal(lease.epoch, 1);
  assert.equal(registry.epoch('r'), 1);
  assert.equal(registry.assertLease('r', lease.token, 1), true);
});

// ---- epoch consumption is exactly-on-commit --------------------------------

test('failed, queued, timed-out and cancelled acquires consume no epoch', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const held = registry.acquire('r', 'a', 100);

  assert.throws(() => registry.acquire('r', 'b'), LeaseTakenError);
  assert.equal(registry.epoch('r'), 1);

  const waiting = registry.acquire('r', 'b', 10, 5);
  assert.equal(registry.epoch('r'), 1, 'queueing consumes no epoch');

  clock.set(6);
  registry.holder('r'); // observe the timeout
  assert.equal(waiting.status, 'expired');
  assert.equal(registry.epoch('r'), 1);

  const cancelled = registry.acquire('r', 'c', 10, 1000);
  assert.equal(registry.cancel(cancelled.waitId), true);
  assert.equal(registry.epoch('r'), 1);

  // Once the holder leaves, the next real grant is exactly epoch 2.
  assert.equal(registry.release(held.token), true);
  assert.equal(registry.acquire('r', 'd').epoch, 2);
});

test('a partially blocked acquireAll rejects the whole group and moves no epoch', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.acquire('b', 'owner');

  assert.throws(
    () => registry.acquireAll(['a', 'b', 'c'], 'h'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN',
  );
  assert.equal(registry.epoch('a'), 0);
  assert.equal(registry.epoch('b'), 1);
  assert.equal(registry.epoch('c'), 0);

  // The untouched resources still open at epoch 1.
  const leases = registry.acquireAll(['a', 'c'], 'h2', 10);
  assert.deepEqual(leases.map((l) => l.epoch), [1, 1]);
});

test('a quota-blocked group moves no epoch on any resource', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.setQuota('q', 1);
  registry.declareResource('a', 10, 'q');
  registry.declareResource('b', 10, 'q');
  registry.acquire('a', 'x');

  assert.throws(
    () => registry.acquireAll(['a', 'b'], 'g'),
    (e) => e instanceof QuotaExceededError && e.code === 'QUOTA_EXCEEDED',
  );
  assert.equal(registry.epoch('a'), 1);
  assert.equal(registry.epoch('b'), 0);
});

test('a failed log write on grant rolls the epoch back', () => {
  const p = logPath('rollback-epoch');
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  registry.acquire('r', 'h');

  sabotage(p);
  try {
    assert.throws(
      () => registry.acquire('s', 'h'),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }
  assert.equal(registry.epoch('s'), 0);
  const retry = registry.acquire('s', 'h');
  assert.equal(retry.epoch, 1);
});

// ---- acquireAll epochs -----------------------------------------------------

test('acquireAll commits every resource epoch together and once', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const leases = registry.acquireAll(['a', 'b', 'a'], 'h', [40, 50, 60]);
  assert.deepEqual(leases.map((l) => l.resource), ['a', 'b', 'a']);
  assert.deepEqual(leases.map((l) => l.epoch), [1, 1, 2]);
  assert.equal(registry.epoch('a'), 2);
  assert.equal(registry.epoch('b'), 1);
});

test('a woken group carries the next epoch on every resource', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h1');
  const b = registry.acquire('b', 'h2');
  const group = registry.acquireAll(['a', 'b'], 'G', [40, 60], 1000);

  clock.set(10);
  registry.release(b.token);
  assert.equal(group.status, 'waiting');
  registry.release(a.token);
  assert.equal(group.status, 'granted');
  assert.deepEqual(group.leases.map((l) => l.epoch), [2, 2]);
  assert.equal(registry.epoch('a'), 2);
  assert.equal(registry.epoch('b'), 2);
});

test('several groups woken in one cascade draw strictly increasing epochs', () => {
  // A share resource lets two queued groups wake in the same cascade and take
  // one freed share each; their credentials on that one resource must still
  // get strictly increasing epochs.
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.declareResource('pool', 2);
  registry.acquire('pool', 'h', 10);
  registry.acquire('pool', 'h', 10);
  const g1 = registry.acquireAll([{ resource: 'pool', count: 1 }], 'G1', 30, 1000);
  const g2 = registry.acquireAll([{ resource: 'pool', count: 1 }], 'G2', 30, 1000);

  clock.set(10);
  assert.deepEqual(registry.sweep(), ['pool', 'pool']);
  assert.equal(g1.status, 'granted');
  assert.equal(g2.status, 'granted');
  assert.equal(g1.leases[0].epoch, 3);
  assert.equal(g2.leases[0].epoch, 4);
  assert.equal(registry.epoch('pool'), 4);
});

// ---- share resources -------------------------------------------------------

test('a share resource only asserts the credential carrying its last epoch', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.declareResource('pool', 4);
  const a = registry.acquire('pool', 'a');
  assert.equal(registry.assertLease('pool', a.token, 1), true);
  const b = registry.acquire('pool', 'b');
  assert.equal(b.epoch, 2);
  assert.equal(registry.epoch('pool'), 2);

  // Both shares are live at once, but the fence requires the epoch to equal
  // the resource's last committed epoch: the older coexisting credential is
  // fenced while the newest one asserts.
  assertFenced(() => registry.assertLease('pool', a.token, 1), 'pool');
  assert.equal(registry.assertLease('pool', b.token, 2), true);

  // Releasing the newest share advances nothing; its replacement gets epoch 3
  // and is then the only asserting credential.
  assert.equal(registry.release(b.token), true);
  assertFenced(() => registry.assertLease('pool', b.token, 2), 'pool');
  const c = registry.acquire('pool', 'c');
  assert.equal(c.epoch, 3);
  assertFenced(() => registry.assertLease('pool', a.token, 1));
  assert.equal(registry.assertLease('pool', c.token, 3), true);
});

// ---- assertLease -----------------------------------------------------------

test('assertLease returns true only for the current live credential and epoch', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  assert.equal(registry.assertLease('r', lease.token, 1), true);

  // A stale or future epoch argument fences even with the live token.
  assertFenced(() => registry.assertLease('r', lease.token, 2), 'r');
  assertFenced(() => registry.assertLease('r', lease.token, 0), 'r');

  // Taking over fences the previous credential even at its own epoch.
  assert.equal(registry.release(lease.token), true);
  assertFenced(() => registry.assertLease('r', lease.token, 1));
  const next = registry.acquire('r', 'h2');
  assertFenced(() => registry.assertLease('r', lease.token, 1));
  assert.equal(registry.assertLease('r', next.token, 2), true);
});

test('expired, unknown, released and other-holder credentials are fenced', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'h', 10);

  // Unknown resource (never granted) and unknown token.
  assertFenced(() => registry.assertLease('nope', 'whatever', 1), 'nope');
  assertFenced(() => registry.assertLease('r', 'lease-nope', 1), 'r');

  // Expired credential.
  clock.set(11);
  assertFenced(() => registry.assertLease('r', lease.token, 1), 'r');

  // A credential of another resource fences when presented for this one.
  const other = registry.acquire('other', 'hx', 100);
  assertFenced(() => registry.assertLease('r', other.token, 1), 'r');

  // The expired lease is structurally retaken (epoch 2), then released.
  const live = registry.acquire('r', 'h2', 100);
  assert.equal(live.epoch, 2);
  assert.equal(registry.assertLease('r', live.token, 2), true);
  assert.equal(registry.release(live.token), true);
  assertFenced(() => registry.assertLease('r', live.token, 2), 'r');
});

test('assertLease validates its argument types', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  for (const badResource of [1, null, undefined, {}, [], true]) {
    assert.throws(
      () => registry.assertLease(badResource, lease.token, 1),
      TypeError,
    );
  }
  for (const badToken of [1, null, undefined, {}, [], true]) {
    assert.throws(
      () => registry.assertLease('r', badToken, 1),
      TypeError,
    );
  }
  for (const badEpoch of [1.5, NaN, Infinity, -Infinity, '1', null, undefined, true]) {
    assert.throws(
      () => registry.assertLease('r', lease.token, badEpoch),
      TypeError,
    );
  }
});

// ---- persistence -----------------------------------------------------------

test('epochs ride the acquire event and recover without advancing again', () => {
  const p = logPath('recover');
  const clock = fakeClock(0);
  let registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = registry.acquire('r', 'h1');
  registry.release(a.token);
  registry.acquire('r', 'h2', 10); // epoch 2, expires at t=10
  registry.acquire('other', 'h3'); // epoch 1

  // Restart past the epoch-2 lease's expiry.
  registry = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  assert.equal(registry.epoch('r'), 2);
  assert.equal(registry.epoch('other'), 1);
  assert.equal(registry.epoch('gone'), 0);

  // Recovery did not re-grant anything, so the expired-unswept retake is
  // epoch 3 and the counters were restored rather than re-counted.
  const fresh = registry.acquire('r', 'h4', 100);
  assert.equal(fresh.epoch, 3);
  assert.equal(registry.stats().granted, 4);
});

test('credentials reloaded from the log are always fenced', () => {
  const p = logPath('fence-restart');
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const lease = r1.acquire('r', 'h');
  assert.equal(r1.assertLease('r', lease.token, 1), true);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(r2.epoch('r'), 1);
  assertFenced(() => r2.assertLease('r', lease.token, 1), 'r');

  // The lease still occupies its resource after the fence.
  assert.throws(() => r2.acquire('r', 'other'), LeaseTakenError);
});

test('epochs survive compaction for resources with and without a live lease', () => {
  const p = logPath('compact');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  registry.acquire('keep', 'h', 200);
  const freed = registry.acquire('freed', 'h', 200);
  registry.release(freed.token); // freed has no live lease but epoch stays 1
  registry.acquire('freed', 'h2', 200); // epoch 2
  registry.declareResource('pool', 3);
  registry.acquire('pool', 'p1', 200);
  registry.acquire('pool', 'p2', 200);
  registry.compact();

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(250), logPath: p });
  assert.equal(r2.epoch('keep'), 1);
  assert.equal(r2.epoch('freed'), 2);
  assert.equal(r2.epoch('pool'), 2);
  r2.sweep(); // every captured lease is past its ttl at t=250

  // Post-compaction grants continue the recovered epochs.
  assert.equal(r2.acquire('freed', 'h3', 100).epoch, 3);
  assert.equal(r2.acquire('pool', 'p3', 100).epoch, 3);
});

test('post-compaction tail grants replay on top of recovered epochs', () => {
  const p = logPath('compact-tail');
  const clock = fakeClock(0);
  const r1 = createRegistry({ ttlMs: 100, clock, logPath: p });
  const first = r1.acquire('r', 'h');
  r1.release(first.token);
  r1.compact();
  const tail = r1.acquire('r', 'h2', 100); // epoch 2 after compaction
  assert.equal(tail.epoch, 2);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(r2.epoch('r'), 2);
  assertFenced(() => r2.assertLease('r', tail.token, 2));
});

test('a compacted restart matches a full-log restart epoch for epoch', () => {
  const p1 = logPath('full');
  const p2 = logPath('fold');
  function build(p, clock) {
    const r = createRegistry({ ttlMs: 100, clock, logPath: p });
    const a = r.acquire('a', 'h', 100);
    r.acquire('b', 'h', 10);
    r.release(a.token);
    r.acquire('a', 'h2', 100); // epoch 2
    clock.set(20);
    r.sweep(); // b reclaimed
    return r;
  }
  const c1 = fakeClock(0);
  const c2 = fakeClock(0);
  build(p1, c1);
  const folded = build(p2, c2);
  c1.set(20);
  c2.set(20);
  folded.compact();

  const full = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p1 });
  const rec = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p2 });
  for (const resource of ['a', 'b']) {
    assert.equal(rec.epoch(resource), full.epoch(resource), resource);
  }
  assert.equal(rec.epoch('a'), 2);
  assert.equal(rec.epoch('b'), 1);
});

test('a non-monotonic or missing epoch in the log is a LogFileError', () => {
  const p = logPath('bad-log');
  fs.writeFileSync(p, [
    JSON.stringify({
      v: 1, e: 1, type: 'acquire', at: 0, token: 't1', resource: 'r',
      holder: 'h', ttlMs: 100, expiresAt: 100, epoch: 1, batch: 'b1',
    }),
    JSON.stringify({
      v: 1, e: 2, type: 'acquire', at: 1, token: 't2', resource: 'r',
      holder: 'h2', ttlMs: 100, expiresAt: 101, epoch: 1, batch: 'b2',
    }),
  ].join('\n') + '\n');
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  fs.writeFileSync(p, JSON.stringify({
    v: 1, e: 1, type: 'acquire', at: 0, token: 't1', resource: 'r',
    holder: 'h', ttlMs: 100, expiresAt: 100,
  }) + '\n');
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
});

test('a corrupt epoch in a snapshot throws and the untouched log recovers', () => {
  const p = logPath('bad-snapshot');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  r.acquire('r', 'h', 100);
  const fullLog = fs.readFileSync(p, 'utf8');
  r.compact();
  fs.writeFileSync(p, fullLog);

  const imagePath = `${p}.snapshot`;
  const image = JSON.parse(fs.readFileSync(imagePath, 'utf8'));

  // A malformed epoch entry.
  image.epochs.push({ resource: 'r', epoch: 'not a number' });
  fs.writeFileSync(imagePath, `${JSON.stringify(image)}\n`);
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  // A lease whose epoch runs past its resource's last committed epoch.
  image.epochs = [{ resource: 'r', epoch: 1 }];
  image.leases[0].epoch = 9;
  fs.writeFileSync(imagePath, `${JSON.stringify(image)}\n`);
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  // A lease missing its epoch while the image carries the epoch section.
  image.leases[0].epoch = undefined;
  fs.writeFileSync(imagePath, `${JSON.stringify(image)}\n`);
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  // The original full log was never touched: drop the image and recover.
  fs.rmSync(imagePath);
  const recovered = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(recovered.epoch('r'), 1);
});
