import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  createRegistry,
  LeaseFencedError,
  LeaseTakenError,
  QuotaExceededError,
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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-epoch-'));
});

test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

function expectFenced(fn) {
  assert.throws(
    fn,
    (e) => e instanceof LeaseFencedError && e.code === 'LEASE_FENCED',
  );
}

test('epochs start at zero, then one and advance per committed credential', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  assert.equal(registry.epoch('r'), 0);

  const first = registry.acquire('r', 'a', 50);
  assert.equal(first.epoch, 1);
  assert.equal(registry.epoch('r'), 1);
  assert.equal(registry.assertLease('r', first.token, 1), true);
});

test('a release followed by a retake produces the next epoch, same holder too', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const first = registry.acquire('r', 'a', 50);
  registry.release(first.token);
  assert.equal(registry.epoch('r'), 1);

  const second = registry.acquire('r', 'a', 50);
  assert.equal(second.epoch, 2);
  assert.equal(registry.epoch('r'), 2);
  // The first credential is a past epoch and fenced even though it was
  // released cleanly.
  expectFenced(() => registry.assertLease('r', first.token, 1));
  assert.equal(registry.assertLease('r', second.token, 2), true);
});

test('a sweep-then-retake advances; epoch never falls back with no live lease', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const first = registry.acquire('r', 'a', 10);
  clock.set(10);
  assert.deepEqual(registry.sweep(), ['r']);
  assert.equal(registry.epoch('r'), 1);
  expectFenced(() => registry.assertLease('r', first.token, 1));

  clock.set(20);
  const second = registry.acquire('r', 'b', 10);
  assert.equal(second.epoch, 2);
  assert.equal(registry.epoch('r'), 2);
});

test('renew extends the lease but never changes the epoch', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 50);
  clock.set(40);
  assert.equal(registry.renew(lease.token), true);
  assert.equal(registry.epoch('r'), 1);
  assert.equal(registry.assertLease('r', lease.token, 1), true);
  assert.equal(lease.epoch, 1);
});

test('failed contention, queueing, timeout and cancel consume no epoch', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('r', 'a', 100);

  assert.throws(() => registry.acquire('r', 'b', 50), LeaseTakenError);
  assert.equal(registry.epoch('r'), 1);

  const waiting = registry.acquire('r', 'b', 50, 1000);
  assert.equal(waiting.status, 'waiting');
  assert.equal('epoch' in waiting, false);
  assert.equal(registry.epoch('r'), 1);

  clock.set(2000);
  registry.holder('r'); // observe the timeout
  assert.equal(waiting.status, 'expired');
  assert.equal(registry.epoch('r'), 1);

  // Queueing and timing out on a separate held resource.
  const qClock = fakeClock(0);
  const held = createRegistry({ ttlMs: 100, clock: qClock });
  held.acquire('s', 'owner', 10_000);
  const other = held.acquire('s', 'c', 50, 1000);
  assert.equal(other.status, 'waiting');
  assert.equal(held.epoch('s'), 1);
  qClock.set(3000);
  held.holder('s'); // observe the timeout
  assert.equal(other.status, 'expired');
  assert.equal(held.epoch('s'), 1);

  // A later queued request that cancels while still in line.
  const queued = held.acquire('s', 'd', 50, 1000);
  assert.equal(queued.status, 'waiting');
  assert.equal(held.cancel(queued.waitId), true);
  assert.equal(held.cancel(queued.waitId), false);
  assert.equal(held.epoch('s'), 1);
});

test('a woken waiter receives the next epoch and can fence with it', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 100);
  const ticket = registry.acquire('r', 'b', 40, 1000);

  clock.set(10);
  registry.release(lease.token);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.epoch, 2);
  assert.equal(registry.epoch('r'), 2);
  assert.equal(registry.assertLease('r', ticket.token, 2), true);
});

test('acquireAll commits every resource epoch atomically', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const leases = registry.acquireAll(['a', 'b'], 'h', 50);
  assert.deepEqual(leases.map((l) => l.epoch), [1, 1]);
  assert.equal(registry.epoch('a'), 1);
  assert.equal(registry.epoch('b'), 1);
  for (const lease of leases) {
    registry.release(lease.token);
  }

  // The same resource demanded twice commits two consecutive epochs at once.
  const repeated = registry.acquireAll(['a', 'a'], 'h', 50);
  assert.deepEqual(repeated.map((l) => l.epoch), [2, 3]);
  assert.equal(registry.epoch('a'), 3);
});

test('a rejected group changes no epoch on any resource', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.acquire('a', 'x', 50);
  registry.acquire('b', 'y', 50);

  assert.throws(
    () => registry.acquireAll(['a', 'c'], 'z', 50),
    LeaseTakenError,
  );
  assert.equal(registry.epoch('a'), 1);
  assert.equal(registry.epoch('b'), 1);
  assert.equal(registry.epoch('c'), 0);
});

test('a quota refusal moves no epoch', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.setQuota('q', 1);
  registry.declareResource('pool', 5, 'q');
  registry.acquire('pool', 'a', 50);
  assert.throws(
    () => registry.acquire('pool', 'b', 50),
    (e) => e instanceof QuotaExceededError && e.code === 'QUOTA_EXCEEDED',
  );
  assert.equal(registry.epoch('pool'), 1);
});

test('each share of a shared resource carries its own advancing epoch', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.declareResource('pool', 3);
  const two = registry.acquireAll([{ resource: 'pool', count: 2 }], 'a', 50);
  assert.deepEqual(two.map((l) => l.epoch), [1, 2]);
  const third = registry.acquire('pool', 'b', 50);
  assert.equal(third.epoch, 3);

  registry.release(two[0].token);
  const fourth = registry.acquire('pool', 'c', 50);
  assert.equal(fourth.epoch, 4);
  assert.equal(registry.epoch('pool'), 4);
  // A live share from an earlier epoch is not the current fence.
  expectFenced(() => registry.assertLease('pool', third.token, 3));
  expectFenced(() => registry.assertLease('pool', two[1].token, 2));
  assert.equal(registry.assertLease('pool', fourth.token, 4), true);
});

test('assertLease fences every non-current credential', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const first = registry.acquire('r', 'a', 50);
  registry.release(first.token);
  const second = registry.acquire('r', 'b', 50);

  expectFenced(() => registry.assertLease('never', 't', 1));
  expectFenced(() => registry.assertLease('r', 'unknown-token', 2));
  expectFenced(() => registry.assertLease('r', first.token, 1));
  expectFenced(() => registry.assertLease('r', second.token, 1)); // old epoch
  expectFenced(() => registry.assertLease('r', second.token, 3)); // future epoch

  clock.set(60);
  expectFenced(() => registry.assertLease('r', second.token, 2)); // expired
});

test('assertLease and epoch reject malformed arguments with TypeError', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.acquire('r', 'a', 50);
  for (const resource of [null, undefined, 1, {}, ['r']]) {
    assert.throws(() => registry.epoch(resource), TypeError);
    assert.throws(() => registry.assertLease(resource, 't', 1), TypeError);
  }
  for (const token of [null, undefined, 1, {}, ['t']]) {
    assert.throws(() => registry.assertLease('r', token, 1), TypeError);
  }
  for (const epoch of [null, undefined, '1', 1.5, NaN, Infinity, -Infinity]) {
    assert.throws(() => registry.assertLease('r', 't', epoch), TypeError);
  }
  // 0 is a well-formed integer: it fences rather than raising TypeError.
  expectFenced(() => registry.assertLease('r', 't', 0));
});

test('epochs ride the grant events and recover after a restart', () => {
  const p = logPath('restart');
  const clock = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const first = r.acquire('keep', 'h', 40);
  r.release(first.token);
  r.acquire('keep', 'h2', 40); // epoch 2
  r.acquire('gone', 'h3', 10); // epoch 1, reclaimed below
  clock.set(50);
  r.sweep();

  r = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r.epoch('keep'), 2);
  assert.equal(r.epoch('gone'), 1); // survives even with no live lease
  assert.equal(r.epoch('never'), 0);

  // Recovery itself never advances an epoch; the next grant is strictly 3.
  const next = r.acquire('keep', 'h4', 50);
  assert.equal(next.epoch, 3);
  assert.equal(r.epoch('keep'), 3);

  // Credentials issued before the restart are void and fenced.
  expectFenced(() => r.assertLease('keep', first.token, 1));
});

test('epochs survive compaction, including resources without a live lease', () => {
  const p = logPath('compact');
  const clock = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r.acquire('a', 'h', 100);
  const short = r.acquire('freed', 'h2', 10);
  r.release(short.token);
  clock.set(50);
  r.compact();

  const image = JSON.parse(fs.readFileSync(`${p}.snapshot`, 'utf8'));
  assert.ok(Array.isArray(image.epochs));
  assert.deepEqual(
    image.epochs.filter((entry) => entry.resource === 'freed'),
    [{ resource: 'freed', epoch: 1 }],
  );
  assert.equal(image.leases[0].epoch, 1);

  let r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r2.epoch('a'), 1);
  assert.equal(r2.epoch('freed'), 1);
  const retaken = r2.acquire('freed', 'h3', 100);
  assert.equal(retaken.epoch, 2);
  r2.release(retaken.token);
  r2.compact();

  const r3 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r3.epoch('freed'), 2);
  assert.equal(r3.epoch('a'), 1);
  void a;
});

test('post-compact incremental grants continue epochs past the snapshot', () => {
  const p = logPath('increment');
  let r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  r.acquire('r', 'h', 100);
  r.compact();

  r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const existing = r.acquire('s', 'h2', 100);
  assert.equal(existing.epoch, 1);
  r.release(existing.token);
  assert.equal(r.acquire('s', 'h3', 100).epoch, 2);
  r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(r.epoch('r'), 1);
  assert.equal(r.epoch('s'), 2);
});

test('a failed grant write rolls the epoch back and a retry reuses the number', () => {
  const p = logPath('rollback');
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  r.acquire('r1', 'h', 100);

  sabotage(p);
  try {
    assert.throws(() => r.acquire('r2', 'h', 100), LogFileError);
  } finally {
    restore(p);
  }
  assert.equal(r.epoch('r2'), 0);
  const retried = r.acquire('r2', 'h', 100);
  assert.equal(retried.epoch, 1);

  // Failing group on two fresh resources leaves both counters untouched.
  sabotage(p);
  try {
    assert.throws(() => r.acquireAll(['g1', 'g2'], 'h', 100), LogFileError);
  } finally {
    restore(p);
  }
  assert.equal(r.epoch('g1'), 0);
  assert.equal(r.epoch('g2'), 0);
  const group = r.acquireAll(['g1', 'g2'], 'h', 100);
  assert.deepEqual(group.map((l) => l.epoch), [1, 1]);
});

test('a failed waking release rolls the woken epoch back too', () => {
  const p = logPath('wake-rollback');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const lease = r.acquire('r', 'a', 100);
  const ticket = r.acquire('r', 'b', 40, 1000);

  clock.set(10);
  sabotage(p);
  try {
    assert.throws(() => r.release(lease.token), LogFileError);
  } finally {
    restore(p);
  }
  assert.equal(r.epoch('r'), 1);
  assert.equal(ticket.status, 'waiting');

  assert.equal(r.release(lease.token), true);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.epoch, 2);
  assert.equal(r.epoch('r'), 2);
});

test('a non-consecutive epoch in the log is corrupt and rolls back startup', () => {
  const p = logPath('corrupt-log');
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  r.acquire('r', 'h', 100);
  fs.appendFileSync(
    p,
    JSON.stringify({
      v: 1,
      type: 'acquire',
      e: 99,
      at: 0,
      token: 'x',
      resource: 'r',
      holder: 'h2',
      ttlMs: 100,
      expiresAt: 100,
      epoch: 5,
    }) + '\n',
  );
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  // A bad epoch type is corruption as well.
  const p2 = logPath('corrupt-type');
  fs.writeFileSync(
    p2,
    JSON.stringify({
      v: 1,
      type: 'acquire',
      e: 1,
      at: 0,
      token: 't',
      resource: 'r',
      holder: 'h',
      ttlMs: 100,
      expiresAt: 100,
      epoch: 'one',
    }) + '\n',
  );
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p2 }),
    LogFileError,
  );
});

test('a malformed epoch table in the snapshot is refused', () => {
  const p = logPath('corrupt-snapshot');
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  r.acquire('r', 'h', 100);
  r.compact();

  const writeImage = (image) => {
    fs.writeFileSync(`${p}.snapshot`, JSON.stringify(image));
  };
  const base = {
    v: 1,
    type: 'snapshot',
    at: 0,
    seq: 1,
    counters: { granted: 1, renewed: 0, released: 0, reclaimed: 0 },
    leases: [],
  };

  writeImage({ ...base, epochs: [{ resource: 'r', epoch: 0 }] });
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    LogFileError,
  );

  writeImage({
    ...base,
    epochs: [
      { resource: 'r', epoch: 1 },
      { resource: 'r', epoch: 2 },
    ],
  });
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    LogFileError,
  );

  writeImage({ ...base, epochs: 'nope' });
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    LogFileError,
  );

  writeImage({
    ...base,
    leases: [
      { token: 't', resource: 'r', holder: 'h', ttlMs: 100, expiresAt: 100, epoch: -1 },
    ],
  });
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    LogFileError,
  );
});

test('logs and snapshots written before epochs existed still recover', () => {
  const p = logPath('legacy-log');
  fs.writeFileSync(
    p,
    JSON.stringify({
      v: 1,
      type: 'acquire',
      e: 1,
      at: 0,
      token: 'old',
      resource: 'r',
      holder: 'h',
      ttlMs: 100,
      expiresAt: 100,
    }) + '\n',
  );
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(200), logPath: p });
  assert.equal(r.epoch('r'), 1);
  // The legacy credential occupies nothing live; the next grant takes epoch 2.
  assert.equal(r.acquire('r', 'h2', 100).epoch, 2);

  const p2 = logPath('legacy-snapshot');
  fs.writeFileSync(
    `${p2}.snapshot`,
    JSON.stringify({
      v: 1,
      type: 'snapshot',
      at: 0,
      seq: 1,
      counters: { granted: 1, renewed: 0, released: 0, reclaimed: 0 },
      leases: [
        { token: 'old', resource: 'r', holder: 'h', ttlMs: 100, expiresAt: 100 },
      ],
    }),
  );
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(200), logPath: p2 });
  assert.equal(r2.epoch('r'), 1);
  assert.equal(r2.acquire('r', 'h3', 100).epoch, 2);
});
