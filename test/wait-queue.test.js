import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { createRegistry, LeaseTakenError, LogFileError } from '../src/index.js';

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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-queue-'));
});

test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a contended acquire with a wait budget queues and changes nothing', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('res', 'a');

  const w = registry.acquire('res', 'b', 50, 500);

  assert.equal(w.status, 'waiting');
  assert.equal(w.resource, 'res');
  assert.equal(w.holder, 'b');
  assert.equal(w.waitDeadline, 500);
  assert.equal(w.token, undefined);
  assert.equal(w.expiresAt, undefined);
  assert.equal(typeof w.requestId, 'string');

  // Queuing alone neither grants nor disturbs the current holder.
  assert.equal(registry.holder('res'), 'a');
  assert.deepEqual(registry.stats(), {
    granted: 1,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 1,
  });
});

test('acquire without a wait budget keeps throwing LeaseTakenError', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('res', 'a');
  registry.acquire('res', 'b', 50, 500); // queued

  assert.throws(
    () => registry.acquire('res', 'c'),
    (error) => error instanceof LeaseTakenError && error.code === 'LEASE_TAKEN',
  );
  assert.equal(registry.holder('res'), 'a');
  assert.equal(registry.stats().granted, 1);
});

test('an invalid wait budget is rejected', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.acquire('res', 'a');

  for (const bad of [0, -1, NaN, Infinity, 'soon']) {
    assert.throws(() => registry.acquire('res', 'b', 50, bad), TypeError);
  }
  assert.equal(registry.stats().granted, 1);
});

test('the wait budget accepts a number or an options object', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.acquire('res', 'a');

  const byNumber = registry.acquire('res', 'b', 50, 250);
  const byOption = registry.acquire('res', 'c', 50, { waitMs: 300 });

  assert.equal(byNumber.status, 'waiting');
  assert.equal(byNumber.waitDeadline, 250);
  assert.equal(byOption.status, 'waiting');
  assert.equal(byOption.waitDeadline, 300);
});

test('a free resource is granted immediately even when a wait budget is given', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });

  const lease = registry.acquire('res', 'a', 50, 500);

  assert.deepEqual(
    { resource: lease.resource, holder: lease.holder, expiresAt: lease.expiresAt },
    { resource: 'res', holder: 'a', expiresAt: 50 },
  );
  assert.equal(typeof lease.token, 'string');
  assert.equal(lease.status, undefined);
  assert.equal(registry.stats().granted, 1);
});

test('release hands the freed resource to the head of the line', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('res', 'a', 100);
  const w = registry.acquire('res', 'b', 50, 500);

  clock.set(20);
  assert.equal(registry.release(a.token), true);

  assert.equal(w.status, 'granted');
  assert.equal(typeof w.token, 'string');
  // The woken lease carries its own expiry from the handoff moment.
  assert.equal(w.expiresAt, 70);
  assert.equal(registry.holder('res'), 'b');
  assert.deepEqual(registry.stats(), {
    granted: 2,
    renewed: 0,
    released: 1,
    reclaimed: 0,
    live: 1,
  });

  // The woken lease behaves like any other lease.
  assert.equal(registry.renew(w.token), true);
  assert.equal(w.expiresAt, 70); // the view of the request is unchanged
  assert.equal(registry.release(w.token), true);
  assert.equal(registry.holder('res'), null);
});

test('waiters are woken strictly first come first served', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('res', 'a', 100);
  const w1 = registry.acquire('res', 'b', 50, 500);
  const w2 = registry.acquire('res', 'c', 50, 500);
  const w3 = registry.acquire('res', 'd', 50, 500);

  registry.release(a.token);
  assert.equal(w1.status, 'granted');
  assert.equal(w2.status, 'waiting');
  assert.equal(w3.status, 'waiting');
  assert.equal(registry.holder('res'), 'b');

  registry.release(w1.token);
  assert.equal(w2.status, 'granted');
  assert.equal(w3.status, 'waiting');
  assert.equal(registry.holder('res'), 'c');

  registry.release(w2.token);
  assert.equal(w3.status, 'granted');
  assert.equal(registry.holder('res'), 'd');
  assert.equal(registry.stats().granted, 4);
});

test('one freeing wakes exactly one waiter', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('res', 'a', 100);
  const w1 = registry.acquire('res', 'b', 50, 500);
  const w2 = registry.acquire('res', 'c', 50, 500);

  registry.release(a.token);

  assert.equal(w1.status, 'granted');
  assert.equal(w2.status, 'waiting');
  assert.equal(registry.stats().granted, 2);
  assert.equal(registry.stats().live, 1);
});

test('sweep hands reclaimed resources to the head of the line', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('res', 'a', 10);
  const w = registry.acquire('res', 'b', 50, 1000);

  clock.set(15);
  assert.deepEqual(registry.sweep(), ['res']);

  assert.equal(w.status, 'granted');
  assert.equal(w.expiresAt, 65);
  assert.equal(registry.holder('res'), 'b');
  assert.deepEqual(registry.stats(), {
    granted: 2,
    renewed: 0,
    released: 0,
    reclaimed: 1,
    live: 1,
  });
});

test('sweep without waiters is unchanged by the queue mechanism', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('a', 'h', 10);
  registry.acquire('b', 'h', 20);

  clock.set(25);
  assert.deepEqual(registry.sweep(), ['a', 'b']);
  assert.deepEqual(registry.sweep(), []);
  assert.deepEqual(registry.stats(), {
    granted: 2,
    renewed: 0,
    released: 0,
    reclaimed: 2,
    live: 0,
  });
});

test('a request past its wait deadline gives up and is never woken', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('res', 'a', 100);
  const w1 = registry.acquire('res', 'b', 50, 10);
  const w2 = registry.acquire('res', 'c', 50, 1000);

  clock.set(20); // w1's deadline (10) has passed
  registry.release(a.token);

  assert.equal(w1.status, 'timeout');
  assert.equal(w2.status, 'granted');
  assert.equal(registry.holder('res'), 'c');
  // The timed-out request never counted as a grant.
  assert.equal(registry.stats().granted, 2);

  // Nor is it woken by later freeings.
  registry.release(w2.token);
  assert.equal(w1.status, 'timeout');
  assert.equal(registry.holder('res'), null);
});

test('a timed-out waiter does not block a fresh acquisition', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('res', 'a', 10);
  const w = registry.acquire('res', 'b', 50, 20);

  clock.set(30); // lease expired at 10, wait deadline passed at 20
  const c = registry.acquire('res', 'c', 10);

  assert.equal(c.holder, 'c');
  assert.equal(w.status, 'timeout');
  assert.equal(registry.holder('res'), 'c');
  assert.equal(registry.stats().granted, 2);
});

test('a queued request beats a late acquire on an expired resource', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('res', 'a', 10);
  const w = registry.acquire('res', 'b', 50, 1000);

  clock.set(15); // a's lease expired, unswept
  assert.throws(
    () => registry.acquire('res', 'c'),
    (error) => error instanceof LeaseTakenError && error.code === 'LEASE_TAKEN',
  );

  assert.equal(w.status, 'granted');
  assert.equal(w.expiresAt, 65);
  assert.equal(registry.holder('res'), 'b');
  assert.equal(registry.stats().granted, 2);
});

test('a late acquire with a wait budget lines up behind earlier waiters', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('res', 'a', 10);
  const w1 = registry.acquire('res', 'b', 50, 1000);

  clock.set(15); // a's lease expired, unswept
  const w2 = registry.acquire('res', 'c', 50, 1000);

  assert.equal(w1.status, 'granted'); // served first, it queued earlier
  assert.equal(w2.status, 'waiting'); // the late call lines up behind
  assert.equal(registry.holder('res'), 'b');

  registry.release(w1.token);
  assert.equal(w2.status, 'granted');
  assert.equal(registry.holder('res'), 'c');
});

test('cancel withdraws a waiting request exactly once', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('res', 'a', 100);
  const w1 = registry.acquire('res', 'b', 50, 500);
  const w2 = registry.acquire('res', 'c', 50, 500);

  assert.equal(registry.cancel(w1.requestId), true);
  assert.equal(w1.status, 'cancelled');
  // A repeated cancel shows that nothing landed.
  assert.equal(registry.cancel(w1.requestId), false);
  assert.equal(registry.cancel('no-such-request'), false);

  registry.release(a.token);
  assert.equal(w1.status, 'cancelled'); // never woken
  assert.equal(w2.status, 'granted'); // next in line takes it
  assert.equal(registry.holder('res'), 'c');
  assert.equal(registry.stats().granted, 2);
});

test('cancel accepts the request view and never touches a woken request', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('res', 'a', 100);
  const w = registry.acquire('res', 'b', 50, 500);

  registry.release(a.token);
  assert.equal(w.status, 'granted');

  // Already woken: the cancel cannot land.
  assert.equal(registry.cancel(w), false);
  assert.equal(registry.holder('res'), 'b');
});

test('cancel after the wait deadline reports no landing and reads as timeout', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('res', 'a', 100);
  const w = registry.acquire('res', 'b', 50, 10);

  clock.set(10); // deadline reached
  assert.equal(registry.cancel(w.requestId), false);
  assert.equal(w.status, 'timeout');
  assert.equal(registry.stats().granted, 1);
});

test('poll reports waiting, granted, timeout and cancelled outcomes', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('res', 'a', 100);
  const granted = registry.acquire('res', 'b', 50, 500);
  const timedOut = registry.acquire('res', 'c', 50, 10);
  const cancelled = registry.acquire('res', 'd', 50, 500);

  assert.deepEqual(registry.poll(granted.requestId), {
    status: 'waiting',
    requestId: granted.requestId,
    resource: 'res',
    holder: 'b',
    waitDeadline: 500,
  });
  assert.equal(registry.poll('no-such-request'), null);

  registry.cancel(cancelled.requestId);
  clock.set(20); // timedOut's deadline passed
  registry.release(a.token);

  const grantedSnapshot = registry.poll(granted.requestId);
  assert.equal(grantedSnapshot.status, 'granted');
  assert.equal(grantedSnapshot.token, granted.token);
  assert.equal(grantedSnapshot.expiresAt, 70);

  assert.equal(registry.poll(timedOut.requestId).status, 'timeout');
  assert.equal(registry.poll(cancelled.requestId).status, 'cancelled');
});

test('renewing the holder while a line waits keeps everyone waiting', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('res', 'a', 40);
  const w = registry.acquire('res', 'b', 30, 1000);

  clock.set(30);
  assert.equal(registry.renew(a.token), true); // now expires at 70

  clock.set(50);
  assert.equal(w.status, 'waiting');
  assert.equal(registry.holder('res'), 'a');

  clock.set(71);
  assert.deepEqual(registry.sweep(), ['res']);
  assert.equal(w.status, 'granted');
  assert.equal(w.expiresAt, 101); // 71 + 30
  assert.equal(registry.holder('res'), 'b');
});

test('the woken lease drops out of live at its own expiry instant', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('res', 'a', 10);
  const w = registry.acquire('res', 'b', 30, 1000);

  clock.set(15);
  assert.deepEqual(registry.sweep(), ['res']);
  assert.equal(w.status, 'granted'); // expires at 45

  clock.set(44);
  assert.equal(registry.stats().live, 1);
  clock.set(45);
  assert.equal(registry.stats().live, 0);
  assert.equal(registry.stats().reclaimed, 1); // only the original lease so far

  assert.deepEqual(registry.sweep(), ['res']);
  assert.equal(registry.stats().reclaimed, 2);
});

test('handoffs across resources keep per-resource lines and expiry order', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('y', 'a', 20);
  registry.acquire('x', 'a', 10);
  const wx = registry.acquire('x', 'b', 50, 1000);
  const wy = registry.acquire('y', 'c', 50, 1000);

  clock.set(25);
  assert.deepEqual(registry.sweep(), ['x', 'y']);
  assert.equal(wx.status, 'granted');
  assert.equal(wy.status, 'granted');
  assert.equal(wx.expiresAt, 75);
  assert.equal(wy.expiresAt, 75);
  assert.equal(registry.holder('x'), 'b');
  assert.equal(registry.holder('y'), 'c');
  assert.equal(registry.stats().granted, 4);
  assert.equal(registry.stats().reclaimed, 2);
});

test('a failed append on release with a waiting request rolls everything back', () => {
  const p = logPath('rollback-release-handoff');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = registry.acquire('res', 'a', 100);
  const w = registry.acquire('res', 'b', 50, 500);

  sabotage(p);
  try {
    assert.throws(
      () => registry.release(a.token),
      (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }

  // Nothing landed: the lease is intact and the request is still waiting.
  assert.equal(registry.holder('res'), 'a');
  assert.equal(w.status, 'waiting');
  assert.deepEqual(registry.stats(), {
    granted: 1,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 1,
  });

  // The registry keeps working; the handoff happens on the next attempt.
  assert.equal(registry.release(a.token), true);
  assert.equal(w.status, 'granted');
  assert.equal(registry.holder('res'), 'b');
});

test('a failed append on sweep with a waiting request rolls everything back', () => {
  const p = logPath('rollback-sweep-handoff');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  registry.acquire('res', 'a', 10);
  const w = registry.acquire('res', 'b', 50, 500);

  clock.set(20);
  sabotage(p);
  try {
    assert.throws(
      () => registry.sweep(),
      (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }

  assert.equal(w.status, 'waiting');
  assert.equal(registry.stats().reclaimed, 0);
  assert.equal(registry.stats().granted, 1);

  assert.deepEqual(registry.sweep(), ['res']);
  assert.equal(w.status, 'granted');
  assert.equal(registry.stats().reclaimed, 1);
  assert.equal(registry.stats().granted, 2);
});

test('a failed append on a handoff during acquire leaves the line intact', () => {
  const p = logPath('rollback-acquire-handoff');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  registry.acquire('res', 'a', 10);
  const w = registry.acquire('res', 'b', 50, 1000);

  clock.set(15); // a's lease expired, unswept
  sabotage(p);
  try {
    assert.throws(
      () => registry.acquire('res', 'c'),
      (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }

  assert.equal(w.status, 'waiting');
  assert.equal(registry.holder('res'), null); // a is expired
  assert.equal(registry.stats().granted, 1);

  // Once the log works again the head of the line is served first.
  assert.throws(() => registry.acquire('res', 'c'), LeaseTakenError);
  assert.equal(w.status, 'granted');
  assert.equal(registry.holder('res'), 'b');
});

test('a handoff grant is logged and replays as an ordinary acquire', () => {
  const p = logPath('replay-handoff');
  const c1 = fakeClock(0);
  let registry = createRegistry({ ttlMs: 100, clock: c1, logPath: p });
  registry.acquire('res', 'a', 10);
  const w = registry.acquire('res', 'b', 50, 1000);
  c1.set(15);
  assert.deepEqual(registry.sweep(), ['res']);
  assert.equal(w.status, 'granted');

  registry = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  assert.equal(registry.holder('res'), 'b');
  assert.deepEqual(registry.stats(), {
    granted: 2,
    renewed: 0,
    released: 0,
    reclaimed: 1,
    live: 1,
  });
  // The woken lease expires on its own schedule after the restart.
  assert.deepEqual(registry.sweep(64), []);
  assert.deepEqual(registry.sweep(65), ['res']);
  // The pre-restart credential is void, as with any replayed lease.
  assert.equal(registry.renew(w.token), false);
});

test('queued requests themselves do not survive a restart', () => {
  const p = logPath('restart-drops-line');
  const c1 = fakeClock(0);
  let registry = createRegistry({ ttlMs: 100, clock: c1, logPath: p });
  registry.acquire('res', 'a', 100);
  const w = registry.acquire('res', 'b', 50, 1000);

  registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(registry.poll(w.requestId), null);
  assert.equal(registry.cancel(w.requestId), false);
  assert.throws(() => registry.acquire('res', 'c'), LeaseTakenError);
  assert.equal(registry.stats().granted, 1);
});
