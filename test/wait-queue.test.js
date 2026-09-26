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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-wait-'));
});

test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

test('acquire with a wait budget on an occupied resource queues a waiting result', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('r', 'a');

  const ticket = registry.acquire('r', 'b', 50, 1000);

  assert.equal(ticket.status, 'waiting');
  assert.equal(ticket.resource, 'r');
  assert.equal(ticket.holder, 'b');
  assert.equal(typeof ticket.waitId, 'string');
  assert.equal('token' in ticket, false);

  // Queueing moves no counter and does not change ownership.
  assert.equal(registry.holder('r'), 'a');
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });

  // A non-waiting acquire still throws, even with a waiter queued.
  assert.throws(
    () => registry.acquire('r', 'c'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN',
  );
  assert.equal(registry.holder('r'), 'a');
});

test('wait budget must be a positive finite number when given', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.acquire('r', 'a');
  assert.throws(() => registry.acquire('r', 'b', 50, 0), TypeError);
  assert.throws(() => registry.acquire('r', 'b', 50, -1), TypeError);
  assert.throws(() => registry.acquire('r', 'b', 50, NaN), TypeError);
  assert.throws(() => registry.acquire('r', 'b', 50, Infinity), TypeError);
  assert.equal(registry.stats().granted, 1);
});

test('release hands the freed resource to the head of the queue, FIFO', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const first = registry.acquire('r', 'a');
  const w1 = registry.acquire('r', 'b', 40, 1000);
  const w2 = registry.acquire('r', 'c', 60, 1000);

  clock.set(10);
  assert.equal(registry.release(first.token), true);

  // Only the head is woken, with its own independent expiry.
  assert.equal(w1.status, 'granted');
  assert.equal(typeof w1.token, 'string');
  assert.equal(w1.expiresAt, 50); // 10 + its own ttlMs of 40
  assert.equal(w2.status, 'waiting');
  assert.equal(registry.holder('r'), 'b');
  assert.deepEqual(registry.stats(), {
    granted: 2, renewed: 0, released: 1, reclaimed: 0, live: 1,
  });

  // The woken credential is a real lease: renewable and releasable, and its
  // release wakes the next waiter in line.
  clock.set(20);
  assert.equal(registry.renew(w1.token), true);
  assert.equal(registry.release(w1.token), true);
  assert.equal(w2.status, 'granted');
  assert.equal(w2.expiresAt, 80); // 20 + its own ttlMs of 60
  assert.equal(registry.holder('r'), 'c');
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 1, released: 2, reclaimed: 0, live: 1,
  });
});

test('sweep hands reclaimed resources to waiting requests in queue order', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('r', 'a', 10);
  const w1 = registry.acquire('r', 'b', 20, 1000);
  const w2 = registry.acquire('r', 'c', 30, 1000);

  clock.set(10);
  assert.deepEqual(registry.sweep(), ['r']);
  assert.equal(w1.status, 'granted');
  assert.equal(w1.expiresAt, 30); // 10 + 20
  assert.equal(w2.status, 'waiting');
  assert.deepEqual(registry.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 1, live: 1,
  });

  clock.set(30);
  assert.deepEqual(registry.sweep(), ['r']);
  assert.equal(w2.status, 'granted');
  assert.equal(w2.expiresAt, 60); // 30 + 30
  assert.deepEqual(registry.stats(), {
    granted: 3, renewed: 0, released: 0, reclaimed: 2, live: 1,
  });
});

test('a waiter whose budget runs out gives up and is never woken', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a');
  const ticket = registry.acquire('r', 'b', 10, 5); // deadline at t=5

  clock.set(6);
  assert.equal(registry.release(lease.token), true);

  assert.equal(ticket.status, 'expired');
  assert.equal('token' in ticket, false);
  assert.equal(registry.holder('r'), null);
  // The timeout granted nothing and freed nothing extra.
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 1, reclaimed: 0, live: 0,
  });

  // The resource is simply free afterwards.
  const next = registry.acquire('r', 'c');
  assert.equal(next.holder, 'c');
  assert.equal(registry.stats().granted, 2);
});

test('a timed-out head of the queue does not block later waiters', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a');
  const w1 = registry.acquire('r', 'b', 10, 5);
  const w2 = registry.acquire('r', 'c', 10, 1000);

  clock.set(6);
  assert.equal(registry.release(lease.token), true);

  assert.equal(w1.status, 'expired');
  assert.equal(w2.status, 'granted');
  assert.equal(w2.expiresAt, 16);
  assert.equal(registry.holder('r'), 'c');
  assert.equal(registry.stats().granted, 2);
});

test('cancel drops a queued request; repeats and unknown ids do not land', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a');
  const ticket = registry.acquire('r', 'b', 10, 1000);

  assert.equal(registry.cancel(ticket.waitId), true);
  assert.equal(ticket.status, 'cancelled');
  assert.equal(registry.cancel(ticket.waitId), false);
  assert.equal(registry.cancel('wait-does-not-exist'), false);

  // The cancelled waiter is skipped: the resource just becomes free.
  assert.equal(registry.release(lease.token), true);
  assert.equal(ticket.status, 'cancelled');
  assert.equal(registry.holder('r'), null);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 1, reclaimed: 0, live: 0,
  });
});

test('a cancelled waiter does not block the rest of the queue', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a');
  const w1 = registry.acquire('r', 'b', 10, 1000);
  const w2 = registry.acquire('r', 'c', 10, 1000);

  assert.equal(registry.cancel(w1.waitId), true);
  assert.equal(registry.release(lease.token), true);

  assert.equal(w1.status, 'cancelled');
  assert.equal(w2.status, 'granted');
  assert.equal(registry.holder('r'), 'c');
  assert.equal(registry.stats().granted, 2);
});

test('an already woken request can no longer be cancelled', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a');
  const ticket = registry.acquire('r', 'b', 10, 1000);

  assert.equal(registry.release(lease.token), true);
  assert.equal(ticket.status, 'granted');
  assert.equal(registry.cancel(ticket.waitId), false);
  assert.equal(registry.holder('r'), 'b');
});

test('a timed-out request can no longer be cancelled', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('r', 'a');
  const ticket = registry.acquire('r', 'b', 10, 5);

  clock.set(6);
  assert.equal(registry.cancel(ticket.waitId), false);
  assert.equal(ticket.status, 'expired');
});

test('late acquires cannot jump the queue of an expired but unswept resource', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('r', 'a', 10);
  const w1 = registry.acquire('r', 'b', 50, 1000);

  clock.set(11); // a's lease expired, nobody swept yet
  assert.throws(
    () => registry.acquire('r', 'c'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN',
  );
  // A late waiting acquire joins the back of the queue.
  const w2 = registry.acquire('r', 'c', 50, 1000);
  assert.equal(w2.status, 'waiting');
  assert.notEqual(w2.waitId, w1.waitId);

  assert.deepEqual(registry.sweep(), ['r']);
  assert.equal(w1.status, 'granted');
  assert.equal(w1.expiresAt, 61);
  assert.equal(w2.status, 'waiting');
  assert.equal(registry.holder('r'), 'b');
});

test('renewing the holder during the wait keeps waiters queued in order', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 50);
  const ticket = registry.acquire('r', 'b', 40, 1000);

  clock.set(40);
  assert.equal(registry.renew(lease.token), true); // now expires 90
  assert.equal(ticket.status, 'waiting');
  assert.equal(registry.holder('r'), 'a');

  clock.set(90);
  assert.deepEqual(registry.sweep(), ['r']);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.expiresAt, 130); // 90 + 40
});

test('failed, timed-out and cancelled requests never move the granted counter', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  registry.acquire('r', 'a');
  assert.throws(() => registry.acquire('r', 'b'), LeaseTakenError);
  const w1 = registry.acquire('r', 'c', 10, 5);
  const w2 = registry.acquire('r', 'd', 10, 1000);
  clock.set(6); // w1 times out
  assert.equal(registry.cancel(w2.waitId), true);
  registry.holder('r'); // lets the registry observe the timeout
  assert.equal(w1.status, 'expired');

  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
});

test('queueing without a wait budget keeps the exact baseline return shape', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'a', 50);
  assert.deepEqual(lease, {
    resource: 'r',
    holder: 'a',
    token: lease.token,
    expiresAt: 50,
  });
});

test('woken grants are persisted and replay as ordinary leases', () => {
  const p = logPath('wake-replay');
  const clock = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const lease = r.acquire('r', 'a');
  const ticket = r.acquire('r', 'b', 40, 1000);
  clock.set(10);
  assert.equal(r.release(lease.token), true);
  assert.equal(ticket.status, 'granted');
  assert.deepEqual(r.stats(), {
    granted: 2, renewed: 0, released: 1, reclaimed: 0, live: 1,
  });

  r = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assert.equal(r.holder('r'), 'b');
  assert.deepEqual(r.stats(), {
    granted: 2, renewed: 0, released: 1, reclaimed: 0, live: 1,
  });
  // The pre-restart credential is void, the lease keeps occupying.
  assert.equal(r.renew(ticket.token), false);
  assert.equal(r.release(ticket.token), false);
  assert.equal(r.holder('r'), 'b');
  // And it expires on its own schedule: 10 + 40.
  assert.deepEqual(r.sweep(49), []);
  assert.deepEqual(r.sweep(50), ['r']);
});

test('a failed log write during a waking release rolls everything back', () => {
  const p = logPath('wake-rollback');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  const lease = r.acquire('r', 'a');
  const ticket = r.acquire('r', 'b', 40, 1000);

  sabotage(p);
  try {
    assert.throws(
      () => r.release(lease.token),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }

  // Nothing landed: the lease is intact and the waiter is still queued.
  assert.equal(r.holder('r'), 'a');
  assert.equal(ticket.status, 'waiting');
  assert.deepEqual(r.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });

  // The registry keeps working; the retry wakes the waiter.
  assert.equal(r.release(lease.token), true);
  assert.equal(ticket.status, 'granted');
  assert.equal(r.holder('r'), 'b');
  assert.deepEqual(r.stats(), {
    granted: 2, renewed: 0, released: 1, reclaimed: 0, live: 1,
  });
});

test('a failed log write during a waking sweep rolls everything back', () => {
  const p = logPath('sweep-rollback');
  const clock = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock, logPath: p });
  r.acquire('r', 'a', 10);
  const ticket = r.acquire('r', 'b', 40, 1000);

  clock.set(10);
  sabotage(p);
  try {
    assert.throws(() => r.sweep(), (e) => e instanceof LogFileError);
  } finally {
    restore(p);
  }
  assert.equal(ticket.status, 'waiting');
  assert.equal(r.stats().reclaimed, 0);
  assert.equal(r.stats().granted, 1);

  assert.deepEqual(r.sweep(), ['r']);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.expiresAt, 50);
  assert.deepEqual(r.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 1, live: 1,
  });
});

test('release and sweep behave exactly as before when nobody is waiting', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 10);
  assert.equal(registry.release(lease.token), true);
  clock.set(20);
  assert.deepEqual(registry.sweep(), []);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 1, reclaimed: 0, live: 0,
  });
});

test('one freeing wakes exactly one waiter even with several queued', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a');
  const tickets = [
    registry.acquire('r', 'b', 10, 1000),
    registry.acquire('r', 'c', 10, 1000),
    registry.acquire('r', 'd', 10, 1000),
  ];

  assert.equal(registry.release(lease.token), true);
  assert.deepEqual(tickets.map((t) => t.status), ['granted', 'waiting', 'waiting']);
  assert.equal(registry.stats().granted, 2);
  assert.equal(registry.stats().live, 1);
});
