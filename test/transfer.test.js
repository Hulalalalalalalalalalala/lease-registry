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

let dir;
test.beforeEach(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-transfer-'));
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

// ---- success shape and credential swap -------------------------------------

test('transfer hands the resource to a new holder with a fresh credential', () => {
  const clock = fakeClock(10);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 40);

  clock.set(30);
  const result = registry.transfer(lease.token, 'b', 70);
  assert.deepEqual(Object.keys(result).sort(), [
    'epoch', 'expiresAt', 'holder', 'resource', 'token',
  ]);
  assert.equal(result.resource, 'r');
  assert.equal(result.holder, 'b');
  assert.equal(result.token !== lease.token, true);
  assert.equal(typeof result.token, 'string');
  assert.equal(result.expiresAt, 100); // clock reading + given ttl
  assert.equal(result.epoch, 2);

  assert.equal(registry.holder('r'), 'b');
  assert.equal(registry.epoch('r'), 2);
});

test('an omitted ttlMs falls back to the registry default ttl', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a');
  clock.set(25);
  const result = registry.transfer(lease.token, 'b');
  assert.equal(result.expiresAt, 125);
});

test('the old credential dies: renew/release return false and the fence throws', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'a');
  const result = registry.transfer(lease.token, 'b', 100);

  assert.equal(registry.renew(lease.token), false);
  assert.equal(registry.release(lease.token), false);
  assertFenced(() => registry.assertLease('r', lease.token, 1), 'r');
  assert.equal(registry.assertLease('r', result.token, 2), true);
});

test('the new credential can renew, assert and be released', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 60);
  const next = registry.transfer(lease.token, 'b', 60);
  assert.equal(next.expiresAt, 60);

  clock.set(10);
  assert.equal(registry.renew(next.token), true);
  assert.equal(registry.holder('r'), 'b');
  assert.equal(registry.assertLease('r', next.token, 2), true);
  assert.equal(registry.release(next.token), true);
});

test('transfers chain with strictly increasing epochs', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  let current = registry.acquire('r', 'a');
  current = registry.transfer(current.token, 'b', 100);
  assert.equal(current.epoch, 2);
  assert.equal(registry.epoch('r'), 2);
  current = registry.transfer(current.token, 'c', 100);
  assert.equal(current.epoch, 3);
  assert.equal(registry.holder('r'), 'c');
  assert.equal(registry.assertLease('r', current.token, 3), true);
});

test('a transfer never frees the resource or leaves a gap', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'a');
  registry.transfer(lease.token, 'b', 100);
  assert.throws(() => registry.acquire('r', 'c'), LeaseTakenError);
});

// ---- fencing ---------------------------------------------------------------

test('unknown, released, reclaimed and expired tokens are fenced on transfer', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 10);

  assertFenced(() => registry.transfer('lease-nope', 'b', 100));
  assertFenced(() => registry.transfer(lease.token + 'x', 'b', 100));

  clock.set(11);
  assertFenced(() => registry.transfer(lease.token, 'b', 100), 'r');

  // A reclaim deletes the credential entirely.
  assert.deepEqual(registry.sweep(), ['r']);
  assertFenced(() => registry.transfer(lease.token, 'b', 100));

  // A released credential is unknown to the table.
  const other = registry.acquire('s', 'h', 100);
  registry.release(other.token);
  assertFenced(() => registry.transfer(other.token, 'b', 100));
});

test('a credential already superseded by a transfer is fenced on re-transfer', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'a');
  const next = registry.transfer(lease.token, 'b', 100);
  // The old token is no longer a current credential even though it was never
  // explicitly released or reclaimed (it left the table with the handover, so
  // no resource can be recovered from it anymore).
  assertFenced(() => registry.transfer(lease.token, 'c', 100));
  // The current credential transfers again normally.
  assert.equal(registry.transfer(next.token, 'c', 100).epoch, 3);
});

test('a token reacquired away by a retake is fenced on transfer', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 10);
  clock.set(11);
  const retake = registry.acquire('r', 'b', 100);
  assert.equal(retake.epoch, 2);
  // The retake structurally removed the old credential from the table.
  assertFenced(() => registry.transfer(lease.token, 'c', 100));
});

test('a credential minted before a restart is fenced on transfer', () => {
  const p = logPath('legacy');
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const lease = r1.acquire('r', 'a', 200);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assertFenced(() => r2.transfer(lease.token, 'b', 100), 'r');
});

test('a credential of a shareable resource is fenced on transfer', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.declareResource('pool', 4);
  const lease = registry.acquire('pool', 'a');
  assert.equal(lease.epoch, 1);
  assertFenced(() => registry.transfer(lease.token, 'b', 100), 'pool');

  // Even the newest share (carrying the resource's last epoch) is rejected.
  const newest = registry.acquire('pool', 'b');
  assertFenced(() => registry.transfer(newest.token, 'c', 100), 'pool');

  // A declared capacity-1 resource is still a declared share resource.
  registry.declareResource('single-pool', 1);
  const one = registry.acquire('single-pool', 'a');
  assertFenced(() => registry.transfer(one.token, 'b', 100), 'single-pool');
});

test('a backwards clock reading revives the share for a transfer judgment', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a', 40);
  clock.set(50); // expired, unswept
  assertFenced(() => registry.transfer(lease.token, 'b', 100), 'r');
  clock.set(10); // reading moved backwards: live again
  const next = registry.transfer(lease.token, 'b', 100);
  assert.equal(next.epoch, 2);
  assert.equal(registry.assertLease('r', next.token, 2), true);
});

// ---- type validation -------------------------------------------------------

test('transfer validates token, holder and ttl types', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'a');
  for (const badToken of [1, null, undefined, {}, [], true]) {
    assert.throws(
      () => registry.transfer(badToken, 'b', 100),
      TypeError,
    );
  }
  for (const badHolder of [1, null, undefined, {}, [], true, '']) {
    assert.throws(
      () => registry.transfer(lease.token, badHolder, 100),
      TypeError,
    );
  }
  for (const badTtl of [0, -1, NaN, Infinity, -Infinity, '100', null, true]) {
    assert.throws(
      () => registry.transfer(lease.token, 'b', badTtl),
      TypeError,
    );
  }
  // Type errors come first: an unusable token with bad types still reports
  // TypeError, never LeaseFencedError.
  assert.throws(() => registry.transfer('unknown', '', 0), TypeError);
  // Nothing moved.
  assert.equal(registry.epoch('r'), 1);
  assert.equal(registry.assertLease('r', lease.token, 1), true);
});

// ---- stats -----------------------------------------------------------------

test('transfer moves none of the four counters and keeps live at 1', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'a');
  const before = registry.stats();
  assert.deepEqual(before, {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
  const next = registry.transfer(lease.token, 'b', 100);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
  registry.transfer(next.token, 'c', 100);
  assert.equal(registry.stats().live, 1);
});

// ---- atomic groups ----------------------------------------------------------

test('one credential of an atomic group transfers without touching the rest', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const [ra, rb] = registry.acquireAll(['a', 'b'], 'h', [40, 80]);

  const moved = registry.transfer(ra.token, 'g', 100);
  assert.equal(moved.resource, 'a');
  assert.equal(moved.epoch, 2);

  // The sibling credential is fully intact.
  assert.equal(registry.holder('b'), 'h');
  assert.equal(registry.assertLease('b', rb.token, 1), true);
  clock.set(10);
  assert.equal(registry.renew(rb.token), true);
  assert.equal(registry.stats().live, 2);

  // The handed-over credential fences under its old epoch.
  assertFenced(() => registry.assertLease('a', ra.token, 1), 'a');
  assert.equal(registry.assertLease('a', moved.token, 2), true);
});

test('a transfer does not wake or disturb a waiting request', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'a', 100);
  const ticket = registry.acquire('r', 'b', 50, 1000);
  assert.equal(ticket.status, 'waiting');

  const moved = registry.transfer(lease.token, 'c', 100);
  assert.equal(ticket.status, 'waiting', 'queue position survives the handover');
  assert.equal(registry.holder('r'), 'c');
  assert.throws(() => registry.acquire('r', 'd'), LeaseTakenError);

  // The new holder releasing is what wakes the head waiter; FIFO is intact.
  assert.equal(registry.release(moved.token), true);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.holder, 'b');
  assert.equal(ticket.epoch, 3);
});

test('a transfer never wakes a waiter on an unrelated resource', () => {
  // Transfer moves no share, so even a fully blocked queue stays exactly as
  // queued; only a real release/reclaim can fund a wake.
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.declareResource('pool', 1);
  registry.acquire('pool', 'p', 100);
  const waiting = registry.acquire('pool', 'w', 50, 1000);
  const other = registry.acquire('r', 'a', 100);
  const moved = registry.transfer(other.token, 'b', 100);
  assert.equal(waiting.status, 'waiting');

  // Releasing the transferred resource still wakes nobody; the pool waiter
  // only moves when a pool share is actually freed.
  assert.equal(registry.release(moved.token), true);
  assert.equal(waiting.status, 'waiting');
});

// ---- persistence ------------------------------------------------------------

test('a transfer appends exactly one event and recovers unchanged', () => {
  const p = logPath('transfer-log');
  const clock = fakeClock(0);
  const r1 = createRegistry({ ttlMs: 100, clock, logPath: p });
  const lease = r1.acquire('r', 'a', 200);
  clock.set(10);
  const next = r1.transfer(lease.token, 'b', 90);
  assert.equal(next.expiresAt, 100);

  const lines = fs.readFileSync(p, 'utf8').trim().split('\n').map(JSON.parse);
  const transfers = lines.filter((event) => event.type === 'transfer');
  assert.equal(transfers.length, 1);
  assert.deepEqual(transfers[0], {
    v: 1,
    e: 2,
    type: 'transfer',
    at: 10,
    from: lease.token,
    token: next.token,
    resource: 'r',
    holder: 'b',
    ttlMs: 90,
    expiresAt: 100,
    epoch: 2,
  });

  // Restart before expiry: the new credential occupies the resource but is
  // legacy (void), the old/new relationship and epoch come back exactly.
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  assert.equal(r2.epoch('r'), 2);
  assert.equal(r2.holder('r'), 'b');
  assertFenced(() => r2.assertLease('r', lease.token, 1), 'r');
  assertFenced(() => r2.assertLease('r', next.token, 2), 'r');
  assert.equal(r2.renew(next.token), false);
  assert.equal(r2.release(next.token), false);
  assert.throws(() => r2.acquire('r', 'c'), LeaseTakenError);
  assert.deepEqual(r2.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });

  // The next grant continues at epoch 3 once the recovered lease expires.
  const r3 = createRegistry({ ttlMs: 100, clock: fakeClock(101), logPath: p });
  r3.sweep();
  assert.equal(r3.acquire('r', 'c', 100).epoch, 3);
});

test('a transfer survives compaction with both credentials relationship intact', () => {
  const p = logPath('transfer-compact');
  const clock = fakeClock(0);
  const r1 = createRegistry({ ttlMs: 100, clock, logPath: p });
  const lease = r1.acquire('r', 'a', 300);
  const next = r1.transfer(lease.token, 'b', 300);
  r1.compact();

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r2.epoch('r'), 2);
  assert.equal(r2.holder('r'), 'b');
  assertFenced(() => r2.assertLease('r', lease.token, 1), 'r');
  assertFenced(() => r2.assertLease('r', next.token, 2), 'r');
  assert.equal(r2.renew(next.token), false);
  assert.deepEqual(r2.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
});

test('a failed log write rolls the whole transfer back', () => {
  const p = logPath('transfer-rollback');
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const lease = registry.acquire('r', 'a', 100);

  sabotage(p);
  try {
    assert.throws(
      () => registry.transfer(lease.token, 'b', 100),
      (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }

  // The old credential is still fully usable.
  assert.equal(registry.renew(lease.token), true);
  assert.equal(registry.assertLease('r', lease.token, 1), true);
  assert.equal(registry.release(lease.token), true);
  assert.equal(registry.holder('r'), null);

  // Epoch, counters and the log never moved.
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(r2.epoch('r'), 1);
  assert.deepEqual(r2.stats(), {
    granted: 1, renewed: 1, released: 1, reclaimed: 0, live: 0,
  });
  const types = fs.readFileSync(p, 'utf8').trim().split('\n')
    .map((line) => JSON.parse(line).type);
  assert.deepEqual(types, ['acquire', 'renew', 'release']);

  // A fresh transfer after recovery takes the next epoch.
  const again = r2.acquire('r', 'a', 100);
  assert.equal(again.epoch, 2);
  const moved = r2.transfer(again.token, 'b', 100);
  assert.equal(moved.epoch, 3);
});

test('a transfer after a compacted restart still appends one recoverable event', () => {
  const p = logPath('transfer-tail');
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const first = r1.acquire('r', 'a', 10);
  r1.release(first.token);
  r1.compact();
  const tail = r1.acquire('r', 'a2', 200);
  const moved = r1.transfer(tail.token, 'b', 200);
  assert.equal(moved.epoch, 3);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.equal(r2.epoch('r'), 3);
  assert.equal(r2.holder('r'), 'b');
  assertFenced(() => r2.assertLease('r', tail.token, 2), 'r');
  assertFenced(() => r2.assertLease('r', moved.token, 3), 'r');
});

test('a corrupt transfer entry in the log is a LogFileError', () => {
  const p = logPath('transfer-corrupt');
  fs.writeFileSync(p, [
    JSON.stringify({
      v: 1, e: 1, type: 'acquire', at: 0, token: 't1', resource: 'r',
      holder: 'a', ttlMs: 100, expiresAt: 100, epoch: 1,
    }),
    JSON.stringify({
      v: 1, e: 2, type: 'transfer', at: 5, from: 't1', token: 't2',
      resource: 'r', holder: 'b', ttlMs: 100, expiresAt: 105, epoch: 9,
    }),
  ].join('\n') + '\n');
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
  );
});
