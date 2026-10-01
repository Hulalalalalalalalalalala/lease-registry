import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  createRegistry,
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

// ---- basic handover --------------------------------------------------------

test('transfer hands the resource to a new holder with epoch + 1', () => {
  const clock = fakeClock(10);
  const registry = createRegistry({ ttlMs: 100, clock });
  const first = registry.acquire('r', 'alice');
  assert.equal(first.epoch, 1);

  clock.set(40);
  const handed = registry.transfer(first.token, 'bob', 50);
  assert.deepEqual(handed, {
    resource: 'r',
    holder: 'bob',
    token: handed.token,
    expiresAt: 90,
    epoch: 2,
  });
  assert.equal(typeof handed.token, 'string');
  assert.notEqual(handed.token, first.token);
  assert.equal(registry.holder('r'), 'bob');
  assert.equal(registry.epoch('r'), 2);
});

test('omitting ttlMs reuses the registry default ttl', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const first = registry.acquire('r', 'alice');
  clock.set(30);
  const handed = registry.transfer(first.token, 'bob');
  assert.equal(handed.expiresAt, 130);
});

test('the old credential is fenced while the new one is leader', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const first = registry.acquire('r', 'alice');
  const handed = registry.transfer(first.token, 'bob');

  assert.equal(registry.renew(first.token), false);
  assert.equal(registry.release(first.token), false);
  assertFenced(
    () => registry.assertLease('r', first.token, first.epoch),
    'r',
  );
  assert.equal(registry.assertLease('r', handed.token, handed.epoch), true);
});

test('a handover chain advances the epoch once per transfer', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = registry.acquire('r', 'a');
  const b = registry.transfer(a.token, 'b');
  assert.equal(b.epoch, 2);
  const c = registry.transfer(b.token, 'c');
  assert.equal(c.epoch, 3);
  assert.equal(registry.holder('r'), 'c');
  assert.equal(registry.epoch('r'), 3);
  // Only the latest credential leads; every earlier one is fenced.
  for (const old of [a, b]) {
    assert.equal(registry.renew(old.token), false);
    assertFenced(() => registry.assertLease('r', old.token, old.epoch));
  }
  assert.equal(registry.assertLease('r', c.token, c.epoch), true);
});

test('transfer moves no public counter and keeps live at 1', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const first = registry.acquire('r', 'alice');
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
  const handed = registry.transfer(first.token, 'bob');
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
  // The new credential can renew normally and still shows as the one live
  // lease; the old one is gone for good.
  assert.equal(registry.renew(handed.token), true);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 1, released: 0, reclaimed: 0, live: 1,
  });
});

// ---- argument validation ---------------------------------------------------

test('transfer throws TypeError on bad token, holder or ttl arguments', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'alice');
  for (const [token, holder] of [
    [undefined, 'bob'],
    [null, 'bob'],
    [123, 'bob'],
    [{}, 'bob'],
    [lease.token, undefined],
    [lease.token, null],
    [lease.token, 42],
    [lease.token, ''],
  ]) {
    assert.throws(
      () => registry.transfer(token, holder),
      (e) => e instanceof TypeError,
    );
  }
  for (const ttl of [0, -1, -100, NaN, Infinity, -Infinity, '50']) {
    assert.throws(
      () => registry.transfer(lease.token, 'bob', ttl),
      (e) => e instanceof TypeError,
    );
  }
});

test('argument TypeErrors are thrown before any fencing check', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  // Unknown token with a bad holder still reports the TypeError.
  assert.throws(() => registry.transfer('unknown', ''), TypeError);
  assert.throws(() => registry.transfer('unknown', 'bob', 0), TypeError);
  assert.throws(() => registry.transfer(42, 'bob'), TypeError);
});

// ---- fencing ---------------------------------------------------------------

test('an unknown token is fenced', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.acquire('r', 'alice');
  assertFenced(() => registry.transfer('lease-nope', 'bob'));
});

test('a released token is fenced', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'alice');
  assert.equal(registry.release(lease.token), true);
  assertFenced(() => registry.transfer(lease.token, 'bob'));
});

test('a reclaimed token is fenced', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'alice', 50);
  clock.set(60);
  assert.deepEqual(registry.sweep(), ['r']);
  assertFenced(() => registry.transfer(lease.token, 'bob'));
});

test('an expired-but-unswept token is fenced', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'alice', 50);
  clock.set(50);
  assertFenced(() => registry.transfer(lease.token, 'bob'));
});

test('a credential from before a restart is fenced', () => {
  const p = logPath('legacy');
  const first = createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p })
    .acquire('r', 'alice');
  const restarted = createRegistry({ ttlMs: 1000, clock: fakeClock(10), logPath: p });
  assert.equal(restarted.holder('r'), 'alice');
  assertFenced(() => restarted.transfer(first.token, 'bob'));
});

test('a credential of a shareable resource is fenced', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.declareResource('pool', 2);
  const lease = registry.acquire('pool', 'alice', 100);
  assertFenced(() => registry.transfer(lease.token, 'bob'));
  // The share is still held by alice afterwards.
  assert.equal(registry.holder('pool'), 'alice');
});

test('a credential that is no longer current is fenced', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const first = registry.acquire('r', 'alice');
  registry.transfer(first.token, 'bob');
  // The original credential is an older epoch now.
  assertFenced(() => registry.transfer(first.token, 'carol'));
});

test('an expired token is fenced even with a valid-looking ttl', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'alice', 10);
  clock.set(11);
  assertFenced(() => registry.transfer(lease.token, 'bob', 5));
});

// ---- atomic groups, queues and other entries ------------------------------

test('transferring one group credential leaves the others untouched', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const [lx, ly] = registry.acquireAll(['x', 'y'], 'grp');
  const handed = registry.transfer(lx.token, 'newx');

  assert.equal(registry.holder('x'), 'newx');
  assert.equal(registry.holder('y'), 'grp');
  assertFenced(() => registry.assertLease('x', lx.token, lx.epoch));
  assert.equal(registry.assertLease('x', handed.token, handed.epoch), true);
  assert.equal(registry.assertLease('y', ly.token, ly.epoch), true);
  assert.equal(registry.renew(ly.token), true);
});

test('transfer neither wakes a waiter nor changes its queue place or ticket', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const leader = registry.acquire('r', 'alice');
  const ticket = registry.acquire('r', 'waiting-holder', 100, 1000);
  assert.equal(ticket.status, 'waiting');

  // A handover frees nothing: the waiter stays queued.
  const handed = registry.transfer(leader.token, 'bob');
  assert.equal(ticket.status, 'waiting');
  assert.equal(registry.holder('r'), 'bob');

  // Releasing the fresh credential is what wakes the head waiter.
  clock.set(10);
  assert.equal(registry.release(handed.token), true);
  assert.equal(ticket.status, 'granted');
  assert.equal(ticket.holder, 'waiting-holder');
  assert.equal(registry.holder('r'), 'waiting-holder');
  // The woken grant is epoch 3: acquire(1), transfer(2), wake(3).
  assert.equal(ticket.epoch, 3);
});

test('a queued waiter cannot be overtaken by a direct acquire after transfer', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const leader = registry.acquire('r', 'alice');
  registry.acquire('r', 'waiter', 100, 1000);
  const handed = registry.transfer(leader.token, 'bob');
  // Still occupied while the waiter is at the head: a direct take fails.
  assert.throws(() => registry.acquire('r', 'jumping'), /already held/);
  registry.release(handed.token);
});

// ---- persistence -----------------------------------------------------------

test('a transfer appends exactly one transfer event', () => {
  const p = logPath('one-event');
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(5), logPath: p });
  const first = registry.acquire('r', 'alice', 100);
  const handed = registry.transfer(first.token, 'bob', 50);
  const lines = fs.readFileSync(p, 'utf8').trim().split('\n').map(JSON.parse);
  assert.deepEqual(lines.map((l) => l.type), ['acquire', 'transfer']);
  const event = lines[1];
  assert.equal(event.resource, 'r');
  assert.equal(event.from, first.token);
  assert.equal(event.token, handed.token);
  assert.equal(event.holder, 'bob');
  assert.equal(event.ttlMs, 50);
  assert.equal(event.expiresAt, 55);
  assert.equal(event.at, 5);
  assert.equal(event.epoch, 2);
  assert.equal(typeof event.e, 'number');
});

test('replay restores the handover relationship and keeps both tokens void', () => {
  const p = logPath('replay');
  const origin = createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p });
  const a = origin.acquire('r', 'alice', 1000);
  const b = origin.transfer(a.token, 'bob', 1000);

  const restarted = createRegistry({ ttlMs: 1000, clock: fakeClock(10), logPath: p });
  assert.equal(restarted.holder('r'), 'bob');
  assert.equal(restarted.epoch('r'), 2);
  // Credentials issued before the restart are all void.
  assert.equal(restarted.renew(a.token), false);
  assert.equal(restarted.release(a.token), false);
  assert.equal(restarted.renew(b.token), false);
  assertFenced(() => restarted.assertLease('r', a.token, 1));
  assertFenced(() => restarted.assertLease('r', b.token, 2));
  // The transferred lease still occupies until it expires.
  assert.deepEqual(restarted.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
});

test('the next grant after a replayed transfer continues the epoch', () => {
  const p = logPath('epoch-cont');
  const clock = fakeClock(0);
  const origin = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = origin.acquire('r', 'alice', 10);
  const b = origin.transfer(a.token, 'bob', 10);
  clock.set(20);
  origin.sweep();

  const restarted = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  assert.equal(restarted.holder('r'), null);
  const next = restarted.acquire('r', 'carol', 100);
  assert.equal(next.epoch, 3);
});

test('a failed log write rolls the transfer back completely', () => {
  const p = logPath('rollback');
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const first = registry.acquire('r', 'alice', 100);
  const handed = registry.transfer(first.token, 'bob', 100);

  sabotage(p);
  assert.throws(
    () => registry.transfer(handed.token, 'carol', 100),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
  restore(p);

  // The previous credential is still the live leader...
  assert.equal(registry.holder('r'), 'bob');
  assert.equal(registry.epoch('r'), 2);
  assert.equal(registry.renew(handed.token), true);
  assert.equal(registry.assertLease('r', handed.token, 2), true);
  assert.equal(registry.release(handed.token), true);
  // ...no epoch moved, and no third credential was produced.
  assert.equal(registry.epoch('r'), 2);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 1, released: 1, reclaimed: 0, live: 0,
  });
  // The log holds the committed events only - no transfer for carol.
  const types = fs.readFileSync(p, 'utf8').trim().split('\n').map((l) => JSON.parse(l).type);
  assert.deepEqual(types, ['acquire', 'transfer', 'renew', 'release']);
});

test('a failed log write leaves the old credential asserting as leader', () => {
  const p = logPath('rollback-assert');
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const first = registry.acquire('r', 'alice', 100);
  const handed = registry.transfer(first.token, 'bob', 100);

  sabotage(p);
  assert.throws(() => registry.transfer(handed.token, 'carol', 100), LogFileError);
  restore(p);

  assert.equal(registry.assertLease('r', handed.token, 2), true);
  assertFenced(() => registry.assertLease('r', first.token, 1));
});

// ---- compaction ------------------------------------------------------------

test('compaction preserves a transferred lease and the handover epochs', () => {
  const p = logPath('compact');
  const clock = fakeClock(0);
  const origin = createRegistry({ ttlMs: 1000, clock, logPath: p });
  const a = origin.acquire('r', 'alice', 1000);
  const b = origin.transfer(a.token, 'bob', 500);
  clock.set(100);
  origin.compact();

  const restarted = createRegistry({ ttlMs: 1000, clock: fakeClock(100), logPath: p });
  assert.equal(restarted.holder('r'), 'bob');
  assert.equal(restarted.epoch('r'), 2);
  assert.equal(restarted.renew(a.token), false);
  assert.equal(restarted.renew(b.token), false);
  assertFenced(() => restarted.assertLease('r', b.token, 2));
  assert.deepEqual(restarted.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 0, live: 1,
  });
  // The new credential's absolute expiry (0 + 500 = 500) survives folding.
  assert.deepEqual(restarted.sweep(499), []);
  assert.deepEqual(restarted.sweep(500), ['r']);
});

test('a grant after a compacted transfer continues from its epoch', () => {
  const p = logPath('compact-then-take');
  const clock = fakeClock(0);
  const origin = createRegistry({ ttlMs: 10, clock, logPath: p });
  const a = origin.acquire('r', 'alice', 10);
  origin.transfer(a.token, 'bob', 10);
  clock.set(20);
  origin.sweep();
  origin.compact();

  const restarted = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  const next = restarted.acquire('r', 'carol');
  assert.equal(next.epoch, 3);
  assert.equal(restarted.epoch('r'), 3);
});

test('a compacted restart matches an uncompacted restart through transfers', () => {
  const make = (p) => {
    const clock = fakeClock(0);
    const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
    const a = registry.acquire('r', 'alice', 80);
    const b = registry.transfer(a.token, 'bob', 60);
    registry.transfer(b.token, 'carol', 40);
    return { registry, clock };
  };
  const p1 = logPath('full');
  const p2 = logPath('folded');
  make(p1);
  const folded = make(p2);
  folded.registry.compact();

  const full = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p1 });
  const restored = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p2 });
  assert.equal(restored.holder('r'), full.holder('r'));
  assert.equal(restored.epoch('r'), full.epoch('r'));
  assert.deepEqual(restored.stats(), full.stats());
  assert.deepEqual(restored.sweep(40), full.sweep(40));
});

// ---- expiry accounting -----------------------------------------------------

test('a shorter transfer ttl reclaims exactly the new credential once', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 1000, clock });
  const first = registry.acquire('r', 'alice', 1000);
  // Old expiry 1000 is left as a stale heap node; the new lease expires 100.
  const handed = registry.transfer(first.token, 'bob', 100);
  clock.set(100);
  assert.deepEqual(registry.sweep(), ['r']);
  assert.equal(registry.holder('r'), null);
  // The stale 1000-node from the old credential must never free the share
  // again or bump reclaimed a second time.
  clock.set(1000);
  assert.deepEqual(registry.sweep(), []);
  assert.deepEqual(registry.stats(), {
    granted: 1, renewed: 0, released: 0, reclaimed: 1, live: 0,
  });
  assert.equal(registry.epoch('r'), 2);
});

test('a longer transfer ttl keeps the share alive past the old expiry', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const first = registry.acquire('r', 'alice', 50);
  const handed = registry.transfer(first.token, 'bob', 500);
  clock.set(60);
  // Past the old expiry, the new credential is still the live leader.
  assert.equal(registry.holder('r'), 'bob');
  assert.equal(registry.assertLease('r', handed.token, handed.epoch), true);
  assert.deepEqual(registry.sweep(60), []);
});

test('transferring to the same holder name still fences the old credential', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const first = registry.acquire('r', 'alice');
  const again = registry.transfer(first.token, 'alice');
  assert.equal(again.holder, 'alice');
  assert.equal(again.epoch, 2);
  assert.equal(registry.renew(first.token), false);
  assert.equal(registry.assertLease('r', again.token, 2), true);
});

// ---- corrupt log detection -------------------------------------------------

test('a transfer entry with a non-monotonic epoch is a corrupt log', () => {
  const p = logPath('corrupt');
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const first = registry.acquire('r', 'alice', 100);
  registry.transfer(first.token, 'bob', 100);
  // Tamper: claim the handover carried epoch 9 instead of 2.
  const lines = fs.readFileSync(p, 'utf8').trim().split('\n');
  const event = JSON.parse(lines[1]);
  event.epoch = 9;
  fs.writeFileSync(p, `${lines[0]}\n${JSON.stringify(event)}\n`);
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
});

test('a transfer entry on a share-capacity resource is a corrupt log', () => {
  const p = logPath('corrupt-share');
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  registry.declareResource('pool', 2);
  const lease = registry.acquire('pool', 'alice', 100);
  // In-process transfer of a share is refused, so forge the event directly.
  fs.appendFileSync(p, `${JSON.stringify({
    v: 1, type: 'transfer', at: 0, resource: 'pool', from: lease.token,
    token: 'lease-forged', holder: 'bob', ttlMs: 100, expiresAt: 100, epoch: 2,
  })}\n`);
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
});

// ---- quotas ----------------------------------------------------------------

test('a transfer under a quota keeps exactly one share occupied', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  registry.setQuota('q', 1);
  registry.declareResource('r', 1, 'q');
  registry.declareResource('other', 1, 'q');
  const first = registry.acquire('r', 'alice', 100);
  const handed = registry.transfer(first.token, 'bob', 100);
  // The quota still carries the single transferred share: no room left.
  assert.throws(
    () => registry.acquire('other', 'x', 100),
    (e) => e.code === 'QUOTA_EXCEEDED',
  );
  assert.equal(registry.assertLease('r', handed.token, 2), true);
  // Releasing the transferred share frees the quota room.
  assert.equal(registry.release(handed.token), true);
  assert.doesNotThrow(() => registry.acquire('other', 'x', 100));
});
