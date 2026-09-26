import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { createRegistry, LogFileError, LeaseTakenError } from '../src/index.js';

function fakeClock(start = 0) {
  let now = start;
  const clock = () => now;
  clock.set = (value) => {
    now = value;
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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-log-'));
});

test.afterEach(() => {
  fs.rmSync(dir, { recursive: true, force: true });
});

test('replay restores live leases and cumulative counters without extra counts', () => {
  const p = logPath('restart');
  const c1 = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c1, logPath: p });
  const keep = r.acquire('keep', 'h1', 100);
  const renewed = r.acquire('renewed', 'h2', 40);
  const freed = r.acquire('freed', 'h3', 100);
  r.acquire('expired', 'h4', 10);
  c1.set(30);
  assert.equal(r.renew(renewed.token), true);
  assert.equal(r.release(freed.token), true);
  c1.set(50);
  assert.deepEqual(r.sweep(), ['expired']);
  assert.deepEqual(r.stats(), {
    granted: 4, renewed: 1, released: 1, reclaimed: 1, live: 2,
  });

  const c2 = fakeClock(50);
  r = createRegistry({ ttlMs: 100, clock: c2, logPath: p });

  assert.equal(r.holder('keep'), 'h1');
  assert.equal(r.holder('renewed'), 'h2');
  assert.equal(r.holder('freed'), null);
  assert.equal(r.holder('expired'), null);
  assert.deepEqual(r.stats(), {
    granted: 4, renewed: 1, released: 1, reclaimed: 1, live: 2,
  });

  // Credentials issued before the restart are void.
  assert.equal(r.renew(keep.token), false);
  assert.equal(r.release(keep.token), false);
  assert.equal(r.renew(renewed.token), false);
  assert.equal(r.release(freed.token), false);
  assert.deepEqual(r.stats(), {
    granted: 4, renewed: 1, released: 1, reclaimed: 1, live: 2,
  });
});

test('live replayed leases block acquisition after restart', () => {
  const p = logPath('blocked');
  let r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  r.acquire('r', 'a');
  r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });

  assert.throws(
    () => r.acquire('r', 'b'),
    (e) => e instanceof LeaseTakenError && e.code === 'LEASE_TAKEN',
  );
  assert.equal(r.stats().granted, 1);
});

test('after restart sweep reclaims in expiry order and counts once', () => {
  const p = logPath('sweep-order');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.acquire('later', 'h', 50);
  r.acquire('earlier', 'h', 10);
  r.acquire('middle', 'h', 30);

  r = createRegistry({ ttlMs: 100, clock: fakeClock(100), logPath: p });
  assert.deepEqual(r.sweep(), ['earlier', 'middle', 'later']);
  assert.deepEqual(r.sweep(), []);
  assert.equal(r.stats().reclaimed, 3);
});

test('counters and liveness survive a second restart', () => {
  const p = logPath('twice');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const l = r.acquire('r', 'a', 200);
  c.set(50);
  assert.equal(r.renew(l.token), true);

  r = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  r = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r.holder('r'), 'a');
  assert.deepEqual(r.stats(), {
    granted: 1, renewed: 1, released: 0, reclaimed: 0, live: 1,
  });
  assert.equal(r.renew(l.token), false);
});

test('replayed renewals keep the final expiry', () => {
  const p = logPath('renewed-expiry');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const l = r.acquire('r', 'h', 100);
  c.set(90);
  assert.equal(r.renew(l.token), true); // now expires 190

  r = createRegistry({ ttlMs: 100, clock: fakeClock(150), logPath: p });
  assert.equal(r.holder('r'), 'h');
  assert.deepEqual(r.sweep(189), []);
  assert.deepEqual(r.sweep(190), ['r']);
});

test('a missing log starts a fresh registry and creates the file on grant', () => {
  const p = logPath('missing');
  const r = createRegistry({ ttlMs: 10, clock: fakeClock(0), logPath: p });
  assert.deepEqual(r.stats(), {
    granted: 0, renewed: 0, released: 0, reclaimed: 0, live: 0,
  });
  assert.equal(fs.existsSync(p), false);
  r.acquire('r', 'h');
  assert.equal(fs.existsSync(p), true);
});

test('corrupt lines, unknown versions and unknown event types throw LogFileError', () => {
  const corrupt = logPath('corrupt');
  fs.writeFileSync(corrupt, '{"v":1,"type":"acquire","token":"t","ttlMs":10,"expiresAt":10}\n{not json\n');
  assert.throws(
    () => createRegistry({ ttlMs: 10, clock: fakeClock(0), logPath: corrupt }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  const badVersion = logPath('bad-version');
  fs.writeFileSync(badVersion, '{"v":99,"type":"acquire"}\n');
  assert.throws(
    () => createRegistry({ ttlMs: 10, clock: fakeClock(0), logPath: badVersion }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  const badType = logPath('bad-type');
  fs.writeFileSync(badType, '{"v":1,"type":"nope"}\n');
  assert.throws(
    () => createRegistry({ ttlMs: 10, clock: fakeClock(0), logPath: badType }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );

  const dirAsLog = path.join(dir, 'a-directory');
  fs.mkdirSync(dirAsLog);
  assert.throws(
    () => createRegistry({ ttlMs: 10, clock: fakeClock(0), logPath: dirAsLog }),
    (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
  );
});

test('a failed append on acquire rolls state and counters back', () => {
  const p = logPath('rollback-acquire');
  const r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  r.acquire('r1', 'h');
  r.acquire('r2', 'h');

  sabotage(p);
  try {
    assert.throws(
      () => r.acquire('r3', 'h'),
      (e) => e instanceof LogFileError && e.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }
  assert.equal(r.holder('r3'), null);
  assert.equal(r.stats().granted, 2);

  // The registry keeps working; the failed attempt burned no credential.
  const next = r.acquire('r3', 'h');
  assert.equal(next.holder, 'h');
  assert.equal(r.stats().granted, 3);
});

test('a failed append on renew rolls the extension back', () => {
  const p = logPath('rollback-renew');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const l = r.acquire('r', 'h', 100);

  c.set(20);
  sabotage(p);
  try {
    assert.throws(() => r.renew(l.token), (e) => e instanceof LogFileError);
  } finally {
    restore(p);
  }
  assert.equal(r.stats().renewed, 0);
  c.set(100);
  assert.equal(r.holder('r'), null);
});

test('a failed append on release leaves the lease and credential intact', () => {
  const p = logPath('rollback-release');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const l = r.acquire('r', 'h', 100);

  c.set(20);
  sabotage(p);
  try {
    assert.throws(() => r.release(l.token), (e) => e instanceof LogFileError);
  } finally {
    restore(p);
  }
  assert.equal(r.holder('r'), 'h');
  assert.equal(r.stats().released, 0);
  assert.equal(r.release(l.token), true);
  assert.equal(r.stats().released, 1);
});

test('a failed reclaim write puts the popped leases back', () => {
  const p = logPath('rollback-sweep');
  const c = fakeClock(0);
  const r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  r.acquire('a', 'h', 10);
  r.acquire('b', 'h', 20);

  c.set(30);
  sabotage(p);
  try {
    assert.throws(() => r.sweep(), (e) => e instanceof LogFileError);
  } finally {
    restore(p);
  }
  assert.equal(r.stats().reclaimed, 0);
  assert.deepEqual(r.sweep(), ['a', 'b']);
  assert.equal(r.stats().reclaimed, 2);
});

test('registries in the same process never accept each others credentials', () => {
  const p1 = logPath('peer-1');
  const p2 = logPath('peer-2');
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p1 });
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p2 });
  const l1 = r1.acquire('res', 'a');
  const l2 = r2.acquire('res', 'b');

  assert.notEqual(l1.token, l2.token);
  assert.equal(r1.renew(l2.token), false);
  assert.equal(r2.release(l1.token), false);
  assert.equal(r1.holder('res'), 'a');
  assert.equal(r2.holder('res'), 'b');
});

test('new tokens never reuse replayed credentials, including dead ones', () => {
  const p = logPath('no-collision');
  const c = fakeClock(0);
  let r = createRegistry({ ttlMs: 100, clock: c, logPath: p });
  const released = r.acquire('freed', 'h', 100);
  const reclaimed = r.acquire('gone', 'h', 10);
  assert.equal(r.release(released.token), true);
  c.set(10);
  assert.deepEqual(r.sweep(), ['gone']);

  // Restart with a clock reading identical to the original grants.
  r = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const fresh = r.acquire('new', 'h');
  assert.notEqual(fresh.token, released.token);
  assert.notEqual(fresh.token, reclaimed.token);
  assert.equal(r.renew(released.token), false);
  assert.equal(r.release(reclaimed.token), false);
});
