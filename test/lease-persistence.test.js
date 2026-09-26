import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

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

function tempDir(t) {
  const dir = mkdtempSync(join(tmpdir(), 'lease-registry-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

test('restart replays the log: counts and live leases survive, credentials do not', (t) => {
  const dir = tempDir(t);
  const logPath = join(dir, 'leases.log');
  const clock = fakeClock(0);

  const first = createRegistry({ ttlMs: 100, clock, logPath });
  const a = first.acquire('a', 'node-1', 50);
  const b = first.acquire('b', 'node-1', 200);
  const c = first.acquire('c', 'node-1', 10);
  clock.set(5);
  assert.equal(first.renew(a.token), true); // a now expires at 55
  const d = first.acquire('d', 'node-1', 20);
  assert.equal(first.release(d.token), true);
  clock.set(10);
  assert.deepEqual(first.sweep(), ['c']); // c expired at 10
  const before = first.stats();
  assert.deepEqual(before, {
    granted: 4,
    renewed: 1,
    released: 1,
    reclaimed: 1,
    live: 2,
  });

  // Restart: same log, same clock reading. Nothing is counted again.
  const second = createRegistry({ ttlMs: 100, clock, logPath });
  assert.deepEqual(second.stats(), before);
  assert.equal(second.holder('a'), 'node-1');
  assert.equal(second.holder('b'), 'node-1');
  assert.equal(second.holder('c'), null);
  assert.equal(second.holder('d'), null);

  // Recovered leases still occupy their resources.
  assert.throws(
    () => second.acquire('a', 'node-2'),
    (error) => error instanceof LeaseTakenError,
  );

  // Pre-restart credentials are dead and failed attempts count nothing.
  assert.equal(second.renew(a.token), false);
  assert.equal(second.release(a.token), false);
  assert.equal(second.renew(b.token), false);
  assert.equal(second.release(b.token), false);
  assert.deepEqual(second.stats(), before);

  // Recovered leases are reclaimed in expiry order (a@55 before b@200).
  clock.set(300);
  assert.deepEqual(second.sweep(), ['a', 'b']);
  assert.equal(second.stats().reclaimed, 3);

  // New credentials minted after the restart work and persist.
  const e = second.acquire('e', 'node-2', 10);
  assert.equal(second.stats().granted, 5);
  const third = createRegistry({ ttlMs: 100, clock, logPath });
  assert.equal(third.holder('e'), 'node-2');
  assert.deepEqual(third.stats(), {
    granted: 5,
    renewed: 1,
    released: 1,
    reclaimed: 3,
    live: 1,
  });
  assert.equal(third.renew(e.token), false); // minted by `second`, not `third`
});

test('a missing log file starts a fresh registry without throwing', (t) => {
  const dir = tempDir(t);
  const logPath = join(dir, 'never-written.log');
  const registry = createRegistry({ ttlMs: 10, clock: fakeClock(0), logPath });
  assert.deepEqual(registry.stats(), {
    granted: 0,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 0,
  });
});

test('a corrupt log line throws LogFileError with a code', (t) => {
  const dir = tempDir(t);
  const logPath = join(dir, 'leases.log');
  writeFileSync(
    logPath,
    '{"v":1,"type":"grant","token":"lease-1-1-1-0","resource":"r","holder":"h","expiresAt":10,"ttlMs":10}\nnot-json\n',
  );

  let caught;
  assert.throws(
    () => createRegistry({ ttlMs: 10, clock: fakeClock(0), logPath }),
    (error) => {
      caught = error;
      return error instanceof LogFileError;
    },
  );
  assert.equal(caught.code, 'LOG_FILE_ERROR');
});

test('an unrecognized log version throws LogFileError', (t) => {
  const dir = tempDir(t);
  const logPath = join(dir, 'leases.log');
  writeFileSync(logPath, '{"v":99,"type":"grant","token":"x","expiresAt":1,"ttlMs":1}\n');

  let caught;
  assert.throws(
    () => createRegistry({ ttlMs: 10, clock: fakeClock(0), logPath }),
    (error) => {
      caught = error;
      return error instanceof LogFileError;
    },
  );
  assert.equal(caught.code, 'LOG_FILE_ERROR');
});

test('a failed log write aborts acquire and rolls state back', (t) => {
  const dir = tempDir(t);
  // The parent directory does not exist, so appending fails.
  const logPath = join(dir, 'missing-dir', 'leases.log');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 50, clock, logPath }); // missing file is fine

  let caught;
  assert.throws(
    () => registry.acquire('r', 'a'),
    (error) => {
      caught = error;
      return error instanceof LogFileError;
    },
  );
  assert.equal(caught.code, 'LOG_FILE_ERROR');
  assert.deepEqual(registry.stats(), {
    granted: 0,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 0,
  });
  assert.equal(registry.holder('r'), null);
});

test('a failed write during release keeps the lease untouched', (t) => {
  const dir = tempDir(t);
  const logPath = join(dir, 'leases.log');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 50, clock, logPath });
  const lease = registry.acquire('r', 'a');

  // Break the log path: replace the file with a directory.
  rmSync(logPath);
  mkdirSync(logPath);

  assert.throws(
    () => registry.release(lease.token),
    (error) => error instanceof LogFileError,
  );
  assert.equal(registry.holder('r'), 'a');
  assert.deepEqual(registry.stats(), {
    granted: 1,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 1,
  });
  assert.throws(
    () => registry.renew(lease.token), // renew hits the broken log too
    (error) => error instanceof LogFileError,
  );
  assert.equal(registry.stats().renewed, 0);
});

test('a failed write during sweep reclaims nothing and can be retried', (t) => {
  const dir = tempDir(t);
  const logPath = join(dir, 'leases.log');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 10, clock, logPath });
  registry.acquire('r', 'a');
  clock.set(20);

  rmSync(logPath);
  mkdirSync(logPath);
  assert.throws(
    () => registry.sweep(),
    (error) => error instanceof LogFileError,
  );
  assert.equal(registry.stats().reclaimed, 0);

  // Once the log works again the same lease is reclaimed, exactly once.
  rmSync(logPath, { recursive: true });
  assert.deepEqual(registry.sweep(), ['r']);
  assert.equal(registry.stats().reclaimed, 1);
  assert.deepEqual(registry.sweep(), []);
});

test('instances in one process mint unique tokens and do not honor each other', (t) => {
  const dir = tempDir(t);
  const a = createRegistry({
    ttlMs: 100,
    clock: fakeClock(0),
    logPath: join(dir, 'a.log'),
  });
  const b = createRegistry({
    ttlMs: 100,
    clock: fakeClock(0),
    logPath: join(dir, 'b.log'),
  });

  const leaseA = a.acquire('shared-name', 'a');
  const leaseB = b.acquire('shared-name', 'b');
  assert.notEqual(leaseA.token, leaseB.token);

  assert.equal(b.renew(leaseA.token), false);
  assert.equal(b.release(leaseA.token), false);
  assert.equal(a.renew(leaseB.token), false);
  assert.equal(a.release(leaseB.token), false);
  assert.equal(b.holder('shared-name'), 'b');
  assert.deepEqual(b.stats(), {
    granted: 1,
    renewed: 0,
    released: 0,
    reclaimed: 0,
    live: 1,
  });
});

test('only one acquirer wins a contested resource and the loser counts nothing', (t) => {
  const dir = tempDir(t);
  const logPath = join(dir, 'leases.log');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 50, clock, logPath });

  registry.acquire('r', 'winner');
  let caught;
  assert.throws(
    () => registry.acquire('r', 'loser'),
    (error) => {
      caught = error;
      return error instanceof LeaseTakenError;
    },
  );
  assert.equal(caught.code, 'LEASE_TAKEN');
  assert.equal(registry.stats().granted, 1);

  // The failed acquire wrote nothing; replay shows a single grant.
  const recovered = createRegistry({ ttlMs: 50, clock, logPath });
  assert.equal(recovered.stats().granted, 1);
  assert.equal(recovered.holder('r'), 'winner');
});
