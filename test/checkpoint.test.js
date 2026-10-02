import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  createRegistry,
  LeaseFencedError,
  LogFileError,
  CheckpointConflictError,
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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-checkpoint-'));
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

function assertConflict(fn, resource, expectedVersion, version) {
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
    (error) => error instanceof CheckpointConflictError
      && error.code === 'CHECKPOINT_CONFLICT',
  );
  assert.equal(caught.resource, resource);
  assert.equal(caught.expectedVersion, expectedVersion);
  assert.equal(caught.version, version);
}

// ---- basics ---------------------------------------------------------------

test('an unknown checkpoint reads as null', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  assert.equal(registry.readCheckpoint('r'), null);
});

test('the first write commits version 1 and reads back', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  const written = registry.writeCheckpoint(
    'r', lease.token, lease.epoch, 0, { height: 10, hash: 'abc' },
  );
  assert.deepEqual(written, {
    resource: 'r', height: 10, hash: 'abc', version: 1, epoch: lease.epoch,
  });
  assert.deepEqual(registry.readCheckpoint('r'), written);
});

test('every committed write advances the version by exactly one', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });
  const second = registry.writeCheckpoint(
    'r', lease.token, lease.epoch, 1, { height: 2, hash: 'b' },
  );
  assert.equal(second.version, 2);
  const third = registry.writeCheckpoint(
    'r', lease.token, lease.epoch, 2, { height: 3, hash: 'c' },
  );
  assert.equal(third.version, 3);
  assert.deepEqual(registry.readCheckpoint('r'), {
    resource: 'r', height: 3, hash: 'c', version: 3, epoch: lease.epoch,
  });
});

test('a write may lower the height to follow a chain reorganization', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 100, hash: 'x' });
  const reorg = registry.writeCheckpoint(
    'r', lease.token, lease.epoch, 1, { height: 40, hash: 'y' },
  );
  assert.equal(reorg.height, 40);
  assert.equal(reorg.version, 2);
});

test('a stale expected version conflicts and commits nothing', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });
  assertConflict(
    () => registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 2, hash: 'b' }),
    'r', 0, 1,
  );
  assertConflict(
    () => registry.writeCheckpoint('r', lease.token, lease.epoch, 5, { height: 2, hash: 'b' }),
    'r', 5, 1,
  );
  assert.deepEqual(registry.readCheckpoint('r'), {
    resource: 'r', height: 1, hash: 'a', version: 1, epoch: lease.epoch,
  });
});

test('a first write must expect version 0', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  assertConflict(
    () => registry.writeCheckpoint('r', lease.token, lease.epoch, 1, { height: 1, hash: 'a' }),
    'r', 1, 0,
  );
  assert.equal(registry.readCheckpoint('r'), null);
});

test('mutating the input or the returned object never reaches the store', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  const checkpoint = { height: 7, hash: 'h' };
  const written = registry.writeCheckpoint('r', lease.token, lease.epoch, 0, checkpoint);
  checkpoint.height = 999;
  checkpoint.hash = 'mutated';
  written.height = 999;
  written.hash = 'mutated';
  const read = registry.readCheckpoint('r');
  assert.deepEqual(read, {
    resource: 'r', height: 7, hash: 'h', version: 1, epoch: lease.epoch,
  });
  read.height = 1;
  read.hash = 'mutated';
  assert.equal(registry.readCheckpoint('r').height, 7);
  assert.equal(registry.readCheckpoint('r').hash, 'h');
});

// ---- argument validation ---------------------------------------------------

test('write validates every argument before any state is consulted', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  const good = ['r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }];
  const bad = [
    ['', ...good.slice(1)],
    [null, ...good.slice(1)],
    [7, ...good.slice(1)],
    [good[0], '', ...good.slice(2)],
    [good[0], null, ...good.slice(2)],
    [good[0], good[1], 0, ...good.slice(3)],
    [good[0], good[1], -1, ...good.slice(3)],
    [good[0], good[1], 1.5, ...good.slice(3)],
    [good[0], good[1], Number.MAX_SAFE_INTEGER + 1, ...good.slice(3)],
    [good[0], good[1], '1', ...good.slice(3)],
    [...good.slice(0, 3), -1, good[4]],
    [...good.slice(0, 3), 0.5, good[4]],
    [...good.slice(0, 3), Number.MAX_SAFE_INTEGER + 1, good[4]],
    [...good.slice(0, 3), '0', good[4]],
    [...good.slice(0, 4), null],
    [...good.slice(0, 4), 'x'],
    [...good.slice(0, 4), []],
    [...good.slice(0, 4), { height: -1, hash: 'a' }],
    [...good.slice(0, 4), { height: 1.5, hash: 'a' }],
    [...good.slice(0, 4), { height: Number.MAX_SAFE_INTEGER + 1, hash: 'a' }],
    [...good.slice(0, 4), { height: 1 }],
    [...good.slice(0, 4), { height: 1, hash: '' }],
    [...good.slice(0, 4), { height: 1, hash: 7 }],
  ];
  for (const args of bad) {
    assert.throws(() => registry.writeCheckpoint(...args), TypeError);
  }
  assert.equal(registry.readCheckpoint('r'), null);
});

test('read validates the resource argument', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  for (const bad of ['', null, undefined, 7, {}]) {
    assert.throws(() => registry.readCheckpoint(bad), TypeError);
  }
});

test('a non-finite clock reading rejects the write as a TypeError', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'h');
  clock.set(NaN);
  assert.throws(
    () => registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }),
    TypeError,
  );
  clock.set(Infinity);
  assert.throws(
    () => registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }),
    TypeError,
  );
  assert.equal(registry.readCheckpoint('r'), null);
});

test('read never consults the clock', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'h');
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });
  clock.set(NaN);
  assert.equal(registry.readCheckpoint('r').height, 1);
});

// ---- fencing ----------------------------------------------------------------

test('unknown, released, expired and other-resource credentials are fenced', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'h');
  const other = registry.acquire('other', 'h');

  assertFenced(
    () => registry.writeCheckpoint('r', 'lease-unknown', 1, 0, { height: 1, hash: 'a' }),
    null,
  );
  // A live credential of another resource.
  assertFenced(
    () => registry.writeCheckpoint('r', other.token, other.epoch, 0, { height: 1, hash: 'a' }),
    'r',
  );
  // A released credential is gone from the table entirely.
  const gone = registry.acquire('gone', 'h');
  registry.release(gone.token);
  assertFenced(
    () => registry.writeCheckpoint('gone', gone.token, gone.epoch, 0, { height: 1, hash: 'a' }),
  );
  // An expired credential.
  clock.set(1000);
  assertFenced(
    () => registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }),
    'r',
  );
  assert.equal(registry.readCheckpoint('r'), null);
});

test('a stale epoch and a declared resource are fenced', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  assertFenced(
    () => registry.writeCheckpoint('r', lease.token, lease.epoch + 1, 0, { height: 1, hash: 'a' }),
    'r',
  );
  // After a transfer the new credential carrying an older epoch is fenced too.
  const handed = registry.transfer(lease.token, 'h2');
  assertFenced(
    () => registry.writeCheckpoint('r', handed.token, lease.epoch, 0, { height: 1, hash: 'a' }),
    'r',
  );

  registry.declareResource('shared', 2);
  const share = registry.acquire('shared', 'h');
  assertFenced(
    () => registry.writeCheckpoint(
      'shared', share.token, share.epoch, 0, { height: 1, hash: 'a' },
    ),
    'shared',
  );
});

test('a takeover fences the old holder and the checkpoint survives', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const first = registry.acquire('r', 'old');
  registry.writeCheckpoint('r', first.token, first.epoch, 0, { height: 5, hash: 'a' });

  // The lease changes hands: the epoch advances, the checkpoint does not move.
  const second = registry.transfer(first.token, 'new');
  assert.equal(registry.readCheckpoint('r').height, 5);
  assert.equal(registry.readCheckpoint('r').version, 1);

  // The old holder can neither write nor roll the progress back; its
  // credential was replaced by the transfer and is gone from the table.
  assertFenced(
    () => registry.writeCheckpoint('r', first.token, first.epoch, 1, { height: 6, hash: 'b' }),
  );
  // The new holder continues the same version chain.
  const written = registry.writeCheckpoint(
    'r', second.token, second.epoch, 1, { height: 6, hash: 'b' },
  );
  assert.equal(written.version, 2);
  assert.equal(written.epoch, second.epoch);
});

test('a retake after expiry keeps the checkpoint and its version', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const first = registry.acquire('r', 'a');
  registry.writeCheckpoint('r', first.token, first.epoch, 0, { height: 3, hash: 'a' });
  clock.set(1000);
  const second = registry.acquire('r', 'b');
  assert.equal(registry.readCheckpoint('r').version, 1);
  // The displaced credential is gone from the table entirely.
  assertFenced(
    () => registry.writeCheckpoint('r', first.token, first.epoch, 1, { height: 4, hash: 'b' }),
  );
  const written = registry.writeCheckpoint(
    'r', second.token, second.epoch, 1, { height: 4, hash: 'b' },
  );
  assert.equal(written.version, 2);
});

test('a pre-restart credential is fenced even while its lease occupies', () => {
  const p = logPath('legacy');
  const c1 = fakeClock(0);
  let registry = createRegistry({ ttlMs: 1000, clock: c1, logPath: p });
  const lease = registry.acquire('r', 'h');
  registry = createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p });
  assertFenced(
    () => registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }),
    'r',
  );
});

// ---- side-effect freedom ----------------------------------------------------

test('checkpoint writes move no lease, epoch, counter or queue state', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'h');
  const before = registry.exportState();
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });
  registry.writeCheckpoint('r', lease.token, lease.epoch, 1, { height: 2, hash: 'b' });
  registry.readCheckpoint('r');
  const after = registry.exportState();
  assert.deepEqual(after.stats, before.stats);
  assert.deepEqual(after.leases, before.leases);
  assert.deepEqual(after.resources, before.resources);
  assert.deepEqual(after.waits, before.waits);
  assert.equal(registry.epoch('r'), lease.epoch);
  assert.equal(registry.checkConsistency().ok, true);
});

// ---- persistence --------------------------------------------------------------

test('a restart recovers the last committed checkpoint and version', () => {
  const p = logPath('restart');
  const c1 = fakeClock(0);
  let registry = createRegistry({ ttlMs: 1000, clock: c1, logPath: p });
  const lease = registry.acquire('r', 'h');
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 9, hash: 'a' });
  registry.writeCheckpoint('r', lease.token, lease.epoch, 1, { height: 10, hash: 'b' });

  registry = createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p });
  assert.deepEqual(registry.readCheckpoint('r'), {
    resource: 'r', height: 10, hash: 'b', version: 2, epoch: lease.epoch,
  });
  // The version chain continues; it does not restart.
  const fresh = registry.acquire('r2', 'h');
  registry.writeCheckpoint('r2', fresh.token, fresh.epoch, 0, { height: 1, hash: 'x' });
  registry = createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p });
  assert.equal(registry.readCheckpoint('r').version, 2);
  assert.equal(registry.readCheckpoint('r2').version, 1);
});

test('compaction folds checkpoints and the log continues past them', () => {
  const p = logPath('compact');
  const c1 = fakeClock(0);
  let registry = createRegistry({ ttlMs: 1000, clock: c1, logPath: p });
  const lease = registry.acquire('r', 'h');
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 9, hash: 'a' });
  registry.writeCheckpoint('r', lease.token, lease.epoch, 1, { height: 10, hash: 'b' });
  registry.compact();
  registry.writeCheckpoint('r', lease.token, lease.epoch, 2, { height: 11, hash: 'c' });

  registry = createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p });
  assert.deepEqual(registry.readCheckpoint('r'), {
    resource: 'r', height: 11, hash: 'c', version: 3, epoch: lease.epoch,
  });
  // A second restart applies nothing twice.
  registry = createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p });
  assert.equal(registry.readCheckpoint('r').version, 3);
});

test('a failed append commits nothing and the old value stays in force', () => {
  const p = logPath('sabotage');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 1000, clock, logPath: p });
  const lease = registry.acquire('r', 'h');
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });

  sabotage(p);
  assert.throws(
    () => registry.writeCheckpoint('r', lease.token, lease.epoch, 1, { height: 2, hash: 'b' }),
    (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
  );
  restore(p);
  // Value and version are exactly as before the failed write, so the natural
  // retry with the same expected version succeeds.
  assert.deepEqual(registry.readCheckpoint('r'), {
    resource: 'r', height: 1, hash: 'a', version: 1, epoch: lease.epoch,
  });
  const retried = registry.writeCheckpoint(
    'r', lease.token, lease.epoch, 1, { height: 2, hash: 'b' },
  );
  assert.equal(retried.version, 2);
});

test('an unterminated tail line is ignored on recovery', () => {
  const p = logPath('torn');
  const c1 = fakeClock(0);
  let registry = createRegistry({ ttlMs: 1000, clock: c1, logPath: p });
  const lease = registry.acquire('r', 'h');
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });
  fs.appendFileSync(p, '{"v":1,"type":"checkpoint","resource":"r","heigh');
  registry = createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p });
  assert.equal(registry.readCheckpoint('r').version, 1);
});

test('a complete but corrupt checkpoint record fails the recovery', () => {
  const p = logPath('corrupt');
  const c1 = fakeClock(0);
  const registry = createRegistry({ ttlMs: 1000, clock: c1, logPath: p });
  const lease = registry.acquire('r', 'h');
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });
  fs.appendFileSync(
    p,
    `${JSON.stringify({ v: 1, type: 'checkpoint', at: 0, resource: 'r', height: -1, hash: 'x', version: 2, epoch: 1 })}\n`,
  );
  assert.throws(
    () => createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p }),
    (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
  );
});

test('a corrupt snapshot checkpoint section fails the recovery', () => {
  const p = logPath('corrupt-snapshot');
  const c1 = fakeClock(0);
  const registry = createRegistry({ ttlMs: 1000, clock: c1, logPath: p });
  const lease = registry.acquire('r', 'h');
  registry.writeCheckpoint('r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });
  registry.compact();
  const snapshot = JSON.parse(fs.readFileSync(`${p}.snapshot`, 'utf8'));
  snapshot.checkpoints[0].version = 0;
  fs.writeFileSync(`${p}.snapshot`, `${JSON.stringify(snapshot)}\n`);
  assert.throws(
    () => createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p }),
    (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
  );
});

test('logs and snapshots written before checkpoints existed still load', () => {
  const p = logPath('old-format');
  const c1 = fakeClock(0);
  let registry = createRegistry({ ttlMs: 1000, clock: c1, logPath: p });
  registry.acquire('r', 'h');
  registry.compact();
  // Strip the checkpoints section, mimicking an image written by an older build.
  const snapshot = JSON.parse(fs.readFileSync(`${p}.snapshot`, 'utf8'));
  delete snapshot.checkpoints;
  fs.writeFileSync(`${p}.snapshot`, `${JSON.stringify(snapshot)}\n`);
  registry = createRegistry({ ttlMs: 1000, clock: fakeClock(0), logPath: p });
  assert.equal(registry.readCheckpoint('r'), null);
  assert.equal(registry.checkConsistency().ok, true);
});

test('without a logPath checkpoints live in memory only', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'h');
  const written = registry.writeCheckpoint(
    'r', lease.token, lease.epoch, 0, { height: 1, hash: 'a' },
  );
  assert.equal(written.version, 1);
  assert.deepEqual(registry.readCheckpoint('r'), written);
});
