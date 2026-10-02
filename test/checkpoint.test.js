import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  createRegistry,
  CheckpointConflictError,
  LeaseFencedError,
  LogFileError,
} from '../src/index.js';

function makeClock(start = 1000) {
  let now = start;
  return {
    clock: () => now,
    set(value) { now = value; },
    advance(delta) { now += delta; },
  };
}

function tempLogPath() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-checkpoint-'));
  return path.join(dir, 'leases.log');
}

test('first write commits version 1 and read returns the record', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = registry.acquire('res', 'alice');

  const written = registry.writeCheckpoint(
    'res', lease.token, lease.epoch, 0, { height: 10, hash: 'aaa' },
  );
  assert.deepEqual(written, {
    resource: 'res', height: 10, hash: 'aaa', version: 1, epoch: lease.epoch,
  });
  assert.deepEqual(registry.readCheckpoint('res'), written);
});

test('version increments per successful write and conflict is reported', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = registry.acquire('res', 'alice');

  registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });
  const second = registry.writeCheckpoint(
    'res', lease.token, lease.epoch, 1, { height: 2, hash: 'b' },
  );
  assert.equal(second.version, 2);

  assert.throws(
    () => registry.writeCheckpoint('res', lease.token, lease.epoch, 1, { height: 3, hash: 'c' }),
    (error) => {
      assert.ok(error instanceof CheckpointConflictError);
      assert.equal(error.code, 'CHECKPOINT_CONFLICT');
      assert.equal(error.expected, 1);
      assert.equal(error.actual, 2);
      return true;
    },
  );
  // A failed write changes nothing.
  assert.equal(registry.readCheckpoint('res').version, 2);
  assert.equal(registry.readCheckpoint('res').hash, 'b');
});

test('height may decrease for a chain reorganization', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = registry.acquire('res', 'alice');

  registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 100, hash: 'x' });
  const lowered = registry.writeCheckpoint(
    'res', lease.token, lease.epoch, 1, { height: 40, hash: 'y' },
  );
  assert.equal(lowered.height, 40);
  assert.equal(lowered.version, 2);
});

test('read of an unknown checkpoint returns null and validates resource', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  assert.equal(registry.readCheckpoint('nope'), null);
  assert.throws(() => registry.readCheckpoint(''), TypeError);
  assert.throws(() => registry.readCheckpoint(null), TypeError);
  assert.throws(() => registry.readCheckpoint(7), TypeError);
});

test('argument validation throws TypeError before any state check', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = registry.acquire('res', 'alice');
  const good = () => ['res', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }];

  const badCalls = [
    ['', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }],
    [null, lease.token, lease.epoch, 0, { height: 1, hash: 'a' }],
    ['res', '', lease.epoch, 0, { height: 1, hash: 'a' }],
    ['res', lease.token, 0, 0, { height: 1, hash: 'a' }],
    ['res', lease.token, -1, 0, { height: 1, hash: 'a' }],
    ['res', lease.token, 1.5, 0, { height: 1, hash: 'a' }],
    ['res', lease.token, 2 ** 53, 0, { height: 1, hash: 'a' }],
    ['res', lease.token, lease.epoch, -1, { height: 1, hash: 'a' }],
    ['res', lease.token, lease.epoch, 0.5, { height: 1, hash: 'a' }],
    ['res', lease.token, lease.epoch, 0, null],
    ['res', lease.token, lease.epoch, 0, []],
    ['res', lease.token, lease.epoch, 0, 'x'],
    ['res', lease.token, lease.epoch, 0, { height: 1 }],
    ['res', lease.token, lease.epoch, 0, { hash: 'a' }],
    ['res', lease.token, lease.epoch, 0, { height: 1, hash: 'a', extra: 1 }],
    ['res', lease.token, lease.epoch, 0, { height: -1, hash: 'a' }],
    ['res', lease.token, lease.epoch, 0, { height: 1.2, hash: 'a' }],
    ['res', lease.token, lease.epoch, 0, { height: 2 ** 53, hash: 'a' }],
    ['res', lease.token, lease.epoch, 0, { height: 1, hash: '' }],
    ['res', lease.token, lease.epoch, 0, { height: 1, hash: 9 }],
  ];
  for (const args of badCalls) {
    assert.throws(() => registry.writeCheckpoint(...args), TypeError, String(args));
  }
  // None of the failed calls committed anything.
  assert.equal(registry.readCheckpoint('res'), null);
  assert.deepEqual(good().slice(0, 1), ['res']);
});

test('a non-finite clock reading rejects the write with TypeError', () => {
  let now = 1000;
  const registry = createRegistry({ ttlMs: 100, clock: () => now });
  const lease = registry.acquire('res', 'alice');
  now = NaN;
  assert.throws(
    () => registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }),
    TypeError,
  );
  now = Infinity;
  assert.throws(
    () => registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }),
    TypeError,
  );
  assert.equal(registry.readCheckpoint('res'), null);
});

test('fencing rejects stale, foreign, expired and declared-resource credentials', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = registry.acquire('res', 'alice');

  // Unknown token.
  assert.throws(
    () => registry.writeCheckpoint('res', 'nope', lease.epoch, 0, { height: 1, hash: 'a' }),
    (error) => error instanceof LeaseFencedError && error.code === 'LEASE_FENCED',
  );
  // Wrong resource.
  assert.throws(
    () => registry.writeCheckpoint('other', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }),
    LeaseFencedError,
  );
  // Wrong epoch.
  assert.throws(
    () => registry.writeCheckpoint('res', lease.token, lease.epoch + 1, 0, { height: 1, hash: 'a' }),
    LeaseFencedError,
  );
  // Declared (share) resource fences even at capacity 1.
  registry.declareResource('shared', 1);
  const sharedLease = registry.acquire('shared', 'alice');
  assert.throws(
    () => registry.writeCheckpoint(
      'shared', sharedLease.token, sharedLease.epoch, 0, { height: 1, hash: 'a' },
    ),
    LeaseFencedError,
  );
  // Expired credential.
  c.advance(200);
  assert.throws(
    () => registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 1, hash: 'a' }),
    LeaseFencedError,
  );
  assert.equal(registry.readCheckpoint('res'), null);
});

test('a stale holder cannot overwrite a successor checkpoint after takeover', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const first = registry.acquire('res', 'alice');
  registry.writeCheckpoint('res', first.token, first.epoch, 0, { height: 5, hash: 'a' });

  // The lease changes hands; the checkpoint and its version survive.
  const second = registry.transfer(first.token, 'bob');
  assert.throws(
    () => registry.writeCheckpoint('res', first.token, first.epoch, 1, { height: 9, hash: 'old' }),
    LeaseFencedError,
  );
  const written = registry.writeCheckpoint(
    'res', second.token, second.epoch, 1, { height: 6, hash: 'b' },
  );
  assert.equal(written.version, 2);
  assert.equal(written.epoch, second.epoch);
  assert.deepEqual(registry.readCheckpoint('res'), {
    resource: 'res', height: 6, hash: 'b', version: 2, epoch: second.epoch,
  });
});

test('mutating input and output objects does not reach the stored record', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = registry.acquire('res', 'alice');

  const input = { height: 3, hash: 'a' };
  const written = registry.writeCheckpoint('res', lease.token, lease.epoch, 0, input);
  input.height = 99;
  input.hash = 'mutated';
  written.hash = 'mutated';
  const read = registry.readCheckpoint('res');
  assert.deepEqual(read, {
    resource: 'res', height: 3, hash: 'a', version: 1, epoch: lease.epoch,
  });
  read.height = 123;
  assert.equal(registry.readCheckpoint('res').height, 3);
});

test('checkpoint writes and reads move no lease, epoch, counter or queue state', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = registry.acquire('res', 'alice');
  const before = registry.exportState();
  const statsBefore = registry.stats();

  registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });
  registry.readCheckpoint('res');

  assert.deepEqual(registry.stats(), statsBefore);
  assert.deepEqual(registry.exportState(), before);
  assert.equal(registry.epoch('res'), lease.epoch);
  const consistency = registry.checkConsistency();
  assert.deepEqual(consistency.issues, []);
  assert.equal(consistency.ok, true);
});

test('checkpoints survive a restart and folded events are not reapplied', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const lease = registry.acquire('res', 'alice');
  registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 7, hash: 'a' });
  registry.writeCheckpoint('res', lease.token, lease.epoch, 1, { height: 8, hash: 'b' });

  registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.deepEqual(registry.readCheckpoint('res'), {
    resource: 'res', height: 8, hash: 'b', version: 2, epoch: lease.epoch,
  });
  // Pre-restart credentials are void and cannot write.
  assert.throws(
    () => registry.writeCheckpoint('res', lease.token, lease.epoch, 2, { height: 9, hash: 'c' }),
    LeaseFencedError,
  );
  // A fresh credential continues the version sequence, never restarting it.
  c.advance(200_000);
  registry.sweep();
  const next = registry.acquire('res', 'bob');
  const written = registry.writeCheckpoint(
    'res', next.token, next.epoch, 2, { height: 9, hash: 'c' },
  );
  assert.equal(written.version, 3);
});

test('checkpoints survive compaction and continue from the folded version', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const lease = registry.acquire('res', 'alice');
  registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 7, hash: 'a' });
  registry.writeCheckpoint('res', lease.token, lease.epoch, 1, { height: 8, hash: 'b' });
  registry.compact();

  registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.equal(registry.readCheckpoint('res').version, 2);
  assert.equal(registry.readCheckpoint('res').hash, 'b');

  c.advance(200_000);
  registry.sweep();
  const next = registry.acquire('res', 'bob');
  registry.writeCheckpoint('res', next.token, next.epoch, 2, { height: 9, hash: 'c' });
  registry.compact();

  registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.deepEqual(registry.readCheckpoint('res'), {
    resource: 'res', height: 9, hash: 'c', version: 3, epoch: next.epoch,
  });
});

test('a failed append leaves the old checkpoint and version untouched', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const lease = registry.acquire('res', 'alice');
  registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });

  fs.chmodSync(logPath, 0o444);
  try {
    assert.throws(
      () => registry.writeCheckpoint('res', lease.token, lease.epoch, 1, { height: 2, hash: 'b' }),
      (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
    );
  } finally {
    fs.chmodSync(logPath, 0o644);
  }
  assert.deepEqual(registry.readCheckpoint('res'), {
    resource: 'res', height: 1, hash: 'a', version: 1, epoch: lease.epoch,
  });
  // The call is retryable once the log works again.
  const written = registry.writeCheckpoint('res', lease.token, lease.epoch, 1, { height: 2, hash: 'b' });
  assert.equal(written.version, 2);
});

test('a torn tail line is ignored and a corrupt checkpoint record refuses to load', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const lease = registry.acquire('res', 'alice');
  registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });

  // An unterminated fragment never happened.
  fs.appendFileSync(logPath, '{"v":1,"type":"checkpoint","resource":"res"');
  registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.equal(registry.readCheckpoint('res').version, 1);

  // A complete but damaged record is corruption.
  fs.appendFileSync(logPath, '{"v":1,"type":"checkpoint","resource":"res","height":-3,"hash":"x","version":2,"epoch":1}\n');
  assert.throws(
    () => createRegistry({ ttlMs: 100_000, clock: c.clock, logPath }),
    LogFileError,
  );
});

test('a corrupt snapshot checkpoint section refuses to load', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const lease = registry.acquire('res', 'alice');
  registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 1, hash: 'a' });
  registry.compact();

  const snapshot = JSON.parse(fs.readFileSync(`${logPath}.snapshot`, 'utf8'));
  snapshot.checkpoints = [{ resource: 'res', height: 1, hash: 'a', version: 'x', epoch: 1 }];
  fs.writeFileSync(`${logPath}.snapshot`, `${JSON.stringify(snapshot)}\n`);
  assert.throws(
    () => createRegistry({ ttlMs: 100_000, clock: c.clock, logPath }),
    LogFileError,
  );
});

test('old logs and snapshots without checkpoint records still load', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  // Hand-written old-format history: no checkpoint events, no section.
  fs.writeFileSync(logPath, [
    JSON.stringify({ v: 1, type: 'acquire', at: 1000, token: 't1', resource: 'r', holder: 'a', ttlMs: 50, expiresAt: 1050, epoch: 1, batch: 'batch-1-1', e: 1 }),
    JSON.stringify({ v: 1, type: 'release', at: 1001, token: 't1', e: 2 }),
    '',
  ].join('\n'));
  let registry = createRegistry({ ttlMs: 100, clock: c.clock, logPath });
  assert.equal(registry.readCheckpoint('r'), null);
  assert.equal(registry.epoch('r'), 1);

  registry.compact();
  const snapshot = JSON.parse(fs.readFileSync(`${logPath}.snapshot`, 'utf8'));
  delete snapshot.checkpoints;
  fs.writeFileSync(`${logPath}.snapshot`, `${JSON.stringify(snapshot)}\n`);
  registry = createRegistry({ ttlMs: 100, clock: c.clock, logPath });
  assert.equal(registry.readCheckpoint('r'), null);
});

test('without a logPath checkpoints live in memory only', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = registry.acquire('res', 'alice');
  registry.writeCheckpoint('res', lease.token, lease.epoch, 0, { height: 4, hash: 'a' });
  assert.equal(registry.readCheckpoint('res').version, 1);
});
