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
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-checkpoint-all-'));
  return path.join(dir, 'leases.log');
}

function entry(lease, resource, expectedVersion, checkpoint = { height: 1, hash: 'a' }) {
  return {
    resource,
    token: lease.token,
    epoch: lease.epoch,
    expectedVersion,
    checkpoint,
  };
}

test('one batch commits every resource at version 1, in input order and shape', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');

  const written = registry.writeCheckpointAll([
    entry(a, 'res-a', 0, { height: 10, hash: 'aaa' }),
    entry(b, 'res-b', 0, { height: 20, hash: 'bbb' }),
  ]);
  assert.deepEqual(written, [
    { resource: 'res-a', height: 10, hash: 'aaa', version: 1, epoch: a.epoch },
    { resource: 'res-b', height: 20, hash: 'bbb', version: 1, epoch: b.epoch },
  ]);
  assert.deepEqual(registry.readCheckpoint('res-a'), written[0]);
  assert.deepEqual(registry.readCheckpoint('res-b'), written[1]);
});

test('a single-entry batch is the same commit as the single-resource write', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = registry.acquire('res', 'alice');

  const [written] = registry.writeCheckpointAll([
    entry(lease, 'res', 0, { height: 3, hash: 'h' }),
  ]);
  assert.deepEqual(written, {
    resource: 'res', height: 3, hash: 'h', version: 1, epoch: lease.epoch,
  });
  assert.deepEqual(registry.readCheckpoint('res'), written);
});

test('versions continue independently per resource and identical content still advances', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');

  registry.writeCheckpointAll([
    entry(a, 'res-a', 0, { height: 1, hash: 'x' }),
    entry(b, 'res-b', 0, { height: 1, hash: 'y' }),
  ]);
  registry.writeCheckpoint(a.resource, a.token, a.epoch, 1, { height: 5, hash: 'm' });

  // res-a continues at 3 (one batch write plus one single write before it);
  // res-b rewrites byte-identical content and still moves to version 2,
  // including a reorg that lowers the height.
  const written = registry.writeCheckpointAll([
    entry(a, 'res-a', 2, { height: 0, hash: 'x' }),
    entry(b, 'res-b', 1, { height: 1, hash: 'y' }),
  ]);
  assert.equal(written[0].version, 3);
  assert.equal(written[0].height, 0);
  assert.equal(written[1].version, 2);
  assert.equal(written[1].height, 1);
  assert.equal(written[1].hash, 'y');
});

test('credentials may come from different holders and different batches', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100_000, clock: c.clock });
  const first = registry.acquire('res-a', 'alice');
  c.advance(10);
  const second = registry.acquire('res-b', 'bob');

  const written = registry.writeCheckpointAll([
    entry(first, 'res-a', 0, { height: 1, hash: 'a' }),
    entry(second, 'res-b', 0, { height: 2, hash: 'b' }),
  ]);
  assert.equal(written.length, 2);
  assert.equal(registry.holder('res-a'), 'alice');
  assert.equal(registry.holder('res-b'), 'bob');
});

test('a lease handover never resets the checkpoint or its version', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const old = registry.acquire('res', 'alice');
  registry.writeCheckpointAll([entry(old, 'res', 0, { height: 4, hash: 'a' })]);
  const next = registry.transfer(old.token, 'bob');

  const written = registry.writeCheckpointAll([
    entry(next, 'res', 1, { height: 5, hash: 'b' }),
  ]);
  assert.equal(written[0].version, 2);
  assert.equal(written[0].epoch, next.epoch);
  assert.deepEqual(registry.readCheckpoint('res'), {
    resource: 'res', height: 5, hash: 'b', version: 2, epoch: next.epoch,
  });
});

test('batch shape validation throws TypeError before clock or credentials are consulted', () => {
  let now = 1000;
  const registry = createRegistry({ ttlMs: 100, clock: () => now });
  const lease = registry.acquire('res', 'alice');
  const good = (over = {}) => ({
    resource: 'res',
    token: lease.token,
    epoch: lease.epoch,
    expectedVersion: 0,
    checkpoint: { height: 1, hash: 'a' },
    ...over,
  });

  const badBatches = [
    null,
    undefined,
    {},
    'nope',
    [],
    [, good()], // eslint-disable-line no-sparse-arrays
    [null],
    [undefined],
    [42],
    ['x'],
    [[good()]],
    [Object.assign(good(), { extra: 1 })],
    [good({ resource: '' })],
    [good({ resource: null })],
    [good({ resource: 7 })],
    [good({ token: '' })],
    [good({ token: null })],
    [good({ epoch: 0 })],
    [good({ epoch: -1 })],
    [good({ epoch: 1.5 })],
    [good({ epoch: 2 ** 53 })],
    [good({ expectedVersion: -1 })],
    [good({ expectedVersion: 0.5 })],
    [good({ expectedVersion: 2 ** 53 })],
    [good({ checkpoint: null })],
    [good({ checkpoint: [] })],
    [good({ checkpoint: 'x' })],
    [good({ checkpoint: { height: 1 } })],
    [good({ checkpoint: { hash: 'a' } })],
    [good({ checkpoint: { height: 1, hash: 'a', extra: 1 } })],
    [good({ checkpoint: { height: -1, hash: 'a' } })],
    [good({ checkpoint: { height: 1.2, hash: 'a' } })],
    [good({ checkpoint: { height: 2 ** 53, hash: 'a' } })],
    [good({ checkpoint: { height: 1, hash: '' } })],
    [good({ checkpoint: { height: 1, hash: 9 } })],
  ];
  for (const batch of badBatches) {
    assert.throws(() => registry.writeCheckpointAll(batch), TypeError, String(batch));
  }

  // Repeated resource (distinct tokens) and repeated token are shape errors.
  assert.throws(
    () => registry.writeCheckpointAll([
      { resource: 'res', token: lease.token, epoch: lease.epoch, expectedVersion: 0,
        checkpoint: { height: 1, hash: 'a' } },
      { resource: 'res', token: 'other-token', epoch: lease.epoch, expectedVersion: 0,
        checkpoint: { height: 2, hash: 'b' } },
    ]),
    TypeError,
  );
  assert.throws(
    () => registry.writeCheckpointAll([
      { resource: 'res-a', token: lease.token, epoch: lease.epoch, expectedVersion: 0,
        checkpoint: { height: 1, hash: 'a' } },
      { resource: 'res-b', token: lease.token, epoch: lease.epoch, expectedVersion: 0,
        checkpoint: { height: 2, hash: 'b' } },
    ]),
    TypeError,
  );

  // A malformed call is a TypeError even when the clock itself is broken.
  now = NaN;
  assert.throws(() => registry.writeCheckpointAll([null]), TypeError);
  assert.equal(registry.readCheckpoint('res'), null);
});

test('a non-finite clock reading rejects the batch with TypeError', () => {
  let value = 1000;
  const registry = createRegistry({ ttlMs: 100, clock: () => value });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');
  const call = () => registry.writeCheckpointAll([
    entry(a, 'res-a', 0),
    entry(b, 'res-b', 0),
  ]);

  value = NaN;
  assert.throws(call, TypeError);
  value = Infinity;
  assert.throws(call, TypeError);
  assert.equal(registry.readCheckpoint('res-a'), null);
  assert.equal(registry.readCheckpoint('res-b'), null);
});

test('all credentials are fenced before any version is checked, in input order', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');
  registry.writeCheckpointAll([
    entry(a, 'res-a', 0),
    entry(b, 'res-b', 0),
  ]);

  const assertFences = (entries, resource, label) => {
    assert.throws(
      () => registry.writeCheckpointAll(entries),
      (error) => {
        assert.ok(error instanceof LeaseFencedError, label);
        assert.equal(error.code, 'LEASE_FENCED', label);
        assert.equal(error.resource, resource, label);
        return true;
      },
      label,
    );
  };

  // Unknown token on the second entry: the first credential and its stale
  // expected version are never consulted.
  assertFences([
    entry(a, 'res-a', 99),
    { resource: 'res-b', token: 'nope', epoch: b.epoch, expectedVersion: 99,
      checkpoint: { height: 1, hash: 'a' } },
  ], 'res-b', 'unknown token');

  // Resource mismatch.
  assertFences([
    { resource: 'res-other', token: a.token, epoch: a.epoch, expectedVersion: 0,
      checkpoint: { height: 1, hash: 'a' } },
  ], 'res-other', 'resource mismatch');

  // Stale epoch.
  assertFences([
    { resource: 'res-a', token: a.token, epoch: a.epoch + 1, expectedVersion: 1,
      checkpoint: { height: 1, hash: 'a' } },
  ], 'res-a', 'stale epoch');

  // Declared (share) resource fences even at capacity 1.
  registry.declareResource('shared', 1);
  const shared = registry.acquire('shared', 'alice');
  assertFences([entry(shared, 'shared', 0)], 'shared', 'declared resource');

  // Expired credential; the live second entry cannot save the first.
  const d = registry.acquire('res-d', 'dan');
  c.advance(200);
  assertFences([
    entry(a, 'res-a', 1),
    entry(d, 'res-d', 0),
  ], 'res-a', 'expired first');

  // Nothing committed on any failed call.
  assert.equal(registry.readCheckpoint('res-a').version, 1);
  assert.equal(registry.readCheckpoint('res-b').version, 1);
  assert.equal(registry.readCheckpoint('res-d'), null);
});

test('expected versions are checked in order and a conflict commits nothing', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');
  registry.writeCheckpointAll([
    entry(a, 'res-a', 0),
    entry(b, 'res-b', 0, { height: 7, hash: 'z' }),
  ]);

  assert.throws(
    () => registry.writeCheckpointAll([
      entry(a, 'res-a', 1, { height: 2, hash: 'c' }),
      entry(b, 'res-b', 5, { height: 3, hash: 'd' }),
    ]),
    (error) => {
      assert.ok(error instanceof CheckpointConflictError);
      assert.equal(error.code, 'CHECKPOINT_CONFLICT');
      assert.equal(error.resource, 'res-b');
      assert.equal(error.expected, 5);
      assert.equal(error.actual, 1);
      return true;
    },
  );
  // The earlier member did not advance either.
  assert.equal(registry.readCheckpoint('res-a').version, 1);
  assert.equal(registry.readCheckpoint('res-b').version, 1);
  assert.equal(registry.readCheckpoint('res-b').hash, 'z');
});

test('a next version outside the safe integer range rejects the whole batch', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const lease = registry.acquire('res', 'alice');
  registry.writeCheckpointAll([entry(lease, 'res', 0)]);
  registry.compact();

  // Rewrite the folded version to the largest safe integer: the next write
  // would leave the safe integer range and must fail as a TypeError.
  const snapshotPath = `${logPath}.snapshot`;
  const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));
  snapshot.checkpoints[0].version = 2 ** 53 - 1;
  fs.writeFileSync(snapshotPath, `${JSON.stringify(snapshot)}\n`);

  registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  c.advance(200_000);
  registry.sweep();
  const fresh = registry.acquire('res', 'bob');
  assert.throws(
    () => registry.writeCheckpointAll([
      entry(fresh, 'res', 2 ** 53 - 1, { height: 9, hash: 'x' }),
    ]),
    TypeError,
  );
  assert.equal(registry.readCheckpoint('res').version, 2 ** 53 - 1);
});

test('a version mismatch on a later entry beats an overflow on an earlier one', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');
  registry.writeCheckpointAll([
    entry(a, 'res-a', 0, { height: 1, hash: 'a' }),
    entry(b, 'res-b', 0, { height: 1, hash: 'b' }),
  ]);
  registry.compact();

  // Fold res-a at the largest safe version; res-b stays at 1.
  const snapshotPath = `${logPath}.snapshot`;
  const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));
  const rowA = snapshot.checkpoints.find((row) => row.resource === 'res-a');
  rowA.version = 2 ** 53 - 1;
  fs.writeFileSync(snapshotPath, `${JSON.stringify(snapshot)}\n`);

  registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  c.advance(200_000);
  registry.sweep();
  const na = registry.acquire('res-a', 'alice2');
  const nb = registry.acquire('res-b', 'bob2');

  // Every version must be compared before any overflow is raised: entry 0
  // would overflow, but entry 1 mismatches first.
  assert.throws(
    () => registry.writeCheckpointAll([
      entry(na, 'res-a', 2 ** 53 - 1, { height: 2, hash: 'aa' }),
      entry(nb, 'res-b', 99, { height: 2, hash: 'bb' }),
    ]),
    (error) => {
      assert.ok(error instanceof CheckpointConflictError);
      assert.equal(error.resource, 'res-b');
      return true;
    },
  );
  // With the conflict resolved, the overflow now rejects the batch.
  assert.throws(
    () => registry.writeCheckpointAll([
      entry(na, 'res-a', 2 ** 53 - 1, { height: 2, hash: 'aa' }),
      entry(nb, 'res-b', 1, { height: 2, hash: 'bb' }),
    ]),
    TypeError,
  );
});

test('success and failure move no lease, epoch, counter, queue or ticket state', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');
  // A queued waiter that must stay untouched.
  registry.acquire('res-a', 'carol', 100, 10_000);
  const before = registry.exportState();
  const statsBefore = registry.stats();

  registry.writeCheckpointAll([
    entry(a, 'res-a', 0, { height: 1, hash: 'a' }),
    entry(b, 'res-b', 0, { height: 2, hash: 'b' }),
  ]);
  assert.throws(() => registry.writeCheckpointAll([
    entry(a, 'res-a', 99, { height: 3, hash: 'c' }),
  ]), CheckpointConflictError);

  // Checkpoints are invisible to exportState and move no stats; the expired
  // fence below only changes the reading, not the registry.
  assert.deepEqual(registry.stats(), statsBefore);
  assert.deepEqual(registry.exportState(), before);
  assert.equal(registry.epoch('res-a'), a.epoch);
  const consistency = registry.checkConsistency();
  assert.deepEqual(consistency.issues, []);
  assert.equal(consistency.ok, true);

  // After expiry the fence fails and the cumulative counters still do not
  // move (only `live`, which is a reading, does).
  const cumulativeBefore = {
    granted: statsBefore.granted,
    renewed: statsBefore.renewed,
    released: statsBefore.released,
    reclaimed: statsBefore.reclaimed,
  };
  c.advance(200);
  assert.throws(() => registry.writeCheckpointAll([
    entry(a, 'res-a', 1, { height: 4, hash: 'd' }),
  ]), LeaseFencedError);
  const statsAfter = registry.stats();
  assert.deepEqual({
    granted: statsAfter.granted,
    renewed: statsAfter.renewed,
    released: statsAfter.released,
    reclaimed: statsAfter.reclaimed,
  }, cumulativeBefore);
  assert.equal(registry.readCheckpoint('res-a').version, 1);
});

test('mutating entries, payloads or results never reaches the stored records', () => {
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100, clock: c.clock });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');
  const payloadA = { height: 3, hash: 'a' };
  const payloadB = { height: 4, hash: 'b' };
  const entries = [
    entry(a, 'res-a', 0, payloadA),
    entry(b, 'res-b', 0, payloadB),
  ];
  const written = registry.writeCheckpointAll(entries);

  entries[0].resource = 'mutated';
  payloadA.height = 99;
  payloadA.hash = 'mutated';
  written[0].hash = 'mutated';
  written[0].height = 123;
  assert.deepEqual(registry.readCheckpoint('res-a'), {
    resource: 'res-a', height: 3, hash: 'a', version: 1, epoch: a.epoch,
  });
  const read = registry.readCheckpoint('res-b');
  read.hash = 'mutated';
  assert.deepEqual(registry.readCheckpoint('res-b'), {
    resource: 'res-b', height: 4, hash: 'b', version: 1, epoch: b.epoch,
  });
});

test('a failed append leaves every checkpoint untouched and the batch retries verbatim', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');
  const batch = [
    entry(a, 'res-a', 0, { height: 1, hash: 'a' }),
    entry(b, 'res-b', 0, { height: 2, hash: 'b' }),
  ];
  registry.writeCheckpointAll(batch);

  const retry = [
    entry(a, 'res-a', 1, { height: 3, hash: 'c' }),
    entry(b, 'res-b', 1, { height: 4, hash: 'd' }),
  ];
  fs.chmodSync(logPath, 0o444);
  try {
    assert.throws(
      () => registry.writeCheckpointAll(retry),
      (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
    );
  } finally {
    fs.chmodSync(logPath, 0o644);
  }
  assert.deepEqual(registry.readCheckpoint('res-a'), {
    resource: 'res-a', height: 1, hash: 'a', version: 1, epoch: a.epoch,
  });
  assert.deepEqual(registry.readCheckpoint('res-b'), {
    resource: 'res-b', height: 2, hash: 'b', version: 1, epoch: b.epoch,
  });
  // The exact same arguments commit once the log works again.
  const written = registry.writeCheckpointAll(retry);
  assert.equal(written[0].version, 2);
  assert.equal(written[1].version, 2);
});

test('batches apply exactly once across a restart, including mixed single writes', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');
  registry.writeCheckpointAll([
    entry(a, 'res-a', 0, { height: 1, hash: 'a' }),
    entry(b, 'res-b', 0, { height: 2, hash: 'b' }),
  ]);
  registry.writeCheckpoint('res-a', a.token, a.epoch, 1, { height: 2, hash: 'aa' });
  registry.writeCheckpointAll([
    entry(a, 'res-a', 2, { height: 3, hash: 'aaa' }),
    entry(b, 'res-b', 1, { height: 3, hash: 'bb' }),
  ]);

  registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.deepEqual(registry.readCheckpoint('res-a'), {
    resource: 'res-a', height: 3, hash: 'aaa', version: 3, epoch: a.epoch,
  });
  assert.deepEqual(registry.readCheckpoint('res-b'), {
    resource: 'res-b', height: 3, hash: 'bb', version: 2, epoch: b.epoch,
  });

  // Pre-restart credentials are void and fence, naming the first failure.
  assert.throws(
    () => registry.writeCheckpointAll([
      entry(a, 'res-a', 3, { height: 4, hash: 'c' }),
      entry(b, 'res-b', 2, { height: 5, hash: 'd' }),
    ]),
    (error) => error instanceof LeaseFencedError && error.resource === 'res-a',
  );

  // Fresh credentials continue the sequences without resetting them.
  c.advance(200_000);
  registry.sweep();
  const na = registry.acquire('res-a', 'alice2');
  const nb = registry.acquire('res-b', 'bob2');
  const written = registry.writeCheckpointAll([
    entry(na, 'res-a', 3, { height: 4, hash: 'c' }),
    entry(nb, 'res-b', 2, { height: 5, hash: 'd' }),
  ]);
  assert.equal(written[0].version, 4);
  assert.equal(written[1].version, 3);
});

test('batches survive compaction and continue from the folded versions', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');
  registry.writeCheckpointAll([
    entry(a, 'res-a', 0, { height: 1, hash: 'a' }),
    entry(b, 'res-b', 0, { height: 1, hash: 'b' }),
  ]);
  registry.compact();

  registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.equal(registry.readCheckpoint('res-a').version, 1);
  assert.equal(registry.readCheckpoint('res-b').version, 1);

  c.advance(200_000);
  registry.sweep();
  const na = registry.acquire('res-a', 'alice2');
  const nb = registry.acquire('res-b', 'bob2');
  registry.writeCheckpointAll([
    entry(na, 'res-a', 1, { height: 2, hash: 'aa' }),
    entry(nb, 'res-b', 1, { height: 2, hash: 'bb' }),
  ]);
  registry.compact();

  registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.deepEqual(registry.readCheckpoint('res-a'), {
    resource: 'res-a', height: 2, hash: 'aa', version: 2, epoch: na.epoch,
  });
  assert.deepEqual(registry.readCheckpoint('res-b'), {
    resource: 'res-b', height: 2, hash: 'bb', version: 2, epoch: nb.epoch,
  });
});

test('a torn tail batch is ignored; a complete damaged or discontinuous batch refuses to load', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const a = registry.acquire('res-a', 'alice');
  const b = registry.acquire('res-b', 'bob');
  registry.writeCheckpointAll([
    entry(a, 'res-a', 0, { height: 1, hash: 'a' }),
    entry(b, 'res-b', 0, { height: 1, hash: 'b' }),
  ]);

  // An unterminated fragment of a batch record never happened.
  fs.appendFileSync(
    logPath,
    '{"v":1,"type":"checkpoint-all","items":[{"resource":"res-a"',
  );
  registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.equal(registry.readCheckpoint('res-a').version, 1);
  assert.equal(registry.readCheckpoint('res-b').version, 1);

  // A complete but structurally damaged batch record is corruption.
  fs.appendFileSync(logPath, [
    JSON.stringify({
      v: 1,
      type: 'checkpoint-all',
      items: [
        { resource: 'res-a', height: -3, hash: 'x', version: 2, epoch: 1 },
        { resource: 'res-b', height: 2, hash: 'y', version: 2, epoch: 1 },
      ],
    }),
    '',
  ].join('\n'));
  assert.throws(
    () => createRegistry({ ttlMs: 100_000, clock: c.clock, logPath }),
    LogFileError,
  );

  // Remove the damaged line; a version gap inside a batch is also corruption.
  const text = fs.readFileSync(logPath, 'utf8')
    .split('\n')
    .filter((line) => !line.includes('"height":-3'))
    .join('\n');
  fs.writeFileSync(logPath, text);
  fs.appendFileSync(logPath, [
    JSON.stringify({
      v: 1,
      type: 'checkpoint-all',
      items: [
        { resource: 'res-a', height: 2, hash: 'c', version: 2, epoch: 1 },
        { resource: 'res-b', height: 2, hash: 'd', version: 9, epoch: 1 },
      ],
    }),
    '',
  ].join('\n'));
  assert.throws(
    () => createRegistry({ ttlMs: 100_000, clock: c.clock, logPath }),
    LogFileError,
  );
});

test('a duplicate resource inside a persisted batch record is corruption', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  const registry = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const lease = registry.acquire('res-a', 'alice');
  registry.writeCheckpointAll([entry(lease, 'res-a', 0)]);
  fs.appendFileSync(logPath, [
    JSON.stringify({
      v: 1,
      type: 'checkpoint-all',
      items: [
        { resource: 'res-a', height: 2, hash: 'c', version: 2, epoch: lease.epoch },
        { resource: 'res-a', height: 3, hash: 'd', version: 3, epoch: lease.epoch },
      ],
    }),
    '',
  ].join('\n'));
  assert.throws(
    () => createRegistry({ ttlMs: 100_000, clock: c.clock, logPath }),
    LogFileError,
  );
});

test('old logs without batch records and memory-only registries keep working', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  fs.writeFileSync(logPath, [
    JSON.stringify({ v: 1, type: 'acquire', at: 1000, token: 't1', resource: 'r', holder: 'a', ttlMs: 50, expiresAt: 1050, epoch: 1, batch: 'batch-1-1', e: 1 }),
    '',
  ].join('\n'));
  let registry = createRegistry({ ttlMs: 100, clock: c.clock, logPath });
  assert.equal(registry.readCheckpoint('r'), null);
  registry.compact();
  registry = createRegistry({ ttlMs: 100, clock: c.clock, logPath });
  assert.equal(registry.readCheckpoint('r'), null);

  // Without a logPath batches live in memory only.
  const mem = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = mem.acquire('res', 'alice');
  const written = mem.writeCheckpointAll([
    entry(lease, 'res', 0, { height: 8, hash: 'h' }),
  ]);
  assert.equal(written[0].version, 1);
  assert.equal(mem.readCheckpoint('res').hash, 'h');
});
