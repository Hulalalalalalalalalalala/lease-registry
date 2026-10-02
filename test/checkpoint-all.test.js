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

test('batch commit returns records in input order, visible at once', () => {
  const c = makeClock();
  const r = createRegistry({ ttlMs: 100, clock: c.clock });
  const a = r.acquire('resA', 'alice');
  const b = r.acquire('resB', 'bob');

  const written = r.writeCheckpointAll([
    { resource: 'resB', token: b.token, epoch: b.epoch, expectedVersion: 0, checkpoint: { height: 5, hash: 'hb' } },
    { resource: 'resA', token: a.token, epoch: a.epoch, expectedVersion: 0, checkpoint: { height: 9, hash: 'ha' } },
  ]);
  assert.deepEqual(written, [
    { resource: 'resB', height: 5, hash: 'hb', version: 1, epoch: b.epoch },
    { resource: 'resA', height: 9, hash: 'ha', version: 1, epoch: a.epoch },
  ]);
  assert.deepEqual(r.readCheckpoint('resA'), written[1]);
  assert.deepEqual(r.readCheckpoint('resB'), written[0]);

  // Same content again still advances the version.
  const again = r.writeCheckpointAll([
    { resource: 'resA', token: a.token, epoch: a.epoch, expectedVersion: 1, checkpoint: { height: 9, hash: 'ha' } },
  ]);
  assert.equal(again[0].version, 2);
});

test('shape validation throws TypeError before clock or credentials', () => {
  let clockReads = 0;
  const r = createRegistry({ ttlMs: 100, clock: () => { clockReads += 1; return 1000; } });
  const lease = r.acquire('res', 'alice');
  clockReads = 0;
  const good = { resource: 'res', token: lease.token, epoch: lease.epoch, expectedVersion: 0, checkpoint: { height: 1, hash: 'a' } };

  const sparse = [good]; sparse.length = 2; // hole at index 1
  const bad = [
    null, undefined, 7, 'x', [], {},
    [null], [undefined], [7], ['x'], [[]], sparse,
    [{ ...good, extra: 1 }],
    [{ resource: 'res', token: lease.token, epoch: lease.epoch, expectedVersion: 0 }],
    [{ ...good, resource: '' }], [{ ...good, resource: 3 }],
    [{ ...good, token: '' }], [{ ...good, token: null }],
    [{ ...good, epoch: 0 }], [{ ...good, epoch: -1 }], [{ ...good, epoch: 1.5 }], [{ ...good, epoch: 2 ** 53 }],
    [{ ...good, expectedVersion: -1 }], [{ ...good, expectedVersion: 0.5 }],
    [{ ...good, checkpoint: null }], [{ ...good, checkpoint: [] }], [{ ...good, checkpoint: 'x' }],
    [{ ...good, checkpoint: { height: 1 } }],
    [{ ...good, checkpoint: { height: 1, hash: 'a', extra: 1 } }],
    [{ ...good, checkpoint: { height: -1, hash: 'a' } }],
    [{ ...good, checkpoint: { height: 1, hash: '' } }],
    [good, { ...good }], // duplicate resource and token
    [good, { ...good, token: 'other-token' }], // duplicate resource
    [good, { ...good, resource: 'other-res' }], // duplicate token
  ];
  for (const input of bad) {
    assert.throws(() => r.writeCheckpointAll(input), TypeError, JSON.stringify(input));
  }
  assert.equal(clockReads, 0);
  assert.equal(r.readCheckpoint('res'), null);
});

test('non-finite clock reading rejects the batch with TypeError', () => {
  let now = 1000;
  const r = createRegistry({ ttlMs: 100, clock: () => now });
  const lease = r.acquire('res', 'alice');
  now = NaN;
  assert.throws(
    () => r.writeCheckpointAll([{ resource: 'res', token: lease.token, epoch: lease.epoch, expectedVersion: 0, checkpoint: { height: 1, hash: 'a' } }]),
    TypeError,
  );
  assert.equal(r.readCheckpoint('res'), null);
});

test('fence checks run in input order and name the first failing resource', () => {
  const c = makeClock();
  const r = createRegistry({ ttlMs: 100, clock: c.clock });
  const a = r.acquire('resA', 'alice');
  const b = r.acquire('resB', 'bob');

  // Second entry fenced (unknown token); first is fine.
  assert.throws(
    () => r.writeCheckpointAll([
      { resource: 'resA', token: a.token, epoch: a.epoch, expectedVersion: 0, checkpoint: { height: 1, hash: 'a' } },
      { resource: 'resB', token: 'nope', epoch: b.epoch, expectedVersion: 0, checkpoint: { height: 1, hash: 'b' } },
    ]),
    (error) => error instanceof LeaseFencedError && error.code === 'LEASE_FENCED' && error.resource === 'resB',
  );
  // First entry fenced wins over a later failure.
  assert.throws(
    () => r.writeCheckpointAll([
      { resource: 'resB', token: 'nope', epoch: b.epoch, expectedVersion: 0, checkpoint: { height: 1, hash: 'b' } },
      { resource: 'resA', token: 'also-nope', epoch: a.epoch, expectedVersion: 0, checkpoint: { height: 1, hash: 'a' } },
    ]),
    (error) => error instanceof LeaseFencedError && error.resource === 'resB',
  );
  // Wrong epoch, declared resource, expired credential all fence.
  assert.throws(
    () => r.writeCheckpointAll([
      { resource: 'resA', token: a.token, epoch: a.epoch + 1, expectedVersion: 0, checkpoint: { height: 1, hash: 'a' } },
    ]),
    LeaseFencedError,
  );
  r.declareResource('shared', 1);
  const s = r.acquire('shared', 'alice');
  assert.throws(
    () => r.writeCheckpointAll([
      { resource: 'shared', token: s.token, epoch: s.epoch, expectedVersion: 0, checkpoint: { height: 1, hash: 'a' } },
    ]),
    LeaseFencedError,
  );
  c.advance(200);
  assert.throws(
    () => r.writeCheckpointAll([
      { resource: 'resA', token: a.token, epoch: a.epoch, expectedVersion: 0, checkpoint: { height: 1, hash: 'a' } },
    ]),
    LeaseFencedError,
  );
  assert.equal(r.readCheckpoint('resA'), null);
  assert.equal(r.readCheckpoint('resB'), null);
});

test('version conflicts are checked after fencing, in input order', () => {
  const c = makeClock();
  const r = createRegistry({ ttlMs: 100, clock: c.clock });
  const a = r.acquire('resA', 'alice');
  const b = r.acquire('resB', 'bob');
  r.writeCheckpoint('resA', a.token, a.epoch, 0, { height: 1, hash: 'a' });

  assert.throws(
    () => r.writeCheckpointAll([
      { resource: 'resB', token: b.token, epoch: b.epoch, expectedVersion: 3, checkpoint: { height: 1, hash: 'b' } },
      { resource: 'resA', token: a.token, epoch: a.epoch, expectedVersion: 0, checkpoint: { height: 2, hash: 'a2' } },
    ]),
    (error) => error instanceof CheckpointConflictError
      && error.code === 'CHECKPOINT_CONFLICT'
      && error.resource === 'resB'
      && error.expected === 3
      && error.actual === 0,
  );
  // Fencing anywhere beats any version conflict.
  assert.throws(
    () => r.writeCheckpointAll([
      { resource: 'resA', token: a.token, epoch: a.epoch, expectedVersion: 7, checkpoint: { height: 2, hash: 'a2' } },
      { resource: 'resB', token: 'nope', epoch: b.epoch, expectedVersion: 9, checkpoint: { height: 1, hash: 'b' } },
    ]),
    LeaseFencedError,
  );
  assert.equal(r.readCheckpoint('resA').version, 1);
  assert.equal(r.readCheckpoint('resB'), null);
});

test('a next version beyond the safe integer range throws TypeError', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  // Seed the state through a snapshot: a checkpoint already sitting at
  // MAX_SAFE_INTEGER, so the next version cannot be represented.
  const snapshot = {
    v: 1, type: 'snapshot', at: 1000, seq: 0, tokenSeq: 0, batchSeq: 0,
    counters: { granted: 0, renewed: 0, released: 0, reclaimed: 0 },
    quotas: [], resources: [],
    epochs: [{ resource: 'res', epoch: 1 }],
    checkpoints: [{ resource: 'res', height: 1, hash: 'a', version: Number.MAX_SAFE_INTEGER, epoch: 1 }],
    leases: [],
  };
  fs.writeFileSync(`${logPath}.snapshot`, `${JSON.stringify(snapshot)}\n`);
  fs.writeFileSync(logPath, `${JSON.stringify({ v: 1, type: 'snapshot-start', seq: 0 })}\n`);
  const r = createRegistry({ ttlMs: 10 ** 9, clock: c.clock, logPath });
  const lease = r.acquire('res', 'bob');
  assert.throws(
    () => r.writeCheckpointAll([
      { resource: 'res', token: lease.token, epoch: lease.epoch, expectedVersion: Number.MAX_SAFE_INTEGER, checkpoint: { height: 2, hash: 'b' } },
    ]),
    TypeError,
  );
  assert.equal(r.readCheckpoint('res').version, Number.MAX_SAFE_INTEGER);
});

test('failure leaves checkpoints and log history untouched; state unmoved', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  const r = createRegistry({ ttlMs: 100, clock: c.clock, logPath });
  const a = r.acquire('resA', 'alice');
  const b = r.acquire('resB', 'bob');
  r.writeCheckpoint('resA', a.token, a.epoch, 0, { height: 1, hash: 'a' });
  const before = r.exportState();
  const statsBefore = r.stats();
  const logBefore = fs.readFileSync(logPath, 'utf8');

  fs.chmodSync(logPath, 0o444);
  try {
    assert.throws(
      () => r.writeCheckpointAll([
        { resource: 'resA', token: a.token, epoch: a.epoch, expectedVersion: 1, checkpoint: { height: 2, hash: 'a2' } },
        { resource: 'resB', token: b.token, epoch: b.epoch, expectedVersion: 0, checkpoint: { height: 3, hash: 'b' } },
      ]),
      (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
    );
  } finally {
    fs.chmodSync(logPath, 0o644);
  }
  assert.equal(fs.readFileSync(logPath, 'utf8'), logBefore);
  assert.deepEqual(r.exportState(), before);
  assert.deepEqual(r.stats(), statsBefore);
  assert.equal(r.readCheckpoint('resA').version, 1);
  assert.equal(r.readCheckpoint('resB'), null);

  // The whole batch is retryable as-is.
  const written = r.writeCheckpointAll([
    { resource: 'resA', token: a.token, epoch: a.epoch, expectedVersion: 1, checkpoint: { height: 2, hash: 'a2' } },
    { resource: 'resB', token: b.token, epoch: b.epoch, expectedVersion: 0, checkpoint: { height: 3, hash: 'b' } },
  ]);
  assert.equal(written[0].version, 2);
  assert.equal(written[1].version, 1);
  // Success moved no lease, epoch, counter or queue state either.
  assert.deepEqual(r.exportState(), before);
  assert.deepEqual(r.stats(), statsBefore);
  assert.equal(r.epoch('resA'), a.epoch);
  assert.equal(r.epoch('resB'), b.epoch);
  assert.equal(r.checkConsistency().ok, true);
});

test('batch progress survives restart and compaction exactly once', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let r = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const a = r.acquire('resA', 'alice');
  const b = r.acquire('resB', 'bob');
  r.writeCheckpointAll([
    { resource: 'resA', token: a.token, epoch: a.epoch, expectedVersion: 0, checkpoint: { height: 1, hash: 'a' } },
    { resource: 'resB', token: b.token, epoch: b.epoch, expectedVersion: 0, checkpoint: { height: 2, hash: 'b' } },
  ]);

  r = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.equal(r.readCheckpoint('resA').version, 1);
  assert.equal(r.readCheckpoint('resB').version, 1);

  r.compact();
  r = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.deepEqual(r.readCheckpoint('resA'), { resource: 'resA', height: 1, hash: 'a', version: 1, epoch: a.epoch });
  assert.deepEqual(r.readCheckpoint('resB'), { resource: 'resB', height: 2, hash: 'b', version: 1, epoch: b.epoch });

  // A folded batch record left in an un-swapped log is not applied twice.
  const snapshot = JSON.parse(fs.readFileSync(`${logPath}.snapshot`, 'utf8'));
  const folded = JSON.stringify({
    v: 1, type: 'checkpoint-all', at: 1002, e: snapshot.seq,
    items: [
      { resource: 'resA', height: 1, hash: 'a', version: 1, epoch: a.epoch },
      { resource: 'resB', height: 2, hash: 'b', version: 1, epoch: b.epoch },
    ],
  });
  fs.appendFileSync(logPath, `${folded}\n`);
  r = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.equal(r.readCheckpoint('resA').version, 1);
  assert.equal(r.readCheckpoint('resB').version, 1);
});

test('torn tail batch record is ignored; corrupt or non-monotonic records refuse to load', () => {
  const logPath = tempLogPath();
  const c = makeClock();
  let r = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  const a = r.acquire('resA', 'alice');
  r.writeCheckpointAll([
    { resource: 'resA', token: a.token, epoch: a.epoch, expectedVersion: 0, checkpoint: { height: 1, hash: 'a' } },
  ]);

  // Unterminated tail record never happened.
  fs.appendFileSync(logPath, '{"v":1,"type":"checkpoint-all","items":[{"resource":"resA"');
  r = createRegistry({ ttlMs: 100_000, clock: c.clock, logPath });
  assert.equal(r.readCheckpoint('resA').version, 1);

  // Complete but damaged record is corruption.
  fs.appendFileSync(logPath, '{"v":1,"type":"checkpoint-all","items":[{"resource":"resA","height":-1,"hash":"x","version":2,"epoch":1}],"at":1}\n');
  assert.throws(() => createRegistry({ ttlMs: 100_000, clock: c.clock, logPath }), LogFileError);

  // A version gap is corruption too.
  fs.writeFileSync(logPath, [
    JSON.stringify({ v: 1, type: 'acquire', at: 1000, token: 't1', resource: 'resA', holder: 'a', ttlMs: 10 ** 9, expiresAt: 1000 + 10 ** 9, epoch: 1, batch: 'batch-1-1', e: 1 }),
    JSON.stringify({ v: 1, type: 'checkpoint-all', at: 1001, items: [{ resource: 'resA', height: 1, hash: 'a', version: 2, epoch: 1 }], e: 2 }),
    '',
  ].join('\n'));
  assert.throws(() => createRegistry({ ttlMs: 100_000, clock: c.clock, logPath }), LogFileError);

  // A duplicate resource inside one record is corruption.
  fs.writeFileSync(logPath, [
    JSON.stringify({ v: 1, type: 'acquire', at: 1000, token: 't1', resource: 'resA', holder: 'a', ttlMs: 10 ** 9, expiresAt: 1000 + 10 ** 9, epoch: 1, batch: 'batch-1-1', e: 1 }),
    JSON.stringify({ v: 1, type: 'checkpoint-all', at: 1001, items: [
      { resource: 'resA', height: 1, hash: 'a', version: 1, epoch: 1 },
      { resource: 'resA', height: 2, hash: 'b', version: 2, epoch: 1 },
    ], e: 2 }),
    '',
  ].join('\n'));
  assert.throws(() => createRegistry({ ttlMs: 100_000, clock: c.clock, logPath }), LogFileError);
});

test('mutating input and output objects does not reach stored records', () => {
  const c = makeClock();
  const r = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = r.acquire('res', 'alice');
  const checkpoint = { height: 3, hash: 'a' };
  const entries = [{ resource: 'res', token: lease.token, epoch: lease.epoch, expectedVersion: 0, checkpoint }];
  const written = r.writeCheckpointAll(entries);
  checkpoint.height = 99;
  checkpoint.hash = 'mutated';
  entries[0].epoch = 42;
  written[0].hash = 'mutated';
  assert.deepEqual(r.readCheckpoint('res'), {
    resource: 'res', height: 3, hash: 'a', version: 1, epoch: lease.epoch,
  });
});

test('handover does not reset progress; old logs and snapshots still load', () => {
  const c = makeClock();
  const r = createRegistry({ ttlMs: 100, clock: c.clock });
  const first = r.acquire('res', 'alice');
  r.writeCheckpointAll([
    { resource: 'res', token: first.token, epoch: first.epoch, expectedVersion: 0, checkpoint: { height: 5, hash: 'a' } },
  ]);
  const second = r.transfer(first.token, 'bob');
  assert.throws(
    () => r.writeCheckpointAll([
      { resource: 'res', token: first.token, epoch: first.epoch, expectedVersion: 1, checkpoint: { height: 9, hash: 'old' } },
    ]),
    LeaseFencedError,
  );
  const written = r.writeCheckpointAll([
    { resource: 'res', token: second.token, epoch: second.epoch, expectedVersion: 1, checkpoint: { height: 6, hash: 'b' } },
  ]);
  assert.equal(written[0].version, 2);
  assert.equal(written[0].epoch, second.epoch);
});

test('without a logPath batch results live in memory only', () => {
  const c = makeClock();
  const r = createRegistry({ ttlMs: 100, clock: c.clock });
  const lease = r.acquire('res', 'alice');
  r.writeCheckpointAll([
    { resource: 'res', token: lease.token, epoch: lease.epoch, expectedVersion: 0, checkpoint: { height: 4, hash: 'a' } },
  ]);
  assert.equal(r.readCheckpoint('res').version, 1);
});
