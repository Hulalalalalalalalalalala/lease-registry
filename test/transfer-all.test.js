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
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'lease-transfer-all-'));
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

test('transferAll hands every resource to the holder with fresh credentials', () => {
  const clock = fakeClock(10);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h1', 40);
  const b = registry.acquire('b', 'h2', 50);

  clock.set(30);
  const results = registry.transferAll([a.token, b.token], 'new', 70);
  assert.equal(results.length, 2);

  // Input order is preserved and each entry has the transfer shape.
  assert.deepEqual(Object.keys(results[0]).sort(), [
    'epoch', 'expiresAt', 'holder', 'resource', 'token',
  ]);
  assert.equal(results[0].resource, 'a');
  assert.equal(results[1].resource, 'b');
  for (const result of results) {
    assert.equal(result.holder, 'new');
    assert.equal(result.expiresAt, 100); // shared reading + given ttl
    assert.equal(typeof result.token, 'string');
    assert.notEqual(result.token, '');
  }

  // Tokens are all fresh and pairwise distinct.
  const tokens = results.map((r) => r.token);
  assert.equal(new Set(tokens).size, 2);
  assert.equal(tokens.includes(a.token), false);
  assert.equal(tokens.includes(b.token), false);

  // Every resource epoch advanced by exactly one.
  assert.equal(results[0].epoch, 2);
  assert.equal(results[1].epoch, 2);
  assert.equal(registry.epoch('a'), 2);
  assert.equal(registry.epoch('b'), 2);
  assert.equal(registry.holder('a'), 'new');
  assert.equal(registry.holder('b'), 'new');
});

test('an omitted ttlMs falls back to the registry default ttl', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h1');
  const b = registry.acquire('b', 'h2');
  clock.set(25);
  const results = registry.transferAll([a.token, b.token], 'new');
  assert.deepEqual(results.map((r) => r.expiresAt), [125, 125]);
});

test('tokens may come from different holders and different batches', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const solo = registry.acquire('solo', 'h1');
  const [g1, g2] = registry.acquireAll(['g1', 'g2'], 'h2');
  const renewed = registry.acquire('renewed', 'h3');
  registry.renew(renewed.token);

  const results = registry.transferAll(
    [g1.token, solo.token, renewed.token, g2.token],
    'unified',
    100,
  );
  assert.deepEqual(results.map((r) => r.resource), ['g1', 'solo', 'renewed', 'g2']);
  for (const result of results) {
    assert.equal(result.holder, 'unified');
    assert.equal(result.epoch, 2);
  }
  assert.equal(registry.stats().live, 4);
});

test('a group may be handed back to its current holder', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = registry.acquire('a', 'h');
  const b = registry.acquire('b', 'h');
  const results = registry.transferAll([a.token, b.token], 'h', 100);
  assert.equal(results[0].epoch, 2);
  assert.equal(results[1].epoch, 2);
  assert.equal(registry.holder('a'), 'h');
  assert.equal(registry.assertLease('a', results[0].token, 2), true);
});

test('each resource epoch advances from its own high-water mark', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = registry.acquire('a', 'h');
  let b = registry.acquire('b', 'h');
  registry.release(b.token);
  b = registry.acquire('b', 'h'); // epoch 2
  const results = registry.transferAll([a.token, b.token], 'n', 100);
  assert.equal(results[0].epoch, 2);
  assert.equal(results[1].epoch, 3);
});

test('the old credentials die: renew/release return false and the fence throws', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = registry.acquire('a', 'h1');
  const b = registry.acquire('b', 'h2');
  registry.transferAll([a.token, b.token], 'new', 100);

  assert.equal(registry.renew(a.token), false);
  assert.equal(registry.release(a.token), false);
  assert.equal(registry.renew(b.token), false);
  assert.equal(registry.release(b.token), false);
  assertFenced(() => registry.assertLease('a', a.token, 1), 'a');
  assertFenced(() => registry.assertLease('b', b.token, 1), 'b');
});

test('the new credentials renew, release, transfer and pass the fence', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h1', 60);
  const b = registry.acquire('b', 'h2', 60);
  const [na, nb] = registry.transferAll([a.token, b.token], 'new', 60);

  clock.set(10);
  assert.equal(registry.renew(na.token), true);
  assert.equal(registry.assertLease('a', na.token, 2), true);
  assert.equal(registry.assertLease('b', nb.token, 2), true);

  // A new credential takes part in a further single transfer.
  const moved = registry.transfer(nb.token, 'later', 60);
  assert.equal(moved.epoch, 3);
  assert.equal(registry.assertLease('b', moved.token, 3), true);

  assert.equal(registry.release(na.token), true);
  assert.equal(registry.holder('a'), null);
});

test('a group transfer never frees the resources or leaves a gap', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = registry.acquire('a', 'h1');
  const b = registry.acquire('b', 'h2');
  registry.transferAll([a.token, b.token], 'new', 100);
  assert.throws(() => registry.acquire('a', 'x'), LeaseTakenError);
  assert.throws(() => registry.acquire('b', 'x'), LeaseTakenError);
});

// ---- shape validation --------------------------------------------------------

test('transferAll validates the tokens array shape before anything else', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const lease = registry.acquire('r', 'a');

  for (const bad of [undefined, null, 'r', 1, {}, true, []]) {
    assert.throws(() => registry.transferAll(bad, 'h', 100), TypeError);
  }
  for (const bad of ['', 1, null, undefined, {}, [], true]) {
    assert.throws(() => registry.transferAll([bad], 'h', 100), TypeError);
  }
  // No empty slots.
  const sparse = [lease.token];
  sparse[2] = lease.token;
  assert.throws(() => registry.transferAll(sparse, 'h', 100), TypeError);
  // No duplicates.
  assert.throws(
    () => registry.transferAll([lease.token, lease.token], 'h', 100),
    TypeError,
  );
  // Holder must be a non-empty string.
  for (const bad of ['', 1, null, undefined, {}, [], true]) {
    assert.throws(() => registry.transferAll([lease.token], bad, 100), TypeError);
  }
  // ttlMs, when given, must be a positive finite number.
  for (const bad of [0, -1, NaN, Infinity, -Infinity, '100', null, true]) {
    assert.throws(() => registry.transferAll([lease.token], 'h', bad), TypeError);
  }
  // Shape errors win over fencing: an unknown token with a bad shape is a
  // TypeError, never a LeaseFencedError.
  assert.throws(() => registry.transferAll(['unknown', ''], 'h', 100), TypeError);
  assert.throws(() => registry.transferAll([], '', 0), TypeError);

  // Nothing moved.
  assert.equal(registry.epoch('r'), 1);
  assert.equal(registry.assertLease('r', lease.token, 1), true);
});

test('shape validation runs before the clock is read or any token is looked up', () => {
  let reads = 0;
  const clock = () => {
    reads += 1;
    return 0;
  };
  const registry = createRegistry({ ttlMs: 100, clock });
  const lease = registry.acquire('r', 'a');
  reads = 0;

  assert.throws(() => registry.transferAll([], 'h'), TypeError);
  assert.throws(() => registry.transferAll([lease.token, lease.token], 'h'), TypeError);
  assert.throws(() => registry.transferAll([lease.token], '', 100), TypeError);
  assert.throws(() => registry.transferAll([lease.token], 'h', -1), TypeError);
  assert.equal(reads, 0);
});

test('the clock is read exactly once and a non-finite reading is a TypeError', () => {
  let reads = 0;
  let now = 10;
  const clock = () => {
    reads += 1;
    return now;
  };
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h1');
  const b = registry.acquire('b', 'h2');
  reads = 0;

  const results = registry.transferAll([a.token, b.token], 'n', 50);
  assert.equal(reads, 1);
  assert.equal(results[0].expiresAt, 60);
  assert.equal(results[1].expiresAt, 60);

  now = NaN;
  assert.throws(() => registry.transferAll([results[0].token], 'x', 50), TypeError);
  now = Infinity;
  assert.throws(() => registry.transferAll([results[0].token], 'x', 50), TypeError);
  // The reading plus the chosen ttl must stay finite.
  now = Number.MAX_VALUE;
  assert.throws(() => registry.transferAll([results[0].token], 'x', Number.MAX_VALUE), TypeError);

  // The failed calls changed nothing.
  now = 20;
  assert.equal(registry.assertLease('a', results[0].token, 2), true);
  assert.equal(registry.epoch('a'), 2);
});

// ---- fencing ------------------------------------------------------------------

test('unknown, other-instance, released, reclaimed and replaced tokens fence the whole group', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const other = createRegistry({ ttlMs: 100, clock });
  const good = registry.acquire('good', 'h', 100);
  const foreign = other.acquire('f', 'h', 100);

  // Unknown token.
  assertFenced(() => registry.transferAll([good.token, 'lease-nope'], 'n', 100));
  // Minted by another instance.
  assertFenced(() => registry.transferAll([good.token, foreign.token], 'n', 100));

  // Released credential.
  const rel = registry.acquire('rel', 'h', 100);
  registry.release(rel.token);
  assertFenced(() => registry.transferAll([good.token, rel.token], 'n', 100));

  // Reclaimed credential.
  const exp = registry.acquire('exp', 'h', 10);
  clock.set(11);
  assert.deepEqual(registry.sweep(), ['exp']);
  assertFenced(() => registry.transferAll([good.token, exp.token], 'n', 100));

  // Replaced by an earlier transfer.
  const moved = registry.acquire('moved', 'h', 100);
  const next = registry.transfer(moved.token, 'h2', 100);
  assertFenced(() => registry.transferAll([good.token, moved.token], 'n', 100));

  // Every failed call left the valid credential completely intact.
  assert.equal(registry.assertLease('good', good.token, 1), true);
  assert.equal(registry.epoch('good'), 1);
  assert.equal(registry.holder('good'), 'h');
  assert.equal(registry.renew(good.token), true);
  assert.equal(registry.assertLease('moved', next.token, 2), true);
});

test('expired, retaken and declared-resource credentials fence the whole group', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const good = registry.acquire('good', 'h', 100);

  // Expired (even unswept).
  const stale = registry.acquire('stale', 'h', 10);
  clock.set(11);
  assertFenced(() => registry.transferAll([good.token, stale.token], 'n', 100), 'stale');

  // Retaken away: the old token no longer carries the latest epoch.
  const retaken = registry.acquire('stale', 'h2', 100);
  assert.equal(retaken.epoch, 2);
  assertFenced(() => registry.transferAll([good.token, stale.token], 'n', 100));

  // Declared share resource.
  registry.declareResource('pool', 4);
  const share = registry.acquire('pool', 'h', 100);
  assertFenced(() => registry.transferAll([good.token, share.token], 'n', 100), 'pool');

  // Even a declared capacity-1 resource fences.
  registry.declareResource('single-pool', 1);
  const one = registry.acquire('single-pool', 'h', 100);
  assertFenced(() => registry.transferAll([good.token, one.token], 'n', 100), 'single-pool');

  // The valid member of every failed group is untouched.
  assert.equal(registry.assertLease('good', good.token, 1), true);
  assert.equal(registry.holder('good'), 'h');
});

test('a credential minted before a restart fences the whole group', () => {
  const p = logPath('legacy-group');
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const old = r1.acquire('old', 'h', 200);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  const fresh = r2.acquire('fresh', 'h', 200);
  assertFenced(() => r2.transferAll([fresh.token, old.token], 'n', 100), 'old');
  assert.equal(r2.assertLease('fresh', fresh.token, 1), true);
});

test('a backwards clock reading revives the shares for the group judgment', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h1', 40);
  const b = registry.acquire('b', 'h2', 40);
  clock.set(50); // both expired, unswept
  assertFenced(() => registry.transferAll([a.token, b.token], 'n', 100), 'a');
  clock.set(10); // reading moved backwards: live again
  const results = registry.transferAll([a.token, b.token], 'n', 100);
  assert.equal(results[0].epoch, 2);
  assert.equal(registry.assertLease('a', results[0].token, 2), true);
  assert.equal(registry.assertLease('b', results[1].token, 2), true);
});

// ---- atomicity -----------------------------------------------------------------

test('a fenced group changes no lease, epoch, counter, ticket or log', () => {
  const p = logPath('atomicity');
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = registry.acquire('a', 'h1', 100);
  const b = registry.acquire('b', 'h2', 100);
  registry.release(b.token);
  const before = registry.stats();
  const logBefore = fs.readFileSync(p, 'utf8');

  assertFenced(() => registry.transferAll([a.token, b.token], 'n', 100));

  assert.deepEqual(registry.stats(), before);
  assert.equal(registry.epoch('a'), 1);
  assert.equal(registry.holder('a'), 'h1');
  assert.equal(registry.assertLease('a', a.token, 1), true);
  assert.equal(fs.readFileSync(p, 'utf8'), logBefore);
});

test('a failed log write rolls the whole group transfer back', () => {
  const p = logPath('group-rollback');
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const a = registry.acquire('a', 'h1', 100);
  const b = registry.acquire('b', 'h2', 100);

  sabotage(p);
  try {
    assert.throws(
      () => registry.transferAll([a.token, b.token], 'n', 100),
      (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
    );
  } finally {
    restore(p);
  }

  // Both old credentials are still fully usable.
  assert.equal(registry.renew(a.token), true);
  assert.equal(registry.assertLease('a', a.token, 1), true);
  assert.equal(registry.assertLease('b', b.token, 1), true);
  assert.equal(registry.epoch('a'), 1);
  assert.equal(registry.epoch('b'), 1);

  // Counters and the log never moved.
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  assert.deepEqual(r2.stats(), {
    granted: 2, renewed: 1, released: 0, reclaimed: 0, live: 2,
  });
  const types = fs.readFileSync(p, 'utf8').trim().split('\n')
    .map((line) => JSON.parse(line).type);
  assert.deepEqual(types, ['acquire', 'acquire', 'renew']);
});

// ---- counters, occupancy and waiters --------------------------------------------

test('a group transfer moves none of the four counters and keeps occupancy', () => {
  const registry = createRegistry({ ttlMs: 100, clock: fakeClock(0) });
  const a = registry.acquire('a', 'h1');
  const b = registry.acquire('b', 'h2');
  const before = registry.stats();
  assert.deepEqual(before, {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });
  const results = registry.transferAll([a.token, b.token], 'n', 100);
  assert.deepEqual(registry.stats(), before);
  registry.transferAll(results.map((r) => r.token), 'm', 100);
  assert.deepEqual(registry.stats(), before);
});

test('a group transfer neither wakes nor purges waiters, even timed-out ones', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h1', 5000);
  const b = registry.acquire('b', 'h2', 5000);
  const ticket = registry.acquire('a', 'w', 50, 1000);
  assert.equal(ticket.status, 'waiting');

  clock.set(2000); // the ticket's wait deadline is past; both leases live on
  const [na] = registry.transferAll([a.token, b.token], 'n', 100);
  assert.equal(ticket.status, 'waiting', 'the timed-out ticket is not purged');
  assert.equal(registry.holder('a'), 'n');
  assert.throws(() => registry.acquire('a', 'x'), LeaseTakenError);

  // The new holder's release is what finally moves the queue (the expired
  // ticket is purged then, not by the transfer).
  assert.equal(registry.release(na.token), true);
  assert.equal(ticket.status, 'expired');
});

// ---- persistence -----------------------------------------------------------------

test('a group transfer appends exactly one indivisible record', () => {
  const p = logPath('group-log');
  const clock = fakeClock(0);
  const r1 = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r1.acquire('a', 'h1', 200);
  const b = r1.acquire('b', 'h2', 200);
  clock.set(10);
  const results = r1.transferAll([a.token, b.token], 'n', 90);

  const lines = fs.readFileSync(p, 'utf8').trim().split('\n').map(JSON.parse);
  const records = lines.filter((event) => event.type === 'transfer-all');
  assert.equal(records.length, 1);
  assert.deepEqual(records[0], {
    v: 1,
    e: 3,
    type: 'transfer-all',
    at: 10,
    holder: 'n',
    ttlMs: 90,
    items: [
      { from: a.token, token: results[0].token, resource: 'a', expiresAt: 100, epoch: 2 },
      { from: b.token, token: results[1].token, resource: 'b', expiresAt: 100, epoch: 2 },
    ],
  });
});

test('a restart recovers holders, expiries and epochs without double applying', () => {
  const p = logPath('group-restart');
  const clock = fakeClock(0);
  const r1 = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r1.acquire('a', 'h1', 200);
  const b = r1.acquire('b', 'h2', 200);
  clock.set(10);
  const results = r1.transferAll([a.token, b.token], 'n', 90);

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(20), logPath: p });
  assert.equal(r2.epoch('a'), 2);
  assert.equal(r2.epoch('b'), 2);
  assert.equal(r2.holder('a'), 'n');
  assert.equal(r2.holder('b'), 'n');
  const exported = r2.exportState(20);
  const recovered = new Map(exported.leases.map((l) => [l.token, l]));
  assert.equal(recovered.get(results[0].token).expiresAt, 100);
  assert.equal(recovered.get(results[0].token).holder, 'n');
  assert.equal(recovered.get(results[0].token).epoch, 2);
  assert.equal(recovered.get(results[1].token).expiresAt, 100);
  // The pre-restart credentials are void everywhere.
  assertFenced(() => r2.assertLease('a', a.token, 1), 'a');
  assertFenced(() => r2.assertLease('a', results[0].token, 2), 'a');
  assert.equal(r2.renew(results[0].token), false);
  assert.equal(r2.release(results[0].token), false);
  assertFenced(() => r2.transferAll([results[0].token, results[1].token], 'x', 100), 'a');
  // Counters came back exactly once.
  assert.deepEqual(r2.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });

  // The next grant continues from the recovered epoch after expiry.
  const r3 = createRegistry({ ttlMs: 100, clock: fakeClock(101), logPath: p });
  r3.sweep();
  assert.equal(r3.acquire('a', 'z', 100).epoch, 3);
});

test('a group transfer survives compaction with every credential intact', () => {
  const p = logPath('group-compact');
  const clock = fakeClock(0);
  const r1 = createRegistry({ ttlMs: 100, clock, logPath: p });
  const a = r1.acquire('a', 'h1', 300);
  const b = r1.acquire('b', 'h2', 300);
  const results = r1.transferAll([a.token, b.token], 'n', 300);
  r1.compact();

  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(50), logPath: p });
  assert.equal(r2.epoch('a'), 2);
  assert.equal(r2.epoch('b'), 2);
  assert.equal(r2.holder('a'), 'n');
  assert.equal(r2.holder('b'), 'n');
  assertFenced(() => r2.assertLease('a', a.token, 1), 'a');
  assertFenced(() => r2.assertLease('a', results[0].token, 2), 'a');
  assert.equal(r2.renew(results[0].token), false);
  assert.deepEqual(r2.stats(), {
    granted: 2, renewed: 0, released: 0, reclaimed: 0, live: 2,
  });

  // A further group transfer after the compacted restart appends one
  // recoverable record on top of the marker.
  const fresh1 = r2.acquire('x', 'h', 100);
  const fresh2 = r2.acquire('y', 'h', 100);
  const moved = r2.transferAll([fresh1.token, fresh2.token], 'z', 100);
  const r3 = createRegistry({ ttlMs: 100, clock: fakeClock(60), logPath: p });
  assert.equal(r3.holder('x'), 'z');
  assert.equal(r3.holder('y'), 'z');
  assert.equal(r3.epoch('x'), 2);
  assert.equal(r3.epoch('y'), 2);
  assertFenced(() => r3.assertLease('x', moved[0].token, 2), 'x');
});

test('a torn tail record is ignored entirely', () => {
  const p = logPath('group-torn');
  const r1 = createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p });
  const a = r1.acquire('a', 'h1', 200);
  const b = r1.acquire('b', 'h2', 200);
  const results = r1.transferAll([a.token, b.token], 'n', 200);

  // A later append died mid-line: the fragment never happened.
  fs.appendFileSync(p, '{"v":1,"type":"transfer-all","holder":"x","items":[{"');
  const r2 = createRegistry({ ttlMs: 100, clock: fakeClock(10), logPath: p });
  assert.equal(r2.holder('a'), 'n');
  assert.equal(r2.holder('b'), 'n');
  assert.equal(r2.epoch('a'), 2);
  assert.equal(r2.epoch('b'), 2);
  assert.equal(r2.exportState(10).leases.length, 2);
  assertFenced(() => r2.assertLease('a', results[0].token, 2), 'a');
});

test('a complete but corrupt transfer-all record is a LogFileError', () => {
  const acquire = (token, resource, epoch) => JSON.stringify({
    v: 1, e: epoch, type: 'acquire', at: 0, token, resource,
    holder: 'a', ttlMs: 100, expiresAt: 100, epoch,
  });

  // Non-monotonic epoch inside the group.
  const p1 = logPath('group-corrupt-epoch');
  fs.writeFileSync(p1, [
    acquire('t1', 'a', 1),
    acquire('t2', 'b', 1),
    JSON.stringify({
      v: 1, e: 3, type: 'transfer-all', at: 5, holder: 'n', ttlMs: 100,
      items: [
        { from: 't1', token: 't3', resource: 'a', expiresAt: 105, epoch: 2 },
        { from: 't2', token: 't4', resource: 'b', expiresAt: 105, epoch: 9 },
      ],
    }),
  ].join('\n') + '\n');
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p1 }),
    (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
  );

  // A damaged item shape refuses the whole record.
  const p2 = logPath('group-corrupt-shape');
  fs.writeFileSync(p2, [
    acquire('t1', 'a', 1),
    JSON.stringify({
      v: 1, e: 2, type: 'transfer-all', at: 5, holder: 'n', ttlMs: 100,
      items: [{ from: 't1', token: 't3', resource: 'a', expiresAt: 'soon', epoch: 2 }],
    }),
  ].join('\n') + '\n');
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p2 }),
    (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
  );

  // An empty item list is not a group transfer.
  const p3 = logPath('group-corrupt-empty');
  fs.writeFileSync(p3, [
    acquire('t1', 'a', 1),
    JSON.stringify({
      v: 1, e: 2, type: 'transfer-all', at: 5, holder: 'n', ttlMs: 100, items: [],
    }),
  ].join('\n') + '\n');
  assert.throws(
    () => createRegistry({ ttlMs: 100, clock: fakeClock(0), logPath: p3 }),
    (error) => error instanceof LogFileError && error.code === 'LOG_FILE_ERROR',
  );
});

// ---- export and consistency -------------------------------------------------------

test('exportState and checkConsistency reflect the group handover', () => {
  const clock = fakeClock(0);
  const registry = createRegistry({ ttlMs: 100, clock });
  const a = registry.acquire('a', 'h1', 100);
  const b = registry.acquire('b', 'h2', 100);
  const results = registry.transferAll([a.token, b.token], 'n', 100);

  const exported = registry.exportState(10);
  assert.deepEqual(Object.keys(exported), [
    'at', 'stats', 'quotas', 'resources', 'leases', 'waits',
  ]);
  const byToken = new Map(exported.leases.map((l) => [l.token, l]));
  assert.equal(byToken.has(a.token), false, 'the old credential left the table');
  assert.equal(byToken.get(results[0].token).holder, 'n');
  assert.equal(byToken.get(results[0].token).epoch, 2);
  assert.equal(byToken.get(results[0].token).live, true);
  const resources = new Map(exported.resources.map((r) => [r.resource, r]));
  assert.equal(resources.get('a').holder, 'n');
  assert.equal(resources.get('a').epoch, 2);
  assert.equal(resources.get('b').holder, 'n');
  assert.equal(exported.stats.live, 2);

  const check = registry.checkConsistency(10);
  assert.deepEqual(check.issues, []);
  assert.equal(check.ok, true);
});
