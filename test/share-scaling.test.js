import test from 'node:test';
import assert from 'node:assert/strict';

import { createRegistry } from '../src/index.js';

function fakeClock(start = 0) {
  let now = start;
  return { clock: () => now, set: (value) => { now = value; } };
}

// A large number of coexisting shares must not make acquisition, expiry
// cleanup, observation or wake planning linear in the total share count.
test('hot paths do not degrade with the total number of shares', () => {
  const n = 40_000;
  const { clock, set } = fakeClock(0);
  const registry = createRegistry({
    ttlMs: 10_000_000, clock,
    capacities: { c: n, pool: n },
    quotas: { q: n },
  });
  const bulk = registry.acquire(
    { resource: 'c', shares: n - 1 }, 'bulk', 10_000_000,
  );
  const short = registry.acquire('c', 'short', 100);
  registry.acquire({ resource: 'pool', shares: n }, 'pool', 10_000_000);

  const waiters = [];
  for (let i = 0; i < 100; i += 1) {
    waiters.push(registry.acquire('c', `w${i}`, 1000, 10_000_000 + i));
  }

  // The bounds are deliberately generous: the point is independence from n,
  // not tight micro-benchmark numbers. An O(n) implementation takes hundreds
  // of milliseconds here.
  const budgetMs = 25;
  function timed(label, fn) {
    const t0 = process.hrtime.bigint();
    fn();
    const ms = Number(process.hrtime.bigint() - t0) / 1e6;
    assert.ok(
      ms < budgetMs,
      `${label} took ${ms.toFixed(2)}ms (budget ${budgetMs}ms) with ${n} shares`,
    );
  }

  timed('saturated acquire', () => {
    assert.throws(() => registry.acquire('c', 'late'));
  });
  timed('idle sweep', () => assert.deepEqual(registry.sweep(), []));
  timed('idle stats', () => registry.stats());
  set(5_000_000);
  timed('far-future observation', () => registry.stats());
  set(0);

  // Releasing one share wakes exactly the head waiter, not proportional to n.
  timed('one-share release wake', () => registry.release(bulk[0].token));
  assert.equal(waiters[0].status, 'granted');
  assert.ok(waiters.slice(1).every((w) => w.status === 'waiting'));

  // Exactly one of the n shares is due: cleanup pops one heap node.
  set(100);
  let freed;
  timed('single-share expiry sweep', () => {
    freed = registry.sweep();
  });
  assert.deepEqual(freed, ['c']);
  assert.equal(waiters[1].status, 'granted');
  // c: (n-1) bulk minus one released, plus two woken waiters, minus the
  // reclaimed short = n; pool still holds its n unrelated shares.
  assert.equal(registry.stats().live, 2 * n);
  void short;
});

test('wake planning over many saturated resources only touches queue heads', () => {
  const n = 20_000;
  const { clock, set } = fakeClock(0);
  const capacities = {};
  for (let i = 0; i < n; i += 1) {
    capacities[`r${i}`] = 1;
  }
  const registry = createRegistry({ ttlMs: 10_000_000, clock, capacities });
  for (let i = 0; i < n; i += 1) {
    registry.acquire(`r${i}`, 'h', 100);
  }
  // One waiter on just the last resource; expiring everything must wake only
  // that one without scanning the other n-1 queues beyond their heap heads.
  const ticket = registry.acquire(`r${n - 1}`, 'w', 1000, 10_000_000);
  set(100);
  const t0 = process.hrtime.bigint();
  const freed = registry.sweep();
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  assert.equal(freed.length, n);
  assert.equal(ticket.status, 'granted');
  assert.ok(ms < 500, `sweep took ${ms.toFixed(0)}ms`);
});
