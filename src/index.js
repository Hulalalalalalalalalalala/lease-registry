// In-process, time bounded lease registry with append-only log persistence
// and crash-safe snapshots. Time only moves when the injected clock moves;
// nothing expires on its own. Tokens are derived from the injected clock as
// well, never from wall time.
//
// Resources may be declared with a share capacity: several holders then
// occupy the same resource at once, each holding one credential per share
// with its own independent expiry. An undeclared resource stays a
// single-occupancy resource exactly as before. Hierarchical quotas sit above
// resources: a parent quota bounds the sum of the shares occupied under every
// quota below it (and directly declared resources), and every level is
// enforced at once.
//
// Every resource keeps a credential epoch, starting at 1 on its first grant
// and advanced by exactly one per committed credential (retake, same-holder
// reacquisition and woken grants included). Renewals never advance it, nor do
// failed, queued, timed-out, cancelled or partially committed requests; it
// never moves backwards and survives restarts and compactions. The newest
// credential of an exclusive resource is therefore the leader epoch, and
// assertLease is the fence a holder passes only while it still is that
// leader.
//
// Persistence layout:
//   <logPath>           append-only event log, one JSON object per line
//   <logPath>.snapshot  last compacted state image (single JSON line)
// Startup loads the snapshot (if any) and replays only the events past the
// compacted boundary marker. Compaction publishes both files through a
// temp-file + fsync + rename sequence, so a failure at any step leaves the
// previous snapshot and the previous log usable. Every event carries a
// monotonic sequence number; a snapshot found next to an un-exchanged
// original log (the log swap died after the snapshot rename) simply skips
// the already-folded events by their sequence ids, so each event applies
// exactly once regardless of which rename made it to disk.

import fs from 'node:fs';
import path from 'node:path';

const LOG_VERSION = 1;
const SNAPSHOT_VERSION = 1;

export class LeaseTakenError extends Error {
  constructor(resource, holder, requested = undefined, available = undefined) {
    super(`lease for resource ${String(resource)} is already held`);
    this.name = 'LeaseTakenError';
    this.code = LeaseTakenError.code;
    this.resource = resource;
    this.holder = holder ?? null;
    this.requested = requested;
    this.available = available;
  }
}
LeaseTakenError.code = 'LEASE_TAKEN';

export class QuotaExceededError extends Error {
  constructor(quota, holder, requested, available) {
    super(`quota ${String(quota)} has no room for ${requested} requested share(s)`);
    this.name = 'QuotaExceededError';
    this.code = QuotaExceededError.code;
    this.quota = quota;
    this.holder = holder ?? null;
    this.requested = requested;
    this.available = available;
  }
}
QuotaExceededError.code = 'QUOTA_EXCEEDED';

export class LogFileError extends Error {
  constructor(message, cause) {
    super(message);
    this.name = 'LogFileError';
    this.code = LogFileError.code;
    this.cause = cause ?? null;
  }
}
LogFileError.code = 'LOG_FILE_ERROR';

export class LeaseFencedError extends Error {
  constructor(resource) {
    super(`lease for resource ${String(resource)} is fenced`);
    this.name = 'LeaseFencedError';
    this.code = LeaseFencedError.code;
    this.resource = resource;
  }
}
LeaseFencedError.code = 'LEASE_FENCED';

// Binary min-heap ordered by (expiresAt, insertion order). Expiry heaps are
// used three ways: one global heap drives reclamation, each resource gets one
// for lazy capacity accounting, and each quota node gets one for lazy quota
// accounting. Renewals push fresh nodes and retire the old ones, so reclaiming
// expired leases only walks entries that are actually due instead of scanning
// the whole table.
class ExpiryQueue {
  constructor() {
    this.nodes = [];
  }

  get size() {
    return this.nodes.length;
  }

  push(node) {
    const nodes = this.nodes;
    nodes.push(node);
    let i = nodes.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (expiresBefore(nodes[i], nodes[parent])) {
        [nodes[i], nodes[parent]] = [nodes[parent], nodes[i]];
        i = parent;
      } else {
        break;
      }
    }
  }

  peek() {
    return this.nodes[0];
  }

  pop() {
    const nodes = this.nodes;
    const top = nodes[0];
    const last = nodes.pop();
    if (nodes.length > 0) {
      nodes[0] = last;
      let i = 0;
      for (;;) {
        const left = i * 2 + 1;
        const right = left + 1;
        let next = i;
        if (left < nodes.length && expiresBefore(nodes[left], nodes[next])) {
          next = left;
        }
        if (right < nodes.length && expiresBefore(nodes[right], nodes[next])) {
          next = right;
        }
        if (next === i) {
          break;
        }
        [nodes[i], nodes[next]] = [nodes[next], nodes[i]];
        i = next;
      }
    }
    return top;
  }
}

function expiresBefore(a, b) {
  if (a.expiresAt !== b.expiresAt) {
    return a.expiresAt < b.expiresAt;
  }
  return a.order < b.order;
}

// Binary min-heap of wait-request deadlines. Timeout cleanup pops due
// requests in deadline order instead of scanning every pending request, so
// a large waiting population costs O(expired * log n), never O(n) per call.
class DeadlineQueue {
  constructor() {
    this.nodes = [];
  }

  get size() {
    return this.nodes.length;
  }

  push(node) {
    const nodes = this.nodes;
    nodes.push(node);
    let i = nodes.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (deadlineBefore(nodes[i], nodes[parent])) {
        [nodes[i], nodes[parent]] = [nodes[parent], nodes[i]];
        i = parent;
      } else {
        break;
      }
    }
  }

  peek() {
    return this.nodes[0];
  }

  pop() {
    const nodes = this.nodes;
    const top = nodes[0];
    const last = nodes.pop();
    if (nodes.length > 0) {
      nodes[0] = last;
      let i = 0;
      for (;;) {
        const left = i * 2 + 1;
        const right = left + 1;
        let next = i;
        if (left < nodes.length && deadlineBefore(nodes[left], nodes[next])) {
          next = left;
        }
        if (right < nodes.length && deadlineBefore(nodes[right], nodes[next])) {
          next = right;
        }
        if (next === i) {
          break;
        }
        [nodes[i], nodes[next]] = [nodes[next], nodes[i]];
        i = next;
      }
    }
    return top;
  }
}

function deadlineBefore(a, b) {
  if (a.deadline !== b.deadline) {
    return a.deadline < b.deadline;
  }
  return a.tie < b.tie;
}

let registrySequence = 0;
let atomicFileSequence = 0;

export function createRegistry({ ttlMs, clock = Date.now, logPath = null } = {}) {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new TypeError('ttlMs must be a positive finite number');
  }
  if (typeof clock !== 'function') {
    throw new TypeError('clock must be a function');
  }

  const snapshotPath = logPath === null ? null : `${logPath}.snapshot`;
  const defaultTtlMs = ttlMs;
  // resource -> FIFO array of queue positions. A group request listing the
  // same resource in several rows occupies one position per row.
  const waitQueues = new Map();
  // requestId -> group, while the request is still queued.
  const pendingGroups = new Map();
  // token -> { resource, holder, expiresAt, ttlMs, legacy, heapSeq, chain,
  // epoch, counted }
  const leases = new Map();
  // Every token this instance knows about, including replayed credentials
  // that were already released or reclaimed, so a freshly minted token can
  // never reuse a dead credential's string.
  const knownTokens = new Set();
  // resource -> Set of tokens currently attached to it. A share resource
  // usually carries several live credentials; the set makes release and
  // reclaim O(1) regardless of how many shares coexist.
  const slots = new Map();
  // resource -> last committed credential epoch. The first grant of a
  // resource opens it at 1 and every later committed credential advances it
  // by exactly one, including retakes by the same holder and credentials
  // handed to woken requests. A release, reclaim, expiry or restart never
  // moves it backwards, so a resource with no live lease still reports the
  // epoch its last credential carried.
  const epochs = new Map();
  // resource -> runtime bookkeeping. Created lazily; an undeclared resource
  // is a capacity-1 resource with no quota chain.
  const resourceStates = new Map();
  // quota id -> { id, parentId, limit, live, heap, resources }. `live` is the
  // current share occupation counted against the limit, maintained per share
  // grant and released lazily by the quota's own expiry heap.
  const quotas = new Map();
  // The global expiry heap drives sweep(); resource and quota heaps drive
  // lazy, sub-linear availability checks.
  const expiryQueue = new ExpiryQueue();
  // One node per queued wait request, ordered by wait deadline. Nodes outlive
  // their request only until the next lazy pop, flagged `dead`.
  const deadlineQueue = new DeadlineQueue();

  const counters = {
    granted: 0,
    renewed: 0,
    released: 0,
    reclaimed: 0,
  };

  // Per-instance identity makes tokens unique across registries living in the
  // same process; one instance can never present a token another owns.
  registrySequence += 1;
  const registryId = registrySequence;
  let tokenSequence = 0;
  let queueOrder = 0;
  let deadlineTie = 0;
  let requestSequence = 0;
  let batchSequence = 0;
  // Monotonic id stamped on every persisted event; a snapshot records the
  // last id it folded in, so a log still containing the folded prefix (the
  // log swap failed) replays those events as no-ops instead of twice.
  let eventSeq = 0;

  function mintToken(now, seq = tokenSequence) {
    // Pure candidate: nothing is committed until the event is on disk, so a
    // failed append rolls all the way back. Pids and instance ids can coincide
    // after a restart, so skip any sequence that collides with a credential
    // reconstructed from the log. The caller threads the sequence through when
    // it mints several tokens for one atomic write.
    let token;
    do {
      seq += 1;
      token = `lease-${process.pid}-${registryId}-${seq}-${now.toString(36)}`;
    } while (knownTokens.has(token));
    return { token, seq };
  }

  function newExpiryNode(token, expiresAt, heapSeq) {
    return { token, expiresAt, heapSeq, order: queueOrder++ };
  }

  // One node in every heap a lease occupies. The nodes are independent: each
  // heap retires its own stale entries lazily. Every push also bumps the
  // owner's heap version, so a reconcile at an unchanged reading still
  // re-judges after a grant or renewal changed the heap contents.
  // Push one lease node to the global expiry heap and to every owner
  // whose heap drives lazy observation. A share already recorded as
  // retired at an owner (a replay structurally superseding an unswept
  // lease) only needs its global node - it must be reclaimable - and is
  // skipped on that owner's heap, since its occupation was never
  // reconstructed there. Every other push bumps the owner's heap
  // version, so a reconcile at an unchanged reading still re-judges
  // after a grant or renewal changed the heap contents.
  function pushLeaseHeaps(token, lease) {
    const node = newExpiryNode(token, lease.expiresAt, lease.heapSeq);
    expiryQueue.push(node);
    const rs = resourceStates.get(lease.resource);
    rs.heap.push(newExpiryNode(token, lease.expiresAt, lease.heapSeq));
    rs.version += 1;
    for (const quota of lease.chain) {
      quota.heap.push(newExpiryNode(token, lease.expiresAt, lease.heapSeq));
      quota.version += 1;
    }
  }

  // Peek an owner heap for a lease that is live at `now` without
  // mutating anything: stale nodes are inspected but left in place for a
  // later reconcile to retire or for a dropStaleSlots displacement.
  function hasLiveHeapNode(owner, now) {
    for (const node of owner.heap.nodes) {
      const lease = leases.get(node.token);
      if (lease !== undefined && lease.heapSeq === node.heapSeq && node.expiresAt > now) {
        return true;
      }
    }
    return false;
  }

  // Remove every node naming `token` from one expiry heap. Used when a
  // replay structurally supersedes an expired, unswept lease: the stale
  // share never occupied the reconstructed owner table, so its node leaves
  // the owner heap without any live-usage movement. Tokens are globally
  // unique, so matching the token alone (not the heap sequence) separates a
  // superseded lease from a fresh credential that happens to share seq 0.
  function removeHeapNodes(heap, token) {
    let i = 0;
    while (i < heap.nodes.length) {
      if (heap.nodes[i].token === token) {
        heap.nodes.splice(i, 1);
      } else {
        i += 1;
      }
    }
    // Restore the heap invariant in linear time after the removals.
    const nodes = heap.nodes;
    for (let j = (nodes.length >>> 1) - 1; j >= 0; j -= 1) {
      let parent = j;
      for (;;) {
        const left = parent * 2 + 1;
        const right = left + 1;
        let smallest = parent;
        if (left < nodes.length && expiresBefore(nodes[left], nodes[smallest])) {
          smallest = left;
        }
        if (right < nodes.length && expiresBefore(nodes[right], nodes[smallest])) {
          smallest = right;
        }
        if (smallest === parent) {
          break;
        }
        [nodes[parent], nodes[smallest]] = [nodes[smallest], nodes[parent]];
        parent = smallest;
      }
    }
  }

  function isLive(lease, now) {
    return lease !== undefined && lease.expiresAt > now;
  }

  function resourceStateFor(resource) {
    let rs = resourceStates.get(resource);
    if (rs === undefined) {
      rs = {
        capacity: 1,
        chain: [],
        declared: false,
        live: 0,
        heap: new ExpiryQueue(),
        // Shares observed expired (and so not counted at the current
        // reading) but still in the lease table awaiting a sweep. The map is
        // reversible: a reading that goes backwards revives entries that are
        // live again, exactly as a fresh isLive judgment would.
        retired: new Map(),
        watermark: -Infinity,
        checkedVersion: -1,
        // Heap layout version, bumped whenever a node is pushed (a grant or
        // a renewal moves a share's expiry): a reconcile at an unchanged
        // reading must still run again after a renewal so the new expiry is
        // judged at that reading.
        version: 0,
        // quota -> number of queued positions on this resource under it; the
        // set drives quota-indexed wake seeding.
        waitRefs: new Map(),
      };
      resourceStates.set(resource, rs);
    }
    return rs;
  }

  function quotaChainFor(leafId) {
    const chain = [];
    let current = leafId;
    while (current !== null && current !== undefined) {
      const node = quotas.get(current);
      if (node === undefined) {
        throw new TypeError(`unknown quota ${String(current)}`);
      }
      chain.push(node);
      current = node.parentId;
    }
    chain.reverse();
    return chain;
  }

  // Forget a lease from every owner's retired set without touching its
  // live usage: the share was already observed expired (a sweep projected
  // the retirement before its reclaim event landed).
  function purgeRetired(lease) {
    const rs = resourceStates.get(lease.resource);
    if (rs !== undefined) {
      rs.retired.delete(lease.token);
    }
    for (const quota of lease.chain) {
      quota.retired.delete(lease.token);
    }
  }

  // Permanently remove a lease from the table (early release, reclaim, a
  // single-occupancy retake, or a replay superseding it). Owners that were
  // still counting the share lose it; owners that had already observed it
  // expired simply forget its retired entry.
  function retirePermanent(lease) {
    if (lease === undefined) {
      return;
    }
    const rs = resourceStates.get(lease.resource);
    if (rs !== undefined) {
      if (!rs.retired.has(lease.token)) {
        rs.live -= 1;
      }
      rs.retired.delete(lease.token);
    }
    for (const quota of lease.chain) {
      if (!quota.retired.has(lease.token)) {
        quota.live -= 1;
      }
      quota.retired.delete(lease.token);
    }
  }

  // Reconcile one occupancy owner (a resource or one quota level) with the
  // passed clock reading. Occupancy is judged the way isLive judges it -
  // `expiresAt > now` - at every call, so a reading that goes backwards
  // revives shares an earlier reading had already retired. The owner's
  // watermark makes a monotonic sequence of readings pop lazily in expiry
  // order, and a backwards reading walk the retired set back onto the heap.
  function reconcile(owner, now) {
    const retired = [];
    if (now === owner.watermark && owner.version === owner.checkedVersion) {
      return retired;
    }
    if (now > owner.watermark) {
      while (owner.heap.size > 0) {
        const node = owner.heap.peek();
        if (node.expiresAt > now) {
          break;
        }
        owner.heap.pop();
        const lease = leases.get(node.token);
        if (lease === undefined) {
          // Released or reclaimed.
          continue;
        }
        if (lease.heapSeq !== node.heapSeq) {
          // Superseded by a renewal: discard the old node without retiring
          // anything (its heap entry is stale) and keep scanning.
          continue;
        }
        if (owner.retired.has(node.token)) {
          // Either a share that revived after a backwards reading and
          // expired again, or a share a replay retake structurally
          // superseded: its first retirement already moved this owner's
          // usage, so the heap node is simply dropped.
          continue;
        }
        owner.retired.set(node.token, {
          heapSeq: node.heapSeq,
          expiresAt: node.expiresAt,
        });
        owner.live -= 1;
        retired.push(node);
      }
    } else {
      // The reading moved backwards: every retired share that is live again
      // under the earlier reading returns, and its node goes back on the heap
      // so a later forward reading retires it in proper expiry order.
      for (const [token, record] of owner.retired) {
        const lease = leases.get(token);
        if (lease === undefined || lease.heapSeq !== record.heapSeq) {
          // Released/reclaimed meanwhile, or renewed after a revival: the
          // old record describes nothing current.
          owner.retired.delete(token);
          continue;
        }
        if (record.expiresAt > now) {
          owner.retired.delete(token);
          const node = {
            token,
            expiresAt: record.expiresAt,
            heapSeq: record.heapSeq,
            order: queueOrder++,
          };
          owner.heap.push(node);
          owner.live += 1;
          retired.push({ node, revived: true });
        }
      }
    }
    owner.watermark = now;
    owner.checkedVersion = owner.version;
    return retired;
  }

  // Project a batch of global reclaim nodes through every owner they
  // occupy before wake planning in a sweep. Returns a record able to put
  // every owner exactly back if the log write fails.
  function projectReclaims(due, now) {
    const touched = [];
    const seen = new Set();
    const touch = (owner) => {
      if (owner !== undefined && !seen.has(owner)) {
        seen.add(owner);
        touched.push({ owner, watermark: owner.watermark, checkedVersion: owner.checkedVersion, retired: [] });
      }
    };
    for (const node of due) {
      const lease = leases.get(node.token);
      touch(resourceStates.get(lease.resource));
      for (const quota of lease.chain) {
        touch(quota);
      }
    }
    for (const entry of touched) {
      entry.retired = reconcile(entry.owner, now);
    }
    return touched;
  }

  function revertProjections(touched) {
    for (const entry of touched) {
      const { owner } = entry;
      for (const item of entry.retired) {
        const node = item.node ?? item;
        owner.retired.delete(node.token);
        if (item.revived) {
          owner.live -= 1;
        } else {
          owner.live += 1;
        }
        owner.heap.push(node);
      }
      owner.watermark = entry.watermark;
    }
  }

  function getSlots(resource) {
    let set = slots.get(resource);
    if (set === undefined) {
      set = new Set();
      slots.set(resource, set);
    }
    return set;
  }

  function addSlot(resource, token) {
    getSlots(resource).add(token);
  }

  function removeSlot(resource, token) {
    const set = slots.get(resource);
    if (set === undefined) {
      return false;
    }
    const removed = set.delete(token);
    if (removed && set.size === 0) {
      slots.delete(resource);
    }
    return removed;
  }

  // Drop expired-but-unswept credentials from a single-occupancy resource
  // that is about to be granted: the resource is free, and a retaken lease
  // must never be reported as reclaimed later. Mirrors the retake rule. Share
  // resources never displace anyone, so they never call this.
  function dropStaleSlots(resource, now) {
    const rs = resourceStates.get(resource);
    if (rs === undefined) {
      return;
    }
    // Permanent removal consults each owner's retired set itself and
    // only decrements owners that were still counting the share, so no
    // prior reconcile is needed here.
    const set = slots.get(resource);
    if (set === undefined) {
      return;
    }
    for (const token of [...set]) {
      const lease = leases.get(token);
      if (!isLive(lease, now)) {
        retirePermanent(lease);
        leases.delete(token);
        set.delete(token);
      }
    }
    if (set.size === 0) {
      slots.delete(resource);
    }
  }

  // Any live holder of a resource. Reconcile first retires every share due
  // at the passed reading, so the heap top that still names a current lease
  // is a live one; stale nodes (renewed, released, reclaimed) pop away.
  // Work is O(expired * log n), never a scan of every share.
  function liveHolder(resource, now) {
    const rs = resourceStateFor(resource);
    reconcile(rs, now);
    while (rs.heap.size > 0) {
      const node = rs.heap.peek();
      const lease = leases.get(node.token);
      if (lease === undefined || lease.heapSeq !== node.heapSeq) {
        rs.heap.pop();
        continue;
      }
      return lease.holder;
    }
    return null;
  }

  function fsyncDirectory(file) {
    // Best-effort durability for the rename itself: on platforms where
    // fsyncing a directory handle is unsupported there is nothing to sync,
    // and the rename has already committed by the time this runs.
    try {
      const fd = fs.openSync(path.dirname(file), 'r');
      try {
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
    } catch {
      // Ignored: atomicity of the swap does not depend on it.
    }
  }

  // Publish a file via temp + fsync + rename so the target is either the old
  // complete content or the new complete content, never a mixture. Any failure
  // leaves the target untouched and surfaces as an assertable LogFileError.
  function writeAtomic(target, content) {
    atomicFileSequence += 1;
    const tmp = `${target}.tmp-${process.pid}-${atomicFileSequence}`;
    try {
      const fd = fs.openSync(tmp, 'wx');
      try {
        fs.writeFileSync(fd, content, 'utf8');
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      fs.renameSync(tmp, target);
      fsyncDirectory(target);
    } catch (cause) {
      try {
        fs.rmSync(tmp, { force: true });
      } catch {
        // The original error is the one the caller must act on.
      }
      throw new LogFileError(
        `failed to write lease file ${target}`,
        cause,
      );
    }
  }

  function appendEvents(events) {
    if (logPath === null) {
      return;
    }
    // One write for the whole batch: an operation that frees shares and
    // wakes requests either lands completely or not at all. Sequence ids are
    // numbered up front but only committed after the write lands, so a failed
    // append leaves both the log and the counter exactly as before.
    let text = '';
    let seq = eventSeq;
    for (const event of events) {
      seq += 1;
      event.e = seq;
      text += `${JSON.stringify(event)}\n`;
    }
    try {
      fs.appendFileSync(logPath, text);
    } catch (cause) {
      throw new LogFileError(
        `failed to append to lease log at ${logPath}`,
        cause,
      );
    }
    eventSeq = seq;
  }

  function appendEvent(event) {
    appendEvents([event]);
  }

  // Declare a hierarchical quota node. The limit is the maximum number of
  // shares that may be live at once under the node, counting every descendant
  // level together. The parent (if any) must already be declared, so quota
  // ids always form a forest and chains are captured once per grant.
  function setQuota(quotaId, limit, parentId = undefined) {
    if (quotaId === null || quotaId === undefined) {
      throw new TypeError('quota id must not be null or undefined');
    }
    if (!Number.isInteger(limit) || limit <= 0) {
      throw new TypeError('quota limit must be a positive integer');
    }
    // An explicitly named parent on an existing quota attempts to re-parent
    // the node (a forest never allows); omitting the argument just updates
    // the limit in place, so the existing parent is kept.
    if (parentId !== null && parentId !== undefined && !quotas.has(parentId)) {
      throw new TypeError(`unknown parent quota ${String(parentId)}`);
    }
    const existing = quotas.get(quotaId);
    if (existing !== undefined) {
      if (parentId !== undefined && parentId !== existing.parentId) {
        throw new TypeError(`quota ${String(quotaId)} is already declared`);
      }
      const event = {
        v: LOG_VERSION,
        type: 'quota',
        at: clock(),
        quota: quotaId,
        parent: existing.parentId,
        limit,
      };
      appendEvent(event);
      existing.limit = limit;
      return;
    }
    if (parentId === quotaId) {
      // A brand-new node cannot be its own parent.
      throw new TypeError(`quota ${String(quotaId)} cannot parent itself`);
    }

    const event = {
      v: LOG_VERSION,
      type: 'quota',
      at: clock(),
      quota: quotaId,
      parent: parentId ?? null,
      limit,
    };
    appendEvent(event);
    quotas.set(quotaId, {
      id: quotaId,
      parentId: parentId ?? null,
      limit,
      live: 0,
      heap: new ExpiryQueue(),
      resources: new Set(),
      // Resources whose wait queue currently holds at least one request; a
      // share freed anywhere under this quota seeds those queues' heads.
      waitingResources: new Set(),
      retired: new Map(),
      watermark: -Infinity,
      version: 0,
      checkedVersion: -1,
    });
  }

  // Declare a resource's share capacity and the quota leaf (if any) its
  // shares count against. An undeclared resource behaves as capacity 1 with
  // no quota.
  function declareResource(resource, capacity, quotaId = null) {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new TypeError('capacity must be a positive integer');
    }
    let chain = [];
    if (quotaId !== null && quotaId !== undefined) {
      if (!quotas.has(quotaId)) {
        throw new TypeError(`unknown quota ${String(quotaId)}`);
      }
      chain = quotaChainFor(quotaId);
    }
    const rs = resourceStateFor(resource);
    if (rs.declared) {
      throw new TypeError(`resource ${String(resource)} is already declared`);
    }

    const event = {
      v: LOG_VERSION,
      type: 'resource',
      at: clock(),
      resource,
      capacity,
      quota: quotaId ?? null,
    };
    appendEvent(event);

    rs.declared = true;
    rs.capacity = capacity;
    rs.chain = chain;
    for (const quota of chain) {
      quota.resources.add(resource);
    }
  }

  function detachGroup(group) {
    for (const position of group.positions) {
      const queue = waitQueues.get(position.resource);
      if (queue !== undefined) {
        const index = queue.indexOf(position);
        if (index !== -1) {
          queue.splice(index, 1);
          if (queue.length === 0) {
            waitQueues.delete(position.resource);
          }
        }
      }
      const rs = resourceStates.get(position.resource);
      if (rs !== undefined) {
        for (const quota of rs.chain) {
          const refs = rs.waitRefs.get(quota) ?? 0;
          if (refs <= 1) {
            rs.waitRefs.delete(quota);
            quota.waitingResources.delete(position.resource);
          } else {
            rs.waitRefs.set(quota, refs - 1);
          }
        }
      }
    }
    pendingGroups.delete(group.requestId);
    // Its deadline node retires lazily on the next purge.
    group.deadlineNode.dead = true;
  }

  // A queued request gives up once the clock reaches its deadline and is
  // never woken afterwards. Only the deadline heap is consulted: requests
  // that are still live are never even looked at, no matter how many wait.
  function purgeExpiredWaiters(now) {
    while (deadlineQueue.size > 0) {
      const node = deadlineQueue.peek();
      if (node.dead) {
        deadlineQueue.pop();
        continue;
      }
      if (node.deadline > now) {
        break;
      }
      deadlineQueue.pop();
      const group = pendingGroups.get(node.requestId);
      if (group === undefined || group.deadlineNode !== node) {
        continue;
      }
      detachGroup(group);
      group.ticket.status = 'expired';
    }
  }

  function enqueueGroup(rows, ttlList, holder, now, waitMs, single) {
    requestSequence += 1;
    const requestId = `wait-${registryId}-${requestSequence}`;
    const resources = rows.map((row) => row.resource);
    const ticket = single
      // Exact baseline shape for single-resource callers.
      ? { status: 'waiting', resource: resources[0], holder, waitId: requestId }
      : { status: 'waiting', resources, holder, waitId: requestId };
    deadlineTie += 1;
    const deadlineNode = { requestId, deadline: now + waitMs, tie: deadlineTie, dead: false };
    const group = {
      requestId,
      arrival: requestSequence,
      holder,
      deadline: now + waitMs,
      deadlineNode,
      single,
      ticket,
      positions: [],
    };
    deadlineQueue.push(deadlineNode);
    // One position per demand row, pushed onto each resource's queue in the
    // given order. Each position claims `count` shares atomically.
    rows.forEach((row, index) => {
      const position = {
        group,
        resource: row.resource,
        count: row.count,
        ttlMs: ttlList[index],
        granted: false,
        tokens: [],
        expiresAt: [],
        epochs: [],
      };
      group.positions.push(position);
      let queue = waitQueues.get(row.resource);
      if (queue === undefined) {
        queue = [];
        waitQueues.set(row.resource, queue);
      }
      queue.push(position);

      // Index this resource's queue under every quota of its chain, so a
      // share freed anywhere under a quota can seed the head requests that
      // quota alone was blocking.
      const rs = resourceStateFor(row.resource);
      for (const quota of rs.chain) {
        const refs = rs.waitRefs.get(quota) ?? 0;
        if (refs === 0) {
          quota.waitingResources.add(row.resource);
        }
        rs.waitRefs.set(quota, refs + 1);
      }
    });
    pendingGroups.set(requestId, group);
    return group;
  }

  function nextBatchId() {
    batchSequence += 1;
    return `batch-${registryId}-${batchSequence}`;
  }

  // Plan every grant one request needs (one credential per demanded share),
  // touching no state. The caller commits only after all events are on disk.
  // batchId ties the credentials of one atomic batch together for replay.
  // epochCursor is shared across every batch planned in one cascade (a
  // release or sweep commits all the wake grants together), so a resource
  // granted twice in that cascade still hands out strictly increasing
  // epochs; nothing advances the committed counter until the write lands.
  function planGroupGrants(group, now, seqCursor, epochCursor) {
    const batchId = nextBatchId();
    const grants = [];
    let seq = seqCursor;
    for (const position of group.positions) {
      if (position.granted) {
        continue;
      }
      const rs = resourceStateFor(position.resource);
      const chain = rs.chain;
      for (let share = 0; share < position.count; share += 1) {
        const minted = mintToken(now, seq);
        seq = minted.seq;
        const expiresAt = now + position.ttlMs;
        const nextEpoch = (epochCursor.get(position.resource)
          ?? epochs.get(position.resource) ?? 0) + 1;
        epochCursor.set(position.resource, nextEpoch);
        grants.push({
          position,
          group,
          resource: position.resource,
          holder: group.holder,
          ttlMs: position.ttlMs,
          token: minted.token,
          seq: minted.seq,
          epoch: nextEpoch,
          expiresAt,
          chain,
          event: {
            v: LOG_VERSION,
            type: 'acquire',
            at: now,
            token: minted.token,
            resource: position.resource,
            holder: group.holder,
            ttlMs: position.ttlMs,
            expiresAt,
            epoch: nextEpoch,
            requestId: group.requestId,
            batch: batchId,
          },
        });
      }
    }
    return { group, grants, seq, batchId };
  }

  function applyGrant(grant, batchId, now) {
    tokenSequence = grant.seq;
    knownTokens.add(grant.token);
    const rs = resourceStateFor(grant.resource);
    const lease = {
      resource: grant.resource,
      holder: grant.holder,
      expiresAt: grant.expiresAt,
      ttlMs: grant.ttlMs,
      legacy: false,
      heapSeq: 0,
      batch: batchId,
      epoch: grant.epoch,
      chain: rs.chain,
    };
    leases.set(grant.token, lease);
    epochs.set(grant.resource, grant.epoch);
    if (rs.capacity === 1) {
      dropStaleSlots(grant.resource, now);
    }
    addSlot(grant.resource, grant.token);
    rs.live += 1;
    for (const quota of rs.chain) {
      quota.live += 1;
    }
    pushLeaseHeaps(grant.token, lease);
    counters.granted += 1;

    grant.position.tokens.push(grant.token);
    grant.position.expiresAt.push(grant.expiresAt);
    grant.position.epochs.push(grant.epoch);
  }

  function applyPlannedGrants(plan, now) {
    for (const grant of plan.grants) {
      applyGrant(grant, plan.batchId, now);
      grant.position.granted = true;
    }

    // The batch holds everything it asked for; it leaves every queue at once.
    const { group } = plan;
    const { ticket } = group;
    detachGroup(group);
    ticket.status = 'granted';
    const granted = [];
    for (const position of group.positions) {
      for (let i = 0; i < position.tokens.length; i += 1) {
        granted.push({
          resource: position.resource,
          holder: group.holder,
          token: position.tokens[i],
          expiresAt: position.expiresAt[i],
          epoch: position.epochs[i],
        });
      }
    }
    if (group.single) {
      ticket.token = granted[0].token;
      ticket.expiresAt = granted[0].expiresAt;
      ticket.epoch = granted[0].epoch;
    } else {
      // One credential per demanded share, each with its own expiry.
      ticket.leases = granted;
      // Alias for callers that read the batch outcome as a flat list.
      ticket.results = granted;
    }
  }

  function positionPlanned(plans, position) {
    return plans.some((plan) =>
      plan.grants.some((grant) => grant.position === position));
  }

  // Aggregate the share demand of one request: per resource and per quota,
  // adding a position's count once for every quota its resource sits under.
  function groupDemand(group) {
    const byResource = new Map();
    const byQuota = new Map();
    for (const position of group.positions) {
      if (position.granted) {
        continue;
      }
      byResource.set(
        position.resource,
        (byResource.get(position.resource) ?? 0) + position.count,
      );
      const rs = resourceStates.get(position.resource);
      for (const quota of rs.chain) {
        byQuota.set(quota, (byQuota.get(quota) ?? 0) + position.count);
      }
    }
    return { byResource, byQuota };
  }

  // A queued request is fillable exactly when every position reaches the
  // front of its resource's queue and the triggering event leaves enough
  // free shares on the resource and on every quota level of its chain,
  // counting shares other requests in this same cascade are about to take.
  // Only live credentials occupy: an expired but unswept share is already
  // gone, while a surviving share of a released duplicate credential keeps
  // its quota occupied.
  function groupFillable(group, plans, freedResources, freedQuotas, now) {
    for (const position of group.positions) {
      if (position.granted) {
        continue;
      }
      const queue = waitQueues.get(position.resource);
      if (queue === undefined) {
        return false;
      }
      const index = queue.indexOf(position);
      for (let i = 0; i < index; i += 1) {
        const ahead = queue[i];
        if (ahead.group === group) {
          // Own repeated row: its positions are granted together.
          continue;
        }
        if (positionPlanned(plans, ahead)) {
          continue;
        }
        return false;
      }
    }

    const demand = groupDemand(group);
    for (const [resource, wanted] of demand.byResource) {
      const rs = resourceStates.get(resource);
      // Every owner is at the planning reading already (a release reconciles
      // lazily first; a sweep projects the batch it is about to reclaim), so
      // the projected occupancy is the current usage minus the shares this
      // event vacates, plus what other requests in this cascade will take.
      const occupied = rs.live - (freedResources.get(resource) ?? 0)
        + plannedResourceUse(plans, group, (grant) => grant.resource === resource);
      if (!rs.declared) {
        // Single-occupancy resource.
        if (occupied > 0) {
          return false;
        }
        continue;
      }
      if (occupied + wanted > rs.capacity) {
        return false;
      }
    }

    // Quotas only ever sit above declared resources, so every chain check
    // uses share arithmetic including this request's own demand.
    for (const [quota, wanted] of demand.byQuota) {
      const occupied = quota.live - (freedQuotas.get(quota) ?? 0)
        + plannedResourceUse(plans, group, (grant) => grant.chain.includes(quota));
      if (occupied + wanted > quota.limit) {
        return false;
      }
    }
    return true;
  }

  function plannedResourceUse(plans, selfGroup, matches) {
    let use = 0;
    for (const plan of plans) {
      if (plan.group === selfGroup) {
        continue;
      }
      for (const grant of plan.grants) {
        if (matches(grant)) {
          use += 1;
        }
      }
    }
    return use;
  }

  // Grant every queued request whose shares become available through the
  // given freeing event, cascading across resources, quotas and queues.
  // Entries are { token, resource, chain }: a release vacates live shares,
  // and a sweep hands in the whole batch of globally due shares. In sweep
  // mode the shares the owners have not lazily retired yet are projected out
  // of each owner's usage, so a share retired at one level but still counted
  // at another vacates room exactly where it was still occupying; a failed
  // append then rolls the whole projection back.
  //
  // Only requests standing at the head of a queue touched by the event
  // (directly, through a quota, or exposed behind a batch planned in this
  // cascade) can possibly be fillable: a request was not fillable when it
  // queued, and nothing else changed. Wake planning therefore never inspects
  // requests waiting on unrelated resources, regardless of how many shares
  // or waiters exist in total.
  function planWakeups(freedEntries, now, sweepMode) {
    const freedResources = new Map();
    const freedQuotas = new Map();
    const touchedResources = new Set();
    const touchedQuotas = new Set();
    for (const entry of freedEntries) {
      touchedResources.add(entry.resource);
      for (const quota of entry.chain) {
        touchedQuotas.add(quota);
      }
    }
    if (!sweepMode) {
      // A release removes one live share; any other share due at the reading
      // is already gone as well, so reconcile every owner whose room a
      // candidate might ask about before testing the queues.
      for (const resource of touchedResources) {
        reconcile(resourceStates.get(resource), now);
      }
      for (const quota of touchedQuotas) {
        reconcile(quota, now);
      }
    }
    for (const entry of freedEntries) {
      const rs = resourceStates.get(entry.resource);
      const vacatesResource = !sweepMode || !rs.retired.has(entry.token);
      if (vacatesResource) {
        freedResources.set(
          entry.resource,
          (freedResources.get(entry.resource) ?? 0) + 1,
        );
      }
      for (const quota of entry.chain) {
        if (!sweepMode || !quota.retired.has(entry.token)) {
          freedQuotas.set(quota, (freedQuotas.get(quota) ?? 0) + 1);
        }
      }
    }

    const plans = [];
    const planned = new Set();
    const candidates = new Set();
    const seedHead = (resource) => {
      const queue = waitQueues.get(resource);
      if (queue !== undefined && queue.length > 0) {
        candidates.add(queue[0].group);
      }
    };
    for (const resource of touchedResources) {
      seedHead(resource);
    }
    // A share freed on one resource can fund a head request on a different
    // resource that only lacked room on a shared ancestor quota. Such a head
    // still needs spare local capacity (this event freed nothing there, and
    // only shares due in this sweep can leave silently), so fully packed
    // queues are never added as candidates: seeding work stays proportional
    // to queues the freed quota room could actually unblock.
    for (const quota of touchedQuotas) {
      for (const resource of quota.waitingResources) {
        const rs = resourceStates.get(resource);
        if (rs !== undefined) {
          if (rs.live - (freedResources.get(resource) ?? 0) >= rs.capacity) {
            continue;
          }
        }
        seedHead(resource);
      }
    }

    let seq = tokenSequence;
    // One cursor for the whole cascade: the batches commit together, so
    // credentials of several woken groups on the same resource still draw
    // strictly increasing epochs in planning order.
    const epochCursor = new Map();
    for (;;) {
      // Candidates are picked strictly by arrival order: the earliest
      // pending request whose batch is now complete wins, so no resource's
      // queue can jump ahead of an earlier request.
      let next = null;
      for (const group of candidates) {
        if (planned.has(group)) {
          continue;
        }
        if (!groupFillable(group, plans, freedResources, freedQuotas, now)) {
          continue;
        }
        if (next === null || group.arrival < next.arrival) {
          next = group;
        }
      }
      if (next === null) {
        break;
      }
      const plan = planGroupGrants(next, now, seq, epochCursor);
      seq = plan.seq;
      plans.push(plan);
      planned.add(next);
      candidates.delete(next);
      // The granted batch leaves its queues; expose the first request behind
      // it on each resource it took, as a candidate for the same cascade.
      const resources = new Set(plan.grants.map((grant) => grant.resource));
      for (const resource of resources) {
        const queue = waitQueues.get(resource);
        if (queue === undefined) {
          continue;
        }
        for (const position of queue) {
          if (!planned.has(position.group)) {
            candidates.add(position.group);
            break;
          }
        }
      }
    }
    return plans;
  }

  // Resolve the ttl argument into one positive finite ttl per demand row.
  function resolveTtls(rows, ttlMsValue) {
    if (ttlMsValue === undefined) {
      return rows.map(() => defaultTtlMs);
    }
    if (Array.isArray(ttlMsValue)) {
      if (ttlMsValue.length !== rows.length) {
        throw new TypeError('ttlMs must match the resources list length');
      }
      return ttlMsValue.map((value) => {
        if (!Number.isFinite(value) || value <= 0) {
          throw new TypeError('ttlMs must be a positive finite number');
        }
        return value;
      });
    }
    if (!Number.isFinite(ttlMsValue) || ttlMsValue <= 0) {
      throw new TypeError('ttlMs must be a positive finite number');
    }
    return rows.map(() => ttlMsValue);
  }

  // Normalize group input into one { resource, count } row per entry. A bare
  // resource value asks for one share; { resource, count } asks for several
  // shares of the same resource in one atomic request.
  function normalizeRows(entries) {
    if (!Array.isArray(entries) || entries.length === 0) {
      throw new TypeError('resources must be a non-empty array');
    }
    return entries.map((entry) => {
      if (entry !== null && typeof entry === 'object' && !Array.isArray(entry)) {
        if (entry.resource === null || entry.resource === undefined) {
          throw new TypeError('each resource entry needs a resource');
        }
        if (!Number.isInteger(entry.count) || entry.count <= 0) {
          throw new TypeError('share count must be a positive integer');
        }
        return { resource: entry.resource, count: entry.count };
      }
      return { resource: entry, count: 1 };
    });
  }

  // Check a direct (non-queued) request against every capacity and quota
  // level. Returns null when every demanded share fits, otherwise a descriptor
  // of the first constraint that blocks it.
  function directBlocker(rows, now) {
    for (const row of rows) {
      if (waitQueues.has(row.resource)) {
        return { kind: 'taken', resource: row.resource };
      }
    }
    // Declared (share) resources reconcile lazily at this observation:
    // an expired share vacates capacity and quota room before the
    // request is checked. An undeclared single-occupancy resource is
    // tested non-mutatingly below instead - its stale credentials are
    // displaced structurally by the grant's own dropStaleSlots, and
    // mutating owner state in this pre-check would move the stale share
    // twice.
    for (const row of rows) {
      const rs = resourceStates.get(row.resource);
      if (rs !== undefined && rs.declared) {
        reconcile(rs, now);
      }
    }
    const wantedByResource = new Map();
    for (const row of rows) {
      wantedByResource.set(
        row.resource,
        (wantedByResource.get(row.resource) ?? 0) + row.count,
      );
    }
    for (const [resource, wanted] of wantedByResource) {
      const rs = resourceStates.get(resource) ?? resourceStateFor(resource);
      if (!rs.declared) {
        // Single-occupancy resource: the request's own repeated rows stack
        // freely, so any live third-party credential blocks it outright.
        // Liveness is read off the heap without mutating owner state;
        // dropStaleSlots handles a structural displacement instead.
        if (hasLiveHeapNode(rs, now)) {
          return { kind: 'taken', resource, available: 0, requested: wanted };
        }
        continue;
      }
      if (rs.live + wanted > rs.capacity) {
        return {
          kind: 'taken',
          resource,
          available: rs.capacity - rs.live,
          requested: wanted,
        };
      }
    }
    const wantedByQuota = new Map();
    const quotaOrder = [];
    for (const row of rows) {
      const rs = resourceStates.get(row.resource);
      for (const quota of rs.chain) {
        if (!wantedByQuota.has(quota)) {
          wantedByQuota.set(quota, 0);
          quotaOrder.push(quota);
        }
        wantedByQuota.set(quota, wantedByQuota.get(quota) + row.count);
      }
    }
    for (const quota of quotaOrder) {
      reconcile(quota, now);
      const wanted = wantedByQuota.get(quota);
      if (quota.live + wanted > quota.limit) {
        return {
          kind: 'quota',
          quota: quota.id,
          available: quota.limit - quota.live,
          requested: wanted,
        };
      }
    }
    return null;
  }

  // Persist and commit one credential per demanded share, all in a single log
  // write. A failed write leaves every resource and quota exactly as before.
  function commitDirectGrants(rows, ttlList, holder, now) {
    const batchId = nextBatchId();
    const events = [];
    const grants = [];
    let seq = tokenSequence;
    // Every epoch this batch will take is reserved up front, so a group
    // demanding the same resource several times gets consecutive epochs and
    // the committed counter only moves once the whole write lands.
    const epochCursor = new Map();
    rows.forEach((row, index) => {
      for (let share = 0; share < row.count; share += 1) {
        const ttlMsValue = ttlList[index];
        const minted = mintToken(now, seq);
        seq = minted.seq;
        const expiresAt = now + ttlMsValue;
        const nextEpoch = (epochCursor.get(row.resource)
          ?? epochs.get(row.resource) ?? 0) + 1;
        epochCursor.set(row.resource, nextEpoch);
        events.push({
          v: LOG_VERSION,
          type: 'acquire',
          at: now,
          token: minted.token,
          resource: row.resource,
          holder,
          ttlMs: ttlMsValue,
          expiresAt,
          epoch: nextEpoch,
          batch: batchId,
        });
        grants.push({
          resource: row.resource,
          ttlMs: ttlMsValue,
          token: minted.token,
          expiresAt,
          seq: minted.seq,
          epoch: nextEpoch,
        });
      }
    });
    appendEvents(events);

    tokenSequence = seq;
    const result = [];
    for (const grant of grants) {
      knownTokens.add(grant.token);
      const rs = resourceStateFor(grant.resource);
      const lease = {
        resource: grant.resource,
        holder,
        expiresAt: grant.expiresAt,
        ttlMs: grant.ttlMs,
        legacy: false,
        heapSeq: 0,
        batch: batchId,
        epoch: grant.epoch,
        chain: rs.chain,
        };
      leases.set(grant.token, lease);
      epochs.set(grant.resource, grant.epoch);
      if (rs.capacity === 1) {
        dropStaleSlots(grant.resource, now);
      }
      addSlot(grant.resource, grant.token);
      rs.live += 1;
      for (const quota of rs.chain) {
        quota.live += 1;
      }
      pushLeaseHeaps(grant.token, lease);
      counters.granted += 1;
      result.push({
        resource: grant.resource,
        holder,
        token: grant.token,
        expiresAt: grant.expiresAt,
        epoch: grant.epoch,
      });
    }
    return result;
  }

  // Atomic multi-resource, multi-share acquisition. Every demanded share
  // gets its own credential and independent expiry; either all of them are
  // held, or none are (the whole request queues instead). ttlMs may be one
  // number or one ttl per demand row.
  function acquireAll(entries, holder, ttlMs = undefined, waitMs = undefined) {
    const rows = normalizeRows(entries);
    const ttlList = resolveTtls(rows, ttlMs);
    const wantsWait = validateWait(waitMs);
    const now = clock();
    purgeExpiredWaiters(now);

    // Earlier waiters keep their place; beyond that every capacity and every
    // quota level must have room for the whole demand at once.
    const blocker = directBlocker(rows, now);

    if (blocker !== null) {
      if (!wantsWait) {
        if (blocker.kind === 'quota') {
          throw new QuotaExceededError(
            blocker.quota, holder, blocker.requested, blocker.available,
          );
        }
        throw new LeaseTakenError(
          blocker.resource, liveHolder(blocker.resource, now),
          blocker.requested, blocker.available,
        );
      }
      return enqueueGroup(rows, ttlList, holder, now, waitMs, false).ticket;
    }

    return commitDirectGrants(rows, ttlList, holder, now);
  }

  function validateWait(waitMs) {
    const wantsWait = waitMs !== undefined && waitMs !== null;
    if (wantsWait && (!Number.isFinite(waitMs) || waitMs <= 0)) {
      throw new TypeError('waitMs must be a positive finite number');
    }
    return wantsWait;
  }

  function acquire(resourceOrResources, holder, ttlMs = undefined, waitMs = undefined) {
    // Array input is the multi-resource form: same entry point, atomic rules.
    if (Array.isArray(resourceOrResources)) {
      return acquireAll(resourceOrResources, holder, ttlMs, waitMs);
    }
    const resource = resourceOrResources;
    const effectiveTtl = ttlMs === undefined ? defaultTtlMs : ttlMs;
    if (!Number.isFinite(effectiveTtl) || effectiveTtl <= 0) {
      throw new TypeError('ttlMs must be a positive finite number');
    }
    const wantsWait = validateWait(waitMs);
    const now = clock();
    purgeExpiredWaiters(now);

    const rows = [{ resource, count: 1 }];
    const blocker = directBlocker(rows, now);
    if (blocker !== null) {
      if (!wantsWait) {
        if (blocker.kind === 'quota') {
          throw new QuotaExceededError(
            blocker.quota, holder, blocker.requested, blocker.available,
          );
        }
        throw new LeaseTakenError(
          resource, liveHolder(resource, now),
          blocker.requested, blocker.available,
        );
      }
      // Occupied or reserved: take a place in line instead of throwing. No
      // share changes hands just because a waiter showed up.
      return enqueueGroup(
        rows, [effectiveTtl], holder, now, waitMs, true,
      ).ticket;
    }

    const [granted] = commitDirectGrants(
      rows, [effectiveTtl], holder, now,
    );
    return granted;
  }

  function renew(token) {
    const lease = leases.get(token);
    if (lease === undefined || lease.legacy) {
      return false;
    }
    const now = clock();
    // Bring every owner of this share to the passed reading first: a clock
    // that moved backwards may have revived the share, and liveness must be
    // judged from the reading as given.
    const rs = resourceStates.get(lease.resource);
    reconcile(rs, now);
    for (const quota of lease.chain) {
      reconcile(quota, now);
    }
    if (lease.expiresAt <= now) {
      return false;
    }
    purgeExpiredWaiters(now);
    const expiresAt = now + lease.ttlMs;
    appendEvent({ v: LOG_VERSION, type: 'renew', at: now, token, expiresAt });

    lease.expiresAt = expiresAt;
    lease.heapSeq += 1;
    pushLeaseHeaps(token, lease);
    counters.renewed += 1;
    return true;
  }

  function release(token) {
    const lease = leases.get(token);
    if (lease === undefined || lease.legacy) {
      return false;
    }
    const now = clock();
    if (lease.expiresAt <= now) {
      return false;
    }
    purgeExpiredWaiters(now);

    // The freed shares go to the requests at the heads of the queues, on the
    // resource and up every quota level; a group only wakes when every share
    // it asked for is free. All wakeup grants ride in the same write as the
    // release, so a failed log leaves the lease and every queue untouched.
    const plans = planWakeups(
      [{ token, resource: lease.resource, chain: lease.chain }], now, false,
    );
    const events = [{ v: LOG_VERSION, type: 'release', at: now, token }];
    for (const plan of plans) {
      for (const grant of plan.grants) {
        events.push(grant.event);
      }
    }
    appendEvents(events);

    // The record comes off permanently; its expiry nodes retire lazily.
    retirePermanent(lease);
    leases.delete(token);
    removeSlot(lease.resource, token);
    counters.released += 1;
    for (const plan of plans) {
      applyPlannedGrants(plan, now);
    }
    return true;
  }

  // Cancelling only lands on a request that is still queued: an already
  // woken, expired or cancelled request reports false, as does an unknown id.
  function cancel(requestId) {
    purgeExpiredWaiters(clock());
    const group = pendingGroups.get(requestId);
    if (group === undefined) {
      return false;
    }
    detachGroup(group);
    group.ticket.status = 'cancelled';
    return true;
  }

  function sweep(now = clock()) {
    purgeExpiredWaiters(now);
    const due = [];
    while (expiryQueue.size > 0) {
      const node = expiryQueue.peek();
      if (node.expiresAt > now) {
        break;
      }
      expiryQueue.pop();
      const lease = leases.get(node.token);
      if (lease === undefined || lease.heapSeq !== node.heapSeq) {
        // Retired node: release, retake or renewal already superseded it.
        continue;
      }
      due.push(node);
    }

    if (due.length === 0) {
      return [];
    }

    // Every due node still carries the current lease record (retired nodes
    // were filtered above) and a current lease still occupies a slot. The
    // wake planner projects each such share out of every owner that has not
    // lazily retired it yet (checked through the owner's retired set), so a
    // share can fund room at one level while still occupying another.
    const reclaimItems = due.map((node) => {
      const lease = leases.get(node.token);
      return {
        token: node.token,
        resource: lease.resource,
        chain: lease.chain,
      };
    });
    const events = [{
      v: LOG_VERSION,
      type: 'reclaim',
      at: now,
      items: reclaimItems.map(({ token, resource }) => ({ token, resource })),
    }];
    // Project the due shares through every resource and quota owner
    // before planning wakes: the sweep is a single committed operation, so
    // wake planning sees the reclaimed room at every level even though the
    // reclaim lands with the log write below. Every owner is reconciled to
    // the sweep reading; a later observation at the same reading is a
    // no-op, and a backwards reading revives owners that the sweep itself
    // never reclaimed.
    const projected = projectReclaims(due, now);
    const plans = planWakeups(
      reclaimItems.map(({ token, resource, chain }) => ({
        token, resource, chain,
      })),
      now,
      true,
    );
    for (const plan of plans) {
      for (const grant of plan.grants) {
        events.push(grant.event);
      }
    }

    try {
      appendEvents(events);
    } catch (error) {
      // The write never happened: put the popped global nodes back and
      // undo every projected retirement, so the registry is byte-for-byte
      // and figure-for-figure the same as before the call.
      revertProjections(projected);
      for (const node of due) {
        expiryQueue.push(node);
      }
      throw error;
    }

    const freed = [];
    for (const node of due) {
      const lease = leases.get(node.token);
      // The global reclaim is committed. Usage was projected out of every
      // owner before the write, so here the share is only forgotten from
      // retired sets and the table; no owner moves usage a second time.
      purgeRetired(lease);
      leases.delete(node.token);
      if (removeSlot(lease.resource, node.token)) {
        counters.reclaimed += 1;
        freed.push(lease.resource);
      }
    }
    for (const plan of plans) {
      applyPlannedGrants(plan, now);
    }
    return freed;
  }

  function holder(resource) {
    const now = clock();
    purgeExpiredWaiters(now);
    return liveHolder(resource, now);
  }

  // Last committed credential epoch of a resource: 0 before its first grant,
  // otherwise the epoch of its most recently granted credential. Releases,
  // reclaims, expiries and restarts never roll it back, so a free resource
  // still reports the epoch its last credential carried.
  function epoch(resource) {
    return epochs.get(resource) ?? 0;
  }

  // Leader fence check. Returns true only when `token` is a current, live
  // credential of `resource` minted in this process, carrying exactly the
  // resource's last committed epoch. Every other situation - an unknown
  // resource or token, a released/reclaimed/expired credential, a credential
  // captured before a restart, an older epoch, or another holder's
  // credential - fences the caller instead of returning false.
  function assertLease(resource, token, epochValue) {
    if (typeof resource !== 'string') {
      throw new TypeError('resource must be a string');
    }
    if (typeof token !== 'string') {
      throw new TypeError('token must be a string');
    }
    if (!Number.isInteger(epochValue)) {
      throw new TypeError('epoch must be a finite integer');
    }
    const lease = leases.get(token);
    if (
      lease === undefined ||
      lease.legacy ||
      lease.resource !== resource ||
      lease.expiresAt <= clock() ||
      lease.epoch !== epochValue ||
      epochs.get(resource) !== epochValue
    ) {
      throw new LeaseFencedError(resource);
    }
    return true;
  }

  function stats() {
    const now = clock();
    purgeExpiredWaiters(now);
    // Reconcile each resource owner independently: work is proportional to
    // the shares actually due since the last observation plus the number
    // of distinct resources, never to the total number of live shares.
    // Quota usage follows the same reconciliation lazily through acquires,
    // releases and sweeps.
    let live = 0;
    for (const rs of resourceStates.values()) {
      reconcile(rs, now);
      live += rs.live;
    }
    // Resource reconciliation retires a share at the resource only; every
    // quota level is an independent owner and must observe the reading too,
    // so an expired share vacates ancestor room at this observation.
    for (const quota of quotas.values()) {
      reconcile(quota, now);
    }
    return {
      granted: counters.granted,
      renewed: counters.renewed,
      released: counters.released,
      reclaimed: counters.reclaimed,
      live,
    };
  }

  // Fold the whole history into one snapshot and restart the event log from
  // that point. The snapshot is published first (temp + fsync + rename) and
  // the trimmed log second; a failure at either step leaves the previously
  // usable files in place and throws LogFileError. Credentials captured by
  // the snapshot are void afterwards, exactly as across a restart.
  function compact(now = clock()) {
    if (logPath === null) {
      throw new LogFileError('cannot compact a registry without a logPath');
    }

    // Read the log up front: an unreadable log must fail compaction rather
    // than replacing it. Every complete line is already reflected in the
    // in-memory state; an unterminated fragment at the tail (an append that
    // died with the process) is folded nowhere and is dropped by the rewrite.
    try {
      fs.readFileSync(logPath, 'utf8');
    } catch (cause) {
      if (cause.code !== 'ENOENT') {
        throw new LogFileError(`failed to read lease log at ${logPath}`, cause);
      }
    }

    // The image carries every lease still in the table, including expired
    // ones nobody has swept yet, so a sweep after compaction frees and counts
    // them in exactly the same order as before. Only live credentials show in
    // ownership and liveness either way.
    //
    // Equal expiry times are tie-broken by expiry-heap insertion order
    // (renewals push a fresh node), so remember each lease's current node
    // order and serialize in heap-pop order: reclaim order is then identical
    // after a restart even when several credentials expire at the same
    // instant.
    const activeOrder = new Map();
    for (const node of expiryQueue.nodes) {
      const lease = leases.get(node.token);
      if (lease !== undefined && lease.heapSeq === node.heapSeq) {
        activeOrder.set(node.token, node.order);
      }
    }
    const leaseEntries = [];
    for (const [token, lease] of leases.entries()) {
      leaseEntries.push({
        token,
        resource: lease.resource,
        holder: lease.holder,
        expiresAt: lease.expiresAt,
        ttlMs: lease.ttlMs,
        epoch: lease.epoch,
        batch: lease.batch ?? null,
        // Local tie-breaker for the sort below; stripped before writing.
        order: activeOrder.get(token) ?? 0,
      });
    }
    leaseEntries.sort((a, b) =>
      a.expiresAt !== b.expiresAt
        ? a.expiresAt - b.expiresAt
        : a.order - b.order);
    for (const entry of leaseEntries) {
      delete entry.order;
    }

    // Quota forest and resource declarations come first in the image so the
    // leases below can be attached to reconstructed chains; per-level quota
    // occupation is rebuilt from the lease entries themselves, so the usage
    // figures can never disagree with the shares they count.
    const quotaEntries = [];
    for (const [id, quota] of quotas.entries()) {
      quotaEntries.push({ quota: id, parent: quota.parentId, limit: quota.limit });
    }
    const resourceEntries = [];
    for (const [resource, rs] of resourceStates.entries()) {
      if (rs.declared) {
        const leaf = rs.chain.length === 0 ? null : rs.chain[rs.chain.length - 1].id;
        resourceEntries.push({ resource, capacity: rs.capacity, quota: leaf });
      }
    }

    // The last committed epoch of every resource that has ever been granted,
    // including resources with no live credential: a free resource keeps
    // reporting its last epoch after a compacted restart, and the next grant
    // continues from it instead of restarting at 1.
    const epochEntries = [];
    for (const [resource, value] of epochs.entries()) {
      epochEntries.push({ resource, epoch: value });
    }

    const snapshot = {
      v: SNAPSHOT_VERSION,
      type: 'snapshot',
      at: now,
      seq: eventSeq,
      tokenSeq: tokenSequence,
      batchSeq: batchSequence,
      counters: { ...counters },
      quotas: quotaEntries,
      resources: resourceEntries,
      epochs: epochEntries,
      leases: leaseEntries,
    };

    // 1. Publish the snapshot. If this fails, nothing else has moved and the
    //    original log stays the sole source of truth.
    writeAtomic(snapshotPath, `${JSON.stringify(snapshot)}\n`);
    // 2. Replace the log with a boundary marker naming the last folded event
    //    sequence. Every complete line was folded into the image, so the only
    //    thing dropped here is an unterminated fragment; later events append
    //    after the marker. If this rename never happens after a committed
    //    snapshot, startup finds no marker, folds the whole log and skips
    //    every event whose sequence is already in the image; either order
    //    yields each event exactly once.
    const marker = {
      v: LOG_VERSION,
      type: 'snapshot-start',
      seq: eventSeq,
    };
    writeAtomic(logPath, `${JSON.stringify(marker)}\n`);

    return { seq: eventSeq, leases: leaseEntries.length };
  }

  // Credentials of the atomic batch currently being replayed: a batch takes
  // over a resource as one unit, so its own earlier credentials must not be
  // treated as the stale lease it replaces.
  let replayBatchId = null;
  let replayBatchTokens = new Set();
  // Highest batch/event number seen in the log, so ids minted after a restart
  // can never collide with an earlier process's.
  let replayMaxBatchSeq = 0;
  let replayMaxEventSeq = 0;
  let replayMaxTokenSeq = 0;

  load();
  batchSequence = Math.max(batchSequence, replayMaxBatchSeq);
  eventSeq = Math.max(eventSeq, replayMaxEventSeq);
  // Folded events are skipped during replay, so their credentials (including
  // released or reclaimed ones absent from the image) can never come back
  // from the minting loop alone; the snapshot carries the high-water mark.
  tokenSequence = Math.max(tokenSequence, replayMaxTokenSeq);

  return {
    acquire,
    acquireAll,
    acquireGroup: acquireAll,
    acquireMany: acquireAll,
    declareResource,
    setQuota,
    renew,
    release,
    sweep,
    holder,
    epoch,
    assertLease,
    stats,
    cancel,
    compact,
  };

  function applyReplayQuota(entry) {
    const existing = quotas.get(entry.quota);
    if (existing === undefined) {
      quotas.set(entry.quota, {
        id: entry.quota,
        parentId: entry.parent,
        limit: entry.limit,
        live: 0,
        heap: new ExpiryQueue(),
        resources: new Set(),
        waitingResources: new Set(),
        retired: new Map(),
        watermark: -Infinity,
        version: 0,
        checkedVersion: -1,
      });
    } else {
      existing.limit = entry.limit;
    }
  }

  function applyReplayResource(entry) {
    const rs = resourceStateFor(entry.resource);
    rs.declared = true;
    rs.capacity = entry.capacity;
    rs.chain = entry.quota === null ? [] : quotaChainFor(entry.quota);
    for (const quota of rs.chain) {
      quota.resources.add(entry.resource);
    }
  }

  function load() {
    if (logPath === null) {
      return;
    }

    const snapshot = readSnapshot();
    if (snapshot === 'empty') {
      // A snapshot file with no content declares the old history gone:
      // start from a brand-new log and remove the stale files.
      removeIfPresent(snapshotPath);
      removeIfPresent(logPath);
      return;
    }

    let text;
    try {
      text = fs.readFileSync(logPath, 'utf8');
    } catch (cause) {
      if (cause.code === 'ENOENT') {
        text = '';
      } else {
        throw new LogFileError(`failed to read lease log at ${logPath}`, cause);
      }
    }

    // A write that died mid-line leaves bytes after the final newline.
    // Complete entries stay in force; the unterminated fragment never
    // happened, and is trimmed so later appends start on a fresh line.
    const lastNewline = text.lastIndexOf('\n');
    const validLength = lastNewline === -1 ? 0 : lastNewline + 1;
    if (validLength < text.length) {
      try {
        fs.truncateSync(logPath, validLength);
      } catch (cause) {
        throw new LogFileError(
          `failed to trim torn tail from lease log at ${logPath}`,
          cause,
        );
      }
    }

    const validText = text.slice(0, validLength);
    if (snapshot === null) {
      replayStream(validText, 0, false, 0);
      return;
    }

    // A compacted log is always exactly a marker line followed by the tail
    // events. No marker means the swap never committed and the log still
    // holds the whole folded history; the sequence ids make those events
    // apply exactly once.
    const lines = validText === '' ? [] : validText.split('\n');
    let startsAt = 0;
    if (lines.length > 0 && lines[0] !== '') {
      const marker = parseMarker(lines[0], 1);
      if (marker !== null) {
        replayMaxEventSeq = Math.max(replayMaxEventSeq, marker.seq);
        startsAt = 1;
      }
    }

    applySnapshot(snapshot);
    replayStream(lines.slice(startsAt).join('\n'), startsAt, true, snapshot.seq);
  }

  // Structural boundary line a compacted log begins with. It carries the
  // last folded event sequence, mirroring the snapshot.
  function parseMarker(line, lineNumber) {
    let value;
    try {
      value = JSON.parse(line);
    } catch (cause) {
      throw new LogFileError(
        `corrupt lease log entry on line ${lineNumber} of ${logPath}`,
        cause,
      );
    }
    if (
      value === null ||
      typeof value !== 'object' ||
      value.v !== LOG_VERSION ||
      value.type !== 'snapshot-start'
    ) {
      return null;
    }
    if (!Number.isInteger(value.seq) || value.seq < 0) {
      throw new LogFileError(
        `corrupt snapshot marker on line ${lineNumber} of ${logPath}`,
      );
    }
    return value;
  }

  function removeIfPresent(file) {
    try {
      fs.rmSync(file, { force: true });
    } catch (cause) {
      if (cause.code !== 'ENOENT') {
        throw new LogFileError(`failed to remove stale lease file ${file}`, cause);
      }
    }
  }

  // Read and classify the snapshot file:
  //   null      no snapshot file: replay the log from the beginning
  //   'empty'   file present but with no content: start a brand-new log
  //   object    a usable snapshot
  // A damaged image or an unrecognized snapshot version throws LogFileError;
  // the original log is never touched, so it stays a complete fallback.
  function readSnapshot() {
    let raw;
    try {
      raw = fs.readFileSync(snapshotPath, 'utf8');
    } catch (cause) {
      if (cause.code === 'ENOENT') {
        return null;
      }
      throw new LogFileError(`failed to read lease snapshot at ${snapshotPath}`, cause);
    }

    if (raw.trim() === '') {
      // Content missing: the declared behaviour is a fresh start.
      return 'empty';
    }

    let value;
    try {
      value = JSON.parse(raw);
    } catch (cause) {
      throw new LogFileError(
        `corrupt lease snapshot at ${snapshotPath}`,
        cause,
      );
    }

    if (value === null || typeof value !== 'object') {
      throw new LogFileError(`corrupt lease snapshot at ${snapshotPath}`);
    }
    if (value.v !== SNAPSHOT_VERSION || value.type !== 'snapshot') {
      // Unknown version or damaged envelope: refuse to guess.
      throw new LogFileError(
        `unrecognized lease snapshot version at ${snapshotPath}`,
      );
    }
    if (
      value.counters === undefined ||
      value.leases === undefined ||
      value.seq === undefined
    ) {
      // Recognizable envelope, no payload: content is missing.
      return 'empty';
    }
    if (
      typeof value.counters !== 'object' ||
      value.counters === null ||
      !Array.isArray(value.leases) ||
      !Number.isInteger(value.seq) ||
      value.seq < 0 ||
      (value.tokenSeq !== undefined &&
        (!Number.isInteger(value.tokenSeq) || value.tokenSeq < 0)) ||
      !Number.isFinite(value.at) ||
      (value.batchSeq !== undefined &&
        (!Number.isInteger(value.batchSeq) || value.batchSeq < 0)) ||
      (value.quotas !== undefined && !Array.isArray(value.quotas)) ||
      (value.resources !== undefined && !Array.isArray(value.resources)) ||
      (value.epochs !== undefined && !Array.isArray(value.epochs))
    ) {
      throw new LogFileError(`corrupt lease snapshot at ${snapshotPath}`);
    }

    for (const key of ['granted', 'renewed', 'released', 'reclaimed']) {
      if (!Number.isFinite(value.counters[key])) {
        throw new LogFileError(`corrupt lease snapshot counters at ${snapshotPath}`);
      }
    }
    if (value.quotas !== undefined) {
      const quotaIds = new Set();
      for (const entry of value.quotas) {
        if (
          entry === null ||
          typeof entry !== 'object' ||
          entry.quota === null || entry.quota === undefined ||
          (entry.parent !== null && entry.parent === undefined) ||
          !Number.isInteger(entry.limit) || entry.limit <= 0
        ) {
          throw new LogFileError(`corrupt lease snapshot quota at ${snapshotPath}`);
        }
        if (quotaIds.has(entry.quota)) {
          throw new LogFileError(`duplicate quota in snapshot at ${snapshotPath}`);
        }
        quotaIds.add(entry.quota);
      }
      // The quota forest has to be reconstructable: every parent must be
      // named in the same image, otherwise a chain could not be rebuilt.
      for (const entry of value.quotas) {
        if (entry.parent !== null && !quotaIds.has(entry.parent)) {
          throw new LogFileError(
            `snapshot quota with unknown parent at ${snapshotPath}`,
          );
        }
      }
    }
    if (value.resources !== undefined) {
      for (const entry of value.resources) {
        if (
          entry === null ||
          typeof entry !== 'object' ||
          entry.resource === null || entry.resource === undefined ||
          !Number.isInteger(entry.capacity) || entry.capacity <= 0 ||
          (entry.quota !== null && entry.quota === undefined) ||
          (value.quotas !== undefined &&
            entry.quota !== null &&
            !value.quotas.some((quota) => quota.quota === entry.quota))
        ) {
          throw new LogFileError(`corrupt lease snapshot resource at ${snapshotPath}`);
        }
      }
    }
    if (value.epochs !== undefined) {
      const epochIds = new Set();
      for (const entry of value.epochs) {
        if (
          entry === null ||
          typeof entry !== 'object' ||
          entry.resource === null || entry.resource === undefined ||
          !Number.isInteger(entry.epoch) || entry.epoch <= 0
        ) {
          throw new LogFileError(`corrupt lease snapshot epoch at ${snapshotPath}`);
        }
        if (epochIds.has(entry.resource)) {
          throw new LogFileError(`duplicate epoch in snapshot at ${snapshotPath}`);
        }
        epochIds.add(entry.resource);
      }
    }
    for (const entry of value.leases) {
      if (
        entry === null ||
        typeof entry !== 'object' ||
        typeof entry.token !== 'string' ||
        !Number.isFinite(entry.ttlMs) ||
        !Number.isFinite(entry.expiresAt) ||
        (entry.epoch !== undefined &&
          (!Number.isInteger(entry.epoch) || entry.epoch <= 0))
      ) {
        throw new LogFileError(`corrupt lease snapshot lease at ${snapshotPath}`);
      }
    }

    return value;
  }

  function applySnapshot(snapshot) {
    counters.granted = snapshot.counters.granted;
    counters.renewed = snapshot.counters.renewed;
    counters.released = snapshot.counters.released;
    counters.reclaimed = snapshot.counters.reclaimed;
    replayMaxEventSeq = snapshot.seq;
    replayMaxTokenSeq = snapshot.tokenSeq ?? 0;
    replayMaxBatchSeq = snapshot.batchSeq ?? 0;

    // Configuration first: each lease below hooks into the reconstructed
    // quota chains and rebuilds per-level occupation as its shares are added.
    for (const entry of snapshot.quotas ?? []) {
      applyReplayQuota(entry);
    }
    for (const entry of snapshot.resources ?? []) {
      applyReplayResource(entry);
    }

    // Last committed epochs first, so every lease below can be checked
    // against the resource epoch it was supposed to carry. Recovery never
    // advances an epoch: the map comes back exactly as it was folded, and
    // only tail acquire events replayed afterwards move it on.
    const hasEpochSection = Array.isArray(snapshot.epochs);
    if (hasEpochSection) {
      for (const entry of snapshot.epochs) {
        epochs.set(entry.resource, entry.epoch);
      }
    }

    // Each live credential carries the epoch it was granted in; those epochs
    // are unique per resource and can never run past the resource's last
    // committed epoch.
    const leaseEpochsByResource = new Map();
    for (const entry of snapshot.leases) {
      // An image written by this build always records the credential epoch;
      // its absence is only possible in an older image without the section.
      const leaseEpoch = Number.isInteger(entry.epoch) ? entry.epoch : 0;
      if (hasEpochSection) {
        if (leaseEpoch <= 0) {
          throw new LogFileError(
            `snapshot lease without an epoch at ${snapshotPath}`,
          );
        }
        const lastEpoch = epochs.get(entry.resource) ?? 0;
        if (leaseEpoch > lastEpoch) {
          throw new LogFileError(
            `snapshot lease ahead of its resource epoch at ${snapshotPath}`,
          );
        }
        let seen = leaseEpochsByResource.get(entry.resource);
        if (seen === undefined) {
          seen = new Set();
          leaseEpochsByResource.set(entry.resource, seen);
        }
        if (seen.has(leaseEpoch)) {
          throw new LogFileError(
            `duplicate lease epoch in snapshot at ${snapshotPath}`,
          );
        }
        seen.add(leaseEpoch);
      }
      const rs = resourceStates.get(entry.resource) ?? resourceStateFor(entry.resource);
      const lease = {
        resource: entry.resource,
        holder: entry.holder,
        expiresAt: entry.expiresAt,
        ttlMs: entry.ttlMs,
        // Credentials captured before this process started are void: the
        // lease keeps occupying its shares, but its token cannot renew or
        // release them.
        legacy: true,
        heapSeq: 0,
        epoch: leaseEpoch,
        batch: entry.batch ?? null,
        chain: rs.chain,
        };
      leases.set(entry.token, lease);
      addSlot(entry.resource, entry.token);
      knownTokens.add(entry.token);
      rs.live += 1;
      for (const quota of rs.chain) {
        quota.live += 1;
      }
      pushLeaseHeaps(entry.token, lease);
    }
  }

  // Replay one chunk of complete log lines. In folded mode the snapshot is
  // already applied and events at or before foldedSeq are skipped; events
  // without a sequence id are folded history written by an older format and
  // are skipped as well. firstLineNumber only labels error messages.
  function replayStream(text, firstLineNumber, folded, foldedSeq) {
    if (text === '') {
      return;
    }
    const lines = text.split('\n');
    for (let i = 0; i < lines.length; i += 1) {
      const line = lines[i];
      if (line === '') {
        continue;
      }
      const lineNumber = firstLineNumber + i + 1;
      let event;
      try {
        event = JSON.parse(line);
      } catch (cause) {
        throw new LogFileError(
          `corrupt lease log entry on line ${lineNumber} of ${logPath}`,
          cause,
        );
      }
      if (folded) {
        if (
          event === null ||
          typeof event !== 'object' ||
          !Number.isInteger(event.e) ||
          event.e <= foldedSeq
        ) {
          // Already part of the snapshot image (or a legacy event folded in
          // before sequence ids existed).
          continue;
        }
      }
      replayEvent(event, lineNumber);
    }
  }

  function replayEvent(event, lineNumber) {
    if (event === null || typeof event !== 'object' || event.v !== LOG_VERSION) {
      throw new LogFileError(
        `unrecognized lease log version on line ${lineNumber} of ${logPath}`,
      );
    }
    if (Number.isInteger(event.e)) {
      replayMaxEventSeq = Math.max(replayMaxEventSeq, event.e);
    }

    switch (event.type) {
      case 'quota': {
        if (
          event.quota === null || event.quota === undefined ||
          (event.parent !== null && event.parent === undefined) ||
          !Number.isInteger(event.limit) || event.limit <= 0
        ) {
          throw new LogFileError(
            `corrupt quota entry on line ${lineNumber} of ${logPath}`,
          );
        }
        if (event.parent === event.quota) {
          throw new LogFileError(
            `quota entry parenting itself on line ${lineNumber} of ${logPath}`,
          );
        }
        if (event.parent !== null && !quotas.has(event.parent)) {
          throw new LogFileError(
            `quota entry with unknown parent on line ${lineNumber} of ${logPath}`,
          );
        }
        applyReplayQuota({ quota: event.quota, parent: event.parent, limit: event.limit });
        break;
      }
      case 'resource': {
        if (
          event.resource === null || event.resource === undefined ||
          !Number.isInteger(event.capacity) || event.capacity <= 0 ||
          (event.quota !== null && event.quota === undefined)
        ) {
          throw new LogFileError(
            `corrupt resource entry on line ${lineNumber} of ${logPath}`,
          );
        }
        if (event.quota !== null && !quotas.has(event.quota)) {
          throw new LogFileError(
            `resource entry with unknown quota on line ${lineNumber} of ${logPath}`,
          );
        }
        applyReplayResource({
          resource: event.resource,
          capacity: event.capacity,
          quota: event.quota ?? null,
        });
        break;
      }
      case 'acquire': {
        if (
          typeof event.token !== 'string' ||
          !Number.isFinite(event.ttlMs) ||
          !Number.isFinite(event.expiresAt) ||
          !Number.isInteger(event.epoch) ||
          event.epoch <= 0
        ) {
          throw new LogFileError(
            `corrupt acquire entry on line ${lineNumber} of ${logPath}`,
          );
        }
        // Replay applies every committed grant exactly once, so the logged
        // epoch must be exactly one past the resource's recovered epoch: a
        // gap, a repeat or a smaller value means the log can no longer
        // reconstruct a consistent leader fence and is corrupt.
        const expectedEpoch = (epochs.get(event.resource) ?? 0) + 1;
        if (event.epoch !== expectedEpoch) {
          throw new LogFileError(
            `non-monotonic lease epoch on line ${lineNumber} of ${logPath}`,
          );
        }
        // Credentials minted together for one atomic batch superseded any
        // expired, unswept lease on a single-occupancy resource as one unit:
        // only slots held by a different batch count as the stale lease being
        // retaken. Share resources keep every coexisting credential.
        const rs = resourceStateFor(event.resource);
        if (event.batch !== undefined && event.batch !== null) {
          const logged = /^batch-[^-]+-(\d+)$/.exec(String(event.batch));
          if (logged !== null) {
            replayMaxBatchSeq = Math.max(
              replayMaxBatchSeq,
              Number(logged[1]),
            );
          }
          if (event.batch !== replayBatchId) {
            replayBatchId = event.batch;
            replayBatchTokens = new Set();
          }
        } else {
          replayBatchId = null;
          replayBatchTokens = new Set();
        }
        if (rs.capacity === 1) {
          const occupantTokens = slots.get(event.resource);
          if (occupantTokens !== undefined) {
            for (const oldToken of [...occupantTokens]) {
              if (!replayBatchTokens.has(oldToken)) {
                const oldLease = leases.get(oldToken);
                // Structural supersede, exactly like an in-process
                // retake: vacate the slot and the table. The stale
                // heap node is dropped right away from every owner
                // heap, so the reconstructed table never observes a
                // share it never held, even before the batch's
                // reclaim event (if any) is replayed.
                if (oldLease !== undefined) {
                  const oldRs = resourceStates.get(oldLease.resource);
                  removeHeapNodes(oldRs.heap, oldToken);
                  for (const q of oldLease.chain) {
                    removeHeapNodes(q.heap, oldToken);
                  }
                }
                retirePermanent(oldLease);
                leases.delete(oldToken);
                removeSlot(event.resource, oldToken);
              }
            }
          }
        }
        const lease = {
          resource: event.resource,
          holder: event.holder,
          expiresAt: event.expiresAt,
          ttlMs: event.ttlMs,
          // Credentials issued before this process started are void: the
          // lease keeps occupying its shares, but its token cannot renew or
          // release them.
          legacy: true,
          heapSeq: 0,
          epoch: event.epoch,
          batch: event.batch ?? null,
          chain: rs.chain,
            };
        leases.set(event.token, lease);
        addSlot(event.resource, event.token);
        replayBatchTokens.add(event.token);
        knownTokens.add(event.token);
        epochs.set(event.resource, event.epoch);
        rs.live += 1;
        for (const quota of rs.chain) {
          quota.live += 1;
        }
        pushLeaseHeaps(event.token, lease);
        counters.granted += 1;
        break;
      }
      case 'renew': {
        if (
          typeof event.token !== 'string' ||
          !Number.isFinite(event.expiresAt)
        ) {
          throw new LogFileError(
            `corrupt renew entry on line ${lineNumber} of ${logPath}`,
          );
        }
        const lease = leases.get(event.token);
        if (lease !== undefined) {
          lease.expiresAt = event.expiresAt;
          lease.heapSeq += 1;
          pushLeaseHeaps(event.token, lease);
        }
        counters.renewed += 1;
        break;
      }
      case 'release': {
        if (typeof event.token !== 'string') {
          throw new LogFileError(
            `corrupt release entry on line ${lineNumber} of ${logPath}`,
          );
        }
        const lease = leases.get(event.token);
        if (lease !== undefined) {
          retirePermanent(lease);
          leases.delete(event.token);
          removeSlot(lease.resource, event.token);
        }
        counters.released += 1;
        break;
      }
      case 'reclaim': {
        if (!Array.isArray(event.items)) {
          throw new LogFileError(
            `corrupt reclaim entry on line ${lineNumber} of ${logPath}`,
          );
        }
        for (const item of event.items) {
          if (item === null || typeof item !== 'object' ||
              typeof item.token !== 'string') {
            throw new LogFileError(
              `corrupt reclaim entry on line ${lineNumber} of ${logPath}`,
            );
          }
          const token = item.token;
          const lease = leases.get(token);
          if (lease !== undefined) {
            retirePermanent(lease);
            leases.delete(token);
            removeSlot(lease.resource, token);
          }
          counters.reclaimed += 1;
        }
        break;
      }
      default: {
        throw new LogFileError(
          `unknown lease log event type on line ${lineNumber} of ${logPath}`,
        );
      }
    }
  }
}
