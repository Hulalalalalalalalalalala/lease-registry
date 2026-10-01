# lease-registry

In-process lease registry that hands out time bounded leases to named holders, renews them, and reclaims the ones whose holder stopped renewing, so a resource is never held forever by a process that went away. It also supports atomic multi-resource acquisition: one call either holds every listed resource or holds none.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createRegistry({ ttlMs, clock = Date.now, logPath = null }) -> Registry`.
- `Registry.acquire(resource, holder, ttlMs) -> { resource, holder, token, expiresAt, epoch }`.
  Passing an array of resources as the first argument is the atomic group
  form, identical to `acquireAll`.
- `Registry.acquireAll(resources, holder, ttlMs, waitMs)` (aliases
  `acquireGroup`, `acquireMany`) acquires several resources atomically; see
  "Atomic groups" below. Each entry may be a bare resource string
  (one share) or `{ resource, count }` asking for several shares of
  one resource in one atomic request; see "Shareable resources and
  hierarchical quotas" below.
- `Registry.renew(token) -> boolean` extends a lease that is still live.
- `Registry.release(token) -> boolean` ends a lease early.
- `Registry.sweep(now) -> string[]` reclaims expired leases and names the resources it freed, ordered by expiry time.
- `Registry.holder(resource) -> string | null` reports the current holder.
- `Registry.stats() -> { granted, renewed, released, reclaimed, live }`.
- `Registry.cancel(waitId) -> boolean` withdraws a still queued request.
- `Registry.compact(now) -> { seq, leases }` folds the whole history into a
  crash-safe snapshot and restarts the event log from that point.
- `Registry.declareResource(resource, capacity, quotaId = null)` registers a
  resource's share capacity before its resources are applied for; see
  "Shareable resources and hierarchical quotas" below.
- `Registry.setQuota(quotaId, limit, parentId)` registers a
  quota node or changes an existing node's limit; see below.
- `Registry.epoch(resource) -> number` returns the resource's last committed
  credential epoch (0 if it was never granted); see "Lease epochs and leader
  fencing" below.
- `Registry.assertLease(resource, token, epoch) -> true` validates a current
  credential against the leader fence, throwing `LeaseFencedError` otherwise;
  see below.
- `LeaseFencedError` exported class carrying the fixed `code`
  `'LEASE_FENCED'`, plus the `resource` field.
- `LeaseTakenError` exported class carrying a `code` property.
- `QuotaExceededError` exported class carrying a `code` property
  (`'QUOTA_EXCEEDED'`), plus `quota`, `holder`, `requested` and
  `available` fields.
- `LogFileError` exported class carrying a `code` property (`'LOG_FILE_ERROR'`).

### Atomic groups

`acquireAll(resources, holder, ttlMs, waitMs)` (also available as
`acquireGroup`, `acquireMany`, or by passing the resource array straight to
`acquire`) applies for several resources in one call. The call either holds
**all** listed resources or **none** of them.

On success it returns one lease per listed occurrence, in list order, each
with its own credential and its own absolute expiry:
`[{ resource, holder, token, expiresAt, epoch }, ...]`. `ttlMs` may be one number
applied to every occurrence or one ttl per listed resource. A resource may
be listed more than once; each occurrence is treated as a separate demand
and receives its own credential — the holder then owns that resource once
per occurrence, and the resource is only free again once every one of those
credentials is released or reclaimed.

If any listed resource is held or reserved by an earlier queued request and
no `waitMs` is given, the call throws `LeaseTakenError` (with its `code`)
and leaves no lease, no queue position and no counter movement anywhere.
With `waitMs` the whole request queues instead: it takes one place at the
tail of **each** listed resource's queue and returns
`{ status: 'waiting', resources, holder, waitId }`. Per-resource FIFO is
strict when single and group requests are mixed — a group never overtakes an
earlier request on any resource, and a later request never overtakes the
group. The request is woken only once every listed position reaches the head
of its queue and the corresponding resource is free; its ticket then reads
`{ status: 'granted', resources, holder, waitId, leases }` with the same
lease array a direct success returns, each lease expiring independently from
the wake instant. A single free (release or reclaim) wakes whole batches in
arrival order, cascading across resources in the same operation.

When the wait budget runs out the entire request gives up: every queue
position is removed, no resource is held, no grant counter moves and the
ticket reads `status: 'expired'`. `cancel(waitId)` aborts a still queued
request the same way and lands exactly once; repeats, unknown ids, and
already granted or expired requests report `false`. Timeouts and cancels
never count as grants. Every credential of one successful batch is appended
in a single log write (the wakeup grants ride along in the same write as the
release or reclaim that triggered them); if that write fails, the whole
operation rolls back to before the call and throws `LogFileError`.

### Wait queue

`acquire` accepts an optional fourth argument, `waitMs`; omitting it keeps
the behavior above exactly. When the resource is held and `waitMs` is given,
the call queues instead of throwing and returns a waiting result
`{ status: 'waiting', resource, holder, waitId }`; queueing itself moves no
counter and writes no log line. Waiters for one resource are served in FIFO
order: a lease freed by `release` or reclaimed by `sweep` goes straight to
the head of that resource's queue, and the woken ticket then reads
`{ status: 'granted', resource, holder, waitId, token, expiresAt, epoch }` with its
own fresh expiry. A waiter whose budget runs out — judged by the injected
clock — gives up: its ticket reads `status: 'expired'` and it is never
woken. `Registry.cancel(waitId) -> boolean` drops a still queued request and
reports `true`; already granted, expired, cancelled or unknown requests
report `false`. Timed-out, cancelled and failed requests never increment
`granted` and never change ownership, and a late acquire cannot jump ahead
of a live queue. Only the eventual grant is written to the log (in the same
write as the release or reclaim that triggered it), so queued requests do
not survive a restart.

When `logPath` points at a file, every grant, renewal, early release and
reclaim is appended to it as one JSON line. On startup the history is replayed
in order, so unexpired leases keep occupying their resources and the four
cumulative counters come back exactly as they were; replay itself adds no
counts. Credentials issued before the restart are all void: renewing or
releasing them returns `false`, though the leases they pointed at stay live
until they expire. A missing log file starts an empty registry; a corrupt
line or an unrecognized log version throws `LogFileError`. If appending an
event fails, the operation does not take effect and registry state is rolled
back to before the call. With `logPath = null` nothing is persisted.

### Snapshots and compaction

`compact(now)` writes one snapshot containing, for every lease still alive in
the table, its resource, holder, absolute expiry and ttl, together with the
four cumulative counters and the internal id high-water marks; queued wait
requests are deliberately not part of it. The snapshot goes to
`<logPath>.snapshot` and the log is then restarted with a boundary marker.
Both files are published through a temp file, fsync and atomic rename, so at
any failure point the previous snapshot and previous log stay complete and
usable, and the call throws `LogFileError` (assertable via its `code`). If a
process dies between the snapshot rename and the log rename, startup still
applies every event exactly once: folded events are recognized by their
sequence numbers and skipped.

On startup the registry loads the snapshot first and replays only the
incremental events written after it. Ownership, every credential's expiry and
all four counters come back item-for-item identical to a replay of the
uncompacted log; rebuilding adds no counts, and credentials issued before
compaction are void exactly like credentials issued before any restart.
Queued requests never survive: after recovery the wait queues are empty and
a grant can only be triggered by a request that queued afterwards.

A snapshot file whose version is unknown or whose content is corrupt throws
`LogFileError`; neither it nor the original log is modified, so removing the
bad image and reopening falls back to replaying the original log without
losing any lease. A snapshot with missing content (an empty image) starts a
brand-new log.

If an earlier append was interrupted mid-line, the unterminated fragment at
the tail is treated as if it had never happened: entries are bounded by
newlines, every complete line before the fragment stays in force, and the
fragment is trimmed so the next append begins a clean line.

### Wait timeout organization

Wait-request deadlines are kept in a deadline min-heap rather than scanned
linearly, so with many waiters queued at once timeout judgment and wake
planning do not degrade with the total number of waiting requests; the
observable behaviour is unchanged.

## Tests

    npm test



## Shareable resources and hierarchical quotas

By default every resource is exclusive and single-occupancy, exactly as
above. A resource can instead be registered as a shareable pool with a
finite capacity before any of its shares are applied for:

    registry.declareResource(resource, capacity)
    registry.declareResource(resource, capacity, quotaId = null)

`declareResource` is the only way to mark a resource as shareable;
its name and positive-integer capacity must be registered before the first
acquire on it. Undeclared resources keep the single-occupancy behavior
forever; an undeclared resource used by an acquire still behaves as the
exclusive resource even when it has never been declared before.

On a shareable resource an acquire asks for one or more of its shares.
Each share gets its own credential and its own absolute expiry; several
holders may occupy the resource at once, and a released or reclaimed share is
immediately grantable again. A holder taking several shares at once uses the
multi-share group form:

    registry.declareResource('pool', 4)
    const leases = registry.acquire('pool', 'a', 50, undefined, 2)

`acquireAll` entries can demand several shares by giving
`{ resource, count }` rows:

    registry.acquireAll([{ resource: 'pool', count: 3 }], 'h', 50)

Every demanded share succeeds as one atomic grant in a single log write: the
whole request either holds every share or none of them. A demand larger
than the resource's capacity throws `LeaseTakenError` carrying
`requested` and `available` and leaves nothing behind.

### Hierarchical quotas

Quota nodes form a forest:

    registry.setQuota('root', 6)
    registry.setQuota('east', 3, 'root')
    registry.setQuota('east-db', 1, 'east')
    registry.declareResource('zone/a', 10, 'east-db')

A quota bounds the total number of shares that may be live under the node
at once, counting every descendant share together no matter who holds
them. A parent must be registered before its child; the forest order
can be declared: `setQuota(id, limit)` or
`setQuota(id, limit, parentId)` to change the limit in place
(parent omitted keeps the existing parent). Every level is enforced independently.

A group request is constrained by every level at once: capacity
shortage on one resource throws `LeaseTakenError`; shortage on
any quota level throws `QuotaExceededError` (with assertable `code`,
`quota`, `requested` and `available`) and leaves not one share,
no counters move and no queue position is left. Capacity and quota
room are both observed lazily against the clock reading handed in: an expired but
unswept share is already gone, and moving the clock backwards
revives shares that were live once more, exactly as the reading given would.
Reclaiming shares reclaimed at share granularity per credential; renewing only the
sweep reclaims that share as the next grant. Freed shares
fund waiting requests at the head of that resource's queue, and a
share freed on a different resource under the same quota wakes them in strict
arrival order.

## Lease epochs and leader fencing

Every credential carries an **epoch**: a strictly increasing integer on its
resource, beginning at 1 for the resource's first successful grant and
increasing by exactly one per committed credential. A successful `acquire`
returns `epoch` on its result, and every credential of a successful
`acquireAll` array (and every granted wait ticket's lease) carries the epoch
it committed:

- first grant: 1;
- re-acquisition after a previous holder's `release` or after a `sweep`:
  the next number, including when the same holder takes the resource again;
- a grant that wakes a queued request: the next number;
- each demanded share of a shared resource and each repeated occurrence in
  an atomic group: its own consecutive epoch, all committed together.

Nothing else moves an epoch: `renew` only extends `expiresAt`; a failed
contention (`LeaseTakenError`), a quota refusal (`QuotaExceededError`),
queueing, wait timeout, `cancel`, a failed log write, and a rejected or
partially failed group never consume one. `acquireAll` advances the epochs of
all involved resources at once only when the whole group commits; a rejected
group leaves every epoch exactly where it was.

`Registry.epoch(resource) -> number` reads the last committed epoch: 0 for a
resource that was never granted, otherwise the high-water mark. The number
never rolls back after `release` or `sweep`, even while no lease on the
resource is currently live. On an exclusive resource the last committed epoch
is exactly the current leader's takeover epoch.

`Registry.assertLease(resource, token, epoch) -> true` is the leader fence:
it returns `true` only when `token` names a resource's currently live
credential and `epoch` is both that credential's epoch and the resource's
last committed epoch. It throws `LeaseFencedError`
(`code: 'LEASE_FENCED'`) for an unknown resource, an unknown token, an
expired or released credential, an older or newer epoch, another holder's
token, and for credentials voided by a restart (they keep occupying but can
no longer fence). Non-string `resource` or `token`, or a non-finite-integer
`epoch`, raises `TypeError`; `0` is a well-formed epoch and fences (it cannot
match a resource that was ever granted).

When `logPath` is set, each grant's epoch is written atomically with its
acquire event, and the snapshot records every resource's last committed
epoch, including resources with no live lease. Epochs recover identically
after restart and compaction without re-advancing; a log record whose epoch
is not exactly the next expected number, or a malformed snapshot epoch
table, is treated as corruption and throws `LogFileError`. Logs and snapshots
written before epochs existed are still accepted: the missing epochs are
reconstructed sequentially. With `logPath = null` everything stays purely
in-memory.
