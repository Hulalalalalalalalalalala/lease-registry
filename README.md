# lease-registry

In-process lease registry that hands out time bounded leases to named holders, renews them, and reclaims the ones whose holder stopped renewing, so a resource is never held forever by a process that went away. It also supports atomic multi-resource acquisition: one call either holds every listed resource or holds none. A resource may instead declare a **capacity**, in which case several holders occupy it at once, one independent share per credential, and shares may be constrained by a parent/child **quota hierarchy**.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createRegistry({ ttlMs, clock = Date.now, logPath = null, capacities = null, quotas = null }) -> Registry`.
- `Registry.acquire(resource, holder, ttlMs) -> { resource, holder, token, expiresAt }`.
  Passing an array of resources as the first argument is the atomic group
  form, identical to `acquireAll`. Passing `{ resource, shares }` applies
  for several shares of a capacity resource in one atomic call and returns
  one credential per share.
- `Registry.acquireAll(resources, holder, ttlMs, waitMs)` (aliases
  `acquireGroup`, `acquireMany`) acquires several resources atomically; see
  "Atomic groups" below.
- `Registry.renew(token) -> boolean` extends a lease that is still live.
- `Registry.release(token) -> boolean` ends one share's lease early.
- `Registry.sweep(now) -> string[]` reclaims expired shares and names their
  resources, ordered by expiry time (one entry per reclaimed credential).
- `Registry.holder(resource) -> string | null` reports a current holder.
- `Registry.stats() -> { granted, renewed, released, reclaimed, live }`;
  `live` counts the currently valid credentials, one per share.
- `Registry.cancel(waitId) -> boolean` withdraws a still queued request.
- `Registry.compact(now) -> { seq, leases }` folds the whole history into a
  crash-safe snapshot and restarts the event log from that point.
- `Registry.declareResource(resource, capacity, quota = null)` and
  `Registry.declareQuota(node, limit, parent = null)` declare shapes at
  runtime (before a resource is first used); both persist to the log.
- `Registry.capacity(resource) -> number` and
  `Registry.quotaUsage(node) -> number` report the declared capacity and the
  shares currently charged to a quota level.
- `LeaseTakenError` exported class carrying a `code` property
  (`'LEASE_TAKEN'`). `QuotaExceededError` extends it with
  `code: 'QUOTA_EXCEEDED'` and a `quota` property naming the blocking level.
- `LogFileError` exported class carrying a `code` property (`'LOG_FILE_ERROR'`).

### Capacity resources and hierarchical quotas

A resource that never declares a capacity behaves exactly as before: one
occupant at a time. Capacities and quotas are given to `createRegistry` as a
plain object or `Map` (the shapes are fixed from the first grant on and may
also be declared at runtime):

```js
createRegistry({
  ttlMs: 1000,
  quotas: {
    root: 10,
    team: { limit: 4, parent: 'root' },
  },
  capacities: {
    // capacity, and the quota node the resource's shares charge to:
    seats: { capacity: 4, quota: 'team' },
    pool: 3,                       // capacity with no quota attachment
  },
})
```

`quotas` maps a node name to a positive limit, optionally with a `parent`.
Every share held on an attached resource counts against its own node **and
every ancestor**, and all levels are enforced together: a grant that fits the
resource capacity and the leaf quota but would overflow the root fails just
the same. `capacities` maps a resource to a positive capacity or to
`{ capacity, quota }`. Quota entries may appear in any order; parents are
wired in dependency order, and a cycle or an unknown parent throws `TypeError`.

Applying for shares uses the same entry points:

- `acquire({ resource: 'seats', shares: 2 }, holder, ttlMs?, waitMs?)` is an
  atomic application for two shares of `seats`. Taking N shares is exactly
  equivalent to listing that resource N times in one group: the call either
  holds all N or none, and on success returns N lease objects, **one
  credential per share**, each with its own token and its own absolute expiry.
  A plain `acquire(resource, ...)` still asks for exactly one share and keeps
  its single-lease return shape.
- Inside `acquireAll`, entries may likewise be `{ resource, shares }`.
  `ttlMs` is per listed entry, so every share minted for one entry shares that
  entry's ttl but still expires as its own credential.

Released or reclaimed shares become grantable again immediately. Expiry,
renewal and early release are per credential (per share): one share expiring
reclaims just that token and its share at every quota level, and renewing one
share never moves the expiry of the other shares of the resource, whoever
holds them. The four cumulative counters keep their exact meaning and count
credentials, which is now the same as counting shares; `stats().live` is the
number of currently valid shares.

A group application is constrained by capacity and quota together. If it
cannot hold every requested share, the no-`waitMs` call throws
(`LeaseTakenError` with `code 'LEASE_TAKEN'` for a capacity/queue conflict and
`QuotaExceededError` with `code 'QUOTA_EXCEEDED'` for a quota level), leaves no
credential, no queue position and no counter movement, and changes no existing
ownership; with a wait budget the whole request queues instead. Freed shares
still go to the queue heads first, single and group requests strictly in
arrival order even when mixed, and a share freed on one quota branch wakes a
waiter queued on another branch when that is what made its levels fit.

Declarations persist like everything else: runtime `declareResource` and
`declareQuota` calls append to the log, and compaction folds capacities, quota
definitions and per-level usage into the snapshot (the recorded usage is
cross-checked against the lease image). After a restart or a compaction the
share ownership, each credential's expiry, every quota level's occupancy and
the four counters come back item for item; queued requests do not survive.


### Atomic groups

`acquireAll(resources, holder, ttlMs, waitMs)` (also available as
`acquireGroup`, `acquireMany`, or by passing the resource array straight to
`acquire`) applies for several resources in one call. The call either holds
**all** listed resources or **none** of them.

On success it returns one lease per listed occurrence, in list order, each
with its own credential and its own absolute expiry:
`[{ resource, holder, token, expiresAt }, ...]`. `ttlMs` may be one number
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
`{ status: 'granted', resource, holder, waitId, token, expiresAt }` with its
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

### Share organization

Each resource's shares sit in their own expiry min-heap, so deciding which
shares are due only pops the heap head: acquiring on a saturated capacity
resource, observing `holder`/`stats`, reclaiming when just a few of many
shares expire, and planning the wake a free triggers all stay independent of
the total number of coexisting shares. Queued requests are indexed per quota
level, so freeing quota on one branch reaches the waiters on another branch
without scanning shares held elsewhere.

## Tests

    npm test

## Limits

Single process only; no distributed coordination.
Time comes from the injected clock and does not advance on its own; clock
readings going backwards are taken as given. Persistence is an append-only
file replayed by one process at a time, not shared storage across processes
or machines.
