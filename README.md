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
- `Registry.acquire(resource, holder, ttlMs) -> { resource, holder, token, expiresAt }`.
  Passing an array of resources as the first argument is the atomic group
  form, identical to `acquireAll`.
- `Registry.acquireAll(resources, holder, ttlMs, waitMs)` (aliases
  `acquireGroup`, `acquireMany`) acquires several resources atomically; see
  "Atomic groups" below.
- `Registry.renew(token) -> boolean` extends a lease that is still live.
- `Registry.release(token) -> boolean` ends a lease early.
- `Registry.sweep(now) -> string[]` reclaims expired leases and names the resources it freed, ordered by expiry time.
- `Registry.holder(resource) -> string | null` reports the current holder.
- `Registry.stats() -> { granted, renewed, released, reclaimed, live }`.
- `Registry.cancel(waitId) -> boolean` withdraws a still queued request.
- `LeaseTakenError` exported class carrying a `code` property.
- `LogFileError` exported class carrying a `code` property (`'LOG_FILE_ERROR'`).

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

## Tests

    npm test

## Limits

Single process only; no distributed coordination.
Time comes from the injected clock and does not advance on its own; clock
readings going backwards are taken as given. Persistence is an append-only
file replayed by one process at a time, not shared storage across processes
or machines.
