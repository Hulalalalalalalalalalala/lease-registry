# lease-registry

In-process lease registry that hands out time bounded leases to named holders, renews them, and reclaims the ones whose holder stopped renewing, so a resource is never held forever by a process that went away. One call can also atomically acquire several resources: the request either ends up holding every one of them or none of them.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createRegistry({ ttlMs, clock = Date.now, logPath = null }) -> Registry`.
- `Registry.acquire(resource, holder, ttlMs) -> { resource, holder, token, expiresAt }`.
- `Registry.acquireAll(resources, holder, ttlMs?, waitMs?) -> ...` acquires several resources atomically; passing an array as the first argument of `acquire` is the same call (`acquireGroup`, `acquireMany` and `acquireBatch` are aliases).
- `Registry.renew(token) -> boolean` extends a lease that is still live.
- `Registry.release(token) -> boolean` ends a lease early.
- `Registry.sweep(now) -> string[]` reclaims expired leases and names the resources it freed, ordered by expiry time.
- `Registry.holder(resource) -> string | null` reports the current holder.
- `Registry.stats() -> { granted, renewed, released, reclaimed, live }`.
- `Registry.cancel(waitId) -> boolean` drops a still queued request.
- `LeaseTakenError` exported class carrying a `code` property (`'LEASE_TAKEN'`).
- `LogFileError` exported class carrying a `code` property (`'LOG_FILE_ERROR'`).

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

### Atomic group acquisition

`acquireAll(resources, holder, ttlMs, waitMs)` applies to every listed
resource at once. `ttlMs` may be one positive finite number used for all of
them, an array with one such ttl per listed resource (its length must
match), or omitted to fall back to the registry default. `waitMs` is
optional and follows the same rules as the single-resource queue.

When every resource is free, the call returns an array ordered exactly like
the input, each entry `{ resource, holder, token, expiresAt }` with its own
fresh credential and its own independent expiry. Listing the same resource
several times acquires it that many times: one credential is issued per
occurrence, the resource stays held until every credential is released or
reclaimed, and the credentials renew and release independently. Each issued
credential increments `granted` once.

If any resource is held or already reserved by an earlier queued request,
the whole call fails as a unit:

- without a wait budget it throws `LeaseTakenError` (carrying `code` and
  the first blocking `resource`), leaves no lease on any resource and moves
  no counter;
- with a wait budget it queues one position per listed occurrence at the
  tail of each resource's queue and returns
  `{ status: 'waiting', resources, holder, waitId, requestId }`, holding
  nothing. The positions reserve even the resources that are currently free,
  so a later single-resource request cannot take them.

Per-resource queues stay strictly first come first served while single and
group requests are mixed: a group only wins a resource once every position
it holds there reaches the front, and groups are woken in their overall
arrival order. A group is granted only when **all** its resources are free
in the same freeing event; freeing a subset leaves it waiting and grants
nothing. When it is granted its ticket reads
`{ status: 'granted', resources, holder, waitId, requestId, leases, results }`
where `leases` (and its `results` alias) is the ordered list of
`{ resource, holder, token, expiresAt }`, and the whole batch rides in the
same log write as the release or reclaim that completed it.

A request whose wait budget runs out before it can complete abandons every
queue position: the ticket reads `status: 'expired'`, no resource changes
hands and no counter moves. `cancel(waitId)` does the same while the request
is still queued and lands exactly once; repeats and cancels of granted or
expired requests report `false`. Timed-out and cancelled groups unblock the
requests behind them on every resource they had queued on.

### Persistence

When `logPath` points at a file, every grant, renewal, early release and
reclaim is appended to it as one JSON line. A group grant writes one
`acquire` line per credential; credentials minted together share a `batch`
marker, and woken groups additionally carry their request id. The whole
batch is appended in one write, together with the release or reclaim that
triggered the wake, so the operation either lands completely or not at all.

On startup the history is replayed in order, so unexpired leases keep
occupying their resources and the four cumulative counters come back
exactly as they were; replay itself adds no counts. Credentials issued
before the restart are all void: renewing or releasing them returns
`false`, though the leases they pointed at stay live until they expire. A
missing log file starts an empty registry; a corrupt line or an
unrecognized log version throws `LogFileError`. If appending an event
fails, the operation does not take effect and registry state is rolled
back to before the call, for single grants and whole groups alike. With
`logPath = null` nothing is persisted. Queued requests live in memory
only and never survive a restart.

## Tests

    npm test

## Limits

Single process only; no distributed coordination.
Time comes from the injected clock and does not advance on its own; clock
readings going backwards are taken as given. Persistence is an append-only
file replayed by one process at a time, not shared storage across processes
or machines.
