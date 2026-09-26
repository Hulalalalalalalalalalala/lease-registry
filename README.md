# lease-registry

In-process lease registry that hands out time bounded leases to named holders, renews them, and reclaims the ones whose holder stopped renewing, so a resource is never held forever by a process that went away.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createRegistry({ ttlMs, clock = Date.now, logPath = null }) -> Registry`.
- `Registry.acquire(resource, holder, ttlMs, waitMs = undefined) -> { resource, holder, token, expiresAt }`. With a wait budget and a taken resource it queues instead of throwing; see "Waiting in line" below.
- `Registry.renew(token) -> boolean` extends a lease that is still live.
- `Registry.release(token) -> boolean` ends a lease early.
- `Registry.sweep(now) -> string[]` reclaims expired leases and names the resources it freed, ordered by expiry time.
- `Registry.holder(resource) -> string | null` reports the current holder.
- `Registry.stats() -> { granted, renewed, released, reclaimed, live }`.
- `Registry.poll(requestId) -> object | null` snapshots a queued request's current outcome.
- `Registry.cancel(requestId) -> boolean` withdraws a request that is still waiting.
- `LeaseTakenError` exported class carrying a `code` property.
- `LogFileError` exported class carrying a `code` property (`'LOG_FILE_ERROR'`).

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

## Waiting in line

`acquire(resource, holder, ttlMs, waitMs)` takes an optional wait budget (a
positive number of milliseconds, or `{ waitMs }`). When the resource is taken
and a budget is given, the call queues instead of throwing and returns a
waiting result `{ status: 'waiting', requestId, resource, holder,
waitDeadline }`. Queued requests are served first come first served: a
resource freed by an early release or an expiry reclaim — or found expired by
a later acquire — is handed to the head of its line, and the woken request's
result reads `status: 'granted'` with a fresh `token` and an `expiresAt` of
its own (`now + ttlMs` at the handoff moment). A request whose wait deadline
passes, judged by the injected clock, gives up (`status: 'timeout'`) and is
never woken. `poll(requestId)` returns a snapshot of the current outcome
(`waiting`, `granted`, `timeout` or `cancelled`), or `null` for an unknown
id. `cancel(requestId)` withdraws a still-waiting request and returns `true`;
it returns `false` for requests already granted, timed out or cancelled, and
for unknown ids.

Queuing itself never moves the counters: only a successful handoff counts as
a grant, while timing out, cancelling and failed acquisitions count nothing
and move nothing. A handoff is logged as an ordinary acquire event in the
same atomic append as the release or reclaim that freed the resource, so a
failed log write rolls the whole operation back and leaves the line
untouched. The wait line lives in memory only — queued requests do not
survive a restart, but a handoff that already happened replays like any other
grant.

## Tests

    npm test

## Limits

Single process only; no distributed coordination.
Time comes from the injected clock and does not advance on its own; clock
readings going backwards are taken as given. Persistence is an append-only
file replayed by one process at a time, not shared storage across processes
or machines.
