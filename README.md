# lease-registry

In-process lease registry that hands out time bounded leases to named holders, renews them, and reclaims the ones whose holder stopped renewing, so a resource is never held forever by a process that went away.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createRegistry({ ttlMs, clock = Date.now, logPath }) -> Registry`.
- `Registry.acquire(resource, holder, ttlMs) -> { resource, holder, token, expiresAt }`.
- `Registry.renew(token) -> boolean` extends a lease that is still live.
- `Registry.release(token) -> boolean` ends a lease early.
- `Registry.sweep(now) -> string[]` reclaims expired leases and names the resources it freed, in expiry order.
- `Registry.holder(resource) -> string | null` reports the current holder.
- `Registry.stats() -> { granted, renewed, released, reclaimed, live }`.
- `LeaseTakenError` exported class carrying a `code` property.
- `LogFileError` exported class carrying a `code` property, thrown when the log cannot be parsed (corrupt line, unknown version) or written.

`logPath` is optional. When given, every grant, renew, release and reclaim is
appended to that file and the log is replayed on startup, so leases that have
not expired yet survive a restart with their counters intact. Credentials
minted before the restart are never honored again: renewing or releasing them
returns `false`. A missing log file starts a fresh registry.

## Tests

    npm test

## Limits

Single process only; no distributed coordination.
Time comes from the injected clock and does not advance on its own.
Persistence is opt-in via `logPath`; recovery rebuilds state and counters but invalidates credentials issued before the restart.
