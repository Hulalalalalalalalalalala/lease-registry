# lease-registry

In-process lease registry that hands out time bounded leases to named holders, renews them, and reclaims the ones whose holder stopped renewing, so a resource is never held forever by a process that went away.

## Requirements

Node.js 20 or newer. No runtime dependencies.

## Install

    npm install

## Run

    node --input-type=module -e "import('./src/index.js').then(m => console.log(Object.keys(m)))"

## Public interface

`createRegistry({ ttlMs, clock = Date.now }) -> Registry`.
- `Registry.acquire(resource, holder, ttlMs) -> { resource, holder, token, expiresAt }`.
- `Registry.renew(token) -> boolean` extends a lease that is still live.
- `Registry.release(token) -> boolean` ends a lease early.
- `Registry.sweep(now) -> string[]` reclaims expired leases and names the resources it freed.
- `Registry.holder(resource) -> string | null` reports the current holder.
- `Registry.stats() -> { granted, renewed, released, reclaimed, live }`.
- `LeaseTakenError` exported class carrying a `code` property.

## Tests

    npm test

## Limits

Single process only; no distributed coordination.
Time comes from the injected clock and does not advance on its own.
No persistence across restarts.
