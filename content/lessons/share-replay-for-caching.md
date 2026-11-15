---
slug: share-replay-for-caching
title: "Caching and replaying emissions with shareReplay()"
topic: subjects-multicasting
kind: api
summary: "shareReplay({ bufferSize: 1, refCount: true }) multicasts the source through a ReplaySubject so both concurrent and late subscribers receive the cached result."
tags: [rxjs, sharereplay, caching, http, multicasting]
related: [cold-vs-hot-observables, share-operator, share-replay-refcount-leak, replay-subject]
---
> **In one line:** `shareReplay({ bufferSize: 1, refCount: true })` multicasts a cold source through a `ReplaySubject` so **late subscribers immediately receive the last cached value(s)** without re-running the upstream source.

## The idea

Why isn't plain `share()` enough for caching an HTTP request or a shared configuration stream?

Suppose Component A subscribes to `config$` at `t = 0ms`, the HTTP request completes at `t = 100ms`, and Component B mounts and subscribes at `t = 500ms`:

- With plain **`share()`**, the internal `Subject` holds no history and reset when the HTTP call completed at 100ms — so Component B at 500ms fires a **second HTTP request**!
- With **`shareReplay(1)`**, the stream is multicast through a **`ReplaySubject(1)`**. Component A triggers the single HTTP request at 0ms, and when Component B subscribes at 500ms (even after the HTTP Observable has completed!), `shareReplay` **immediately replays the cached response** with zero network traffic.

| Scenario when Sub B joins late | `source$.pipe(share())` | `source$.pipe(shareReplay(1))` |
|---|---|---|
| Upstream is still open & emitted `X` earlier | Sub B misses `X`; waits for next emission | Sub B **immediately receives `X`** |
| Upstream already emitted `X` and **completed** | Sub B triggers a **2nd upstream execution**! | Sub B **immediately receives cached `X` + `complete`** |
| Upstream **errored** | Sub B triggers a **2nd upstream execution** | Sub B triggers a **2nd upstream execution** (errors are not cached) |

## See it

```typescript
import { of, delay, tap, shareReplay } from 'rxjs';

let networkCalls = 0;

const currentUser$ = of({ id: 1, name: 'Ada Lovelace' }).pipe(
  tap(() => console.log(`[Network call #${++networkCalls}]`)),
  delay(100),
  shareReplay({ bufferSize: 1, refCount: true }),
);

// Sub 1 and Sub 2 subscribe at t=0ms (share the in-flight request):
currentUser$.subscribe((u) => console.log('Sub 1:', u.name));
currentUser$.subscribe((u) => console.log('Sub 2:', u.name));

// Sub 3 subscribes at t=300ms (long after the request completed at 100ms):
setTimeout(() => {
  currentUser$.subscribe((u) => console.log('Sub 3 (late):', u.name));
}, 300);
// [Network call #1] logs ONLY ONCE! All three subscribers receive "Ada Lovelace".
```

## Go deeper

Look carefully at what happens when the upstream source **completes** vs when all subscribers **unsubscribe** before completion:

- Once the upstream HTTP Observable **completes**, its 1 buffered response sits in the internal `ReplaySubject(1)` and is replayed to any future subscriber (like Sub 3 at 300ms above) without re-fetching.
- If the upstream source is **still running** (for example a live WebSocket or polling `interval`) and all subscribers unsubscribe so the subscriber count drops to `0`, `refCount: true` tears down the upstream connection so it doesn't run forever in the background.

## Watch out

- `shareReplay` does **not** cache errors: if the upstream HTTP call fails with an error, a subsequent subscriber will retry the upstream source fresh.
- Always pass the explicit config object `shareReplay({ bufferSize: 1, refCount: true })` unless you intentionally want a permanent background subscription — see the next bite for why!

## Remember

- `shareReplay` multicasts through an internal `ReplaySubject(bufferSize)`.
- Unlike `share()`, late subscribers immediately receive the last `bufferSize` cached emissions.
- After a finite source (like an HTTP call) completes, `shareReplay(1)` serves the cached response to future subscribers.
