---
slug: share-replay-refcount-leak
title: "The shareReplay(1) refCount: false memory-leak trap"
topic: subjects-multicasting
kind: pitfall
summary: "Shorthand shareReplay(1) defaults to refCount: false, keeping infinite upstream sources subscribed forever even after all subscribers unsubscribe."
tags: [rxjs, sharereplay, refcount, memory-leaks, pitfall]
related: [share-replay-for-caching, share-operator, what-causes-rxjs-memory-leaks]
---
> **In one line:** shorthand `shareReplay(1)` defaults to **`refCount: false`**, which means an infinite upstream source (`interval`, `fromEvent`, WebSocket) **keeps running forever even after every subscriber has unsubscribed**!

## The idea

There are two ways to call `shareReplay`:

1. **Shorthand:** `shareReplay(1)` — which is equivalent to `shareReplay({ bufferSize: 1, refCount: false })`.
2. **Config object:** `shareReplay({ bufferSize: 1, refCount: true })`.

What does `refCount` control when the last subscriber unsubscribes (subscriber count drops `1 → 0`) while the upstream source has **not yet completed**?

| Call on an infinite source (`interval(1000)`) | When subscriber count drops `1 → 0` | When a new subscriber joins later |
|---|---|---|
| **`shareReplay(1)`** (`refCount: false`) | **Does NOT unsubscribe upstream!** The `interval` keeps ticking in the background forever! | Immediately replays the latest tick; upstream never stopped |
| **`shareReplay({ bufferSize: 1, refCount: true })`** | **Unsubscribes upstream** and clears the replay buffer | Starts a fresh upstream subscription |

If you put shorthand `shareReplay(1)` on an infinite stream like `interval(1000)`, `fromEvent`, or a WebSocket, and the component unmounts, the upstream timer or socket **never stops** — a classic production memory and CPU leak!

## See it

Compare shorthand `shareReplay(1)` leaking an `interval` after unsubscribe versus `shareReplay({ bufferSize: 1, refCount: true })` cleaning it up:

```typescript
import { interval, tap, shareReplay } from 'rxjs';

// Before (LEAK): refCount defaults to false — interval keeps ticking after unsubscribe!
const leaky$ = interval(200).pipe(
  tap((n) => console.log('leaky upstream tick:', n)),
  shareReplay(1),
);

// After (SAFE): refCount: true unsubscribes upstream when subscriber count hits 0
const safe$ = interval(200).pipe(
  tap((n) => console.log('safe upstream tick:', n)),
  shareReplay({ bufferSize: 1, refCount: true }),
);

const sub = safe$.subscribe((n) => console.log('Sub received:', n));
setTimeout(() => {
  sub.unsubscribe(); // Count drops 1 -> 0: safe$ stops the interval immediately!
}, 500);
```

## Go deeper

What if you want **both**:
1. Unsubscribe from an infinite upstream source if no one is listening (`refCount: true`), **and**
2. Keep the cached value across a completed finite HTTP source permanently?

In RxJS 7+, `share({ connector: () => new ReplaySubject(1), resetOnComplete: false, resetOnError: true, resetOnRefCountZero: true })` gives you explicit, granular control over each reset condition independently!

> **Tip:** Make `shareReplay({ bufferSize: 1, refCount: true })` your team's default lint rule, and only use `refCount: false` on finite, app-wide singleton caches (like app startup config) that you intentionally want cached for the entire browser session.

## Watch out

- Never use bare `shareReplay(1)` on a stream that does not complete on its own (`interval`, `timer(0, period)`, `fromEvent`, or a long-lived `Subject`).
- Remember that with `refCount: true`, when subscriber count drops to `0` on an **uncompleted** stream, the replay buffer is also reset so the next subscriber won't see a stale pre-disconnect value.

## Remember

- `shareReplay(1)` sets `refCount: false` by default — it never unsubscribes from an uncompleted upstream source when subscribers leave.
- Always pass `shareReplay({ bufferSize: 1, refCount: true })` on open-ended or component-scoped streams to prevent leaks.
- Reserve `refCount: false` for finite, app-lifetime singleton caches.
