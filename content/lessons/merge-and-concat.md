---
slug: merge-and-concat
title: "Interleaving vs sequencing: merge() and concat()"
topic: combining-joining
kind: api
summary: "merge(a$, b$) subscribes to all inputs at once and interleaves emissions; concat(a$, b$) subscribes one at a time, waiting for each stream to complete."
tags: [rxjs, merge, concat, joining, combining]
related: [zip-pair-by-index, fork-join, joining-stream-pitfalls, pipeable-with-operators]
---
> **In one line:** `merge(a$, b$)` subscribes to all sources **simultaneously** and emits values as they arrive, while `concat(a$, b$)` subscribes to sources **one after another**, waiting for `a$` to complete before subscribing to `b$`.

## The idea

Unlike `combineLatest` (which pairs values from multiple streams into tuples), `merge` and `concat` blend values from multiple streams into a **single stream of single values** — like merging lanes of traffic:

- **`merge(a$, b$, ...)` (OR-like interleaving):** subscribes to every input Observable right away. Whenever *any* input emits a value, `merge` forwards that value immediately. It completes only when **all** inputs have completed.
- **`concat(a$, b$, ...)` (strict sequential handoff):** subscribes **only to `a$`** at first. Every value from `a$` is forwarded. Only when `a$` **completes** does `concat` subscribe to `b$`, and so on. It completes when the **last** input completes.

| Operator | Subscribes to `b$` when? | Output order | If `a$` never completes |
|---|---|---|---|
| **`merge(a$, b$)`** | Immediately at `subscribe()` | Interleaved by arrival time | `b$`'s values still emit freely |
| **`concat(a$, b$)`** | **Only after `a$` completes** | All of `a$`, then all of `b$` | **`b$` is never subscribed to!** |

## See it

```typescript
import { timer, take, map, merge, concat } from 'rxjs';

const fast$ = timer(0, 100).pipe(
  take(2),
  map((i) => `fast-${i}`), // "fast-0" at 0ms, "fast-1" at 100ms, then completes
);
const slow$ = timer(50, 100).pipe(
  take(2),
  map((i) => `slow-${i}`), // "slow-0" at 50ms, "slow-1" at 150ms, then completes
);

// 1. merge(): both run at the same time -> interleaved by timestamp
merge(fast$, slow$).subscribe((v) => console.log('merge:', v));
// -> fast-0 (0ms), slow-0 (50ms), fast-1 (100ms), slow-1 (150ms)

// 2. concat(): slow$ doesn't even start until fast$ completes at 100ms!
concat(fast$, slow$).subscribe((v) => console.log('concat:', v));
// -> fast-0 (0ms), fast-1 (100ms), slow-0 (150ms), slow-1 (250ms)
```

## Go deeper

Because `concat(a$, b$)` waits to subscribe to `b$` until `a$` completes, `b$` **must be cold** if you expect `b$`'s work to start after `a$` finishes (which is true for all standard RxJS creation functions like `of`, `defer`, `ajax`, and `fromFetch`). It also makes `concat(of(initialValue), liveUpdates$)` equivalent to `liveUpdates$.pipe(startWith(initialValue))`.

Meanwhile, `merge` is the go-to operator whenever multiple independent event sources should drive the same logic — for example `merge(fromEvent(saveBtn, 'click'), ctrlSKeydown$)`.

## Watch out

- Do not pass an array to `merge` or `concat`: write `merge(a$, b$)` and `concat(a$, b$)` with rest arguments (unlike `combineLatest([a$, b$])` and `forkJoin([a$, b$])`, which take arrays!).
- Never put an infinite stream like `interval(1000)` or `fromEvent(...)` as the **first** argument to `concat(infinite$, next$)` — `next$` will never be reached.

## Remember

- `merge(a$, b$)` runs all streams concurrently and interleaves their emissions in real time.
- `concat(a$, b$)` runs streams one at a time in sequence, subscribing to `b$` only after `a$` completes.
- Both take **rest arguments** `merge(a$, b$)`, not an array.
