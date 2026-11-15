---
slug: reduce-vs-scan
title: "scan() vs reduce()"
topic: pipes-transforming
kind: concept
summary: "scan() emits the running accumulator on every source value; reduce() accumulates silently and emits only the single final total when the source completes."
tags: [rxjs, scan, reduce, completion, accumulation]
related: [scan-running-accumulator, map-operator, immutable-accumulators-and-tap-pitfalls]
---
> **In one line:** `scan` emits the running total **on every step** (and works on infinite streams), whereas `reduce` stays silent until the source **completes** and then emits **only the final result**.

## The idea

`scan` and `reduce` take the exact same arguments — `(accumulator, seed)` — and run the exact same accumulation logic. The only difference is **when they emit**:

| Operator | When it emits | Emissions for `of(1, 2, 3, 4)` (seed `0`) | Works on infinite streams (`interval`, `fromEvent`)? |
|---|---|---|---|
| **`scan`** | On **every** source `next` notification | `1`, `3`, `6`, `10`, then `complete` | **Yes** — emits live updates forever |
| **`reduce`** | **Once**, only when the source **completes** | `10`, then `complete` | **No** — never emits if the source never completes! |

In marble terms:

```text
source:  --1--2--3--4--|
scan:    --1--3--6--10-|   (emits every step)
reduce:  -----------(10|)  (emits once on completion)
```

## See it

Run `scan` and `reduce` side by side on the same finite stream of events:

```typescript
import { of, scan, reduce } from 'rxjs';

const events$ = of('login', 'view', 'purchase');

// scan: emits the growing history array after EACH event
events$
  .pipe(scan((history: string[], e) => [...history, e], []))
  .subscribe((h) => console.log('scan step:', h));
// scan step: ['login']
// scan step: ['login', 'view']
// scan step: ['login', 'view', 'purchase']

// reduce: emits ONLY the final array once events$ completes
events$
  .pipe(reduce((history: string[], e) => [...history, e], []))
  .subscribe((h) => console.log('reduce final:', h));
// reduce final: ['login', 'view', 'purchase']
```

## Go deeper

In fact, `reduce` is just `scan` followed by `takeLast(1)` (with `defaultIfEmpty(seed)`). That explains the classic `reduce` trap: if you attach `reduce` to an open-ended stream like `fromEvent(button, 'click')` or `interval(1000)` without bounding it first (for example with `take(5)` or `takeUntil`), your subscriber will **never receive a single value**, because `reduce` is still waiting for the source to complete.

> **Tip:** If you want an array of all values emitted by a completing stream without writing `reduce((acc, x) => [...acc, x], [])` by hand, use RxJS's built-in **`toArray()`** operator.

## Watch out

- Using `reduce` (or `toArray`) on an infinite stream like `interval` or `fromEvent` produces zero emissions forever unless a completing operator like `take` sits above it.
- If a source completes without emitting any values and you passed a `seed`, `reduce` emits `seed` and completes, whereas `scan` completes without emitting `seed`.

## Remember

- Use `scan` for live, evolving state (counters, UI stores, running totals).
- Use `reduce` (or `toArray`) when you only care about the final summary of a finite, completing stream.
- `reduce` never emits on an infinite stream unless that stream is bounded to complete.
