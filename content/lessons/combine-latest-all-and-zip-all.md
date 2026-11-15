---
slug: combine-latest-all-and-zip-all
title: "Combining inner streams: combineLatestAll() and zipAll()"
topic: flattening-higher-order
kind: api
summary: "When an outer Observable completes after emitting several inner Observables, combineLatestAll() and zipAll() join those inner streams with combineLatest or zip."
tags: [rxjs, combinelatestall, zipall, higher-order, combining]
related: [all-flattening-operators, combine-latest, zip-pair-by-index]
---
> **In one line:** `combineLatestAll()` and `zipAll()` wait for a **finite outer Observable to complete**, collect all the inner Observables it emitted, and then join them using `combineLatest` or `zip`.

## The idea

What if a higher-order Observable emits 3 inner Observables and then completes, and instead of interleaving their values into single items (like `mergeAll`), you want to **combine their values into tuples** using `combineLatest` or `zip`?

That is what **`combineLatestAll()`** and **`zipAll()`** do:

1. They collect every inner Observable emitted by the **outer** Observable.
2. **Crucial rule:** the **outer Observable must complete** first! Until the outer Observable completes, RxJS doesn't know how many inner Observables belong in the combined tuple `[v1, v2, ...]`.
3. Once the outer Observable completes, `combineLatestAll()` subscribes to all collected inner Observables using `combineLatest([...])` semantics, while `zipAll()` subscribes to all of them using `zip(...)` lockstep index semantics.

| Higher-order operator | Waits for outer `complete`? | How it joins the collected inner streams |
|---|---|---|
| **`mergeAll()`** | No (subscribes immediately) | Interleaves single values `T` as they arrive |
| **`combineLatestAll()`** | **Yes!** | Emits `[latest_0, latest_1, ...]` whenever any inner stream emits |
| **`zipAll()`** | **Yes!** | Pairs `[0th_0, 0th_1, ...]`, then `[1st_0, 1st_1, ...]` by index |

## See it

```typescript
import { of, map, interval, take, combineLatestAll, zipAll } from 'rxjs';

// Outer stream emits 2 inner Observables and IMMEDIATELY completes:
const higherOrder$ = of('A', 'B').pipe(
  map((letter) =>
    interval(100).pipe(
      take(2),
      map((i) => `${letter}${i}`), // "A0", "A1" and "B0", "B1"
    ),
  ),
);

// 1. combineLatestAll: waits for outer of('A', 'B') to complete, then runs combineLatest:
higherOrder$
  .pipe(combineLatestAll())
  .subscribe((tuple) => console.log('combineLatestAll:', tuple));
// -> ["A0", "B0"], ["A1", "B0"], ["A1", "B1"]

// 2. zipAll: waits for outer of('A', 'B') to complete, then pairs by index:
higherOrder$
  .pipe(zipAll())
  .subscribe((pair) => console.log('zipAll:', pair));
// -> ["A0", "B0"], ["A1", "B1"]
```

## Go deeper

Look closely at step 2 in "The idea": why did `higherOrder$` above use `of('A', 'B')` (which completes) or `interval(100).pipe(take(2))` as its **outer** stream?

If the **outer** stream is infinite (for example `fromEvent(button, 'click')` without `take(n)`), the outer stream **never completes** — so `combineLatestAll()` and `zipAll()` will **never subscribe to a single inner Observable and will emit nothing forever**! Always make sure the outer stream is finite before piping into `combineLatestAll()` or `zipAll()`.

## Watch out

- Never use `combineLatestAll()` or `zipAll()` on an infinite outer stream without bounding the outer stream first (e.g. `take(3)` *before* `combineLatestAll()`).
- In RxJS 7+, the old name `combineAll` is deprecated — always use the clearer name **`combineLatestAll`**.

## Remember

- `combineLatestAll()` and `zipAll()` turn an `Observable<Observable<T>>` into an `Observable<T[]>`.
- Both wait for the **outer** Observable to **complete** before subscribing to the collected inner Observables.
- Prefer `combineLatestAll()` over the deprecated `combineAll()` alias.
