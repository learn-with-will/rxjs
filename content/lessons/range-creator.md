---
slug: range-creator
title: "Finite number sequences with range()"
topic: creating-observables
kind: api
summary: "range(start, count) synchronously emits count sequential integers starting at start, then completes; the second argument is a count, not an end value."
tags: [rxjs, range, numbers, creation]
related: [of-and-from, interval-and-timer, take-bounding-streams]
---
> **In one line:** `range(start, count)` synchronously emits `count` consecutive integers beginning at `start` and then completes — its second argument is how *many* numbers to emit, not where to stop.

## The idea

When you need a finite sequence of consecutive integers — for generating test items, retry indices, or page numbers — `range(start, count)` builds it without allocating an intermediate array.

Unlike Python's `range(start, stop)` or many loop helpers in other libraries, RxJS's `range` takes **`(start, count)`**:

- **`start`**: the first integer to emit.
- **`count`**: how many integers to emit in total.

All values are emitted **synchronously** when you subscribe, and the stream immediately **completes** after the `count`-th emission.

| Call | First value (`start`) | How many (`count`) | Values emitted before `complete` |
|---|---|---|---|
| `range(1, 5)` | `1` | `5` | `1, 2, 3, 4, 5` |
| `range(0, 4)` | `0` | `4` | `0, 1, 2, 3` |
| `range(5, 3)` | `5` | `3` | `5, 6, 7` |
| `range(10, 0)` | `10` | `0` | *(none — completes immediately)* |

## See it

```typescript
import { range } from 'rxjs';

// Emits 5 numbers starting at 1: 1, 2, 3, 4, 5
range(1, 5).subscribe((n) => console.log('range(1, 5):', n));

// Watch out: the second argument is COUNT (3 values), not the end value!
range(5, 3).subscribe({
  next: (n) => console.log('range(5, 3):', n), // 5, 6, 7
  complete: () => console.log('completed'),
});
```

## Go deeper

Because `range` is synchronous and cold, pairing it with a completing operator like `take` stops generation early. For example, `range(1, 1_000_000).pipe(take(3))` only emits `1, 2, 3` and stops immediately without looping through the remaining 999,997 numbers:

```typescript
import { range, filter, take } from 'rxjs';

range(1, 1000)
  .pipe(
    filter((n) => n % 7 === 0),
    take(3),
  )
  .subscribe((n) => console.log('multiple of 7:', n)); // 7, 14, 21
```

> **Note:** As with `of` and `from`, passing a scheduler as a third argument to `range` is deprecated in RxJS 7. When you need a range delivered asynchronously on a scheduler, use `scheduled(range(start, count), asyncScheduler)` instead.

You will also see `range` paired with flattening operators like `concatMap` or `mergeMap` to fetch paginated endpoints in order (for example, `range(1, totalPages).pipe(concatMap(page => fetchPage$(page)))`) or inside `zip(source$, range(1, maxRetries))` to count retry attempts with an exact upper bound. Because `range` computes each integer on demand during subscription rather than allocating an array up front like `Array.from({ length: count })`, its memory footprint stays constant regardless of `count`.

## Watch out

- Do not read `range(5, 3)` as "from 5 to 3" — it means "start at 5 and emit 3 numbers", which produces `5, 6, 7`.
- `range` emits synchronously on subscribe; if you want numbers spaced over time, use `interval` or `timer` (optionally piped through `take(count)`).

## Remember

- `range(start, count)` emits `count` sequential integers starting at `start`.
- The second parameter is the **count** of emissions, not the upper bound.
- It runs synchronously on subscribe and completes right after the last number.
