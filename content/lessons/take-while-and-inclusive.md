---
slug: take-while-and-inclusive
title: "Conditional completion with takeWhile()"
topic: filtering-limiting
kind: api
summary: "takeWhile(predicate, inclusive?) emits while predicate holds and completes on the first false; pass inclusive: true to also emit the failing value."
tags: [rxjs, takewhile, completion, filtering]
related: [filter-operator, take-bounding-streams, which-filtering-operators-complete]
---
> **In one line:** `takeWhile(predicate, inclusive?)` forwards values as long as `predicate` returns `true` and **completes the stream** on the first `false` — optionally emitting that boundary value when `inclusive` is `true`.

## The idea

People often confuse `filter(predicate)` and `takeWhile(predicate)` because both take a boolean predicate. Their difference is whether the stream keeps listening after the first `false`:

- **`filter(pred)`** skips non-matching values and **keeps listening** for later values that might match.
- **`takeWhile(pred, inclusive?)`** treats the first `false` as a **stop sign**: the moment a value fails the predicate, `takeWhile` **completes the entire stream** and unsubscribes from the source.

`takeWhile` also takes an optional second boolean parameter, **`inclusive`** (default `false`):

| Call on `of(2, 4, 6, 7, 8)` | Emits | Why |
|---|---|---|
| `filter(n => n % 2 === 0)` | `2, 4, 6, 8` | Skips `7`, keeps listening, emits `8` |
| `takeWhile(n => n % 2 === 0)` | `2, 4, 6` | Stops and completes at `7`; `7` is **not** emitted |
| `takeWhile(n => n % 2 === 0, true)` | `2, 4, 6, 7` | Stops and completes at `7`, **including** the failing boundary value `7` |

## See it

```typescript
import { of, interval, map, takeWhile } from 'rxjs';

// 1. Default (inclusive = false): stops before the failing value (7)
of(2, 4, 6, 7, 8)
  .pipe(takeWhile((n) => n % 2 === 0))
  .subscribe((n) => console.log('exclusive:', n)); // 2, 4, 6

// 2. With inclusive = true: also emits the failing boundary value (7), then completes
of(2, 4, 6, 7, 8)
  .pipe(takeWhile((n) => n % 2 === 0, true))
  .subscribe((n) => console.log('inclusive:', n)); // 2, 4, 6, 7

// 3. Countdown timer that includes 0 as its final emitted value:
interval(200)
  .pipe(
    map((tick) => 3 - tick),          // 3, 2, 1, 0, -1...
    takeWhile((sec) => sec > 0, true), // emits 3, 2, 1, and the boundary 0, then completes!
  )
  .subscribe({
    next: (s) => console.log('countdown:', s),
    complete: () => console.log('liftoff!'),
  });
```

## Go deeper

Why is `inclusive: true` so handy? Imagine polling a job status endpoint `{ status: 'pending' | 'done' }` with `takeWhile(job => job.status !== 'done', true)`. Without `inclusive: true`, the stream would complete the moment `status === 'done'` arrived **without emitting that final `'done'` object** to your UI! Setting `inclusive: true` delivers the final `'done'` state to your subscriber and then immediately completes the polling stream.

## Watch out

- Once `takeWhile` sees a single `false`, the stream is permanently complete — later values like `8` in `2, 4, 6, 7, 8` are never evaluated.
- By default (`inclusive: false`), the value that caused the predicate to return `false` is dropped; pass `true` as the second argument when downstream needs that final boundary value.

## Remember

- `takeWhile(predicate)` emits while `predicate` is true and completes on the first false.
- Unlike `filter`, `takeWhile` unsubscribes from the source and ends the stream.
- Pass `inclusive: true` as the second argument to emit the boundary value that failed the test before completing.
