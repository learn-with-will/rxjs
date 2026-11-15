---
slug: first-and-last-operators
title: "Single-value selection with first() and last()"
topic: filtering-limiting
kind: api
summary: "first(pred?, default?) emits the first matching value and completes; last(pred?, default?) waits for completion and emits the final matching value."
tags: [rxjs, first, last, emptyerror, filtering]
related: [empty-error-and-take-one, take-bounding-streams, take-last-and-skip]
---
> **In one line:** `first(predicate?, defaultValue?)` emits the first (matching) value and completes immediately, while `last(predicate?, defaultValue?)` waits for the source to complete and emits the final (matching) value — and both throw `EmptyError` if no value matched and no default was given.

## The idea

When you want **exactly one value** from a stream — optionally the first or last value that satisfies a predicate, with an optional fallback if none does — RxJS provides `first` and `last`:

| Call | What it emits | When it completes | If source completes with no match |
|---|---|---|---|
| **`first()`** | The very first value | Immediately on that first value | Errors with **`EmptyError`** |
| **`first(pred)`** | The first value where `pred(v)` is true | Immediately when a match arrives | Errors with **`EmptyError`** |
| **`first(pred, fallback)`** | The first matching value, or `fallback` | On match, or on source completion | Emits `fallback` and completes cleanly |
| **`last(pred?, fallback?)`** | The last (matching) value | Only **after** the source completes | Emits `fallback`, or errors with **`EmptyError`** |

## See it

```typescript
import { of, first, last } from 'rxjs';

// 1. first() with no arguments: emits 10 and completes immediately
of(10, 20, 30)
  .pipe(first())
  .subscribe((n) => console.log('first():', n)); // 10

// 2. first(predicate): emits the first value > 15 and completes
of(10, 20, 30)
  .pipe(first((n) => n > 15))
  .subscribe((n) => console.log('first > 15:', n)); // 20

// 3. first(predicate, defaultValue): falls back to -1 instead of EmptyError
of(1, 2, 3)
  .pipe(first((n) => n > 100, -1))
  .subscribe((n) => console.log('with fallback:', n)); // -1

// 4. last(): waits for completion and emits 30
of(10, 20, 30)
  .pipe(last())
  .subscribe((n) => console.log('last():', n)); // 30
```

## Go deeper

Think of `first` as an assertion: *"I expect this stream to produce at least one matching value; give me the first one, or raise an `EmptyError` (or use my defaultValue) if the stream finishes without one."* And if you want to pass a `defaultValue` without filtering by a predicate, pass `undefined` as the first argument: `first(undefined, 'default')`.

## Watch out

- Like `takeLast` and `reduce`, `last()` waits for the upstream source to **complete** — on an infinite stream like `interval`, `last()` will never emit.
- Calling `first()` or `last()` without a `defaultValue` on a stream that can complete empty will throw `EmptyError`; see the next bite for `first()` vs `take(1)`.

## Remember

- `first(pred?, default?)` emits the first matching value and completes right away.
- `last(pred?, default?)` waits for source completion and emits the last matching value.
- Pass a `defaultValue` second argument when you want a fallback value instead of an `EmptyError`.
