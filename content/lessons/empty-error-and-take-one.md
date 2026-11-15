---
slug: empty-error-and-take-one
title: "The EmptyError gotcha: first() vs take(1)"
topic: filtering-limiting
kind: pitfall
summary: "On an empty stream, first() errors with EmptyError unless given a defaultValue, whereas take(1) quietly completes without emitting or erroring."
tags: [rxjs, first, take, emptyerror, pitfall]
related: [first-and-last-operators, take-bounding-streams, first-value-from-and-last-value-from]
---
> **In one line:** when a source completes without emitting any value, **`first()` throws an `EmptyError`**, whereas **`take(1)` quietly completes with zero values**.

## The idea

At first glance, `first()` and `take(1)` seem identical: on a stream that emits `10, 20, 30`, both emit `10` and immediately complete.

The difference appears when the source **completes without emitting anything** (for example `EMPTY`, or a `filter` upstream that rejected every item):

| Operator on `EMPTY` | `next` | `error` | `complete` | Meaning |
|---|---|---|---|---|
| **`first()`** | None | **`EmptyError`** | Does not run | *"There **must** be a first value; empty is an error"* |
| **`first(undefined, fallback)`** | `fallback` | None | Runs | *"Emit the first value, or `fallback` if empty"* |
| **`take(1)`** | None | None | **Runs cleanly** | *"Take **up to** 1 value; 0 values is fine"* |

## See it

Compare `first()` and `take(1)` when subscribed to `EMPTY`:

```typescript
import { EMPTY, first, take } from 'rxjs';

// Before: first() on an empty stream triggers the error callback with EmptyError!
EMPTY.pipe(first()).subscribe({
  next: (v) => console.log('first next:', v),
  error: (err: Error) => console.log('first error:', err.name), // "EmptyError"
  complete: () => console.log('first complete'),
});

// After (Option 1): use take(1) when an empty stream should simply complete quietly
EMPTY.pipe(take(1)).subscribe({
  next: (v) => console.log('take(1) next:', v),
  error: (err: Error) => console.log('take(1) error:', err.name),
  complete: () => console.log('take(1) completed cleanly with 0 values'),
});

// After (Option 2): pass a defaultValue to first() so empty emits a fallback
EMPTY.pipe(first(undefined, 'fallback')).subscribe((v) => {
  console.log('first with default:', v); // "fallback"
});
```

## Go deeper

How do you choose between `take(1)` and `first()` in real code?

- Choose **`take(1)`** when "at most one emission, and zero is okay" matches your intent — for example, a one-shot click listener (`fromEvent(btn, 'click').pipe(take(1))`) or reading a single emission before teardown.
- Choose **`first()`** when an empty stream indicates a genuine bug or missing record that *should* enter your error handling path (or when you need `first(predicate, defaultValue)` to search for a matching item).

You can also inspect or catch `EmptyError` specifically by importing the `EmptyError` class from `'rxjs'` and checking `err instanceof EmptyError` inside `catchError` or your Observer's `error` handler when you want custom recovery logic for empty streams.

## Watch out

- The exact same `EmptyError` rule applies to `last()` and to the Promise helpers `firstValueFrom(obs$)` and `lastValueFrom(obs$)` when a stream completes without emitting.
- If you put `filter(pred)` before `first()`, and no item matches `pred` before the source completes, `first()` will throw `EmptyError`.

## Remember

- `first()` demands at least one value and throws `EmptyError` if the source completes empty.
- `take(1)` takes *up to* one value and completes quietly if the source is empty.
- Pass `first(pred, defaultValue)` when you want a fallback value instead of `EmptyError`.
