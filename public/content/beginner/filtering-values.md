---
id: lesson-07
slug: filtering-values
title: "Filtering Values: filter, take, first, and friends"
level: beginner
order: 07
duration: 24
tags:
  - rxjs
  - filter
  - take
  - first
  - filtering
  - completion
---

# Learning Objectives

By the end of this lesson you will be able to:

- Keep only the values you want with `filter`.
- Limit a stream with `take`, `takeWhile`, and `takeLast`.
- Drop leading values with `skip`.
- Grab a single value with `first` and `last`, and handle the empty case.
- Identify which filtering operators **complete** the stream and which just pass values through.
- Use bounding operators to stop infinite streams safely.

# Why This Concept Exists

Streams often carry more than you need: every keystroke when you only want the first, every number when you only want the even ones, an infinite timer when you only want five ticks. Filtering operators let you **select** and, importantly, **stop** — and knowing which ones complete the stream is the key to preventing leaks on infinite sources.

# Concept Explanation

### `filter` — let through what matches

`filter(predicate)` emits only the values for which the predicate returns true. It does **not** stop the stream; non-matching values are simply skipped, and completion passes through from the source.

```ts
import { of, filter } from 'rxjs';

of(1, 2, 3, 4, 5, 6)
  .pipe(filter((n) => n % 2 === 0))
  .subscribe(console.log); // 2, 4, 6
```

### `take` — the first N, then complete

`take(count)` emits the first `count` values and then **completes** — unsubscribing from the source. This is the standard way to bound an infinite stream.

```ts
import { interval, take } from 'rxjs';

interval(1000).pipe(take(3)).subscribe(console.log);
// 0, 1, 2, then complete — the interval is torn down, no leak
```

### `takeWhile` — while a condition holds

`takeWhile(predicate, inclusive?)` emits values while the predicate is true; the moment it's false, the stream **completes**. Pass `inclusive: true` to also emit the value that failed the test.

```ts
import { of, takeWhile } from 'rxjs';

of(2, 4, 6, 7, 8).pipe(takeWhile((n) => n % 2 === 0)).subscribe(console.log);
// 2, 4, 6, then complete (stops at 7, which is not emitted)

of(2, 4, 6, 7, 8).pipe(takeWhile((n) => n % 2 === 0, true)).subscribe(console.log);
// 2, 4, 6, 7, then complete (7 IS emitted because inclusive is true)
```

### `takeLast` — the final N, after completion

`takeLast(count)` waits for the source to **complete**, then emits the last `count` values. It never emits on an infinite stream.

### `skip` — ignore the first N

`skip(count)` discards the first `count` values and emits the rest. (Siblings `skipWhile` and `skipUntil` skip based on a predicate or another Observable.)

```ts
import { of, skip } from 'rxjs';

of('a', 'b', 'c', 'd').pipe(skip(2)).subscribe(console.log); // 'c', 'd'
```

### `first` and `last` — a single value, with an empty-case gotcha

`first(predicate?, defaultValue?)` emits the first value (optionally the first matching the predicate) and completes. `last(...)` does the same for the last value on completion.

**The gotcha:** if the source completes without any (matching) value and you gave **no default**, both **error** with an `EmptyError`.

```ts
import { of, first, EMPTY } from 'rxjs';

of(10, 20, 30).pipe(first()).subscribe(console.log);            // 10
of(1, 2, 3).pipe(first((n) => n > 5, -1)).subscribe(console.log); // -1 (default)
EMPTY.pipe(first()).subscribe({ error: (e) => console.log(e.name) }); // 'EmptyError'
```

If an empty source is acceptable and you don't want an error, prefer `take(1)`: it emits the first value if there is one and simply completes if there isn't — no error.

### Which operators complete the stream?

- **Complete early on their own:** `take`, `takeWhile`, `first` (and `takeUntil`, next lessons).
- **Emit only after the source completes:** `takeLast`, `last`.
- **Just pass values through** (complete when the source does): `filter`, `skip`.

# Key Terminology

- **`filter`** — emits values matching a predicate; does not complete the stream.
- **`take(n)`** — emits the first `n` values then completes.
- **`takeWhile(pred, inclusive?)`** — emits while the predicate holds, then completes.
- **`takeLast(n)`** — emits the last `n` values after the source completes.
- **`skip(n)`** — discards the first `n` values.
- **`first` / `last`** — emit a single value; error with `EmptyError` if none and no default is given.
- **`EmptyError`** — the error thrown by `first`/`last` (and `single`) when there is no value to emit.

# Syntax Overview

```ts
import { of, interval, filter, take, takeWhile, takeLast, skip, first, last } from 'rxjs';

of(1, 2, 3, 4).pipe(filter((n) => n > 2));       // 3, 4
interval(100).pipe(take(3));                      // 0, 1, 2, |   (bounds infinity)
of(1, 3, 5, 6, 7).pipe(takeWhile((n) => n % 2)); // 1, 3, 5, |
of(1, 2, 3, 4).pipe(takeLast(2));                // 3, 4, |      (after complete)
of(1, 2, 3, 4).pipe(skip(1));                     // 2, 3, 4
of(1, 2, 3).pipe(first());                        // 1, |
of(1, 2, 3).pipe(last());                         // 3, |
of(1, 2, 3).pipe(first((n) => n > 10, -1));       // -1, |       (default avoids EmptyError)
```

# Real World Analogy

Filtering operators are the **doorstaff at a venue**. `filter` is the bouncer checking IDs — anyone under age is turned away, but the queue keeps moving. `take(50)` is the fire-code limit: admit the first fifty, then lock the doors (**complete**). `takeWhile(sober)` admits people until the first clearly-drunk guest, then closes. `skip(10)` waves the first ten VIPs straight through the side entrance and starts counting after them. `first(isVIP)` finds the first VIP and immediately shuts the door — and if no VIP ever shows before closing time, staff radio in a complaint (`EmptyError`) unless you told them a fallback plan.

# Examples

## Example 1 — Basic: filter then take

```ts
import { range, filter, take } from 'rxjs';

range(1, 100)
  .pipe(
    filter((n) => n % 7 === 0), // multiples of 7
    take(3),                    // just the first three
  )
  .subscribe(console.log); // 7, 14, 21, then complete
```

**Why this is good:** It composes selection (`filter`) with a hard limit (`take`). `take(3)` completes the stream as soon as it has three values, so the `range` stops early — efficient and leak-free.

## Example 2 — Real-world: react to the first click only

```ts
import { fromEvent, take } from 'rxjs';

const button = document.querySelector('#confirm')!;

fromEvent(button, 'click')
  .pipe(take(1)) // handle exactly one click, then auto-unsubscribe
  .subscribe(() => console.log('confirmed once; listener removed'));
```

**Why this is good:** A one-shot handler is a very common need. `take(1)` emits the first click and completes, which tears down the `fromEvent` listener automatically — no manual `unsubscribe`, no leak, and it never errors even if the button is never clicked.

## Example 3 — Best practice: bound an infinite stream with takeWhile

```ts
import { interval, takeWhile, map } from 'rxjs';

// Count up once per 100ms but stop once we pass 5 (inclusive keeps the 5).
interval(100)
  .pipe(
    map((n) => n + 1),                 // 1, 2, 3, ...
    takeWhile((n) => n <= 5, true),    // 1, 2, 3, 4, 5, then complete
  )
  .subscribe({ next: console.log, complete: () => console.log('done') });
// 1 2 3 4 5 done
```

**Why this is good:** It shows the idiomatic way to give an infinite source a natural stopping condition. `inclusive: true` keeps the boundary value (`5`), and completion means the interval is cleaned up automatically.

# Common Mistakes

- **Thinking `filter` stops the stream.** It only removes values; the source keeps running. **Fix:** use `take`/`takeWhile`/`takeUntil` to actually complete.
- **`first()`/`last()` erroring on empty streams.** With no value and no default they throw `EmptyError`. **Fix:** pass a default (`first(pred, fallback)`), or use `take(1)` if empty is acceptable.
- **Expecting `takeWhile` to emit the failing value.** By default it does not. **Fix:** pass `inclusive: true` to include the boundary value.
- **Using `takeLast`/`last` on infinite streams.** They only emit after completion, which never comes. **Fix:** ensure the source completes first.
- **Leaving infinite sources unbounded.** `interval`/`fromEvent` with no `take`/`takeUntil` leak. **Fix:** always give infinite streams a completion condition or unsubscribe.

# Best Practices

- Bound every infinite stream with a completing operator (`take`, `takeWhile`, `takeUntil`) or an explicit `unsubscribe`.
- Prefer **`take(1)`** over `first()` when an empty source should quietly complete rather than error.
- When you *do* want an error (or a default) for an empty stream, use `first`/`last` with an intentional `defaultValue`.
- Reach for `takeWhile(pred, true)` when the boundary value itself matters.
- Filter **early** in the pipe to reduce work for downstream operators.

# Summary

- **`filter`** keeps matching values and passes completion through; it does not stop the stream.
- **`take(n)`** emits the first `n` values then **completes** — the go-to bound for infinite streams.
- **`takeWhile(pred, inclusive?)`** emits while the predicate holds, then completes (optionally keeping the boundary value).
- **`takeLast(n)`** and **`last`** emit only after the source **completes**.
- **`skip(n)`** discards leading values; **`first`/`last`** grab a single value but **error with `EmptyError`** on empty streams unless given a default.
- Prefer **`take(1)`** for a safe one-shot; always bound infinite sources.

# Flash Cards

Q: Does `filter` complete the stream when a value fails its predicate?
A: No. `filter` only skips non-matching values; the stream keeps running and completes only when the source does. Use `take`/`takeWhile` to complete early.

Q: What does `take(3)` do to an `interval(1000)` source?
A: It emits 0, 1, 2 and then completes, which unsubscribes from the interval — a safe, leak-free way to bound an infinite stream.

Q: What happens if you use `first()` on an empty stream with no default?
A: It errors with an `EmptyError`. Provide a default (`first(pred, fallback)`) or use `take(1)`, which just completes without emitting or erroring.

Q: By default, does `takeWhile` emit the value that fails its predicate?
A: No. Pass `inclusive: true` (the second argument) to also emit the boundary value before completing.

Q: Which of these emit only after the source completes: take, takeLast, filter, last?
A: `takeLast` and `last` wait for completion. `take` completes early on its own, and `filter` just passes values through.

Q: When would you choose `take(1)` over `first()`?
A: When an empty source should quietly complete rather than throw — `take(1)` never errors, while `first()` throws `EmptyError` on an empty stream without a default.

# Exercises

### Easy

From `of(5, 12, 8, 130, 44, 3)`, use `filter` to keep values greater than or equal to 10, then `take(2)` to emit only the first two survivors. Log them and confirm the stream completes.

### Medium

Create an `interval(500)` that counts up, and use `takeWhile` to stop once the value exceeds 4 — first without `inclusive`, then with `inclusive: true`. Compare the two outputs in comments and explain why one includes the boundary value.

### Challenging

Given a possibly-empty source, demonstrate the difference between `first()` and `take(1)`: subscribe both to `EMPTY` and observe that `first()` triggers the `error` handler with an `EmptyError` while `take(1)` triggers only `complete`. Then show `first((n) => n > 100, -1)` returning the default `-1` on `of(1, 2, 3)`.

# Further Reading

- [RxJS — filter](https://rxjs.dev/api/index/function/filter)
- [RxJS — take](https://rxjs.dev/api/index/function/take) and [takeWhile](https://rxjs.dev/api/index/function/takeWhile)
- [RxJS — first](https://rxjs.dev/api/index/function/first) and [last](https://rxjs.dev/api/index/function/last)
- [RxJS — skip](https://rxjs.dev/api/index/function/skip) and [takeLast](https://rxjs.dev/api/index/function/takeLast)
