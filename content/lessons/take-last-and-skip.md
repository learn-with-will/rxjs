---
slug: take-last-and-skip
title: "Dropping and trailing values: skip() and takeLast()"
topic: filtering-limiting
kind: api
summary: "skip(count) ignores the first count emissions and forwards the rest; takeLast(count) waits for the source to complete and then emits the final count values."
tags: [rxjs, skip, takelast, filtering, completion]
related: [take-bounding-streams, first-and-last-operators, which-filtering-operators-complete]
---
> **In one line:** `skip(n)` drops the first `n` values as they arrive, while `takeLast(n)` buffers the last `n` values and emits them **only after the source completes**.

## The idea

While `take(n)` keeps the first `n` values, `skip` and `takeLast` handle the other ends of a stream:

- **`skip(count)`** discards the first `count` emissions from the source and forwards every emission after that as they arrive. It does not complete the stream early. (Its siblings `skipWhile(predicate)` and `skipUntil(notifier$)` drop leading values until a condition turns false or another Observable emits.)
- **`takeLast(count)`** remembers the most recent `count` values in a ring buffer while the source runs. **Only when the source completes** does `takeLast(count)` synchronously emit those buffered final values in order and complete.

| Operator | On `of('a', 'b', 'c', 'd')` | Emits when? | Works on infinite streams? |
|---|---|---|---|
| **`skip(2)`** | `'c'`, `'d'` | Immediately as each value after #2 arrives | **Yes** |
| **`takeLast(2)`** | `'c'`, `'d'` | **Only after** the source completes | **No** — never emits if the source never completes! |

## See it

```typescript
import { of, skip, skipWhile, takeLast } from 'rxjs';

// 1. skip(2): ignore the first 2 values, emit the rest live
of('a', 'b', 'c', 'd')
  .pipe(skip(2))
  .subscribe((v) => console.log('skip(2):', v)); // 'c', 'd'

// 2. skipWhile: ignore leading values while < 10, then emit everything after
of(2, 5, 12, 3, 15)
  .pipe(skipWhile((n) => n < 10))
  .subscribe((n) => console.log('skipWhile:', n)); // 12, 3, 15 (note 3 IS emitted!)

// 3. takeLast(2): waits for completion, then emits the final 2 values
of(10, 20, 30, 40)
  .pipe(takeLast(2))
  .subscribe((n) => console.log('takeLast(2):', n)); // 30, 40
```

## Go deeper

Why is `skip(1)` so common in real apps? When you subscribe to a `BehaviorSubject`, it immediately emits its current initial seed value, followed by future updates. If a listener only wants to react to **future changes** after subscribing (and ignore that initial seed emission), `behaviorSubject$.pipe(skip(1))` drops the initial synchronous emission cleanly.

Also notice in the `skipWhile` example above: once `12` fails `n < 10`, `skipWhile` opens the gate permanently and emits `12, 3, 15` — even though `3` is less than `10`.

## Watch out

- Never put `takeLast(n)` on an unbounded infinite stream like `interval(1000)` — because `interval` never completes, `takeLast` will wait forever and never emit a single value.
- `skipWhile` only skips *leading* values while the condition holds; once the condition returns `false` once, every subsequent value passes through without being tested again.

## Remember

- `skip(n)` drops the first `n` emissions and forwards the rest live; `skip(1)` is great for ignoring a `BehaviorSubject`'s initial value.
- `skipWhile(pred)` drops leading values until `pred` first returns `false`, then forwards everything.
- `takeLast(n)` emits the final `n` values only after the source completes.
