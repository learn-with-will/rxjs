---
slug: filter-operator
title: "Selecting values with filter()"
topic: filtering-limiting
kind: api
summary: "filter(predicate) forwards only the values for which predicate returns true; it never completes the stream on its own."
tags: [rxjs, filter, filtering, operators, type-guard]
related: [take-bounding-streams, take-while-and-inclusive, which-filtering-operators-complete]
---
> **In one line:** `filter((value) => boolean)` lets matching values through and skips non-matching ones, **without ever completing the stream early**.

## The idea

Just like `Array.prototype.filter`, RxJS's `filter(predicate)` tests each incoming `next` value against your predicate function:

- If the predicate returns truthy, the value is forwarded downstream immediately.
- If the predicate returns falsy, the value is silently dropped.
- **Completion and errors pass straight through** from the source.

Crucially, `filter` **never stops or completes the stream** when a value fails the predicate — it simply ignores that one value and keeps listening for future values that might pass.

```flow
src: Source Stream | 1, 2, 3, 4, 5 @primary
f: filter(n => n % 2 === 0) | drops 1, 3, 5 @blue
out: Output Stream | 2, 4 @green
src -> f: 1, 2, 3, 4, 5
f -> out: 2, 4
```

## See it

In TypeScript, `filter` also supports **type guard predicates** (`(v): v is T => ...`), which narrow the stream's type for every operator below it — for example stripping `null` and `undefined` out of an `Observable<string | null>`:

```typescript
import { of, filter, map } from 'rxjs';

// 1. Basic numeric predicate:
of(1, 2, 3, 4, 5, 6)
  .pipe(filter((n) => n % 2 === 0))
  .subscribe((n) => console.log('even:', n)); // 2, 4, 6

// 2. TypeScript type-guard predicate narrows Observable<string | null> to Observable<string>:
of('Ada', null, 'Alan', null)
  .pipe(
    filter((name): name is string => name !== null),
    map((name) => name.toUpperCase()), // TypeScript knows `name` is string here!
  )
  .subscribe((name) => console.log('user:', name)); // ADA, ALAN
```

## Go deeper

Because `filter` never completes the stream on its own, piping `interval(1000).pipe(filter(n => n < 5))` will emit `0, 1, 2, 3, 4` and then stay **silently subscribed forever**, testing `5, 6, 7, …` every second in the background! When you want to *stop* the stream as soon as a condition turns false, use `takeWhile` (or `take`) instead of `filter`.

You can also pass `Boolean` as a shorthand predicate (`filter(Boolean)`) in JavaScript, or write a reusable non-null type guard helper `const isNonNull = <T>(v: T): v is NonNullable<T> => v != null;` and drop `filter(isNonNull)` into any pipeline where an upstream API or store might emit `null` or `undefined` before the real data arrives. Like `map`, `filter` also passes a zero-based emission `index` as the second argument to your predicate (`filter((value, index) => index % 2 === 0)`).

## Watch out

- Do not use `filter(n => n < 5)` on a monotonic counter like `interval` expecting the stream to complete after `4` — `filter` keeps the interval running forever; use `takeWhile(n => n < 5)` to complete.
- Write a TypeScript type guard `(x): x is NonNullable<typeof x> => x != null` when filtering out `null`/`undefined` so downstream operators get the narrowed type.

## Remember

- `filter(predicate)` forwards matching values and skips non-matching ones.
- `filter` never completes a stream early — it completes only when its source completes.
- Use a TypeScript type-guard predicate `(x): x is T => ...` to narrow the stream's type downstream.
