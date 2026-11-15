---
slug: operators-are-pure
title: "Operators are pure: sources are never mutated"
topic: pipes-transforming
kind: concept
summary: "Calling source$.pipe(...) never modifies source$; it returns a brand-new Observable, leaving the original source untouched and reusable across pipelines."
tags: [rxjs, pipe, purity, immutability, operators]
related: [what-is-a-pipeable-operator, chaining-with-pipe, tap-for-side-effects]
---
> **In one line:** `source$.pipe(...)` **never mutates `source$`** — it always returns a **new Observable**, so you must assign or subscribe to the return value and can safely reuse `source$` in multiple pipelines.

## The idea

Just as `array.map(fn)` returns a new array without changing the original array, calling `source$.pipe(map(fn))` returns a **new Observable** and leaves `source$` completely unchanged.

This purity has two practical consequences:

1. **You can branch multiple pipelines off one source.** You can define `nums$` once and derive `evens$` and `doubled$` from it without the two pipelines interfering with each other.
2. **Ignoring the return value of `.pipe()` is a bug.** Writing `nums$.pipe(map(n => n * 2));` on its own line and then subscribing to `nums$` will emit the original un-doubled numbers.

```flow
src: nums$ | of(1, 2, 3) @primary
b1: doubled$ | nums$.pipe(map(n => n * 2)) @blue
b2: tenfold$ | nums$.pipe(map(n => n * 10)) @green
src -> b1: 1, 2, 3 (untouched)
src -> b2: 1, 2, 3 (untouched)
```

## See it

Compare ignoring `.pipe()`'s return value with storing the new Observable it returns:

```typescript
import { of, map } from 'rxjs';

const nums$ = of(1, 2, 3);

// Before (bug): calling .pipe() without capturing the returned Observable
nums$.pipe(map((n) => n * 2));
nums$.subscribe((n) => console.log('nums$ (unchanged):', n)); // 1, 2, 3

// After: capture the new Observable returned by .pipe()
const doubled$ = nums$.pipe(map((n) => n * 2));
doubled$.subscribe((n) => console.log('doubled$:', n)); // 2, 4, 6
```

## Go deeper

When you subscribe to `doubled$`, it internally subscribes to `nums$`, transforms each value, and forwards the result to your subscriber. Because `nums$` is cold and untouched, subscribing to both `nums$` and `doubled$` gives each its own clean run. Keep the callbacks you pass into operators like `map` and `filter` **pure** as well — avoid mutating external variables or objects in place inside `map`.

```typescript
import { of, filter, map } from 'rxjs';

// Branching two independent streams from one shared definition:
const baseNumbers$ = of(1, 2, 3, 4);
const evens$ = baseNumbers$.pipe(filter((n) => n % 2 === 0));
const squares$ = baseNumbers$.pipe(map((n) => n * n));

evens$.subscribe((n) => console.log('even:', n));     // 2, 4
squares$.subscribe((n) => console.log('square:', n)); // 1, 4, 9, 16
```

Because neither `evens$` nor `squares$` alters `baseNumbers$`, you can export a base stream from a service module and let different UI components pipe their own projections and filters on top of it without stepping on each other.

## Watch out

- Calling `source$.pipe(op())` as a standalone statement does nothing; always assign `const out$ = source$.pipe(op())` or chain `.subscribe()` directly onto `.pipe()`.
- Mutating an emitted object inside `map((user) => { user.name = 'X'; return user; })` mutates the object reference for any other subscriber sharing that stream — return a new object `{ ...user, name: 'X' }` instead.

## Remember

- Pipeable operators are pure: they return a new Observable and never modify the source.
- You can safely derive multiple independent pipelines from the same source Observable.
- Always capture or subscribe to the Observable returned by `.pipe()`.
