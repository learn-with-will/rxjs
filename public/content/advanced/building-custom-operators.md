---
id: lesson-19
slug: building-custom-operators
title: "Building Custom Operators"
level: advanced
order: 19
duration: 28
tags:
  - rxjs
  - custom-operators
  - operatorfunction
  - pipe
  - composition
---

# Learning Objectives

By the end of this lesson you will be able to:

- Define an operator as a function `(source: Observable<T>) => Observable<R>`.
- Build custom operators by **composing** existing ones with `pipe()`.
- Parameterise operators and type them with `OperatorFunction` / `MonoTypeOperatorFunction`.
- Write a **low-level** operator with the `Observable` constructor when composition isn't enough.
- Correctly forward `next`/`error`/`complete` and return teardown.
- Choose composition over hand-rolling wherever possible.

# Why This Concept Exists

You'll repeat the same operator sequences across a codebase — "trim, drop short strings, debounce, distinct" for every search box; "retry with backoff, then fall back" for every request. Wrapping those sequences in a **named custom operator** makes pipelines readable, testable, and DRY. And because an operator is *just a function*, building one is far simpler than it sounds.

# Concept Explanation

### An operator is a function

A pipeable operator has the type `OperatorFunction<T, R>` — a function that takes a source `Observable<T>` and returns an `Observable<R>`. Anything matching that shape works inside `.pipe()`.

```ts
import { Observable, OperatorFunction } from 'rxjs';

// The literal shape of every pipeable operator:
type MyOperator = <T, R>(source: Observable<T>) => Observable<R>;
```

`MonoTypeOperatorFunction<T>` is the special case where input and output types are the same (`T → T`).

### The easy way: compose with `pipe`

The standalone `pipe()` composes existing operators into one. Most custom operators need nothing more:

```ts
import { pipe, map, filter, OperatorFunction } from 'rxjs';

// Parameterised, fully typed, reusable.
const multiplyThenKeepBig = (factor: number, min: number): OperatorFunction<number, number> =>
  pipe(
    map((n) => n * factor),
    filter((n) => n >= min),
  );

// Usage — reads like a built-in operator:
source$.pipe(multiplyThenKeepBig(3, 10));
```

A parameterless operator can even be a plain reference to another operator:

```ts
const doubled = (): OperatorFunction<number, number> => map((n) => n * 2);
```

### The low-level way: the `Observable` constructor

When you need behaviour no combination of operators provides, return a function that builds a new `Observable`, subscribes to the source, and **forwards every notification** — plus a teardown.

```ts
import { Observable, MonoTypeOperatorFunction } from 'rxjs';

function log<T>(label: string): MonoTypeOperatorFunction<T> {
  return (source) =>
    new Observable<T>((subscriber) => {
      const subscription = source.subscribe({
        next: (value) => {
          console.log(label, value);
          subscriber.next(value);   // forward
        },
        error: (err) => subscriber.error(err),     // forward
        complete: () => subscriber.complete(),     // forward
      });
      return () => subscription.unsubscribe();      // teardown
    });
}
```

You **must** forward `error` and `complete` (or the stream hangs or leaks) and return teardown that unsubscribes from the source. This is exactly what built-in operators do internally — but you rarely need to write it, because composition covers most cases.

### Prefer composition

Reach for the `Observable` constructor only when you truly need custom subscription logic. Composition with `pipe` is simpler, less error-prone, and automatically handles teardown and the contract.

# Key Terminology

- **`OperatorFunction<T, R>`** — the type of a pipeable operator: `(source: Observable<T>) => Observable<R>`.
- **`MonoTypeOperatorFunction<T>`** — an operator whose input and output type are the same.
- **Composition** — building an operator by combining existing ones via `pipe`.
- **Forwarding** — passing the source's `next`/`error`/`complete` to the output subscriber.
- **Teardown** — the cleanup returned from the `Observable` constructor to unsubscribe from the source.

# Syntax Overview

```ts
import { pipe, map, filter, debounceTime, distinctUntilChanged, Observable,
         OperatorFunction, MonoTypeOperatorFunction } from 'rxjs';

// Composition (preferred):
const cleanQuery = (ms: number): OperatorFunction<string, string> =>
  pipe(
    map((s) => s.trim()),
    filter((s) => s.length >= 2),
    debounceTime(ms),
    distinctUntilChanged(),
  );

// Low-level (only when needed):
const identityLike = <T>(): MonoTypeOperatorFunction<T> =>
  (source) => new Observable<T>((sub) => source.subscribe(sub));

source$.pipe(cleanQuery(300));
```

# Real World Analogy

A custom operator is a **kitchen prep station you build once**. "Mise en place for salads" bundles wash → dry → chop → portion into a single labelled step every recipe can call. Composing with `pipe` is assembling that station from tools you already own (the knife, the spinner) — quick and safe. Reaching for the `Observable` constructor is *forging a brand-new tool* from raw metal: sometimes necessary, but you must handle every edge yourself (forward the food along the line, and clean the station when service ends), so you only do it when no existing tool will do.

# Examples

## Example 1 — Basic: a parameterised composed operator

```ts
import { of, pipe, map, filter, OperatorFunction } from 'rxjs';

const scaleAbove = (factor: number, min: number): OperatorFunction<number, number> =>
  pipe(
    map((n) => n * factor),
    filter((n) => n >= min),
  );

of(1, 2, 3, 4, 5).pipe(scaleAbove(2, 6)).subscribe(console.log);
// 6, 8, 10   (1->2 and 2->4 are dropped; 3->6, 4->8, 5->10 pass)
```

**Why this is good:** It packages a common two-step transform into one named, typed, reusable operator using only `pipe` — no low-level code, full type inference, and it reads like a built-in.

## Example 2 — Real-world: a reusable search-input operator

```ts
import { pipe, map, filter, debounceTime, distinctUntilChanged, OperatorFunction } from 'rxjs';

// One operator that turns raw input strings into clean, settled queries.
const toSearchQuery = (debounce = 300): OperatorFunction<string, string> =>
  pipe(
    map((raw) => raw.trim()),
    filter((text) => text.length >= 2),
    debounceTime(debounce),
    distinctUntilChanged(),
  );

// Every search box in the app reuses it:
inputText$.pipe(toSearchQuery()).subscribe((query) => console.log('search:', query));
```

**Why this is good:** The four-operator "clean a search query" recipe appears in many places; wrapping it once removes duplication and gives every search box identical, tested behaviour. Changing the policy (say, min length) is now a one-line edit in one place.

## Example 3 — Best practice: a correct low-level operator

```ts
import { Observable, MonoTypeOperatorFunction } from 'rxjs';

// Emit each value, but also invoke a callback — a minimal, correct `tap`.
function onEach<T>(fn: (value: T) => void): MonoTypeOperatorFunction<T> {
  return (source) =>
    new Observable<T>((subscriber) => {
      const subscription = source.subscribe({
        next: (value) => {
          try {
            fn(value);
          } catch (err) {
            subscriber.error(err); // surface callback errors into the stream
            return;
          }
          subscriber.next(value);
        },
        error: (err) => subscriber.error(err), // forward
        complete: () => subscriber.complete(), // forward
      });
      return () => subscription.unsubscribe(); // teardown
    });
}
```

**Why this is good:** It demonstrates the three obligations of a hand-written operator: **forward** all notifications, return **teardown**, and guard the user callback so its errors flow through the stream instead of crashing. This is precisely why composition is preferred — the constructor form has more to get right.

# Common Mistakes

- **Forgetting to forward `error`/`complete`.** The stream then hangs (no completion) or swallows errors. **Fix:** forward all three notifications.
- **Omitting teardown.** The source subscription leaks when the consumer unsubscribes. **Fix:** return `() => subscription.unsubscribe()`.
- **Hand-rolling what `pipe` can do.** More code, more bugs. **Fix:** compose existing operators; only use the constructor when necessary.
- **Losing types.** Untyped operators break inference downstream. **Fix:** annotate with `OperatorFunction<T, R>` / `MonoTypeOperatorFunction<T>` and use generics.
- **Letting a user callback throw unguarded.** An exception in your callback can bypass the contract. **Fix:** wrap it and route errors via `subscriber.error`.

# Best Practices

- **Compose first:** build custom operators from existing ones with `pipe`; it's simpler and handles the contract for you.
- **Type everything** with `OperatorFunction`/`MonoTypeOperatorFunction` and generics for reuse.
- Make operators **parameterised** functions returning the operator, so call sites read naturally.
- In low-level operators, always **forward** `next`/`error`/`complete` and **return teardown**.
- Keep each operator **focused and pure**; name it for what it does at the call site.

# Summary

- An operator is just a function `(source: Observable<T>) => Observable<R>` — the type `OperatorFunction<T, R>`.
- The easy, preferred way to build one is **composing existing operators with `pipe`**.
- Parameterise operators as functions that return an `OperatorFunction`, and type them for reuse.
- For custom subscription logic, use the **`Observable` constructor**, **forwarding** all notifications and returning **teardown**.
- Guard user callbacks so their errors flow through `subscriber.error`.
- Prefer composition; drop to the constructor only when no combination of operators suffices.

# Flash Cards

Q: What is the type/shape of a pipeable operator?
A: `OperatorFunction<T, R>` — a function that takes a source `Observable<T>` and returns an `Observable<R>`. `MonoTypeOperatorFunction<T>` is the T→T case.

Q: What is the simplest way to build a custom operator?
A: Compose existing operators with the standalone `pipe()`, e.g. `const op = () => pipe(map(...), filter(...))`. It handles teardown and the contract automatically.

Q: When writing a low-level operator with `new Observable`, what three things must you do?
A: Forward next/error/complete to the output subscriber, return a teardown that unsubscribes from the source, and guard your callbacks so errors route through subscriber.error.

Q: Why prefer composition over the Observable constructor for custom operators?
A: Composition is simpler and less error-prone — it automatically forwards notifications and manages teardown, whereas the constructor form makes you handle all of that manually.

Q: How do you make a custom operator configurable, like `debounceTime(300)`?
A: Write a function that takes parameters and returns the operator, e.g. `const toQuery = (ms: number): OperatorFunction<string,string> => pipe(...)`.

Q: What happens if a hand-written operator forgets to forward `complete`?
A: Downstream never completes — the subscription hangs open (and may leak), because the completion notification never reaches the output subscriber.

# Exercises

### Easy

Write a parameterised operator `addTax(rate: number): OperatorFunction<number, number>` using `pipe` and `map` that multiplies each price by `(1 + rate)`. Apply it to `of(100, 200)` with a rate of `0.2` and log the results.

### Medium

Build a reusable `evenSquares(): OperatorFunction<number, number>` that filters to even numbers and squares them, fully typed. Apply it to `range(1, 10)` and confirm the output. Then extract a second operator `topN(n)` using `take` and compose both.

### Challenging

Implement a low-level `withIndex<T>(): OperatorFunction<T, { index: number; value: T }>` using the `Observable` constructor that pairs each value with its zero-based index. Forward errors and completion, return teardown, and verify it works on `of('a', 'b', 'c')`. Then note in a comment how you could achieve the same with `map` + `scan` via composition.

# Further Reading

- [RxJS — Operators (creating custom operators)](https://rxjs.dev/guide/operators#creating-custom-operators)
- [RxJS — OperatorFunction](https://rxjs.dev/api/index/interface/OperatorFunction)
- [RxJS — pipe](https://rxjs.dev/api/index/function/pipe)
- [RxJS — Observable](https://rxjs.dev/guide/observable)
