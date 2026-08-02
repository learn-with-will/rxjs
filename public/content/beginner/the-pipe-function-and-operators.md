---
id: lesson-05
slug: the-pipe-function-and-operators
title: "The pipe() Function and Pipeable Operators"
level: beginner
order: 05
duration: 20
tags:
  - rxjs
  - pipe
  - operators
  - composition
  - pipeable
---

# Learning Objectives

By the end of this lesson you will be able to:

- Define a **pipeable operator** as a function from Observable to Observable.
- Chain operators with `.pipe()` and read the flow left to right.
- Explain why operators are **pure** and never mutate the source Observable.
- Import operators correctly in RxJS 7+.
- See why operator **order** changes the result.
- Compose a reusable pipeline with the standalone `pipe()` function.

# Why This Concept Exists

Creating a stream is only half the story; the power of RxJS is **transforming** it. Operators do that — but the way you attach them matters. Early RxJS patched operators onto the Observable prototype (`source.map(...).filter(...)`), which bloated bundles and caused subtle import bugs. Modern RxJS uses **pipeable operators** composed through `.pipe()`: explicit, tree-shakeable, and easy to reason about. Understanding `pipe` is the gateway to every operator in the rest of this course.

# Concept Explanation

### An operator is a function that returns a function

There are two families of operators:

- **Creation operators** — standalone functions that make a new Observable (`of`, `from`, `interval` — the previous lessons).
- **Pipeable operators** — functions like `map(fn)` that, when **called**, return another function. That returned function takes a source Observable and returns a new, transformed Observable.

```ts
import { map } from 'rxjs';

const double = map((x: number) => x * 2);
// `double` is an OperatorFunction<number, number>:
//   (source: Observable<number>) => Observable<number>
```

### `.pipe()` chains them left to right

Every Observable has a `.pipe()` method. You pass operators as arguments and they apply **in order**, each receiving the output of the one before:

```ts
import { of, filter, map } from 'rxjs';

of(1, 2, 3, 4, 5)
  .pipe(
    filter((n) => n % 2 === 0), // keep 2, 4
    map((n) => n * 10),         // -> 20, 40
  )
  .subscribe(console.log);      // 20, 40
```

`source.pipe(a, b)` is exactly equivalent to `b(a(source))` — but far more readable top-to-bottom. Think of it as: values flow **down** through each stage.

### Operators are pure — the source is never mutated

A pipeable operator does not change its source Observable; it **returns a new one**. The original stays usable:

```ts
import { of, map } from 'rxjs';

const nums$ = of(1, 2, 3);
const doubled$ = nums$.pipe(map((n) => n * 2));

nums$.subscribe((n) => console.log('original', n));   // 1, 2, 3
doubled$.subscribe((n) => console.log('doubled', n)); // 2, 4, 6
```

Each `.pipe()` builds a new Observable that, when subscribed, subscribes to *its* source, transforms the notifications, and passes them downstream. Nothing runs until the final subscribe.

### Importing operators in RxJS 7+

Since RxJS 7.2, operators are exported from the main `rxjs` entry point:

```ts
import { of, map, filter, take } from 'rxjs';
```

Older code (and pre-7.2 versions) imports pipeable operators from `rxjs/operators` instead:

```ts
import { map, filter } from 'rxjs/operators'; // still works, older style
```

This course uses the modern `from 'rxjs'` form throughout.

### The standalone `pipe()` for reuse

`pipe` also exists as a standalone function that composes operators into **one reusable operator**, without a source:

```ts
import { pipe, filter, map } from 'rxjs';

const evensTimesTen = pipe(
  filter((n: number) => n % 2 === 0),
  map((n) => n * 10),
);
// Use it like any operator: source$.pipe(evensTimesTen)
```

# Key Terminology

- **Operator** — a function that operates on Observables; either a creation operator or a pipeable operator.
- **Pipeable operator** — a function `(source: Observable<T>) => Observable<R>`, usually produced by calling a factory like `map(fn)`.
- **`OperatorFunction<T, R>`** — the TypeScript type of a pipeable operator.
- **`.pipe(...)`** — the Observable method that applies operators left to right.
- **`pipe(...)` (standalone)** — a function that composes operators into a single reusable operator.
- **Pure** — operators produce a new Observable without mutating the source or leaking side effects.

# Syntax Overview

```ts
import { of, pipe, map, filter, take } from 'rxjs';

// Method form: transform a specific source.
of(1, 2, 3, 4)
  .pipe(
    filter((n) => n > 1),  // 2, 3, 4
    map((n) => n * n),     // 4, 9, 16
    take(2),               // 4, 9   (then complete)
  )
  .subscribe(console.log);

// Standalone form: build a reusable operator you can drop into any pipe.
const firstTwoSquares = pipe(
  map((n: number) => n * n),
  take(2),
);
```

# Real World Analogy

`.pipe()` is a **factory assembly line**. The source Observable is the raw material dropped onto the belt. Each operator is a station: one paints, the next inspects and rejects defects (`filter`), the next stamps a logo (`map`). The material moves station to station in order, and what rolls off the end is the finished product — but the **raw material bin is untouched** (operators are pure), so you can run the same source through a *different* line and get a different product. And the belt only moves when someone turns it on at the end (subscribe).

# Examples

## Example 1 — Basic: filter then map

```ts
import { of, filter, map } from 'rxjs';

of(1, 2, 3, 4, 5, 6)
  .pipe(
    filter((n) => n % 2 === 0), // 2, 4, 6
    map((n) => `#${n}`),        // '#2', '#4', '#6'
  )
  .subscribe(console.log);
```

**Why this is good:** It's the canonical two-operator pipeline. Reading top-to-bottom, you can narrate exactly what each value becomes — the mental model you'll use for every pipe.

## Example 2 — Real-world: shaping input events

```ts
import { fromEvent, map, filter } from 'rxjs';

const input = document.querySelector<HTMLInputElement>('#search')!;

const query$ = fromEvent(input, 'input').pipe(
  map((event) => (event.target as HTMLInputElement).value.trim()),
  filter((text) => text.length >= 2), // ignore very short queries
);

query$.subscribe((text) => console.log('search for:', text));
```

**Why this is good:** It turns a raw DOM event stream into a clean stream of meaningful query strings using just `map` and `filter` — a real, useful transformation and a preview of the typeahead pattern.

## Example 3 — Best practice: order matters, and reuse with `pipe`

```ts
import { of, pipe, map, filter } from 'rxjs';

// Filtering BEFORE mapping avoids doing expensive work on values you'll drop.
const process = pipe(
  filter((n: number) => n % 2 === 0), // cheap check first
  map((n) => expensiveTransform(n)),  // only runs on survivors
);

of(1, 2, 3, 4).pipe(process).subscribe(console.log);

function expensiveTransform(n: number) {
  return n ** 2;
}
```

**Why this is good:** It shows two ideas at once — put cheap `filter`s before expensive `map`s so you don't transform values you'll discard, and extract the pipeline into a named, reusable operator with the standalone `pipe`.

# Common Mistakes

- **Expecting the source to change.** `source$.pipe(map(...))` returns a **new** Observable; `source$` is unchanged. **Fix:** assign the result — `const out$ = source$.pipe(...)`.
- **Forgetting to subscribe.** A pipe builds a description; nothing runs until subscribe. **Fix:** subscribe to the piped Observable (or bind it, e.g., Angular's `async`).
- **Using deprecated prototype chaining** like `source$.map(...).filter(...)`. That patched style is gone. **Fix:** put operators inside `.pipe(...)`.
- **Getting operator order wrong.** `map` then `filter` can differ from `filter` then `map`. **Fix:** order deliberately; filter early, transform late.
- **Calling the operator without invoking it.** `pipe(map)` (passing `map` itself) is wrong; you must call it: `pipe(map(fn))`. **Fix:** always call the factory with its callback.

# Best Practices

- Keep operator callbacks **pure** — no shared mutation; use `tap` for deliberate side effects (next lesson).
- Put cheap **`filter`s before expensive `map`s** to avoid wasted work.
- Write **one operator per line** in `.pipe()` for readable, diff-friendly pipelines.
- Extract repeated sequences into a named operator with the standalone **`pipe`**.
- Import operators from **`rxjs`** (7.2+); reserve `rxjs/operators` for legacy code.

# Summary

- A **pipeable operator** is a function from Observable to Observable, produced by calling a factory like `map(fn)`.
- **`.pipe(a, b, c)`** applies operators left to right and equals `c(b(a(source)))`.
- Operators are **pure**: they return a new Observable and never mutate the source, so sources are reusable.
- Nothing runs until you **subscribe** to the piped Observable.
- Import operators from **`rxjs`** in modern code (`rxjs/operators` is the legacy path).
- **Order matters**, and the standalone **`pipe()`** composes operators into one reusable unit.

# Flash Cards

Q: What is a pipeable operator, precisely?
A: A function that takes a source Observable and returns a new Observable — typically produced by calling a factory such as `map(fn)`. Its type is `OperatorFunction<T, R>`.

Q: What is `source$.pipe(a, b)` equivalent to in plain function terms?
A: `b(a(source$))` — the operators apply left to right, each receiving the previous one's output.

Q: Does `source$.pipe(map(...))` change `source$`?
A: No. Operators are pure; they return a new Observable and leave the source unchanged, so the source can be reused in other pipelines.

Q: Where do you import `map` and `filter` from in RxJS 7.2+?
A: From `'rxjs'` directly. The older path `'rxjs/operators'` still works but is legacy.

Q: Why generally place `filter` before an expensive `map`?
A: So the expensive transform only runs on values that survive the filter — avoiding wasted work on items you'll discard.

Q: What does the standalone `pipe(op1, op2)` return?
A: A single composed operator function you can reuse inside any `source$.pipe(...)`.

# Exercises

### Easy

Starting from `of(5, 10, 15, 20)`, build a pipe that keeps values greater than 8 and converts each to a string like `"value: 10"`. Subscribe and log the results.

### Medium

Take `range(1, 10)` and pipe it through `filter` (keep multiples of 3) then `map` (cube each survivor). Then swap the order of the two operators and explain in a comment how (and why) the output changes.

### Challenging

Create a reusable operator with the standalone `pipe` called `positiveDoubles` that filters out non-positive numbers and doubles the rest. Apply it to `of(-2, 0, 3, 5)` and to `of(10, -10, 20)`. Confirm the same operator works on both sources, and explain why the sources are unaffected by being piped.

# Further Reading

- [RxJS — Operators (pipeable operators, pipe)](https://rxjs.dev/guide/operators)
- [RxJS — map](https://rxjs.dev/api/index/function/map)
- [RxJS — pipe](https://rxjs.dev/api/index/function/pipe)
- [RxJS — Importing instructions](https://rxjs.dev/guide/importing)
