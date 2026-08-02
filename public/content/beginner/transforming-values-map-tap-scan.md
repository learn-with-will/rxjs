---
id: lesson-06
slug: transforming-values-map-tap-scan
title: "Transforming Values: map, tap, and scan"
level: beginner
order: 06
duration: 24
tags:
  - rxjs
  - map
  - tap
  - scan
  - reduce
  - transformation
---

# Learning Objectives

By the end of this lesson you will be able to:

- Reshape each emission with `map`.
- Run side effects (logging, debugging) without altering the stream using `tap`.
- Accumulate state across emissions with `scan`, emitting each step.
- Contrast `scan` with `reduce`, which emits a single final value on completion.
- Keep accumulators **immutable** and transformation callbacks **pure**.
- Choose the right operator for "change it", "watch it", and "accumulate it".

# Why This Concept Exists

Most streams don't arrive in exactly the shape you need. You get a DOM event but want the input's text; you get individual numbers but want a running total; you want to peek at values mid-pipeline without disturbing them. `map`, `tap`, and `scan` are the three everyday transformation tools that cover the vast majority of these needs — reshape, observe, and accumulate.

# Concept Explanation

### `map` — transform each value

`map(project)` applies a function to every emission and emits the result, exactly like `Array.prototype.map` but over time.

```ts
import { of, map } from 'rxjs';

of(1, 2, 3).pipe(map((n) => n * 10)).subscribe(console.log); // 10, 20, 30
```

`map` is **one-in, one-out** and should be **pure** — no side effects, just return the new value.

### `tap` — side effects without changing the stream

`tap` lets you *do* something for each notification — log, update a counter, trigger analytics — while passing every value through **unchanged**. It's the RxJS "spy" and the go-to debugging tool. (It was called `do` in older RxJS.)

```ts
import { of, tap, map } from 'rxjs';

of(1, 2, 3)
  .pipe(
    tap((n) => console.log('before:', n)), // side effect only
    map((n) => n * 2),
    tap((n) => console.log('after:', n)),
  )
  .subscribe();
// before: 1 / after: 2 / before: 2 / after: 4 / before: 3 / after: 6
```

`tap` can also take a full observer to watch `error` and `complete`:

```ts
source$.pipe(tap({ next: v => {}, error: e => {}, complete: () => {} }));
```

Because `tap` never changes values, if you find yourself "transforming" inside a `tap`, you actually want `map`.

### `scan` — a running accumulator that emits each step

`scan(accumulator, seed)` is like `reduce`, but it emits the **current accumulated value after every source emission**. Perfect for running totals and state built up over time.

```ts
import { of, scan } from 'rxjs';

of(1, 2, 3, 4)
  .pipe(scan((acc, n) => acc + n, 0))
  .subscribe(console.log); // 1, 3, 6, 10  (running sum)
```

The seed is optional; when provided it's the initial accumulator. Keep the accumulator **immutable** — return a new object/array rather than mutating the old one.

### `reduce` — one final value on completion

`reduce(accumulator, seed)` accumulates the same way but emits **only once**, when the source **completes**.

```ts
import { of, reduce } from 'rxjs';

of(1, 2, 3, 4)
  .pipe(reduce((acc, n) => acc + n, 0))
  .subscribe(console.log); // 10   (just the final sum)
```

The relationship: `reduce` = `scan` + "keep only the last value." Crucially, `reduce` needs the source to **complete** — on an infinite stream like `interval`, it never emits.

```text
scan:    1--3--6--10-->      (emits every step)
reduce:  ---------(10|)      (emits once, only when the source completes)
```

# Key Terminology

- **`map`** — transforms each emission via a projection function; one-in, one-out.
- **`tap`** — performs side effects per notification and passes values through unchanged (formerly `do`).
- **`scan`** — accumulates across emissions and emits the running result each time.
- **`reduce`** — accumulates and emits a single final value when the source completes.
- **Seed / initial accumulator** — the starting value for `scan`/`reduce`.
- **Pure function** — depends only on its inputs and has no side effects; the right shape for `map` and accumulators.

# Syntax Overview

```ts
import { of, map, tap, scan, reduce } from 'rxjs';

of(2, 4, 6).pipe(
  tap((n) => console.log('saw', n)),   // side effect, value unchanged
  map((n) => n / 2),                   // transform: 1, 2, 3
  scan((acc, n) => acc + n, 0),        // running sum: 1, 3, 6  (emits each)
);

of(2, 4, 6).pipe(
  reduce((acc, n) => acc + n, 0),      // 12  (emits once, on complete)
);
```

# Real World Analogy

Picture parcels moving along a conveyor. **`map`** is the relabelling station — every parcel gets a new address sticker and moves on, one for one. **`tap`** is the CCTV camera — it *watches* each parcel and can log it or ring a bell, but it never opens or changes the parcel. **`scan`** is the running weight display that updates the *total* kilograms shipped after each parcel passes. **`reduce`** is the end-of-day report that prints the grand total only once the belt stops for the night — which is why it never prints if the belt runs forever.

# Examples

## Example 1 — Basic: map to reshape objects

```ts
import { from, map } from 'rxjs';

const users = [
  { id: 1, first: 'Ada', last: 'Lovelace' },
  { id: 2, first: 'Alan', last: 'Turing' },
];

from(users)
  .pipe(map((u) => `${u.first} ${u.last}`))
  .subscribe(console.log); // 'Ada Lovelace' / 'Alan Turing'
```

**Why this is good:** It's the everyday use of `map` — projecting a rich object down to exactly the shape the rest of the pipeline needs, purely and predictably.

## Example 2 — Real-world: a live click counter with scan (and tap to debug)

```ts
import { fromEvent, scan, tap } from 'rxjs';

const button = document.querySelector('#tally')!;

fromEvent(button, 'click')
  .pipe(
    scan((count) => count + 1, 0),        // 1, 2, 3, ... per click
    tap((count) => console.log('debug:', count)), // observe, don't change
  )
  .subscribe((count) => {
    button.textContent = `Clicked ${count} times`;
  });
```

**Why this is good:** `scan` turns discrete click events into evolving state (a running count) that emits on every click, while `tap` gives a non-intrusive debug hook — the two operators doing exactly their intended jobs.

## Example 3 — Best practice: immutable accumulation, and scan vs reduce

```ts
import { of, scan, reduce } from 'rxjs';

const events$ = of('login', 'view', 'purchase');

// scan: emit the growing history after each event (return a NEW array).
events$
  .pipe(scan((history: string[], e) => [...history, e], []))
  .subscribe((h) => console.log('history:', h));
// ['login'] / ['login','view'] / ['login','view','purchase']

// reduce: emit the full history only once, when the stream completes.
events$
  .pipe(reduce((history: string[], e) => [...history, e], []))
  .subscribe((h) => console.log('final:', h));
// ['login','view','purchase']
```

**Why this is good:** It contrasts the two accumulators on identical input and models the immutable pattern (`[...history, e]`), which keeps each emitted value a distinct snapshot instead of a shared, mutating reference.

# Common Mistakes

- **Transforming inside `tap`.** `tap` ignores your return value — the stream is unchanged. **Fix:** use `map` to transform; reserve `tap` for side effects.
- **Expecting `reduce` to emit on an infinite stream.** It only emits when the source **completes**. **Fix:** use `scan` for running values, or ensure the stream completes (e.g., with `take`).
- **Mutating the accumulator in `scan`/`reduce`.** `acc.push(x); return acc;` leaks a shared, changing reference. **Fix:** return a new value — `[...acc, x]` or `{ ...acc }`.
- **Putting side effects in `map`.** Logging or DOM writes inside `map` make it impure and harder to test. **Fix:** move side effects to `tap`.
- **Forgetting `scan`'s seed when you need a starting shape.** Without a seed the first value becomes the accumulator. **Fix:** pass an explicit seed (e.g., `0` or `[]`) when the type matters.

# Best Practices

- Keep `map` and accumulator callbacks **pure**; they should depend only on their inputs.
- Use `tap` deliberately — for logging, analytics, or DOM effects — and consider removing debug taps before shipping.
- Prefer **immutable** updates in `scan` so each emission is an independent snapshot.
- Reach for `scan` when you want to *see the value evolve*, and `reduce` when you want only the *final* result of a completing stream.
- Provide an explicit **seed** to make the accumulator's type and starting state clear.

# Summary

- **`map`** transforms each value, one-in-one-out, and should be pure.
- **`tap`** runs side effects (logging, debugging) and passes values through **unchanged**.
- **`scan`** accumulates across emissions and emits the running result **every step**.
- **`reduce`** accumulates but emits a **single** value, only when the source **completes**.
- `reduce` = `scan` + "last value"; on infinite streams `reduce` never emits.
- Keep accumulators **immutable** and transformation callbacks **pure**.

# Flash Cards

Q: What does `map` do in RxJS?
A: It applies a projection function to each emission and emits the result — one-in, one-out — exactly like Array.map but over time.

Q: What is `tap` for, and how does it affect the values flowing through it?
A: `tap` runs side effects (logging, analytics, DOM updates) for each notification and passes every value through unchanged. It cannot transform values.

Q: How does `scan` differ from `reduce`?
A: `scan` emits the running accumulator after every source emission; `reduce` emits a single accumulated value only when the source completes.

Q: Why does `reduce` never emit on an `interval(1000)` source?
A: Because `reduce` emits only on completion, and `interval` never completes. Use `scan` for running values on infinite streams.

Q: What is the immutable way to append `x` to an array accumulator in `scan`?
A: Return a new array, e.g. `scan((acc, x) => [...acc, x], [])`, rather than mutating and returning the old one.

Q: If you catch yourself transforming a value inside `tap`, which operator do you actually want?
A: `map` — `tap` is for side effects only and ignores your return value.

# Exercises

### Easy

From `of(1, 2, 3, 4, 5)`, use `map` to emit each number's square, and add a `tap` before the map that logs each incoming number. Confirm the tap does not alter the emitted squares.

### Medium

Using `fromEvent` on a button's `click`, build a running total that starts at 0 and adds 5 on each click with `scan`, updating the button label. Then add a `tap` that logs the current total. Explain in a comment why `scan` (not `reduce`) is the right choice here.

### Challenging

Given `of({ type: 'add', amount: 10 }, { type: 'add', amount: 5 }, { type: 'remove', amount: 3 })`, use `scan` with an object seed `{ balance: 0 }` to emit the evolving balance after each action (immutably). Then write a `reduce` version that emits only the final balance, and note what would happen if the source never completed.

# Further Reading

- [RxJS — map](https://rxjs.dev/api/index/function/map)
- [RxJS — tap](https://rxjs.dev/api/index/function/tap)
- [RxJS — scan](https://rxjs.dev/api/index/function/scan)
- [RxJS — reduce](https://rxjs.dev/api/index/function/reduce)
