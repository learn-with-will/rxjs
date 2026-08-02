---
id: lesson-02
slug: creating-observables
title: "Creating Observables"
level: beginner
order: 02
duration: 24
tags:
  - rxjs
  - creation
  - of
  - from
  - fromevent
  - interval
---

# Learning Objectives

By the end of this lesson you will be able to:

- Create Observables from static values with `of` and `from`.
- Turn DOM events into a stream with `fromEvent`.
- Produce time-based streams with `interval` and `timer`.
- Emit a range of numbers with `range`.
- Use the special Observables `EMPTY`, `NEVER`, and `throwError`.
- Defer creation per subscription with `defer`, and know which creators complete and which run forever.

# Why This Concept Exists

You almost never hand-write `new Observable(...)`. Real sources — arrays, Promises, events, timers — follow predictable patterns, so RxJS ships **creation functions** that wrap them for you. Learning this toolkit means you can convert *anything* into a stream in one line, which is the entry point to every pipeline you'll build later.

# Concept Explanation

### `of` — emit the arguments you give it

`of(...values)` emits each argument **in order, synchronously**, then completes.

```ts
import { of } from 'rxjs';

of(1, 2, 3).subscribe(console.log); // 1, 2, 3, then complete
```

`of` treats each argument as one emission. `of([1, 2, 3])` emits a **single value**: the array itself.

### `from` — convert an existing collection or Promise

`from(input)` converts something iterable or Promise-like into an Observable. It accepts arrays, array-likes, iterables (including strings), Promises, and other Observables.

```ts
import { from } from 'rxjs';

from([10, 20, 30]).subscribe(console.log);       // 10, 20, 30, complete
from('hi').subscribe(console.log);                // 'h', 'i', complete
from(fetch('/api/user')).subscribe(console.log);  // resolved Response, then complete
```

The `of` vs `from` distinction matters: `from([1, 2, 3])` emits three numbers; `of([1, 2, 3])` emits one array.

### `fromEvent` — turn DOM (or Node) events into a stream

```ts
import { fromEvent } from 'rxjs';

const clicks$ = fromEvent<MouseEvent>(document, 'click');
clicks$.subscribe((event) => console.log(event.clientX, event.clientY));
```

`fromEvent` attaches the listener on subscribe and removes it on unsubscribe. It **never completes on its own** — the stream of clicks is open-ended, so you are responsible for unsubscribing.

### `interval` and `timer` — streams driven by the clock

`interval(period)` emits `0, 1, 2, 3, …` every `period` milliseconds. The first value arrives **after** the first period, and it runs **forever** until unsubscribed.

```text
interval(1000):  ----0----1----2----3---->   (one number per second)
```

`timer(dueTime, period?)` is more flexible. With one argument it emits `0` once after `dueTime` and completes. With a second argument it emits `0` after `dueTime`, then `1, 2, 3, …` every `period`.

```ts
import { interval, timer } from 'rxjs';

interval(1000).subscribe(console.log);      // 0,1,2,... forever, 1s apart
timer(3000).subscribe(console.log);         // 0 after 3s, then complete
timer(0, 1000).subscribe(console.log);      // 0 immediately, then 1,2,3,... every 1s
```

### `range` — a finite run of numbers

`range(start, count)` emits `count` sequential integers beginning at `start`, synchronously, then completes. The second argument is a **count, not an end value**.

```ts
import { range } from 'rxjs';

range(1, 5).subscribe(console.log); // 1, 2, 3, 4, 5, complete  (five numbers)
```

### Special Observables: `EMPTY`, `NEVER`, `throwError`

- **`EMPTY`** — a constant Observable that emits **no** values and completes immediately. Useful as a "do nothing but finish" fallback.
- **`NEVER`** — a constant Observable that never emits and never completes. Rare, mostly for testing.
- **`throwError(() => error)`** — emits nothing and errors immediately. In RxJS 7 you pass an **error factory** (a function returning the error), not the error directly.

```ts
import { EMPTY, throwError } from 'rxjs';

EMPTY.subscribe({ complete: () => console.log('done, no values') });
throwError(() => new Error('boom')).subscribe({ error: (e) => console.log(e.message) });
```

### `defer` — build the Observable lazily, per subscriber

`defer(factory)` waits until subscription, then calls your factory to produce the Observable to use. This gives each subscriber a **fresh** source — important when the source captures "now".

```ts
import { defer, of } from 'rxjs';

const eager$ = of(Date.now());          // timestamp captured once, at creation
const lazy$ = defer(() => of(Date.now())); // timestamp captured per subscribe
```

# Key Terminology

- **Creation function** — a function that builds an Observable from some source (values, events, time, Promises).
- **`of`** — emits its arguments in order, then completes.
- **`from`** — converts an array, iterable, Promise, or Observable into an Observable.
- **`fromEvent`** — adapts an event target (DOM element, Node EventEmitter) into an event stream.
- **`interval` / `timer`** — clock-driven Observables.
- **`range`** — emits a finite sequence of integers.
- **`EMPTY` / `NEVER`** — constants that complete-with-nothing / do-nothing-forever.
- **`throwError`** — an Observable that errors immediately, given an error factory.
- **`defer`** — delays Observable creation until subscription, once per subscriber.

# Syntax Overview

```ts
import { of, from, fromEvent, interval, timer, range, EMPTY, throwError, defer } from 'rxjs';

of(1, 2, 3);                       // 1,2,3,|   (synchronous)
from([1, 2, 3]);                   // 1,2,3,|
from(Promise.resolve('ok'));       // 'ok',|
fromEvent(button, 'click');        // click,click,click...  (never completes)
interval(1000);                    // 0,1,2,3...             (never completes)
timer(2000);                       // 0 after 2s, |
timer(2000, 1000);                 // 0 after 2s, then 1,2,3... every 1s
range(0, 4);                       // 0,1,2,3,|
EMPTY;                             // |                      (completes, no values)
throwError(() => new Error('x'));  // #                      (errors immediately)
defer(() => of(Math.random()));    // fresh value per subscribe
```

# Real World Analogy

The creation functions are like different **taps** feeding the same plumbing. `of` and `from` are a bottle you pour in all at once. `fromEvent` is a tap wired to a doorbell — it drips a drop each time someone presses it, and never runs dry on its own. `interval` is a tap on an automatic timer that releases a drop every second. `EMPTY` is a tap you open to find it was already empty, and `throwError` is a tap that bursts the moment you open it. Whatever the source, once it's flowing through RxJS pipes, the same operators work on all of them.

# Examples

## Example 1 — Basic: `of` vs `from`

```ts
import { of, from } from 'rxjs';

of([1, 2, 3]).subscribe((v) => console.log('of:', v));
// of: [1, 2, 3]        — ONE emission (the array)

from([1, 2, 3]).subscribe((v) => console.log('from:', v));
// from: 1 / from: 2 / from: 3   — THREE emissions
```

**Why this is good:** It isolates the single most common creation gotcha. `of` emits its arguments; `from` iterates its argument. Getting this right saves hours of confusion.

## Example 2 — Real-world: a polling clock with `timer`

```ts
import { timer } from 'rxjs';

// Fire immediately, then every 5 seconds — a common polling shape.
const poll$ = timer(0, 5000);

const sub = poll$.subscribe((tick) => {
  console.log('poll #', tick); // 0, 1, 2, ... every 5s starting now
});

// Because timer(0, 5000) never completes, you must stop it yourself:
// sub.unsubscribe();
```

**Why this is good:** `timer(0, period)` is the idiomatic way to run something "now and then repeatedly." The comment flags that it never completes — a reminder that clock-driven streams need explicit teardown.

## Example 3 — Best practice: `defer` for per-subscription freshness

```ts
import { defer, of } from 'rxjs';

const random$ = defer(() => of(Math.random()));

random$.subscribe((n) => console.log('A', n)); // e.g. A 0.42
random$.subscribe((n) => console.log('B', n)); // e.g. B 0.87  — a DIFFERENT number

// Compare: const random$ = of(Math.random()) would capture ONE number
// at creation and replay that same value to every subscriber.
```

**Why this is good:** It shows *why* `defer` exists — to move value creation from definition time to subscription time, so every subscriber gets its own fresh computation (a fresh timestamp, a fresh `fetch`, a fresh random number).

# Common Mistakes

- **Confusing `of([...])` with `from([...])`.** `of([1,2,3])` emits one array; `from([1,2,3])` emits three numbers. **Fix:** use `from` to spread a collection, `of` to emit values as-is.
- **Forgetting that `interval`, `timer(x, y)`, and `fromEvent` never complete.** They keep the subscription (and any listeners/timers) alive. **Fix:** unsubscribe, or use a completing operator like `take` (a later lesson).
- **Passing the error directly to `throwError`.** `throwError(new Error('x'))` is the deprecated form. **Fix (RxJS 7):** pass a factory — `throwError(() => new Error('x'))`.
- **Reading `range(start, count)` as `range(start, end)`.** `range(5, 3)` emits `5, 6, 7`, not `5, 6, 7, …` up to `3`. **Fix:** remember the second argument is how *many* values.
- **Using `defer`'s factory for side effects you expected once.** The factory runs on **every** subscribe. **Fix:** that's the point — but be aware each subscription re-runs it.

# Best Practices

- Reach for the **most specific** creation function: `from` for a Promise, `fromEvent` for events, `timer`/`interval` for the clock — rather than hand-rolling `new Observable`.
- Prefer `timer(0, period)` over `interval(period)` when you want the **first** value immediately instead of after one period.
- Use `defer` whenever the source must be created **at subscribe time** (fresh Promises, current time, per-subscriber state).
- Use `EMPTY` as a clean "complete with nothing" branch (handy inside `catchError`, a later lesson).
- Always plan the teardown for non-completing sources (`fromEvent`, `interval`) before you ship.

# Summary

- **`of`** emits its arguments then completes; **`from`** converts arrays, iterables, Promises, and Observables.
- **`fromEvent`** streams events and **never self-completes**; unsubscribe to remove the listener.
- **`interval(period)`** counts up forever; **`timer(due, period?)`** delays the first emission and optionally repeats.
- **`range(start, count)`** emits `count` integers from `start`, synchronously, then completes.
- **`EMPTY`** completes with no values, **`NEVER`** does nothing, and **`throwError(() => err)`** errors immediately.
- **`defer`** builds the Observable at subscribe time, giving each subscriber a fresh source.

# Flash Cards

Q: What is the difference between `of([1,2,3])` and `from([1,2,3])`?
A: `of([1,2,3])` emits one value — the array. `from([1,2,3])` iterates it and emits three separate values: 1, 2, 3.

Q: Does `interval(1000)` ever complete on its own?
A: No. It emits 0, 1, 2, … every second forever; you must unsubscribe (or use an operator like `take`) to stop it.

Q: How do `timer(2000)` and `timer(2000, 1000)` differ?
A: `timer(2000)` emits 0 once after 2s then completes. `timer(2000, 1000)` emits 0 after 2s, then 1, 2, 3, … every 1s and never completes.

Q: In RxJS 7, how do you create an Observable that errors immediately?
A: `throwError(() => new Error('...'))` — you pass an error *factory* function, not the error itself.

Q: What does `range(10, 3)` emit?
A: 10, 11, 12, then complete — the second argument is a count (3 values), not an end value.

Q: Why use `defer(() => of(Date.now()))` instead of `of(Date.now())`?
A: `defer` runs its factory on each subscribe, so every subscriber gets the current timestamp; `of(Date.now())` captures the time once at creation and replays it.

# Exercises

### Easy

Using `of` and then `from`, print the numbers `1, 2, 3` two different ways. Then show that `of([1, 2, 3])` emits the array as a single value by logging its `length` inside the subscriber.

### Medium

Create a stream that emits `"tick"` immediately and then every 2 seconds. Subscribe and log each tick with a counter. Add a comment explaining why the stream never completes and what you would do to stop it after 5 ticks.

### Challenging

Build a `fromEvent` stream of `keydown` events on `document`. Log each pressed key (`event.key`). Then rewrite it to also work with `defer`, so that a fresh `fromEvent` is created on each subscribe. Finally, describe in a comment which creators in this lesson complete on their own (`of`, `from` array, `range`, `timer(x)`, `EMPTY`) and which do not (`fromEvent`, `interval`, `timer(x, y)`, `NEVER`).

# Further Reading

- [RxJS — of](https://rxjs.dev/api/index/function/of)
- [RxJS — from](https://rxjs.dev/api/index/function/from)
- [RxJS — fromEvent](https://rxjs.dev/api/index/function/fromEvent)
- [RxJS — interval](https://rxjs.dev/api/index/function/interval) and [timer](https://rxjs.dev/api/index/function/timer)
- [RxJS — range](https://rxjs.dev/api/index/function/range), [EMPTY](https://rxjs.dev/api/index/const/EMPTY), [throwError](https://rxjs.dev/api/index/function/throwError), [defer](https://rxjs.dev/api/index/function/defer)
