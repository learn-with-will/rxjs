---
id: lesson-17
slug: schedulers-and-concurrency
title: "Schedulers and Concurrency"
level: advanced
order: 17
duration: 32
tags:
  - rxjs
  - schedulers
  - concurrency
  - observeon
  - subscribeon
  - async
---

# Learning Objectives

By the end of this lesson you will be able to:

- Explain what a Scheduler is and that RxJS is **synchronous by default**.
- Compare `asyncScheduler`, `asapScheduler`, `queueScheduler`, and `animationFrameScheduler`.
- Control *where notifications are delivered* with `observeOn`.
- Control *where subscription work runs* with `subscribeOn`.
- Use `scheduled()` instead of the deprecated scheduler argument on `of`/`from`/`range`.
- Pick a scheduler deliberately for timing, animation, or avoiding UI jank.

# Why This Concept Exists

By default, `of(1, 2, 3)` emits **synchronously** — right now, blocking. That's usually fine, but sometimes you need control over *timing and execution context*: run heavy work without freezing the UI, deliver updates in sync with the browser's paint, defer emissions until after the current call stack, or make a recursive stream safe from stack overflow. **Schedulers** are RxJS's abstraction for "when and on what queue does this work happen".

# Concept Explanation

### What a Scheduler is

A Scheduler is a controller that decides **when** a unit of work executes and provides a notion of **time** (`scheduler.now()`). Operators that involve timing (`interval`, `timer`, `debounceTime`, …) use a scheduler internally — `asyncScheduler` by default. You can also inject one explicitly.

### The four built-in schedulers

- **`asyncScheduler`** — schedules work as a **macrotask** (via `setInterval`/`setTimeout`). Asynchronous; the basis of time-based operators. Use for delays, timeouts, and breaking up heavy work.
- **`asapScheduler`** — schedules work as a **microtask** (via the Promise job queue). Runs as soon as the current synchronous code finishes, **before** timers and rendering.
- **`queueScheduler`** — schedules work **synchronously** on the current frame, but **queues** re-entrant scheduling (a trampoline) to avoid stack overflow in recursion.
- **`animationFrameScheduler`** — schedules work via `requestAnimationFrame`, so emissions align with the browser's paint cycle — ideal for animations.

```ts
import { asyncScheduler } from 'rxjs';

console.log('before');
asyncScheduler.schedule(() => console.log('scheduled')); // runs later (macrotask)
console.log('after');
// before / after / scheduled
```

### `observeOn` — where notifications are *delivered*

`observeOn(scheduler)` reschedules each `next`/`error`/`complete` **downstream** onto the given scheduler. It changes *when subscribers receive values* from that point in the pipe.

```ts
import { of, observeOn, asyncScheduler } from 'rxjs';

console.log('start');
of(1, 2, 3)
  .pipe(observeOn(asyncScheduler)) // deliver emissions asynchronously
  .subscribe((n) => console.log('value', n));
console.log('end');
// start / end / value 1 / value 2 / value 3   (values now come AFTER sync code)
```

### `subscribeOn` — where subscription *starts*

`subscribeOn(scheduler)` schedules the **subscription itself** (running the producer) on the given scheduler. It affects **when the source starts**, not how each value is delivered.

```ts
import { of, subscribeOn, asyncScheduler } from 'rxjs';

console.log('start');
of('produced')
  .pipe(subscribeOn(asyncScheduler)) // defer the whole subscription
  .subscribe((v) => console.log(v));
console.log('end');
// start / end / produced   (the producer ran after the sync code)
```

Rule of thumb: **`subscribeOn`** affects the *start* of the whole chain (applied once, wherever it sits); **`observeOn`** affects *delivery* from its position downstream.

### Creating scheduled Observables the modern way

Passing a scheduler as the last argument to `of`, `from`, or `range` is **deprecated** in RxJS 7. Use **`scheduled()`** instead:

```ts
import { scheduled, asyncScheduler } from 'rxjs';

// Deprecated: of(1, 2, 3, asyncScheduler)
scheduled([1, 2, 3], asyncScheduler).subscribe(console.log); // async emissions
```

# Key Terminology

- **Scheduler** — controls when work runs and supplies a clock (`now()`).
- **`asyncScheduler`** — macrotask scheduling (`setTimeout`-style); default for time operators.
- **`asapScheduler`** — microtask scheduling (Promise queue).
- **`queueScheduler`** — synchronous, trampolined scheduling for safe recursion.
- **`animationFrameScheduler`** — `requestAnimationFrame`-based scheduling.
- **`observeOn`** — reschedules downstream notification delivery.
- **`subscribeOn`** — reschedules the subscription (producer start).
- **`scheduled()`** — creates an Observable from an input that emits on a given scheduler.

# Syntax Overview

```ts
import {
  of, scheduled, observeOn, subscribeOn,
  asyncScheduler, asapScheduler, queueScheduler, animationFrameScheduler,
} from 'rxjs';

scheduled([1, 2, 3], asyncScheduler);              // async emissions (not of(..., scheduler))
source$.pipe(observeOn(animationFrameScheduler));   // deliver in sync with paint
source$.pipe(subscribeOn(asyncScheduler));          // start the producer asynchronously

asapScheduler.schedule(() => doMicrotaskWork());
queueScheduler.schedule(() => doSyncRecursiveWork());
```

# Real World Analogy

Schedulers are the **dispatch policies of a kitchen**. The default (no scheduler) is a cook who does the task **the instant** it's requested, standing still until it's done (synchronous, blocking). `asyncScheduler` is "add it to the ticket rail and get to it on the next pass" (macrotask). `asapScheduler` is "I'll do it the moment I finish *this* exact motion, before anything else" (microtask). `queueScheduler` is a strict one-at-a-time line so a cook who keeps generating sub-tasks doesn't topple the whole stack of pans (trampolined recursion). `animationFrameScheduler` is plating dishes only when the waiter is about to walk out — perfectly timed to the dining room's rhythm (the paint cycle). `observeOn` changes when the *diners are served*; `subscribeOn` changes when the *order is first fired*.

# Examples

## Example 1 — Basic: making a synchronous stream asynchronous

```ts
import { of, observeOn, asyncScheduler } from 'rxjs';

console.log('A: before subscribe');

of('x', 'y')
  .pipe(observeOn(asyncScheduler))
  .subscribe((v) => console.log('C: got', v));

console.log('B: after subscribe');
// A: before subscribe / B: after subscribe / C: got x / C: got y
```

**Why this is good:** It proves RxJS is synchronous by default and shows `observeOn(asyncScheduler)` pushing delivery onto the macrotask queue — the values arrive *after* the surrounding synchronous code, a common way to avoid blocking.

## Example 2 — Real-world: smooth updates with animationFrameScheduler

```ts
import { interval, animationFrameScheduler, map, observeOn } from 'rxjs';

// Drive a progress bar in sync with the browser's paint cycle.
interval(0, animationFrameScheduler) // tick every frame
  .pipe(map((frame) => Math.min(frame / 60, 1)))
  .subscribe((progress) => setBarWidth(progress));

function setBarWidth(p: number) {
  /* update DOM width to p * 100% */
}
```

**Why this is good:** Animations should update once per frame, not faster. `animationFrameScheduler` aligns emissions with `requestAnimationFrame`, giving smooth, efficient visual updates without wasted work between paints.

## Example 3 — Best practice: subscribeOn vs observeOn, and scheduled()

```ts
import { scheduled, asyncScheduler, tap, observeOn, subscribeOn } from 'rxjs';

const source$ = scheduled([1, 2, 3], asyncScheduler).pipe(
  tap((n) => console.log('produced', n)),
);

source$
  .pipe(
    subscribeOn(asyncScheduler),      // start the producer on the async scheduler
    observeOn(asyncScheduler),        // deliver notifications on the async scheduler
    tap((n) => console.log('observed', n)),
  )
  .subscribe();
```

**Why this is good:** It uses the modern `scheduled()` factory (not the deprecated scheduler argument) and shows the two knobs together — `subscribeOn` governing *when production begins* and `observeOn` governing *when values are delivered downstream*.

# Common Mistakes

- **Passing a scheduler to `of`/`from`/`range`.** Deprecated in RxJS 7. **Fix:** use `scheduled(input, scheduler)`.
- **Confusing `subscribeOn` and `observeOn`.** `subscribeOn` schedules the *subscription/start*; `observeOn` schedules *each delivery*. **Fix:** choose by whether you're moving the *start* or the *emissions*.
- **Assuming RxJS is async by default.** `of`/`from` are synchronous. **Fix:** add a scheduler (or a time operator) when you need async behaviour.
- **Using `asapScheduler` expecting timer semantics.** It's a **microtask**, running before timers and rendering. **Fix:** use `asyncScheduler` for macrotask timing.
- **Reaching for schedulers unnecessarily.** They add complexity. **Fix:** only introduce a scheduler for a concrete reason (animation, jank, ordering, testing).

# Best Practices

- Leave the **default** scheduling alone unless you have a specific need.
- Use **`animationFrameScheduler`** for visual/animation streams.
- Use **`asyncScheduler`** to break up heavy synchronous work or defer to a macrotask; **`asapScheduler`** for microtask ordering.
- Use **`queueScheduler`** to make recursive/self-scheduling streams stack-safe.
- Always create scheduled Observables with **`scheduled()`**, not the deprecated argument form.
- Remember the TestScheduler (next lesson) virtualises all of this for deterministic tests.

# Summary

- A **Scheduler** controls *when* work runs and provides a clock; RxJS is **synchronous by default**.
- **`asyncScheduler`** (macrotask), **`asapScheduler`** (microtask), **`queueScheduler`** (sync/trampolined), **`animationFrameScheduler`** (rAF) are the built-ins.
- **`observeOn`** reschedules downstream *delivery*; **`subscribeOn`** reschedules the *subscription/start*.
- Passing a scheduler to `of`/`from`/`range` is deprecated — use **`scheduled()`**.
- Pick schedulers deliberately: animation, deferring work, avoiding UI jank, or stack-safe recursion.

# Flash Cards

Q: Is `of(1, 2, 3)` synchronous or asynchronous by default?
A: Synchronous — it emits immediately and blocks. RxJS only becomes asynchronous when a scheduler or a time-based operator is involved.

Q: What is the difference between `observeOn` and `subscribeOn`?
A: `observeOn` reschedules how/when notifications are delivered downstream from its position; `subscribeOn` reschedules when the subscription (the producer start) runs, affecting the whole chain.

Q: Which scheduler runs work on the microtask queue, before timers and rendering?
A: `asapScheduler` — it schedules via the Promise/microtask queue. `asyncScheduler` uses macrotasks (setTimeout-style).

Q: Which scheduler should drive an animation, and why?
A: `animationFrameScheduler`, because it schedules via requestAnimationFrame, aligning emissions with the browser's paint cycle for smooth updates.

Q: How do you make `of([1,2,3])`-style emissions asynchronous in RxJS 7 without the deprecated scheduler argument?
A: Use `scheduled([1, 2, 3], asyncScheduler)` — the modern factory for creating scheduled Observables.

Q: What problem does `queueScheduler` solve for recursive streams?
A: It executes synchronously but trampolines re-entrant scheduling, queuing nested tasks instead of recursing — avoiding stack overflow.

# Exercises

### Easy

Log `"before"`, subscribe to `of('a', 'b')` piped through `observeOn(asyncScheduler)` logging each value, then log `"after"`. Predict and then verify that the values print after `"before"` and `"after"`.

### Medium

Create `scheduled([1, 2, 3, 4, 5], asyncScheduler)` and subscribe, logging each value and a final `"done"`. Confirm the emissions are asynchronous. Then compare against `scheduled([...], queueScheduler)` and note that it emits synchronously.

### Challenging

Drive a counter with `interval(0, animationFrameScheduler)` and log the frame number for two seconds using `takeUntil(timer(2000))`. Compare the number of emissions to `interval(16)` and discuss in a comment why `animationFrameScheduler` is preferable for visual updates. Then demonstrate `subscribeOn(asyncScheduler)` deferring when a `tap('producing')` in the source first runs.

# Further Reading

- [RxJS — Scheduler](https://rxjs.dev/guide/scheduler)
- [RxJS — observeOn](https://rxjs.dev/api/index/function/observeOn) and [subscribeOn](https://rxjs.dev/api/index/function/subscribeOn)
- [RxJS — scheduled](https://rxjs.dev/api/index/function/scheduled)
- [RxJS — asyncScheduler](https://rxjs.dev/api/index/const/asyncScheduler) and [animationFrameScheduler](https://rxjs.dev/api/index/const/animationFrameScheduler)
