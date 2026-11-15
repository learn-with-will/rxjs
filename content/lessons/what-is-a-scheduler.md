---
slug: what-is-a-scheduler
title: "What is a Scheduler in RxJS?"
topic: schedulers-testing
kind: concept
summary: "A Scheduler controls when and on which JavaScript execution queue (synchronous, microtask, macrotask, animation frame, or virtual clock) notifications run."
tags: [rxjs, scheduler, concurrency, event-loop, timing]
related: [built-in-schedulers, observe-on-vs-subscribe-on, virtual-time-and-test-scheduler]
---
> **In one line:** a **Scheduler** governs the **execution context** (call stack, microtask queue, macrotask queue, or `requestAnimationFrame`) and **clock** for when subscriptions start and notifications are delivered.

## The idea

By default, RxJS does as little scheduling as possible: `of(1, 2, 3)`, `map`, and `filter` run **synchronously on the current call stack** with zero queue overhead.

When an operator *does* deal with time or concurrency (`interval`, `debounceTime`, `delay`, `observeOn`, `subscribeOn`), it delegates to a **`SchedulerLike`** object made of three parts:

1. **A data structure (queue):** stores tasks waiting to be executed in priority/FIFO order.
2. **An execution context:** decides *where* and *when* in the JavaScript runtime those tasks run (`setTimeout`, `Promise.resolve()`, `requestAnimationFrame`, or synchronously in a `while` loop).
3. **A virtual clock (`scheduler.now()`):** returns the current timestamp, allowing tests (`TestScheduler`) to fast-forward 10,000ms of "virtual time" in 1ms of real CPU time!

```flow
op: Operator / Observable | schedules work @primary
q: Queue (Data Structure) | orders pending tasks @blue
ctx: Execution Context | sync stack / microtask / macrotask / rAF @amber
clk: Clock (scheduler.now()) | real wall clock or virtual test clock @green
op -> q: schedule(work, delay)
q -> ctx: flushes on target JS queue
op -> clk: reads current time
```

## See it

Watch how a synchronous `of(1, 2)` runs **before** the line after `subscribe()`, whereas shifting it onto `asyncScheduler` with `observeOn` defers delivery to a macrotask (`setTimeout`) **after** the current call stack finishes:

```typescript
import { of, observeOn, asyncScheduler } from 'rxjs';

console.log('1. Before subscribe');

// Default (no scheduler): runs synchronously right inside subscribe()
of('sync-emission').subscribe((v) => console.log('2.', v));

// With asyncScheduler: queued on macrotask queue (like setTimeout(..., 0))
of('async-emission')
  .pipe(observeOn(asyncScheduler))
  .subscribe((v) => console.log('4.', v));

console.log('3. After subscribe');
// Output order:
// 1. Before subscribe
// 2. sync-emission
// 3. After subscribe
// 4. async-emission
```

## Go deeper

Most of the time in application code, you don't need to pass explicit schedulers at all — RxJS's time-based operators (`interval`, `timer`, `debounceTime`, `delay`, `timeout`) already default to **`asyncScheduler`** under the hood! You reach for schedulers explicitly in three situations:

1. **Testing:** swapping real time for virtual time with `TestScheduler`.
2. **UI rendering / animations:** aligning emissions with 60fps browser repaints using `animationFrameScheduler`.
3. **Breaking synchronous re-entrancy or stack overflows:** using `queueScheduler` or `asapScheduler`.

## Watch out

- Do not sprinkle `observeOn(asyncScheduler)` into pipelines that don't need it — synchronous execution is faster, easier to debug in stack traces, and avoids unnecessary event-loop ticks.
- Passing a scheduler directly to `of(1, 2, asyncScheduler)` is deprecated in RxJS 7+ — use `scheduled([1, 2], asyncScheduler)` or `observeOn(asyncScheduler)` instead.

## Remember

- By default, RxJS operators execute synchronously on the current call stack unless time is involved.
- A Scheduler combines a task **queue**, an **execution context** (stack/microtask/macrotask/rAF), and a **clock** (`now()`).
- Time-based operators (`interval`, `delay`, `debounceTime`) default to `asyncScheduler`.
