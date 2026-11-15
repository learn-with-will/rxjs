---
slug: built-in-schedulers
title: "The four built-in RxJS schedulers and the JS event loop"
topic: schedulers-testing
kind: api
summary: "Compare queueScheduler (sync queue), asapScheduler (microtask), asyncScheduler (macrotask), and animationFrameScheduler (requestAnimationFrame)."
tags: [rxjs, asyncscheduler, asapscheduler, queuescheduler, animationframescheduler]
related: [what-is-a-scheduler, observe-on-vs-subscribe-on, animation-frame-scheduler-in-practice]
---
> **In one line:** within a single turn of the JavaScript event loop, tasks run in strict order: **synchronous stack / `queueScheduler`** → **microtasks (`asapScheduler`)** → **browser paint (`animationFrameScheduler`)** → **macrotasks (`asyncScheduler`)**.

## The idea

RxJS ships four built-in runtime schedulers (plus `TestScheduler` for tests), each backed by a specific JavaScript scheduling primitive:

| Scheduler | Underlying JS mechanism | When 0ms tasks execute | Typical use case |
|---|---|---|---|
| **`queueScheduler`** | Synchronous FIFO trampoline loop | **Synchronously** before `subscribe()` returns | Flattening recursive synchronous emissions without overflowing the call stack |
| **`asapScheduler`** | **Microtask** queue (`Promise.resolve()`) | After current synchronous script, **before** macrotasks or paint | Deferring work until after the current stack finishes, as fast as possible |
| **`animationFrameScheduler`** | `requestAnimationFrame(cb)` | Right before the browser's **next repaint** (~16.6ms at 60Hz) | Smooth DOM animations, canvas drawing, layout measurements |
| **`asyncScheduler`** | **Macrotask** queue (`setInterval` / `setTimeout`) | On a future event-loop macrotask tick; supports `delay > 0` | Default for all time-based operators (`interval`, `timer`, `delay`, `debounceTime`) |

> **Note:** If you pass a non-zero `delay > 0` to `queueScheduler`, `asapScheduler`, or `animationFrameScheduler`, they automatically fall back to `asyncScheduler` for that delayed task!

## See it

Subscribe to all four schedulers in reverse priority order and watch the JavaScript event loop sort their output automatically:

```typescript
import { of, observeOn, asyncScheduler, asapScheduler, queueScheduler } from 'rxjs';

console.log('1. Script start (sync)');

of('4. asyncScheduler (macrotask)').pipe(observeOn(asyncScheduler)).subscribe(console.log);
of('3. asapScheduler (microtask)').pipe(observeOn(asapScheduler)).subscribe(console.log);
of('2. queueScheduler (sync queue)').pipe(observeOn(queueScheduler)).subscribe(console.log);

console.log('2b. Script end (sync)');
// Exact output order:
// 1. Script start (sync)
// 2. queueScheduler (sync queue)
// 2b. Script end (sync)
// 3. asapScheduler (microtask)
// 4. asyncScheduler (macrotask)
```

## Go deeper

Why does `asapScheduler` always print *before* `asyncScheduler` even when `asyncScheduler` was scheduled first? Because in the JavaScript event loop, at the end of the synchronous call stack, the engine drains the **entire microtask queue** (`Promise.then`, `queueMicrotask`, `asapScheduler`) completely empty before picking up the next **macrotask** (`setTimeout`, `setInterval`, `asyncScheduler`).

Where does `queueScheduler` fit if regular `of(...)` is already synchronous? When you schedule work *recursively* inside a `queueScheduler` action, instead of calling the next function directly on the JS call stack (which would throw `RangeError: Maximum call stack size exceeded` after a few thousand iterations), `queueScheduler` pushes the follow-up work onto an internal synchronous array and loops over it in a flat `while` trampoline before returning control to the caller.

## Watch out

- Scheduling an infinite loop of 0ms tasks on `asapScheduler` (microtasks) will starve the browser's rendering and macrotask queues, freezing the tab — use `asyncScheduler` or `animationFrameScheduler` for repeating loops.
- `queueScheduler` is synchronous only when `delay` is `0` (the default).

## Remember

- Execution order for `delay = 0`: synchronous / `queueScheduler` → `asapScheduler` (microtask) → `animationFrameScheduler` (next frame) → `asyncScheduler` (macrotask).
- `asyncScheduler` is the default backing scheduler for all time-based RxJS operators.
- Use `asapScheduler` for microtask deferral and `animationFrameScheduler` for visual frame updates.
