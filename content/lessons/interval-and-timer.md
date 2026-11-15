---
slug: interval-and-timer
title: "Clock streams: interval() and timer()"
topic: creating-observables
kind: api
summary: "interval(period) emits 0, 1, 2... every period ms forever; timer(dueTime, period?) waits dueTime ms for 0 and then either completes or repeats."
tags: [rxjs, interval, timer, time, creation]
related: [from-event, range-creator, non-overlapping-polling-pattern]
---
> **In one line:** `interval(period)` waits one period before emitting `0, 1, 2, …` forever, while `timer(due, period?)` controls the initial delay separately and can either complete after one emission or repeat forever.

## The idea

When you need a stream driven by the clock — for polling, countdowns, or delayed actions — RxJS gives you `interval` and `timer`:

- **`interval(period)`** emits ascending integers `0, 1, 2, 3, …` spaced by `period` milliseconds. Importantly, the first value (`0`) is emitted **after** the first `period` has elapsed, not immediately. It **never completes** on its own.
- **`timer(dueTime)`** (one argument) waits `dueTime` milliseconds, emits `0` once, and **completes** — like an Observable version of `setTimeout`.
- **`timer(dueTime, period)`** (two arguments) waits `dueTime` milliseconds to emit `0`, and then emits `1, 2, 3, …` every `period` milliseconds **forever** without completing.

| Call | First emission (`0`) | Subsequent emissions | Completes on its own? |
|---|---|---|---|
| `interval(1000)` | After 1000 ms | `1, 2, 3…` every 1000 ms | **No** (infinite) |
| `timer(3000)` | After 3000 ms | None | **Yes**, right after `0` |
| `timer(3000, 1000)` | After 3000 ms | `1, 2, 3…` every 1000 ms | **No** (infinite) |
| `timer(0, 1000)` | Immediately (0 ms delay) | `1, 2, 3…` every 1000 ms | **No** (infinite) |

## See it

```typescript
import { interval, timer } from 'rxjs';

// 1. One-shot delay: emits 0 after 2 seconds, then completes
timer(2000).subscribe({
  next: (n) => console.log('one-shot timer:', n),
  complete: () => console.log('timer(2000) completed'),
});

// 2. Polling clock: emits 0 immediately, then 1, 2, 3... every 5 seconds
const pollSub = timer(0, 5000).subscribe((tick) => {
  console.log('poll tick #', tick);
});

// 3. Standard interval: waits 1s first, then emits 0, 1, 2... every 1s
const intSub = interval(1000).subscribe((n) => console.log('interval:', n));

// Infinite clocks must be stopped explicitly:
setTimeout(() => {
  pollSub.unsubscribe();
  intSub.unsubscribe();
}, 6000);
```

## Go deeper

Notice how `timer(0, period)` solves a classic annoyance with `interval(period)`: when a user opens a dashboard, you usually want the first poll to fire **right now**, and then repeat every `period` ms. `interval(5000)` leaves the screen blank for 5 seconds before the first `0`; `timer(0, 5000)` fires `0` immediately and then ticks every 5 seconds.

## Watch out

- Both `interval(period)` and two-argument `timer(due, period)` run forever; leaving them subscribed without `unsubscribe()`, `take`, or `takeUntil` leaks a timer.
- Single-argument `timer(2000)` emits `0` and completes, whereas `interval(2000)` never completes.

## Remember

- `interval(period)` emits `0, 1, 2, …` every `period` ms (first value after one period) and never completes.
- Single-argument `timer(due)` emits `0` once after `due` ms and completes.
- Two-argument `timer(0, period)` starts immediately and repeats every `period` ms forever.
