---
slug: audit-time-and-sample-time
title: "Trailing and periodic sampling: auditTime() and sampleTime()"
topic: rate-limiting-time
kind: api
summary: "auditTime(ms) emits the latest value at the end of a timer started on first emission; sampleTime(ms) samples the latest new value on a fixed clock."
tags: [rxjs, audittime, sampletime, rate-limiting, time]
related: [throttle-time, debounce-time, debounce-vs-throttle-choice]
---
> **In one line:** `auditTime(ms)` starts a `ms` window when a value arrives and emits the **latest value at the end of that window**, while `sampleTime(ms)` ticks on a fixed global clock every `ms` and emits the latest value since the previous tick.

## The idea

Alongside `debounceTime` and `throttleTime`, RxJS provides two more time-window operators for sampling noisy streams:

- **`auditTime(duration)`** is the mirror image of default `throttleTime`. When a value arrives, `auditTime` does **not** emit immediately; instead it starts a `duration`-ms timer. While that timer runs, it keeps updating its saved "latest value". When the timer expires, it emits the **most recent value** from that window and goes idle until the next source emission starts a new window. (It behaves like `throttleTime` with `{ leading: false, trailing: true }`.)
- **`sampleTime(period)`** runs a **fixed, repeating interval timer** every `period` milliseconds from the moment you subscribe. On each tick of that clock, if the source has emitted at least one new value since the previous tick, `sampleTime` emits the most recent value.

| Operator | When does the timer start? | What does it emit? |
|---|---|---|
| **`throttleTime(ms)`** *(default)* | On the first source emission after idle | The **first** value in the window (at the start) |
| **`auditTime(ms)`** | On the first source emission after idle | The **last** value in the window (at the end) |
| **`sampleTime(ms)`** | Immediately on `subscribe()` (fixed periodic clock) | The latest new value at each fixed `ms` tick |

## See it

```typescript
import { fromEvent, auditTime, sampleTime, map } from 'rxjs';

// 1. auditTime(250): when mouse moves, wait 250ms and emit the latest coordinates
fromEvent<MouseEvent>(document, 'mousemove')
  .pipe(
    auditTime(250),
    map((e) => ({ x: e.clientX, y: e.clientY })),
  )
  .subscribe((pos) => console.log('audited pos:', pos));

// 2. sampleTime(1000): every 1s on a fixed clock, emit the latest mouse position (if moved)
fromEvent<MouseEvent>(document, 'mousemove')
  .pipe(
    sampleTime(1000),
    map((e) => ({ x: e.clientX, y: e.clientY })),
  )
  .subscribe((pos) => console.log('1s sample:', pos));
```

## Go deeper

What happens if the source **completes** while `auditTime(250)` is in the middle of its 250ms timer with a pending value? Unlike `debounceTime` (which flushes its pending value on source completion), `auditTime` **discards** the un-expired window and completes immediately without emitting that trailing value.

> **Note:** Both operators also have notifier-based siblings: `audit(value => notifier$)` and `sample(notifier$)`, which close their window or take a snapshot whenever another Observable emits instead of on a millisecond timer.

## Watch out

- If the source completes before `auditTime`'s timer expires, the pending value in that window is **not** emitted.
- `sampleTime(ms)` only emits on a tick if at least one **new** source value arrived since the last tick — it never re-emits a stale value across silent ticks.

## Remember

- `auditTime(ms)` starts a `ms` timer on the first emission after silence and emits the latest value when that timer ends.
- `sampleTime(ms)` samples the source on a fixed periodic clock every `ms` milliseconds.
- Unlike `debounceTime`, neither `auditTime` nor `sampleTime` resets its timer when new values arrive inside the window.
