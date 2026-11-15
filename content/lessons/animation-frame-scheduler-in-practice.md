---
slug: animation-frame-scheduler-in-practice
title: "Smooth 60fps UI updates with animationFrameScheduler and animationFrames()"
topic: schedulers-testing
kind: technique
summary: "Use animationFrames() or auditTime(0, animationFrameScheduler) to align high-frequency DOM updates with the browser's requestAnimationFrame repaint cycle."
tags: [rxjs, animationframescheduler, animationframes, dom, performance]
related: [built-in-schedulers, throttle-time, observe-on-vs-subscribe-on]
since: "7.0"
---
> **In one line:** use **`animationFrames()`** (RxJS 7+) for smooth time-driven animations and **`auditTime(0, animationFrameScheduler)`** to coalesce high-frequency pointer/scroll events into **at most one DOM write per browser repaint**.

## The idea

On modern 60Hz–120Hz displays, `mousemove`, `wheel`, or `touchmove` events can fire hundreds of times per second — multiple times inside a single 16.6ms display frame! Updating DOM styles or canvas elements more than once per browser repaint wastes CPU work on intermediate frames the user's monitor never even displays.

RxJS gives you two tools tied directly to the browser's `requestAnimationFrame` loop:

1. **Coalescing noisy events to 1 per frame:** piping `fromEvent(document, 'mousemove')` through `auditTime(0, animationFrameScheduler)` (or ` observeOn(animationFrameScheduler)`) waits for the browser's next repaint frame and delivers the latest coordinates right before paint.
2. **`animationFrames()` creation function (RxJS 7+):** creates a cold Observable that emits `{ timestamp, elapsed }` on every `requestAnimationFrame` tick, where `elapsed` is the exact milliseconds elapsed since subscription started — and automatically cancels `cancelAnimationFrame` on unsubscribe!

| Tool | What it does | Best for |
|---|---|---|
| **`animationFrames()`** | Emits `{ timestamp, elapsed }` on every `requestAnimationFrame` | Smooth tweening, progress bars, physics loops |
| **`auditTime(0, animationFrameScheduler)`** | Emits at most once per repaint frame with the latest value | Dragging, scroll parallax, canvas cursor tracking |

## See it

Here are both patterns in action — first a smooth 500ms progress animation from `0%` to `100%` using `animationFrames()`, and second frame-throttled pointer tracking:

```typescript
import {
  animationFrames, animationFrameScheduler, fromEvent, map, takeWhile, endWith, auditTime,
} from 'rxjs';

// 1. Smooth 500ms progress animation from 0 to 1 using animationFrames() (RxJS 7+):
animationFrames()
  .pipe(
    map(({ elapsed }) => Math.min(1, elapsed / 500)),
    takeWhile((progress) => progress < 1),
    endWith(1), // Guarantee the final frame lands on 100% (1.0)
  )
  .subscribe((p) => console.log(`Progress: ${Math.round(p * 100)}%`));

// 2. Coalesce rapid mousemove events to at most ONE update per animation frame:
fromEvent<MouseEvent>(document, 'mousemove')
  .pipe(
    auditTime(0, animationFrameScheduler),
    map((e) => ({ x: e.clientX, y: e.clientY })),
  )
  .subscribe(({ x, y }) => console.log('Paint cursor at:', x, y));
```

## Go deeper

Look at the `animationFrames()` progress recipe:
- Because frame intervals vary slightly (e.g. 16.4ms, 16.8ms, or 8.3ms on a 120Hz ProMotion screen), calculating `elapsed / durationMs` from `animationFrames()` makes the animation **frame-rate independent** — it always finishes in 500ms whether the screen runs at 60Hz or 144Hz.
- Adding `endWith(1)` after `takeWhile(p => p < 1)` guarantees that when `elapsed` jumps from `492ms` (`0.984`) to `508ms` (`1.0`), the element cleanly snaps to its exact `1.0` (100%) final position before completing!

## Watch out

- `animationFrames()` and `animationFrameScheduler` rely on `requestAnimationFrame`, which browsers **pause in background tabs** to save battery — do not use them for non-visual background timers or network timeouts.
- Remember that `animationFrames()` is an infinite stream unless bounded with `takeWhile`, `takeUntil`, or `unsubscribe()`.

## Remember

- Use `animationFrames()` in RxJS 7+ to drive smooth, frame-rate-independent animations using `{ elapsed, timestamp }`.
- Chain `takeWhile(p => p < 1)` and `endWith(1)` so time-bounded animations always land on 100% and complete.
- Use `auditTime(0, animationFrameScheduler)` to cap high-frequency pointer or scroll streams to one emission per browser repaint.
