---
slug: window-operators
title: "Splitting a stream into inner streams: windowTime, windowCount, and window"
topic: custom-operators-batching
kind: api
summary: "Split a flat Observable<T> into nested Observable<Observable<T>> time or count windows so you can process items live as they arrive."
tags: [rxjs, window, windowtime, windowcount, higher-order]
related: [buffer-time-count-and-buffer, buffer-vs-window, what-is-a-higher-order-observable]
---
> **In one line:** while `buffer*` waits until the end of a window to emit a finished `T[]` array, `window*` immediately emits a live inner `Observable<T>` at the **start** of each window so downstream operators can react to items as they arrive.

## The idea

Every `buffer*` operator has an exact twin in the `window*` family with identical arguments:
- `window(closingNotifier$)` ↔ `buffer(closingNotifier$)`
- `windowCount(windowSize, startWindowEvery?)` ↔ `bufferCount(bufferSize, startBufferEvery?)`
- `windowTime(windowTimeSpan, windowCreationInterval?, maxWindowSize?)` ↔ `bufferTime(...)`
- `windowToggle(openings$, closingSelector)` ↔ `bufferToggle(openings$, closingSelector)`

Because `window*` emits an inner `Observable<T>` (creating a higher-order `Observable<Observable<T>>`), you always pair it with a flattening operator like `mergeMap`, `switchMap`, or `concatMap` to apply operators (`count()`, `take(2)`, `scan()`) to each individual window!

```sequence
actor src: Clicks | Observable<Event> @blue
actor win: windowTime(1000) | higher-order @primary
actor flat: mergeMap(win$ => ...) | flattens windows @green
win -> flat: 0ms: emit inner Window #1 (Observable<Event>)
src -> win: 200ms: click
src -> win: 600ms: click
win -> flat: 1000ms: Window #1 completes -> count() emits 2!
win -> flat: 1000ms: emit inner Window #2 (Observable<Event>)
```

## See it

Use `windowTime(1000)` to allow at most **2 clicks per second** (emitting those first 2 clicks immediately as they happen, rather than waiting until the end of the 1-second window!):

```typescript
import { interval, take, windowTime, windowCount, mergeMap, toArray } from 'rxjs';

// Simulated rapid clicks every 200ms:
const rapidClicks$ = interval(200).pipe(take(10));

// 1. Open a new 1000ms window every second, and inside each window
// immediately forward at most the first 2 clicks:
rapidClicks$.pipe(
  windowTime(1000),
  mergeMap(window$ => window$.pipe(take(2)))
).subscribe(n => console.log('Allowed immediately:', n));
// 0..1000ms window: emits 0 (at 200ms), 1 (at 400ms); drops 2, 3
// 1000..2000ms window: emits 4 (at 1000ms), 5 (at 1200ms); drops 6, 7...

// 2. Split a stream into windows of 3 items and inspect each window:
rapidClicks$.pipe(
  windowCount(3),
  mergeMap(win$ => win$.pipe(toArray()))
).subscribe(group => console.log('Window of 3:', group));
```

## Go deeper

Look closely at what `windowTime(1000).pipe(mergeMap(w$ => w$.pipe(take(2))))` achieved above:
1. Click `0` at `200ms` and click `1` at `400ms` were emitted **immediately** at `200ms` and `400ms` without waiting for the 1-second timer to finish!
2. Clicks `2` and `3` in the rest of that 1-second window were ignored because the inner `take(2)` completed early and unsubscribed from Window #1.
3. At `1000ms`, a fresh window opened and allowed 2 more clicks.
You could **never** build that live rate-limiter with `bufferTime(1000)`, because `bufferTime(1000)` holds everything in an array until `1000ms` has elapsed before emitting anything downstream!

## Watch out

- Never subscribe to `window*()` without flattening or subscribing to the inner `Observable<T>` streams — unconsumed inner window Subjects sit in memory until the window closes.
- If your windows do not overlap (standard tumbling `windowTime(1000)` or `windowCount(5)`), `mergeMap`, `concatMap`, and `switchMap` all behave identically because only one window is active at a time.

## Remember

- `window*` transforms `Observable<T>` into `Observable<Observable<T>>`, emitting each inner stream as soon as the window opens.
- Pair `window*` with `mergeMap(win$ => win$.pipe(...))` to process, cap, or aggregate each window live.
- Use `windowTime(ms)` + `mergeMap(w$ => w$.pipe(take(maxPerWindow)))` for quota-per-interval rate limiting.
