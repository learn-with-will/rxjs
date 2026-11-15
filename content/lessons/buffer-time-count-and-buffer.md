---
slug: buffer-time-count-and-buffer
title: "Batching into arrays: bufferTime, bufferCount, and buffer"
topic: custom-operators-batching
kind: api
summary: "Collect rapid individual emissions into T[] array batches by time interval, item count, sliding window, or external closing signal."
tags: [rxjs, buffertime, buffercount, buffer, batching]
related: [window-operators, buffer-vs-window, scan-running-accumulator]
---
> **In one line:** the `buffer*` family collects incoming `T` items into an in-memory array and emits that `T[]` batch whenever a count threshold, time interval, or closing Observable fires.

## The idea

Instead of making 50 separate HTTP requests or 50 DOM updates for 50 rapid events, batching operators collect individual `T` emissions and emit `T[]` arrays downstream:

| Operator | When it flushes a `T[]` array | Key feature / parameter |
|---|---|---|
| **`bufferCount(size, startEvery?)`** | Every `size` items (and any partial batch on `complete`) | Omit `startEvery` for tumbling batches; pass `startEvery < size` for overlapping sliding windows (e.g., `bufferCount(3, 1)`) |
| **`bufferTime(ms, interval?, maxSize?)`** | Every `ms` milliseconds | Pass `maxSize` (e.g., `bufferTime(2000, null, 50)`) to flush early if 50 items arrive before 2 seconds! |
| **`buffer(closingNotifier$)`** | Whenever `closingNotifier$` emits | Event-driven batching (e.g., collect edits until the user clicks Save or the browser goes online) |
| **`bufferToggle(openings$, closeSelector)`** | Opens a buffer on `openings$`, closes when `closeSelector(openVal)` emits | Capturing events only during active windows (e.g., mouse movements between `mousedown` and `mouseup`) |

> **Tip:** `bufferTime(2000, null, 50)` combined with `filter(batch => batch.length > 0)` is the gold-standard pattern for telemetry and analytics batching: flush every 2 seconds **or** every 50 events, whichever comes first, and skip empty intervals!

## See it

Use `bufferTime` with a max batch size for log flushing, and `bufferCount(3, 1)` for a 3-item sliding window:

```typescript
import { of, interval, take, bufferCount, bufferTime, filter } from 'rxjs';

// 1. Tumbling batches of 2 vs sliding window of 3 (stepping by 1):
of(1, 2, 3, 4, 5).pipe(bufferCount(2)).subscribe(console.log);
// Output: [1, 2], [3, 4], [5]

of(1, 2, 3, 4).pipe(bufferCount(3, 1)).subscribe(console.log);
// Output: [1, 2, 3], [2, 3, 4], [3, 4], [4]

// 2. Flush every 500ms OR whenever 3 items accumulate (skip empty batches):
interval(150).pipe(
  take(7),
  bufferTime(500, null, 3),
  filter(batch => batch.length > 0)
).subscribe(batch => console.log('Flushed batch:', batch));
```

## Go deeper

Notice the trailing partial arrays at the end of `of(1, 2, 3, 4).pipe(bufferCount(3, 1))`: `[3, 4]` and `[4]`. Why did `bufferCount` emit arrays smaller than 3 at the end? Because when the upstream source completes, `bufferCount` flushes all currently open buffers! If you only want full 3-element windows, pipe `filter(batch => batch.length === 3)` right after `bufferCount(3, 1)`. And notice that `bufferCount(2, 1)` (filtered to length 2) is identical to `pairwise()`!

## Watch out

- `bufferTime(ms)` emits an empty array `[]` on every timer tick if no items arrived during that interval — always follow it with `filter(batch => batch.length > 0)` before sending a network request.
- Calling `buffer(never())` or `bufferCount(Infinity)` accumulates items in memory forever without flushing.

## Remember

- `buffer*` operators transform `Observable<T>` into `Observable<T[]>`.
- Use `bufferTime(ms, null, maxBatchSize)` + `filter(b => b.length > 0)` for time-or-size bounded telemetry flushing.
- Use `bufferCount(size, 1)` for sliding windows of `size` consecutive items.
