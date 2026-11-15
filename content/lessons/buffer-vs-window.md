---
slug: buffer-vs-window
title: "Choosing between buffer* and window* operators"
topic: custom-operators-batching
kind: concept
summary: "Decide when to batch emissions into a T[] array with buffer* versus streaming each batch live as an Observable<T> with window*."
tags: [rxjs, buffer, window, batching, comparison]
related: [buffer-time-count-and-buffer, window-operators, group-by-operator]
---
> **In one line:** choose **`buffer*`** when you want the whole batch as an array `T[]` at the **end** of the period; choose **`window*`** when you need to react to, cap, or short-circuit items **live during** the period.

## The idea

Because every `buffer*` operator can technically be replicated with `window*().pipe(mergeMap(w$ => w$.pipe(toArray())))`, how do you decide which family to use?

| Question | Choose `buffer*` (`Observable<T[]>`) | Choose `window*` (`Observable<Observable<T>>`) |
|---|---|---|
| **When do downstream operators see the items?** | Only **at the end** when the buffer closes and flushes the `T[]` array | **Immediately as each item arrives** inside the open inner `window$` |
| **Can you stop collecting early inside a batch?** | Only via `bufferTime(ms, null, maxSize)` | Yes — pipe `take(N)`, `takeWhile()`, or `first()` on the inner `window$` |
| **Memory footprint for huge batches (e.g., 100,000 items)** | Holds all 100,000 items in a `T[]` array in RAM until flush | Can aggregate incrementally via `window$.pipe(reduce(...))` with O(1) memory! |
| **Complexity** | Simpler (flat `Observable<T[]>`, no flattening operator needed) | Higher-order (`Observable<Observable<T>>`, requires `mergeMap`/`switchMap`) |

> **Tip:** Default to **`buffer*`** whenever you are sending an array payload to an API (like `POST /api/logs` with `logs: LogEntry[]`). Reach for **`window*`** only when you need live emissions during the window or O(1) streaming reduction over huge windows.

## See it

Compare calculating the sum of every 1-second window using `bufferTime` (allocates an array of all items) vs `windowTime` (accumulates a single number in O(1) memory):

```typescript
import { interval, take, bufferTime, map, windowTime, mergeMap, reduce } from 'rxjs';

const source$ = interval(100).pipe(take(10));

// Approach A: bufferTime collects an array [0, 1, 2, ...] in memory, then sums it at the end
source$.pipe(
  bufferTime(350),
  map(arr => arr.reduce((sum, n) => sum + n, 0))
).subscribe(sum => console.log('bufferTime sum:', sum));

// Approach B: windowTime + reduce accumulates the sum incrementally in O(1) memory!
source$.pipe(
  windowTime(350),
  mergeMap(win$ => win$.pipe(reduce((sum, n) => sum + n, 0)))
).subscribe(sum => console.log('windowTime sum:', sum));
```

## Go deeper

Why is `window$` in `windowTime` a hot stream? Because the source Observable is already running and pushing values into the active window's internal `Subject`. If you subscribe to `win$` synchronously inside `mergeMap(win$ => ...)`, you catch every single emission for that window from the moment it opens.

## Watch out

- Do not use `windowTime` + `toArray()` when `bufferTime` does the exact same thing with less overhead and simpler code.
- If you use `windowCount(3, 1)` (overlapping windows) and flatten with `switchMap`, each new window immediately cancels the previous overlapping window! Use `mergeMap` when windows overlap.

## Remember

- Use `buffer*` when your consumer expects a `T[]` batch at the end of each count or time interval.
- Use `window*` when you must emit items live during the window (like `take(N)` quota limiting) or fold massive windows in O(1) memory with `reduce`.
- Flatten non-overlapping windows with `mergeMap` or `concatMap`, and overlapping windows with `mergeMap`.
