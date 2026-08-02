---
id: lesson-20
slug: buffering-windowing-grouping
title: "Buffering, Windowing, and Grouping"
level: advanced
order: 20
duration: 28
tags:
  - rxjs
  - buffer
  - window
  - groupby
  - batching
---

# Learning Objectives

By the end of this lesson you will be able to:

- Batch emissions into arrays with `bufferTime`, `bufferCount`, and `buffer`.
- Slice a stream into child Observables with the `window*` operators.
- Explain the difference between buffering (arrays) and windowing (Observables).
- Partition a stream by key with `groupBy` and process each group.
- Avoid the `groupBy` memory pitfall with a duration/closing selector.

# Why This Concept Exists

Sometimes you don't want values one at a time — you want them in **batches** (send analytics every 5 seconds), in **fixed chunks** (write 100 rows at a time), collected **between events** (all mouse moves during a drag), or **partitioned by key** (separate streams per user). Buffering, windowing, and grouping are the operators that reshape *how many* values travel together and *how they're divided up*.

# Concept Explanation

### Buffering — collect into arrays

Buffer operators gather emissions and emit them as an **array**:

- **`bufferTime(ms)`** — emit an array of everything collected every `ms` milliseconds.
- **`bufferCount(n, startEvery?)`** — emit an array once `n` values accumulate; `startEvery` enables overlapping/sliding buffers.
- **`buffer(closing$)`** — emit the buffer whenever `closing$` emits, then start a new buffer.

```text
source:          --1-2-3-4-5-|
bufferCount(2):  ----[1,2]--[3,4]--([5]|)   (leftover [5] flushed on complete)
```

```ts
import { interval, bufferCount, take } from 'rxjs';

interval(100).pipe(take(5), bufferCount(2)).subscribe(console.log);
// [0, 1] / [2, 3] / [4]   ([4] is the partial buffer flushed at completion)
```

### Windowing — slice into Observables

Window operators mirror the buffer operators but emit a **child Observable** (a "window") instead of an array: `windowTime`, `windowCount`, `window(closing$)`, `windowToggle`, `windowWhen`. Because each window is an Observable, you can apply operators **within** each slice before it closes — then flatten.

```ts
import { interval, windowCount, mergeMap, reduce, take } from 'rxjs';

interval(100)
  .pipe(
    take(6),
    windowCount(3),                       // higher-order: Observable of windows
    mergeMap((win$) => win$.pipe(reduce((a, b) => a + b, 0))), // sum each window
  )
  .subscribe(console.log); // 3 (0+1+2), 12 (3+4+5)
```

Buffer gives you the collected values as data; window gives you a live sub-stream to keep processing.

### Grouping — partition by key

`groupBy(keySelector)` splits a source into many **GroupedObservables**, one per distinct key. Each grouped Observable carries a `.key` and emits only the values for that key. You almost always flatten the groups (e.g., with `mergeMap`) and apply a terminating operator per group.

```ts
import { from, groupBy, mergeMap, toArray } from 'rxjs';

const people = [
  { name: 'Ada', team: 'A' },
  { name: 'Alan', team: 'B' },
  { name: 'Grace', team: 'A' },
];

from(people)
  .pipe(
    groupBy((p) => p.team),
    mergeMap((group$) => group$.pipe(toArray())), // collect each team
  )
  .subscribe(console.log);
// [{Ada,A},{Grace,A}] and [{Alan,B}]
```

### The `groupBy` memory pitfall

By default a group stays alive as long as the source runs. On a long/infinite stream with **unbounded keys**, groups accumulate and never release — a leak. Provide a **duration selector** (a function returning an Observable that, when it emits, closes that group) to expire idle groups:

```ts
import { groupBy, timer } from 'rxjs';

source$.pipe(
  groupBy((x) => x.key, { duration: () => timer(60_000) }), // close each group after 60s
);
```

# Key Terminology

- **Buffer** — a batch of collected emissions delivered as an array.
- **`bufferTime` / `bufferCount` / `buffer`** — batch by time / count / a closing notifier.
- **Window** — a child Observable representing a slice of the source.
- **`window*`** — the windowing counterparts of the buffer operators.
- **`groupBy`** — partitions a source into GroupedObservables keyed by a selector.
- **GroupedObservable** — an Observable with a `.key`, emitting one group's values.
- **Duration selector** — an Observable that closes a group/window when it emits.

# Syntax Overview

```ts
import { bufferTime, bufferCount, buffer, windowTime, groupBy, mergeMap, toArray } from 'rxjs';

source$.pipe(bufferTime(5000));                 // array every 5s
source$.pipe(bufferCount(100));                 // array every 100 values
source$.pipe(buffer(clicks$));                  // array on each click
source$.pipe(windowTime(5000), mergeMap((w) => w.pipe(/* per-window ops */)));
source$.pipe(
  groupBy((x) => x.type),
  mergeMap((g) => g.pipe(toArray())),           // process each group
);
```

# Real World Analogy

Imagine sorting incoming **mail**. **Buffering** is dropping letters into a tray and, every hour (or every 50 letters), carrying the whole tray upstairs at once — you hand over a *stack* (an array). **Windowing** is the same rhythm, but instead of a static stack you hand over a *live conveyor segment* that's still moving, so you can keep sorting the letters *within* that segment before it ends. **Grouping** is the wall of pigeonholes: each letter is filed by recipient (key) into its own slot, turning one mixed pile into many per-person streams — just remember to clear out pigeonholes for people who've moved away, or the wall fills up forever.

# Examples

## Example 1 — Basic: fixed-size batches with bufferCount

```ts
import { from, bufferCount } from 'rxjs';

from([1, 2, 3, 4, 5, 6, 7])
  .pipe(bufferCount(3))
  .subscribe(console.log);
// [1, 2, 3] / [4, 5, 6] / [7]   (final partial batch flushed on complete)
```

**Why this is good:** It's the clearest batching case — turn a stream of items into fixed-size chunks, ideal for paginated writes or bulk API calls. Note the trailing partial batch is emitted at completion.

## Example 2 — Real-world: batch analytics events every few seconds

```ts
import { Subject, bufferTime, filter } from 'rxjs';

const events$ = new Subject<{ type: string }>();

events$
  .pipe(
    bufferTime(5000),                 // collect a batch every 5 seconds
    filter((batch) => batch.length > 0), // skip empty windows
  )
  .subscribe((batch) => sendToServer(batch));

function sendToServer(batch: unknown[]) {
  /* one network call per batch instead of per event */
}
```

**Why this is good:** Instead of a network request per event, `bufferTime` groups them into periodic batches — far fewer requests. The `filter` drops the empty arrays `bufferTime` emits during quiet periods.

## Example 3 — Best practice: aggregate per key with groupBy

```ts
import { from, groupBy, mergeMap, reduce } from 'rxjs';

const sales = [
  { region: 'EU', amount: 100 },
  { region: 'US', amount: 200 },
  { region: 'EU', amount: 50 },
  { region: 'US', amount: 25 },
];

from(sales)
  .pipe(
    groupBy((s) => s.region),
    mergeMap((group$) =>
      group$.pipe(reduce((total, s) => total + s.amount, 0)).pipe(
        // pair the total back with its key
        (obs) => obs, // (illustrative) totals: EU 150, US 225
      ),
    ),
  )
  .subscribe(console.log); // 150, 225
```

**Why this is good:** `groupBy` + `mergeMap` + a terminating operator (`reduce`) is the canonical aggregation pattern — partition by key, then fold each partition. Because the source completes, each group completes and emits its total.

# Common Mistakes

- **Not flattening windows or groups.** They emit **Observables**, not values. **Fix:** `mergeMap` (or `switchMap`/`concatMap`) into them and apply a terminating operator.
- **`groupBy` leaking on unbounded keys.** Groups never close on a long stream. **Fix:** pass a `duration` selector to expire idle groups.
- **Confusing buffer with window.** Buffer → array of collected values; window → live child Observable. **Fix:** pick buffer for batches of data, window to keep processing each slice.
- **Forgetting `bufferTime` emits empty arrays.** Quiet periods still emit `[]`. **Fix:** `filter(batch => batch.length > 0)`.
- **Assuming the last partial batch is dropped.** `bufferCount` flushes the remainder on completion. **Fix:** handle the possibly-smaller final batch.

# Best Practices

- Use **`bufferTime`/`bufferCount`** to reduce the *number* of downstream operations (batched writes, throttled network calls).
- Use **`window*`** when you need to run an operator pipeline **within** each slice before it closes.
- Follow **`groupBy`** with `mergeMap` and a **terminating** inner operator (`toArray`, `reduce`) so groups complete.
- Always **bound `groupBy` memory** with a duration selector on long-lived streams.
- Filter out **empty** buffers/windows when they'd trigger needless work.

# Summary

- **Buffer** operators batch emissions into **arrays**: `bufferTime` (by time), `bufferCount` (by count), `buffer` (by a closing notifier).
- **Window** operators do the same but emit **child Observables**, letting you process each slice before it closes.
- **`groupBy`** partitions a stream into **GroupedObservables** keyed by a selector; flatten and fold each group.
- Windows and groups are **Observables** — you must flatten them (e.g., `mergeMap`).
- `groupBy` can **leak** on unbounded keys; bound it with a **duration** selector.
- `bufferTime` emits **empty arrays** during quiet periods; `bufferCount` flushes a **partial** final batch on completion.

# Flash Cards

Q: What is the difference between buffering and windowing?
A: Buffering collects emissions and emits them as an array; windowing emits a child Observable (a live slice) so you can apply operators within each window before flattening.

Q: What does `bufferCount(3)` emit, including at completion?
A: Arrays of 3 collected values; when the source completes, it flushes any remaining partial buffer (e.g., a final array of 1 or 2 items).

Q: What does `groupBy` emit, and what property identifies each group?
A: It emits GroupedObservables — one per distinct key — and each carries a `.key` property identifying the group.

Q: Why can `groupBy` leak memory, and how do you prevent it?
A: On a long stream with unbounded keys, groups stay alive indefinitely. Provide a `duration` selector that closes each group after a period or condition.

Q: After `groupBy`, why do you usually add `mergeMap` with `toArray` or `reduce`?
A: Because each group is an Observable; you flatten it and apply a terminating operator to aggregate the group's values into a result.

Q: What surprising value does `bufferTime` emit during a quiet period?
A: An empty array `[]` for each interval with no values — filter with `batch.length > 0` if you only want non-empty batches.

# Exercises

### Easy

Take `from([1,2,3,4,5,6,7,8])` and batch it into arrays of 3 with `bufferCount(3)`. Log the batches and confirm the final batch contains the leftover `[7, 8]`.

### Medium

Create a `Subject<string>` of "events" and batch them every 2 seconds with `bufferTime(2000)`, filtering out empty batches. Push a few events, wait, push more, and confirm each non-empty batch logs together. Explain why the `filter` is needed.

### Challenging

Given a stream of `{ userId, action }` objects, use `groupBy(x => x.userId)` and `mergeMap` with `toArray` to collect each user's actions. Then convert it to an infinite stream and add a `duration: () => timer(10000)` to `groupBy`; explain in a comment how that prevents unbounded memory growth and how it changes the per-group output.

# Further Reading

- [RxJS — bufferTime](https://rxjs.dev/api/index/function/bufferTime) and [bufferCount](https://rxjs.dev/api/index/function/bufferCount)
- [RxJS — buffer](https://rxjs.dev/api/index/function/buffer) and [window](https://rxjs.dev/api/index/function/window)
- [RxJS — groupBy](https://rxjs.dev/api/index/function/groupBy)
- [RxJS — GroupedObservable](https://rxjs.dev/api/index/interface/GroupedObservable)
