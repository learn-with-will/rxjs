---
slug: concat-map
title: "Sequential queueing with concatMap()"
topic: flattening-higher-order
kind: api
summary: "concatMap(project) runs one inner Observable at a time in strict FIFO order, queueing subsequent outer values until the current inner Observable completes."
tags: [rxjs, concatmap, flattening, queue, sequential]
related: [what-is-a-higher-order-observable, merge-map, switch-map, choosing-a-flattening-operator]
---
> **In one line:** `concatMap(fn)` processes inner Observables **one at a time in strict sequential order**, waiting for the current inner stream to **complete** before starting the next.

## The idea

When order matters — for example, sending sequential `POST`/`PATCH` mutations to a server, writing steps of a multi-stage checkout, or playing animations one after another — running requests in parallel with `mergeMap` can cause race conditions, while cancelling with `switchMap` would drop data.

**`concatMap(project)`** acts like a single-lane checkout queue (equivalent to `mergeMap(project, 1)`):

1. When the first outer value arrives, `concatMap` immediately subscribes to its inner Observable.
2. If additional outer values arrive while that inner Observable is still running, `concatMap` **buffers them in an internal FIFO queue** without calling `project` yet.
3. Only when the active inner Observable **completes** does `concatMap` dequeue the next outer value and subscribe to its inner Observable.

```sequence
actor src: Outer Source | emits 1, 2 at t=0
actor cm: concatMap | single-lane FIFO queue @primary
actor in1: Inner #1 (200ms) | runs first @blue
actor in2: Inner #2 (50ms) | queued until #1 completes! @green
src -> cm: 1 (starts Inner #1)
cm -> in1: subscribe()
src -> cm: 2 (queued behind #1)
in1 --> cm: "Step 1" + complete() at 200ms
cm -> in2: subscribe() starts at 200ms
in2 --> cm: "Step 2" + complete() at 250ms
```

## See it

Compare this with the `mergeMap` example: even though request #2 only takes 50ms and request #1 takes 200ms, `concatMap` waits for #1 to finish before even starting #2:

```typescript
import { of, concatMap, delay } from 'rxjs';

const saveStep$ = (step: number, ms: number) =>
  of(`Saved step ${step} (${ms}ms)`).pipe(delay(ms));

of({ step: 1, ms: 200 }, { step: 2, ms: 50 }, { step: 3, ms: 100 })
  .pipe(
    concatMap((item) => saveStep$(item.step, item.ms)),
  )
  .subscribe((msg) => console.log(msg));
// Guaranteed strict sequential order:
// Saved step 1 (200ms)  <- at ~200ms
// Saved step 2 (50ms)   <- at ~250ms
// Saved step 3 (100ms)  <- at ~350ms
```

## Go deeper

Notice a subtle detail of `concatMap(project)`: for queued outer values, `project(value)` is called **when that item's turn arrives**, not when the outer value was first queued. That means if `project` reads a timestamp or produces a cold HTTP Observable, the request doesn't start until the previous inner Observable has completed.

What happens if an inner Observable inside `concatMap` **never completes** (for example, `concatMap(() => interval(1000))`)? The first inner stream runs forever, the queue behind it backs up in memory indefinitely, and no second inner stream ever starts!

For that reason, always make sure the inner stream you return from `concatMap` has a clear completion boundary — whether it is a finite HTTP call (`fromFetch`, `ajax`), a bounded stream (`take(1)`, `take(n)`), or an explicit `timeout(5000)` so a hung request cannot block the rest of the queue forever.

## Watch out

- Every inner Observable passed to `concatMap` **must eventually complete**, or the queue will stall forever on the first non-completing inner stream.
- If the outer source emits faster than inner streams complete over a long period, `concatMap`'s internal queue will grow without bound (backpressure).

## Remember

- `concatMap(fn)` runs one inner Observable at a time and queues incoming outer values in FIFO order.
- It never cancels in-flight inner streams and preserves strict 1-to-1 ordering.
- It is functionally identical to `mergeMap(fn, 1)`.
