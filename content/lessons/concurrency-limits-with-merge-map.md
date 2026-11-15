---
slug: concurrency-limits-with-merge-map
title: "Controlling parallel concurrency with mergeMap(fn, concurrent) and mergeAll(concurrent)"
topic: flattening-higher-order
kind: technique
summary: "Pass a concurrency number to mergeMap(project, concurrent) or mergeAll(concurrent) to run at most N inner Observables in parallel and queue the rest."
tags: [rxjs, mergemap, mergeall, concurrency, queue]
related: [merge-map, concat-map, all-flattening-operators]
---
> **In one line:** passing a **`concurrent`** number to `mergeMap(fn, N)` or `mergeAll(N)` runs **at most `N` inner Observables in parallel** and queues any extra arrivals in FIFO order until a slot frees up.

## The idea

Suppose a user selects 50 photos to upload, or you need to fetch details for 100 product IDs:

- **`mergeMap(upload$)`** (default `concurrent = Infinity`) fires **all 50 uploads at the exact same millisecond**, saturating browser sockets and triggering rate-limit errors!
- **`concatMap(upload$)`** (`concurrent = 1`) uploads **1 photo at a time**, taking 50x as long as a single upload.
- **`mergeMap(upload$, 3)`** is the sweet spot: it starts the first **3** uploads immediately, queues the remaining 47, and each time one of the 3 active uploads completes, immediately starts the next queued upload so **at most 3 are ever in flight at once**!

| Call | Max active inner streams | What happens when `N + 1` arrives |
|---|---|---|
| `mergeMap(fn)` / `mergeAll()` | `Infinity` (unbounded) | Subscribed immediately in parallel |
| `mergeMap(fn, 3)` / `mergeAll(3)` | **`3`** (bounded worker pool) | Queued in FIFO order until 1 of the 3 completes |
| `mergeMap(fn, 1)` / `mergeAll(1)` | **`1`** (single-lane queue) | Identical to **`concatMap(fn)`** / **`concatAll()`**! |

## See it

Here we process 5 files with a concurrency pool of **2 workers** (`mergeMap(uploadFile$, 2)`):

```typescript
import { from, of, delay, tap, mergeMap } from 'rxjs';

let inFlight = 0;

const uploadFile$ = (fileId: number) =>
  of(`File #${fileId}`).pipe(
    tap(() => console.log(`Start #${fileId} (inFlight: ${++inFlight})`)),
    delay(100),
    tap(() => console.log(`Done  #${fileId} (inFlight: ${--inFlight})`)),
  );

from([1, 2, 3, 4, 5])
  .pipe(
    mergeMap((id) => uploadFile$(id), 2), // Max 2 uploads in flight at once!
  )
  .subscribe();
// At 0ms:   Start #1 (inFlight: 1), Start #2 (inFlight: 2)
// At 100ms: Done #1, Start #3 (inFlight: 2), Done #2, Start #4 (inFlight: 2)
// At 200ms: Done #3, Start #5 (inFlight: 2), Done #4 (inFlight: 1)
// At 300ms: Done #5 (inFlight: 0)
```

## Go deeper

Look at the signature of `mergeMap`:

- In RxJS 7+, the second parameter of `mergeMap(project, concurrent)` is the numeric concurrency limit (the old 3-argument form with a `resultSelector` function in the middle is deprecated).
- Even though queued items start in FIFO order as worker slots open up, because `N` items run in parallel, a fast item in slot #2 can still finish before a slow item in slot #1 — so **output emissions arrive in completion order**, not strict input index order.

## Watch out

- Never use unbounded `mergeMap(fn)` over a large array `from(hundredsOfIds)` when `fn` makes network or disk calls — always pass a concurrency limit like `mergeMap(fn, 4)`.
- If you need `N` requests to run in parallel **and** emit their results in strict input order, `mergeMap(fn, N)` alone does not preserve output order when response times vary.

## Remember

- `mergeMap(fn, concurrent)` and `mergeAll(concurrent)` cap parallel inner subscriptions at `concurrent`.
- Extra items wait in a FIFO queue and start automatically as active inner streams complete.
- `mergeMap(fn, 1)` is identical to `concatMap(fn)`, and `mergeAll(1)` is identical to `concatAll()`.
