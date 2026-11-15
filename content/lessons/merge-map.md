---
slug: merge-map
title: "Parallel flattening with mergeMap()"
topic: flattening-higher-order
kind: api
summary: "mergeMap(project, concurrent?) subscribes to every inner Observable in parallel and emits their values as they arrive, without cancelling or queueing."
tags: [rxjs, mergemap, flatmap, flattening, concurrency]
related: [what-is-a-higher-order-observable, concat-map, switch-map, concurrency-limits-with-merge-map]
---
> **In one line:** `mergeMap(fn)` (also aliased as `flatMap`) subscribes to **every inner Observable immediately in parallel** and forwards values downstream in whatever order they arrive.

## The idea

When every outer emission represents independent work that should run **concurrently** without cancelling earlier work or waiting in line — such as fetching details for several independent item IDs or logging analytics events — **`mergeMap(project, concurrent?)`** is the tool.

Here is how `mergeMap` handles overlapping inner streams:

1. Each time the outer source emits `x`, `mergeMap` calls `project(x)` and **immediately subscribes** to the returned inner Observable.
2. Any already-running inner Observables **keep running** alongside the new one.
3. Whenever *any* active inner Observable emits a value, `mergeMap` immediately forwards it downstream.
4. The output stream completes only when the **outer** source has completed **and every active inner** Observable has also completed.

```sequence
actor src: Outer Source | emits 1, then 2
actor mm: mergeMap | runs inner streams in parallel @primary
actor in1: Inner #1 (slow: 200ms) | @blue
actor in2: Inner #2 (fast: 50ms) | @green
src -> mm: 1
mm -> in1: subscribe()
src -> mm: 2
mm -> in2: subscribe() (Inner #1 keeps running!)
in2 --> mm: "Result 2" (finishes first at 50ms)
in1 --> mm: "Result 1" (finishes second at 200ms)
```

## See it

Notice how fast inner stream #2 finishes and emits *before* slow inner stream #1, because both run concurrently:

```typescript
import { of, mergeMap, delay } from 'rxjs';

const fetchItem$ = (id: number, ms: number) =>
  of(`Item ${id} (took ${ms}ms)`).pipe(delay(ms));

of({ id: 1, ms: 200 }, { id: 2, ms: 50 }, { id: 3, ms: 100 })
  .pipe(
    mergeMap((req) => fetchItem$(req.id, req.ms)),
  )
  .subscribe((res) => console.log(res));
// Output order (by completion speed, not input order!):
// Item 2 (took 50ms)
// Item 3 (took 100ms)
// Item 1 (took 200ms)
```

## Go deeper

Because `mergeMap` never cancels in-flight inner streams and does not wait for them to finish in order, two things can happen if you aren't careful:

1. **Out-of-order responses:** if request #1 takes 200ms and request #2 takes 50ms, response #2 arrives first and response #1 arrives later, potentially overwriting newer UI state with stale data.
2. **Unbounded concurrency:** if the outer source emits 1,000 items synchronously, `mergeMap(fn)` will launch all 1,000 inner subscriptions at the same instant unless you pass a **`concurrent` limit** as its second argument: `mergeMap(fn, 4)`.

> **Note:** `flatMap` is an exact alias for `mergeMap` in RxJS; prefer the name `mergeMap` so it clearly pairs with `merge` and `mergeAll`.

## Watch out

- Do not use `mergeMap` for search autocomplete or route-parameter loads where an older slow request could resolve after a newer request and overwrite the screen.
- If an inner Observable inside `mergeMap` never completes (such as an unbounded `interval` or `fromEvent`), every outer emission adds another permanent inner subscription and leaks memory!

## Remember

- `mergeMap(fn)` subscribes to every inner Observable immediately in parallel.
- Emissions arrive downstream in **completion order**, not outer emission order.
- Pass a second argument `mergeMap(fn, maxConcurrent)` to cap how many inner streams run at once.
