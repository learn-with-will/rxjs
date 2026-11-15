---
slug: all-flattening-operators
title: "Flattening pre-existing streams: mergeAll, concatAll, switchAll, exhaustAll"
topic: flattening-higher-order
kind: api
summary: "When a stream already emits Observables (Observable<Observable<T>>), flatten it directly with mergeAll, concatAll, switchAll, or exhaustAll."
tags: [rxjs, mergeall, concatall, switchall, exhaustall]
related: [what-is-a-higher-order-observable, map-plus-all-identity, when-to-use-all-vs-map, combine-latest-all-and-zip-all]
---
> **In one line:** `mergeAll()`, `concatAll()`, `switchAll()`, and `exhaustAll()` flatten a stream that **already emits inner Observables** (`Observable<Observable<T>> → Observable<T>`), using the exact same four concurrency strategies as `*Map`.

## The idea

In Topic 6 you learned `mergeMap`, `concatMap`, `switchMap`, and `exhaustMap`, which both **map** each outer value to an inner Observable *and* **flatten** those inner Observables.

What if your stream is **already** a higher-order `Observable<Observable<T>>` (for example, emitted by `window`, `groupBy`, a `Subject<Observable<T>>` task queue, or an array of Observables `from([obs1$, obs2$])`)? You don't need to map — you only need to **flatten**! That is what the four **`*All`** operators do:

| `*All` operator | Concurrency policy on `Observable<Observable<T>>` | Matching `*Map` operator |
|---|---|---|
| **`mergeAll(concurrent?)`** | Subscribe to inner streams in parallel (up to `concurrent` at once) | `mergeMap(fn, concurrent?)` |
| **`concatAll()`** | Subscribe to inner streams strictly one at a time in FIFO order | `concatMap(fn)` |
| **`switchAll()`** | Subscribe to the newest inner stream and unsubscribe the previous one | `switchMap(fn)` |
| **`exhaustAll()`** | Subscribe to the first inner stream and drop new inner streams while busy | `exhaustMap(fn)` |

## See it

Here we have a `Subject<Observable<string>>` that directly receives cold task Observables, and we flatten it sequentially with `concatAll()`:

```typescript
import { Subject, Observable, of, delay, concatAll } from 'rxjs';

// A stream whose values ARE already Observables:
const taskQueue$ = new Subject<Observable<string>>();

taskQueue$
  .pipe(
    concatAll(), // Subscribes to each inner Observable one after another!
  )
  .subscribe((result) => console.log('Completed task:', result));

taskQueue$.next(of('Task 1 (slow: 200ms)').pipe(delay(200)));
taskQueue$.next(of('Task 2 (fast: 50ms)').pipe(delay(50)));
// Output (strictly in queue order):
// Completed task: Task 1 (slow: 200ms)  <- at 200ms
// Completed task: Task 2 (fast: 50ms)   <- at 250ms
```

## Go deeper

Look at `from([task1$, task2$, task3$]).pipe(mergeAll(2))`:

- `from([task1$, task2$, task3$])` synchronously emits the three cold Observable objects one by one.
- Piping it into `mergeAll(2)` subscribes to the first 2 tasks in parallel, and as soon as one finishes, subscribes to `task3$`.
- Piping the exact same `from([task1$, task2$, task3$])` into `concatAll()` runs them strictly one by one (just like `concat(task1$, task2$, task3$)`).

Similarly, if you have a stream of inner streams where each new inner stream should cancel the previous one (such as a `BehaviorSubject<Observable<Config>>` that swaps the entire underlying environment stream when the user switches workspaces), piping into `switchAll()` handles the teardown of the old workspace stream and subscription to the new one automatically.

## Watch out

- In RxJS 7+, the old alias `exhaust()` is deprecated — always use the consistent name **`exhaustAll()`** alongside `mergeAll()`, `concatAll()`, and `switchAll()`.
- Calling `*All()` on a first-order stream of numbers or objects (that aren't `ObservableInput`s) will fail with a TypeScript and runtime error.

## Remember

- `mergeAll`, `concatAll`, `switchAll`, and `exhaustAll` unwrap `Observable<Observable<T>>` into `Observable<T>`.
- They apply the exact same four overlap policies (parallel, queue, cancel-previous, ignore-while-busy) as their `*Map` siblings.
- Use `exhaustAll()` instead of the deprecated `exhaust()` alias in RxJS 7+.
