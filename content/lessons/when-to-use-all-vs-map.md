---
slug: when-to-use-all-vs-map
title: "When do you actually use *All() operators?"
topic: flattening-higher-order
kind: concept
summary: "Reach for mergeAll, concatAll, switchAll, or exhaustAll when an upstream operator like window() or groupBy() already emits inner Observables."
tags: [rxjs, mergeall, concatall, switchall, window, groupby]
related: [all-flattening-operators, map-plus-all-identity, window-operators, group-by-operator]
---
> **In one line:** use `*Map(fn)` 90% of the time when mapping values to streams, and use `*All()` when an upstream operator (`window`, `groupBy`) or collection (`from([obs1$, obs2$])`) **already emits Observables**.

## The idea

Why does RxJS have both `*Map` and `*All` operators if `*Map(fn)` is almost always shorter? Because several sources and operators in RxJS naturally emit **inner Observables** without you writing `map`:

| Source of `Observable<Observable<T>>` | Why it emits inner Observables | Common `*All` / flattening follow-up |
|---|---|---|
| **`from([obs1$, obs2$, obs3$])`** | Turns an array of cold Observables into a stream of Observables | `mergeAll(3)` (bounded pool) or `concatAll()` |
| **`windowTime(1000)` / `window(notifier$)`** | Splits a stream into time-sliced inner `Observable<T>` windows | `map(win$ => win$.pipe(count())), mergeAll()` |
| **` Subject<Observable<T>>`** | A dynamic task/command bus where producers push cold work streams | `concatAll()` or `mergeAll(2)` |
| **`BehaviorSubject<Observable<T>>`** | A switchable data-source pointer | `switchAll()` |

## See it

Here are two idiomatic places where `*All()` shines without any redundant `map`:

```typescript
import { BehaviorSubject, from, of, delay, switchAll, mergeAll } from 'rxjs';

// 1. Switching between live data sources held in a BehaviorSubject<Observable<string>>:
const pollingSource$ = new BehaviorSubject(of('Source A data').pipe(delay(50)));

pollingSource$
  .pipe(
    switchAll(), // Automatically unsubscribes old source$ and subscribes to new source$!
  )
  .subscribe((data) => console.log('Active source emitted:', data));

// Switch the entire underlying stream dynamically:
pollingSource$.next(of('Source B data').pipe(delay(50)));

// 2. Running an array of cold HTTP jobs with a concurrency cap of 2:
const jobs = [
  of('Job 1').pipe(delay(100)),
  of('Job 2').pipe(delay(100)),
  of('Job 3').pipe(delay(100)),
];

from(jobs)
  .pipe(mergeAll(2))
  .subscribe((res) => console.log('Finished:', res));
```

## Go deeper

Look at Example 2: `from(jobs).pipe(mergeAll(2))`. Compare that with `merge(...jobs, 2)`! Both run the array of cold Observables with a concurrency limit of 2, and `from(jobs).pipe(concatAll())` is the pipeline equivalent of `concat(...jobs)`. Knowing both the creation functions (`merge`, `concat`) and the higher-order flattening operators (`mergeAll`, `concatAll`, `switchAll`, `exhaustAll`) lets you handle both static lists of streams and dynamically arriving streams of streams with the exact same mental model.

## Watch out

- If you find yourself writing `from(items).pipe(map(item => fetch$(item)), mergeAll(3))`, simplify it to `from(items).pipe(mergeMap(item => fetch$(item), 3))`.
- Remember that in `from([obs1$, obs2$])`, the inner Observables `obs1$` and `obs2$` must be **cold** (or `defer`red) if you want `concatAll()` or `mergeAll(N)` to control when their work starts!

## Remember

- Use `*Map(fn)` when your stream emits plain values (`T`) that you map to inner Observables.
- Use `*All()` when your stream **already** emits inner Observables (`Observable<Observable<T>>`).
- `from(coldJobs).pipe(mergeAll(N))` is a clean way to run a list of cold Observables with concurrency `N`.
