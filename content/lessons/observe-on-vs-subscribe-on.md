---
slug: observe-on-vs-subscribe-on
title: "observeOn() vs subscribeOn()"
topic: schedulers-testing
kind: api
summary: "observeOn(scheduler) reschedules notifications flowing downstream after it; subscribeOn(scheduler) reschedules when the upstream subscribe() call runs."
tags: [rxjs, observeon, subscribeon, scheduler, pipe]
related: [what-is-a-scheduler, built-in-schedulers, scheduled-creation-function]
---
> **In one line:** **`observeOn(scheduler)`** changes when/where **notifications below it** are delivered (position in `.pipe()` matters!), while **`subscribeOn(scheduler)`** changes when/where the **upstream `subscribe()` call itself** begins (position in `.pipe()` does not matter).

## The idea

Developers frequently mix up `observeOn` and `subscribeOn` because both take a `SchedulerLike` (and an optional `delay` in ms). They act on opposite directions of the pipeline:

- **`observeOn(scheduler, delay?)` acts on notifications flowing DOWNSTREAM (`next`, `error`, `complete`):** every operator **above** `observeOn` still runs in its original context, and `observeOn` intercepts each notification and re-dispatches it on `scheduler` for every operator **below** `observeOn`. You can even place multiple `observeOn` calls at different points in `.pipe()`!
- **`subscribeOn(scheduler, delay?)` acts on the `subscribe()` call flowing UPSTREAM:** instead of invoking the cold source's producer function immediately during `source$.subscribe()`, `subscribeOn` schedules that upstream subscription to start on `scheduler`.

| Feature | `observeOn(scheduler)` | `subscribeOn(scheduler)` |
|---|---|---|
| **What it schedules** | Each `next`, `error`, and `complete` notification | The initial upstream `subscribe()` call |
| **Does position in `.pipe()` matter?** | **Yes** — only affects operators *below* it | **No** — wraps the whole upstream subscription |
| **Effect on a hot `Subject`** | Shifts delivery of emissions to `scheduler` | Virtually none for later `.next()` calls! |

## See it

Watch how `tap` above `observeOn` runs synchronously while `tap` below `observeOn` runs asynchronously — and how `subscribeOn` makes the producer itself start asynchronously:

```typescript
import { Observable, tap, observeOn, subscribeOn, asyncScheduler } from 'rxjs';

// 1. observeOn: producer & first tap run SYNC; second tap & subscribe run ASYNC
new Observable<number>((sub) => {
  console.log('[observeOn] Producer started (SYNC)');
  sub.next(1);
  sub.complete();
})
  .pipe(
    tap(() => console.log('[observeOn] tap ABOVE observeOn (SYNC)')),
    observeOn(asyncScheduler),
    tap(() => console.log('[observeOn] tap BELOW observeOn (ASYNC)')),
  )
  .subscribe();

// 2. subscribeOn: the producer's subscription ITSELF is deferred to asyncScheduler
new Observable<number>((sub) => {
  console.log('[subscribeOn] Producer started (ASYNC)');
  sub.next(1);
  sub.complete();
})
  .pipe(subscribeOn(asyncScheduler))
  .subscribe();

console.log('--- End of synchronous script ---');
```

## Go deeper

Look at the output order of that snippet:
1. `[observeOn] Producer started (SYNC)`
2. `[observeOn] tap ABOVE observeOn (SYNC)`
3. `--- End of synchronous script ---`
4. `[observeOn] tap BELOW observeOn (ASYNC)`
5. `[subscribeOn] Producer started (ASYNC)`

Why does `subscribeOn(asyncScheduler)` have almost no effect on a hot `Subject` if you call `subject.next(1)` synchronously right after `subject.pipe(subscribeOn(asyncScheduler)).subscribe(...)`? Worse than having no effect, because `subscribeOn(asyncScheduler)` delays **when the subscriber registers** with the Subject until the next macrotask tick, a synchronous `subject.next(1)` called on the next line fires **before the subscription has even happened**, so the subscriber misses `1` completely!

## Watch out

- Do not use `subscribeOn(asyncScheduler)` on a hot `Subject` expecting it to make `.next()` emissions asynchronous — it only delays *subscribing* to the Subject, causing you to miss synchronous emissions! Use `observeOn(asyncScheduler)` instead.
- If you put two `subscribeOn` operators in the same pipeline, the **first** (topmost) `subscribeOn` wins for when the source producer starts.

## Remember

- `observeOn(scheduler)` shifts delivery of `next`, `error`, and `complete` for all operators **below** it in `.pipe()`.
- `subscribeOn(scheduler)` shifts when the upstream **producer function starts** on `subscribe()`.
- Position in `.pipe()` matters for `observeOn`, but not for `subscribeOn`.
