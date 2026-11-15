---
slug: why-take-until-goes-last
title: "Why takeUntil(destroy$) must go at the end of .pipe()"
topic: errors-cleanup
kind: pitfall
summary: "Placing flattening, delay, or share operators after takeUntil(destroy$) allows inner streams to keep running and emitting after takeUntil has already completed."
tags: [rxjs, takeuntil, memory-leaks, pipe, pitfall]
related: [take-until-notifier, operator-order-matters, take-until-destroy-pattern]
---
> **In one line:** always place `takeUntil(destroy$)` as the **last operator in `.pipe()`** (or only before `finalize`) — any async operator placed **below** `takeUntil` will keep running even after `destroy$` fires!

## The idea

When `takeUntil(destroy$)` fires, it does two things:
1. It **unsubscribes upward** (everything *above* `takeUntil` in `.pipe()` is torn down).
2. It sends a **`complete()` notification downward** to the operator immediately *below* it.

What happens if you put `switchMap(x => slowRequest$(x))` or `delay(1000)` **below** `takeUntil(destroy$)`?

When `destroy$` emits, `takeUntil` unsubscribes from the outer source and sends `complete()` into `switchMap`. **In RxJS, an outer `complete()` does NOT cancel an already-running inner Observable inside `switchMap` / `mergeMap` / `concatMap`!** The in-flight inner Observable keeps running, finishes 2 seconds later, and calls your subscriber's `next()` callback on an already-destroyed component!

```flow
src: Source Stream | torn down when destroy$ emits @blue
tu: takeUntil(destroy$) | sends complete() downward, NOT unsubscribe()! @amber
sm: switchMap(slowInner$) BELOW takeUntil | BUG: active inner stream keeps running! @red
sub: Subscriber | receives stale emission after component unmounted! @red
src -> tu: outer values
tu -> sm: complete() (does NOT cancel active slowInner$!)
sm -> sub: slowInner$ still emits after destroy$!
```

## See it

Watch how placing `takeUntil(destroy$)` **before** `switchMap` leaks the active inner timer, while placing `takeUntil(destroy$)` **last** cancels everything immediately:

```typescript
import { of, Subject, timer, switchMap, takeUntil } from 'rxjs';

const destroy$ = new Subject<void>();

// Before (BUG): takeUntil is ABOVE switchMap — active inner timer(500) is NOT cancelled!
of('start')
  .pipe(
    takeUntil(destroy$),
    switchMap(() => timer(500)), // Inner timer is already running when destroy$ fires!
  )
  .subscribe(() => console.log('Before (BUG): emitted AFTER destroy$!'));

// After (SAFE): takeUntil(destroy$) is the LAST operator in .pipe()!
of('start')
  .pipe(
    switchMap(() => timer(500)),
    takeUntil(destroy$), // Unsubscribes upward through switchMap, cancelling timer(500)!
  )
  .subscribe(() => console.log('After (safe): never emits after destroy$'));

// Fire destroy$ at 100ms (while timer(500) is in flight):
setTimeout(() => {
  destroy$.next();
  destroy$.complete();
}, 100);
```

## Go deeper

Are there any exceptions where an operator belongs *after* `takeUntil(destroy$)`?

1. **`finalize(() => ...)`**: works either right before or right after `takeUntil`, though placing `finalize` at the very end guarantees it observes teardown of the entire pipeline.
2. **Operators that emit on completion** (`takeLast(n)`, `last()`, `reduce()`, `toArray()`): if you intentionally want `takeLast(3)` to flush its last 3 buffered values when `stop$` emits, then `takeUntil(stop$)` must sit immediately *above* `takeLast(3)` so `takeLast(3)` receives the `complete()` signal. (For component teardown with `destroy$`, however, you do *not* want post-destroy emissions, so `takeUntil(destroy$)` goes **last**!)

## Watch out

- Enable the `eslint-plugin-rxjs` rule `rxjs/no-unsafe-takeuntil` in TypeScript projects — it automatically flags any operator placed after `takeUntil` (except allowed completion/cleanup operators).
- Never place `switchMap`, `mergeMap`, `concatMap`, `exhaustMap`, `delay`, or `auditTime` after `takeUntil(destroy$)`.

## Remember

- `takeUntil(destroy$)` unsubscribes **upward** and sends `complete()` **downward**.
- An outer `complete()` does **not** cancel an active inner Observable inside `switchMap`, `mergeMap`, or `concatMap`.
- Always place `takeUntil(destroy$)` as the **last** operator in `.pipe()` so it tears down the entire chain.
