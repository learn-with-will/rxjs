---
slug: subscription-and-purity-anti-patterns
title: "Subscription and purity anti-patterns to avoid"
topic: patterns-interop
kind: pitfall
summary: "Eliminate the 4 anti-patterns that break RxJS apps: constructor subscribes, nested subscribes, impure map callbacks, and Subject chains."
tags: [rxjs, anti-patterns, architecture, purity, best-practices]
related: [nested-subscribe-anti-pattern, tap-for-side-effects, what-causes-rxjs-memory-leaks]
---
> **In one line:** keep operator pipelines **pure** and **lazy** — expose Observables instead of subscribing inside service constructors, flatten instead of nesting `.subscribe()`, isolate side effects in `tap()`, and accumulate state with `scan()`.

## The idea

Four recurring anti-patterns cause the majority of bugs in production RxJS applications:

| Anti-pattern | Why it hurts | Clean RxJS replacement |
|---|---|---|
| **1. Subscribing inside a service constructor** | Runs eagerly even if nobody uses the service, cannot be cancelled by callers, and makes unit testing difficult | Expose a cold/shared `readonly data$ = ...` property and let consumers (or UI bindings) drive subscription |
| **2. Nested `.subscribe()` calls** | Inner subscription is orphaned, cannot be cancelled with outer teardown, and creates race conditions on rapid emissions | Flatten into a single pipeline with `switchMap`, `concatMap`, `mergeMap`, or `exhaustMap` |
| **3. Mutating external variables inside `map()` or `filter()`** | If 2 subscribers subscribe (or `retry()` re-runs), the external variable is mutated twice! | Use `scan((acc, val) => ..., seed)` for per-subscription state, and keep `map`/`filter` 100% pure |
| **4. Imperative `Subject.next()` chains** | Subscribing to Stream A just to call `subjectB.next(val)` loses backpressure, completion, and error propagation | Derive `streamB$ = streamA$.pipe(map(...))` declaratively |

## See it

Refactor an impure, imperative service into a declarative, pure RxJS stream:

```typescript
import { Observable, fromEvent, map, scan, shareReplay } from 'rxjs';

// ❌ Before: external mutable state, side effects inside map(), eager subscription
let clickCount = 0; // Shared global mutable state!
fromEvent(document, 'click').pipe(
  map(e => {
    clickCount++; // Impure side effect inside map!
    return clickCount;
  })
).subscribe();

// ✅ After: lazy, pure, per-subscription state with scan()
export const clickCount$: Observable<number> = fromEvent(document, 'click').pipe(
  scan(count => count + 1, 0),
  shareReplay({ bufferSize: 1, refCount: true })
);
```

## Go deeper

Look at anti-pattern #4 ("imperative `Subject.next()` chains"): whenever you write `streamA$.subscribe(x => this.mySubject$.next(transform(x)))`, ask yourself: *why does `mySubject$` exist at all?* If `mySubject$` is only ever fed by `streamA$`, delete the `Subject` and the `.subscribe()` call completely, and define `readonly myStream$ = streamA$.pipe(map(transform))`! Only create a `Subject` at the **boundary** between imperative non-RxJS code (like a button `onClick` handler or callback API) and the reactive world.

## Watch out

- Never perform DOM mutations, `localStorage` writes, or console logging inside `map()` or `filter()` — always put side effects inside `tap()` or `subscribe()` so their intent is explicit.
- If a service genuinely needs a shared hot cache across subscribers, expose `obs$.pipe(shareReplay({ bufferSize: 1, refCount: true }))` instead of subscribing inside the constructor.

## Remember

- Keep `map`, `filter`, and `scan` callbacks **pure**; put side effects in `tap` or `subscribe`.
- Replace external `let count = 0` variables with `scan()` so state is isolated per subscription.
- Only use `Subject`s at imperative-to-reactive boundaries; derive all other streams with `.pipe()`.
