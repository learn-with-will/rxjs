---
slug: map-plus-all-identity
title: "The flattening identity: map(fn) + *All() === *Map(fn)"
topic: flattening-higher-order
kind: concept
summary: "Every *Map flattening operator is simply map(fn) followed by its corresponding *All operator: switchMap(fn) is identical to map(fn), switchAll()."
tags: [rxjs, switchmap, switchall, mergemap, mergeall]
related: [all-flattening-operators, what-is-a-higher-order-observable, when-to-use-all-vs-map]
---
> **In one line:** every `*Map` operator is just **`map(fn)` + `*All()`** fused into one step — just like `Array.prototype.flatMap(fn)` is `map(fn).flat()`.

## The idea

Once you see the algebraic identity connecting `map`, `*All`, and `*Map`, the entire higher-order Observable family clicks into a single mental model:

```text
source$.pipe(map(fn), mergeAll())    ===  source$.pipe(mergeMap(fn))
source$.pipe(map(fn), concatAll())   ===  source$.pipe(concatMap(fn))
source$.pipe(map(fn), switchAll())   ===  source$.pipe(switchMap(fn))
source$.pipe(map(fn), exhaustAll())  ===  source$.pipe(exhaustMap(fn))
```

In fact, inside RxJS's own source code, `concatMap(fn)` is literally implemented by calling `mergeMap(fn, 1)`, and `mergeAll(concurrent)` is implemented as `mergeMap(identity, concurrent)`!

```flow
s: Outer Value (x: T) | e.g. userId = 7 @primary
m: Step 1: map(fn) | produces Inner Observable<R> @blue
a: Step 2: switchAll() | subscribes & flattens to R @amber
sm: Shortcut: switchMap(fn) | fuses Step 1 + Step 2! @green
s -> m: x
m -> a: Observable<R>
s -> sm: x (direct shortcut)
```

## See it

Here you can verify that `map(fn)` followed by `switchAll()` produces the exact same emissions and cancellation behavior as `switchMap(fn)`:

```typescript
import { of, delay, map, switchAll, switchMap } from 'rxjs';

const fetchUser$ = (id: number) => of(`User #${id}`).pipe(delay(50));

// 1. Two-step form: map() creates Observable<Observable<string>>, switchAll() flattens it
of(1, 2, 3)
  .pipe(
    map((id) => fetchUser$(id)),
    switchAll(),
  )
  .subscribe((u) => console.log('map + switchAll:', u)); // "User #3"

// 2. Fused one-step form: switchMap() does both at once
of(1, 2, 3)
  .pipe(
    switchMap((id) => fetchUser$(id)),
  )
  .subscribe((u) => console.log('switchMap:', u));       // "User #3"
```

## Go deeper

This identity works in **both directions**:

1. **`map(fn) + *All() → *Map(fn)`**: if you ever see `map(x => someObs$(x))` immediately followed by `mergeAll()`, `concatAll()`, `switchAll()`, or `exhaustAll()`, collapse the two operators into a single `mergeMap`, `concatMap`, `switchMap`, or `exhaustMap`.
2. **`*Map(x => x) → *All()`**: if you ever see someone write `higherOrder$.pipe(switchMap(obs$ => obs$))` (passing the identity function `x => x` to `switchMap` just to flatten an already higher-order stream), replace it with the cleaner `higherOrder$.pipe(switchAll())`!

Having this mental model also demystifies error and completion behavior: because `switchMap(fn)` is just `map(fn)` + `switchAll()`, the outer stream produces inner Observables via `map(fn)` while `switchAll()` manages the inner subscriptions — which is why an outer `complete()` only means "no more inner Observables will be created," while the currently active inner Observable inside `switchAll()` still finishes its own lifecycle.

## Watch out

- Never write `map(x => fetch$(x)), switchAll()` in production code — use `switchMap(x => fetch$(x))` directly.
- Conversely, never write `switchMap(obs$ => obs$)` with an identity function when `switchAll()` expresses "flatten without mapping" directly.

## Remember

- `*Map(fn)` = `map(fn)` + `*All()` (map to an inner Observable, then flatten).
- `*All()` = `*Map(x => x)` (flatten an existing higher-order Observable without mapping).
- Prefer `*Map(fn)` when mapping and flattening together, and `*All()` when the source already emits Observables.
