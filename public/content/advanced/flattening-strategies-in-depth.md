---
id: lesson-16
slug: flattening-strategies-in-depth
title: "Flattening Strategies in Depth: mergeAll, concatAll, switchAll, exhaustAll"
level: advanced
order: 16
duration: 28
tags:
  - rxjs
  - mergeall
  - concatall
  - switchall
  - concurrency
  - higher-order
---

# Learning Objectives

By the end of this lesson you will be able to:

- Flatten a higher-order Observable directly with `mergeAll`, `concatAll`, `switchAll`, and `exhaustAll`.
- Explain the identity `xMap(fn) ≡ map(fn) + xAll()`.
- Limit parallelism with the **concurrency** argument of `mergeMap`/`mergeAll`.
- Decide when to reach for a `*All` operator vs a `*Map` operator.
- Recognise the deprecated `exhaust` and the higher-order combiners `combineLatestAll`/`zipAll`.

# Why This Concept Exists

The `*Map` operators from the intermediate course are actually **compositions**: a `map` that produces inner Observables, followed by a flattening step. The flattening step has its own name — `mergeAll`, `concatAll`, `switchAll`, `exhaustAll` — and understanding them separately demystifies the whole family, unlocks **concurrency control**, and lets you flatten a stream of Observables you *already* have (say, from a Subject) without a redundant `map`.

# Concept Explanation

### The `*All` operators flatten, they don't map

A `*All` operator takes a **higher-order** Observable — `Observable<Observable<T>>` — and subscribes to the inner Observables, merging their values into a flat `Observable<T>`. It does **not** transform values; it only manages the inner subscriptions.

```ts
import { of, mergeAll } from 'rxjs';

const higherOrder$ = of(of(1, 2), of(3, 4)); // Observable of Observables
higherOrder$.pipe(mergeAll()).subscribe(console.log); // 1, 2, 3, 4 (flattened)
```

### The identity: `xMap` = `map` + `xAll`

Each flattening operator pairs with a `*Map` operator:

| Strategy      | `*All`        | `*Map` equivalent           |
| ------------- | ------------- | --------------------------- |
| Concurrent    | `mergeAll`    | `mergeMap` = `map + mergeAll` |
| Sequential    | `concatAll`   | `concatMap` = `map + concatAll` |
| Switch/cancel | `switchAll`   | `switchMap` = `map + switchAll` |
| Ignore/exhaust| `exhaustAll`  | `exhaustMap` = `map + exhaustAll` |

```ts
import { fromEvent, map, switchAll, switchMap } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// These two are equivalent:
clicks$.pipe(map(() => ajax.getJSON('/x')), switchAll());
clicks$.pipe(switchMap(() => ajax.getJSON('/x')));
```

The behaviours (concurrent, sequential, switch, ignore) are exactly those from the higher-order lesson.

### Concurrency control with `mergeAll`/`mergeMap`

`mergeAll(concurrent)` and `mergeMap(fn, concurrent)` take an optional **concurrency limit** — the maximum number of inner Observables active at once. Extra inners **queue** until a slot frees up. Default is `Infinity`.

```ts
import { from, mergeMap, of, delay } from 'rxjs';

// Process at most 2 downloads at a time.
from(urls)
  .pipe(mergeMap((url) => download(url), 2))
  .subscribe();

function download(url: string) {
  return of(`done ${url}`).pipe(delay(1000));
}
```

`concatAll`/`concatMap` are just `mergeAll(1)` — a concurrency of one.

### When to use `*All` vs `*Map`

- Use a **`*Map`** operator when you're mapping each value to an inner Observable **in the same step** (the common case).
- Use a **`*All`** operator when you **already hold** a higher-order Observable — e.g., a `Subject<Observable<T>>`, or the result of a `map` elsewhere — and just need to flatten it.

### Deprecated and related operators

- `exhaust()` is **deprecated** — use `exhaustAll()`.
- Higher-order combiners exist too: `combineLatestAll` (renamed from the deprecated `combineAll`) and `zipAll` combine a stream of Observables once the outer completes.

# Key Terminology

- **Higher-order Observable** — an `Observable<Observable<T>>`.
- **`mergeAll(n?)`** — flatten inners concurrently, up to `n` at once (default `Infinity`).
- **`concatAll`** — flatten sequentially (`mergeAll(1)`).
- **`switchAll`** — subscribe to the latest inner, cancelling the previous.
- **`exhaustAll`** — ignore new inners while one is active (replaces `exhaust`).
- **Concurrency limit** — the max number of simultaneous inner subscriptions.
- **`combineLatestAll` / `zipAll`** — higher-order combiners over a stream of Observables.

# Syntax Overview

```ts
import { of, from, mergeAll, concatAll, switchAll, exhaustAll, map, delay } from 'rxjs';

of(of(1), of(2), of(3)).pipe(mergeAll());        // 1, 2, 3 (concurrent)
of(of(1), of(2)).pipe(concatAll());               // 1, 2   (sequential)
higherOrder$.pipe(switchAll());                    // only the latest inner
higherOrder$.pipe(exhaustAll());                   // ignore new inners while busy

from(items).pipe(map((x) => work(x)), mergeAll(3)); // at most 3 at once
function work(x: unknown) { return of(x).pipe(delay(500)); }
```

# Real World Analogy

Think of a **call centre** receiving a queue of callers (the outer stream), where each caller needs a task done (an inner Observable). `mergeAll` with unlimited agents handles everyone at once; `mergeAll(3)` means only three agents, so callers four and beyond wait on hold. `concatAll` is a single agent handling one caller fully before picking up the next. `switchAll` is an agent who drops the current caller the instant a "priority" caller rings. `exhaustAll` is an agent so committed to the current call that they let new calls ring out. The `*Map` operators are the same call centre, but where each caller is *created* from an incoming ticket in one motion — `map` writes the ticket, `*All` works the queue.

# Examples

## Example 1 — Basic: mergeAll vs concatAll ordering

```ts
import { of, map, mergeAll, concatAll, delay } from 'rxjs';

const source$ = of(300, 100, 200).pipe(
  map((ms) => of(`took ${ms}ms`).pipe(delay(ms))), // -> higher-order Observable
);

source$.pipe(concatAll()).subscribe((v) => console.log('concat:', v));
// concat: took 300ms / concat: took 100ms / concat: took 200ms  (source order, one at a time)

source$.pipe(mergeAll()).subscribe((v) => console.log('merge:', v));
// merge: took 100ms / merge: took 200ms / merge: took 300ms  (by completion time)
```

**Why this is good:** Same higher-order source, two flattening strategies, opposite orderings — `concatAll` preserves source order by running sequentially, while `mergeAll` runs all at once so results arrive by speed.

## Example 2 — Real-world: a bounded download queue

```ts
import { from, mergeMap, of, delay, tap } from 'rxjs';

const urls = ['/a', '/b', '/c', '/d', '/e'];

from(urls)
  .pipe(
    mergeMap(
      (url) => of(`downloaded ${url}`).pipe(delay(1000), tap(() => {})),
      2, // never more than 2 concurrent downloads
    ),
  )
  .subscribe((msg) => console.log(msg));
// Runs a and b first; as each finishes, c, d, e start — max 2 in flight.
```

**Why this is good:** The concurrency argument turns `mergeMap` into a throttled work queue — essential for not overwhelming a server or the browser's connection limit. This is `map(...) + mergeAll(2)` under the hood.

## Example 3 — Best practice: flatten a Subject of Observables with switchAll

```ts
import { Subject, switchAll, of, delay } from 'rxjs';

// A stream whose values are themselves request Observables.
const requests$ = new Subject<ReturnType<typeof makeRequest>>();

requests$.pipe(switchAll()).subscribe((result) => console.log(result));

requests$.next(makeRequest('first'));  // starts
requests$.next(makeRequest('second')); // cancels 'first', starts 'second'

function makeRequest(tag: string) {
  return of(`result ${tag}`).pipe(delay(500));
}
```

**Why this is good:** Here we *already* have a higher-order Observable (a `Subject` of request Observables), so `switchAll` is the natural fit — no redundant `map`. It cancels the previous request when a new one is pushed, exactly like `switchMap` would.

# Common Mistakes

- **Applying `*All` to a stream of plain values.** `mergeAll` expects inner **Observables**; feeding it numbers throws. **Fix:** ensure the source is higher-order (or use a `*Map` to produce inners).
- **Forgetting `mergeAll`/`mergeMap` default to `Infinity` concurrency.** Hundreds of simultaneous requests can result. **Fix:** pass a concurrency limit.
- **Using deprecated `exhaust()`.** **Fix:** use `exhaustAll()`.
- **Reaching for `map + mergeAll` when `mergeMap` is clearer.** Two operators where one suffices. **Fix:** use the `*Map` form unless you already hold a higher-order Observable.
- **Expecting `concatAll` to parallelise.** It's `mergeAll(1)` — strictly sequential. **Fix:** use `mergeAll(n)` for controlled parallelism.

# Best Practices

- Prefer **`*Map`** for the map-then-flatten case; reserve **`*All`** for pre-existing higher-order Observables.
- Always consider a **concurrency limit** on `mergeMap`/`mergeAll` for network or CPU-bound inners.
- Remember the equivalences — they make refactoring between forms mechanical and safe.
- Use `switchAll`/`exhaustAll` on Subjects of Observables to get cancel/ignore semantics without a `map`.
- Reach for `combineLatestAll`/`zipAll` (not the deprecated `combineAll`) when combining a completed stream of Observables.

# Summary

- The `*All` operators **flatten** a higher-order Observable; the `*Map` operators are `map` + the matching `*All`.
- **`mergeAll`** is concurrent (limit with an argument), **`concatAll`** is sequential (`mergeAll(1)`), **`switchAll`** cancels, **`exhaustAll`** ignores.
- The **concurrency argument** on `mergeMap`/`mergeAll` bounds parallelism — a built-in work queue.
- Use `*All` when you already have `Observable<Observable<T>>`; use `*Map` to map-and-flatten in one step.
- `exhaust()` is deprecated (use `exhaustAll`); higher-order combiners are `combineLatestAll` and `zipAll`.

# Flash Cards

Q: How does `mergeMap(fn)` relate to `map` and `mergeAll`?
A: `mergeMap(fn)` is exactly `map(fn)` followed by `mergeAll()` — map produces inner Observables, mergeAll flattens them concurrently. The same identity holds for concat/switch/exhaust.

Q: What does a `*All` operator expect its source to emit?
A: Other Observables — it flattens a higher-order Observable (Observable<Observable<T>>). Feeding it plain values throws.

Q: How do you limit `mergeMap`/`mergeAll` to at most 3 concurrent inner subscriptions?
A: Pass the concurrency argument: `mergeMap(fn, 3)` or `mergeAll(3)`. Extra inners queue until a slot frees up.

Q: What is `concatAll` in terms of `mergeAll`?
A: `concatAll` is `mergeAll(1)` — a concurrency of one, so inners run strictly one at a time in order.

Q: When would you use `switchAll` instead of `switchMap`?
A: When you already have a higher-order Observable (e.g., a Subject of Observables) and don't need a `map` step — `switchAll` flattens it with cancel-previous semantics.

Q: Which flattening operator is deprecated, and what replaces it?
A: `exhaust()` is deprecated; use `exhaustAll()`.

# Exercises

### Easy

Create `of(of('a'), of('b'), of('c'))` and flatten it with `concatAll`, then with `mergeAll`. With synchronous inners the order matches; add `delay` to each inner and observe how `mergeAll` reorders by completion time.

### Medium

Build a queue of five fake downloads (`of(id).pipe(delay(800))`) from `from([1,2,3,4,5])` and flatten with `mergeMap(fn, 2)`. Log start and finish times (via `tap`) and confirm no more than two run at once. Then change the limit to 1 and note it behaves like `concatMap`.

### Challenging

Refactor a `switchMap((term) => search(term))` search pipeline into the equivalent `map((term) => search(term))` + `switchAll()` form and confirm identical behaviour. Then create a `Subject<Observable<string>>`, push several request Observables into it, and flatten with `exhaustAll()` — describe in a comment which pushed requests run and which are ignored.

# Further Reading

- [RxJS — mergeAll](https://rxjs.dev/api/index/function/mergeAll) and [concatAll](https://rxjs.dev/api/index/function/concatAll)
- [RxJS — switchAll](https://rxjs.dev/api/index/function/switchAll) and [exhaustAll](https://rxjs.dev/api/index/function/exhaustAll)
- [RxJS — mergeMap (concurrency)](https://rxjs.dev/api/index/function/mergeMap)
- [RxJS — combineLatestAll](https://rxjs.dev/api/index/function/combineLatestAll) and [zipAll](https://rxjs.dev/api/index/function/zipAll)
