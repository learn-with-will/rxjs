---
id: lesson-09
slug: higher-order-observables-flattening
title: "Higher-Order Observables: mergeMap, concatMap, switchMap, exhaustMap"
level: intermediate
order: 09
duration: 32
tags:
  - rxjs
  - switchmap
  - mergemap
  - concatmap
  - exhaustmap
  - higher-order
---

# Learning Objectives

By the end of this lesson you will be able to:

- Explain what a **higher-order Observable** (a stream of streams) is.
- Flatten inner Observables with `mergeMap`, `concatMap`, `switchMap`, and `exhaustMap`.
- Predict each operator's behaviour when a new outer value arrives before the inner finishes.
- Choose the correct flattening operator for search, saves, parallel work, and submit-once.
- Replace the nested-`subscribe` anti-pattern with a flattening operator.

# Why This Concept Exists

The moment a value in a stream needs to trigger **another** async operation — a click that fires an HTTP request, a search term that fetches results — your `map` returns an Observable. Now you have an `Observable<Observable<T>>`: a stream of streams. Subscribing to that outer stream just hands you inner Observables you'd have to subscribe to yourself. **Flattening operators** subscribe to the inner streams for you and merge their values back into one flat stream — and the *strategy* they use to manage overlapping inners is one of the most important decisions in RxJS.

# Concept Explanation

### Higher-order Observables

If `map`'s projection returns an Observable, the result is *higher-order*:

```ts
import { fromEvent, map, ajax } from 'rxjs';
// clicks$ : Observable<Observable<Response>>  — a stream of request streams
const clicks$ = fromEvent(button, 'click').pipe(map(() => ajax('/api/save')));
```

You rarely want that. You want a flat `Observable<Response>`. The `*Map` operators do `map` **and** flatten in one step; each one differs only in how it handles a new outer value while an inner is still running.

### The four strategies

Assume an outer stream emits `a`, then `b` **before** `a`'s inner Observable has finished, and each inner emits a couple of values over time.

- **`mergeMap`** — subscribe to every inner **concurrently** and merge all their values. `a` and `b`'s inners run at the same time; outputs interleave. Order is **not** guaranteed. Optional concurrency limit as a second argument.
- **`concatMap`** — **queue** inners. Subscribe to `b`'s inner only after `a`'s inner **completes**. Order is preserved. (It is `mergeMap` with concurrency `1`.)
- **`switchMap`** — **switch**. When `b` arrives, **unsubscribe from `a`'s inner** (cancel it) and subscribe to `b`'s. Only the latest inner survives.
- **`exhaustMap`** — **ignore**. While `a`'s inner is still running, **drop `b`** entirely. New outer values are ignored until the current inner completes.

```text
outer:      --a------b---------|      (b arrives while a's inner is mid-flight)

mergeMap:   --a1-a2--b1-b2-----|      run both, interleave (order not guaranteed)
concatMap:  --a1-a2------b1-b2-|      finish a, THEN start b (ordered)
switchMap:  --a1-a2--b1-b2-----|      but a2 is cut off if b arrives first: a is cancelled
exhaustMap: --a1-a2------------|      b ignored because a's inner was still active
```

### Inputs can be Promises, arrays, iterables

The projection can return any `ObservableInput` — an Observable, a Promise, an array, or an iterable — and RxJS converts it (like `from`). So `mergeMap(id => fetch(url))` works because a Promise is a valid inner.

### The nested-subscribe anti-pattern

Never subscribe inside a subscribe:

```ts
// ANTI-PATTERN — do not do this
search$.subscribe((term) => {
  api.search(term).subscribe((results) => render(results)); // no cancellation, leaks, races
});
```

This loses cancellation, composition, and error propagation. The fix is a flattening operator:

```ts
search$.pipe(switchMap((term) => api.search(term))).subscribe(render);
```

# Key Terminology

- **Higher-order Observable** — an Observable whose values are themselves Observables.
- **Inner / outer Observable** — the outer stream produces values; each maps to an inner stream that gets flattened.
- **Flatten** — subscribe to inner Observables and emit their values on a single output stream.
- **`mergeMap`** — flatten concurrently (alias: `flatMap`); optional concurrency limit.
- **`concatMap`** — flatten sequentially, preserving order (concurrency 1).
- **`switchMap`** — cancel the previous inner when a new outer value arrives.
- **`exhaustMap`** — ignore new outer values while an inner is active.
- **`ObservableInput`** — anything convertible to an Observable (Observable, Promise, array, iterable).

# Syntax Overview

```ts
import { fromEvent, of, mergeMap, concatMap, switchMap, exhaustMap, delay } from 'rxjs';

const clicks$ = fromEvent(document, 'click');

clicks$.pipe(mergeMap(() => of('done').pipe(delay(1000))));   // all run in parallel
clicks$.pipe(concatMap(() => of('done').pipe(delay(1000))));  // one at a time, in order
clicks$.pipe(switchMap(() => of('done').pipe(delay(1000))));  // only the latest survives
clicks$.pipe(exhaustMap(() => of('done').pipe(delay(1000)))); // ignore clicks while busy

// Optional concurrency cap on mergeMap (at most 2 inners at once):
clicks$.pipe(mergeMap(() => of('x').pipe(delay(1000)), 2));
```

# Real World Analogy

Imagine a **kitchen taking orders** (outer values), where each order takes time to cook (the inner Observable). `mergeMap` is a kitchen with unlimited cooks — every order starts immediately and dishes come out in whatever order they finish. `concatMap` is a single cook working one order start-to-finish before touching the next — slower, but dishes come out strictly in order. `switchMap` is an indecisive customer: each time they change their mind, the cook **throws away** the half-made dish and starts the new one — only the latest order ever gets served. `exhaustMap` is a cook so focused that they **ignore** any new orders shouted while a dish is on the stove, only listening again once it's plated.

# Examples

## Example 1 — Basic: parallel lookups with mergeMap

```ts
import { from, mergeMap } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const ids = [1, 2, 3];

from(ids)
  .pipe(mergeMap((id) => ajax.getJSON(`/api/users/${id}`)))
  .subscribe((user) => console.log(user));
// Fires all three requests concurrently; results arrive in whatever order they resolve.
```

**Why this is good:** The three lookups are independent, so running them in parallel with `mergeMap` is fastest. Order doesn't matter here, which is exactly when `mergeMap` shines.

## Example 2 — Real-world: search-as-you-type with switchMap

```ts
import { fromEvent, map, debounceTime, distinctUntilChanged, switchMap } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const input = document.querySelector<HTMLInputElement>('#search')!;

fromEvent(input, 'input')
  .pipe(
    map((e) => (e.target as HTMLInputElement).value.trim()),
    debounceTime(300),
    distinctUntilChanged(),
    switchMap((term) => ajax.getJSON(`/api/search?q=${encodeURIComponent(term)}`)),
  )
  .subscribe((results) => render(results));

function render(results: unknown) {}
```

**Why this is good:** `switchMap` cancels the in-flight request whenever a newer term arrives, so stale responses can never overwrite fresh ones — the single most important reason `switchMap` is the default for search.

## Example 3 — Best practice: exhaustMap for submit-once, concatMap for ordered writes

```ts
import { fromEvent, exhaustMap, from, concatMap } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// Login: ignore extra clicks while the request is in flight.
fromEvent(document.querySelector('#login')!, 'click')
  .pipe(exhaustMap(() => ajax.post('/api/login')))
  .subscribe();

// Persisting edits in order: each save must finish before the next starts.
const edits$ = from([{ id: 1 }, { id: 2 }, { id: 3 }]);
edits$
  .pipe(concatMap((edit) => ajax.post('/api/save', edit)))
  .subscribe();
```

**Why this is good:** It matches the strategy to intent — `exhaustMap` prevents duplicate logins from rapid clicks, and `concatMap` guarantees writes are applied in the order they were made, never overlapping.

# Common Mistakes

- **Nested `subscribe`.** Subscribing inside a subscribe loses cancellation and error handling and leaks. **Fix:** use a flattening operator (`switchMap`, `mergeMap`, …).
- **Using `mergeMap` for search.** Concurrent requests race, so an older response can overwrite a newer one. **Fix:** use `switchMap` to cancel stale requests.
- **Using `switchMap` for saves/writes.** A new value cancels the in-flight write, potentially losing data. **Fix:** use `concatMap` (ordered) or `mergeMap` (parallel) for writes.
- **Forgetting inner completion with `concatMap`.** If an inner never completes, the queue stalls forever. **Fix:** ensure inners complete (e.g., `take(1)`, HTTP requests complete naturally).
- **Being surprised `exhaustMap` drops values.** That's intentional — it ignores new outer values while busy. **Fix:** choose it only when ignoring is the desired behaviour.

# Best Practices

- Decide by intent:
  - **`switchMap`** → only the latest matters; cancel the rest (autocomplete, latest route/params).
  - **`concatMap`** → order matters; run one at a time (sequential writes, ordered animations).
  - **`mergeMap`** → independent tasks; maximise concurrency (parallel fetches); cap with the concurrency argument if needed.
  - **`exhaustMap`** → ignore new work until the current finishes (submit/login buttons).
- Never nest subscriptions — flatten instead.
- For writes, avoid `switchMap` (it cancels); for reads-where-only-latest-matters, avoid `mergeMap` (it races).
- Keep inner Observables completing so `concatMap`/`mergeMap` queues drain.

# Summary

- A **higher-order Observable** emits Observables; flattening operators subscribe to the inners and merge their values.
- The four `*Map` operators are `map` + a flattening strategy differing in how they treat overlapping inners.
- **`mergeMap`** runs inners concurrently (no order); **`concatMap`** queues them (ordered, concurrency 1).
- **`switchMap`** cancels the previous inner on each new outer value; **`exhaustMap`** ignores new outer values while an inner runs.
- Inputs may be Observables, Promises, arrays, or iterables.
- Replace **nested `subscribe`** with a flattening operator to keep cancellation, ordering, and error handling.

# Flash Cards

Q: What is a higher-order Observable?
A: An Observable whose emitted values are themselves Observables — for example when `map`'s projection returns an Observable (a stream of streams).

Q: Which flattening operator cancels the previous inner Observable when a new outer value arrives?
A: `switchMap` — it unsubscribes from the in-flight inner and switches to the newest one, so only the latest survives.

Q: Which operator should you use for search-as-you-type, and why?
A: `switchMap`, because it cancels the previous request when a newer search term arrives, preventing stale responses from overwriting fresh ones.

Q: Which operator ignores new outer values while an inner Observable is still active?
A: `exhaustMap` — perfect for submit/login buttons where extra clicks during an in-flight request should be dropped.

Q: How does `concatMap` relate to `mergeMap`?
A: `concatMap` is `mergeMap` with a concurrency of 1 — it queues inners and runs them one at a time, preserving order.

Q: Why is nesting `subscribe` inside `subscribe` an anti-pattern?
A: It loses cancellation, composition, and error propagation and can leak subscriptions. Use a flattening operator instead.

# Exercises

### Easy

Given `of('a', 'b', 'c')`, use `concatMap` to map each letter to `of(letter).pipe(delay(500))` and log the results. Confirm they arrive in order, 500ms apart. Then swap to `mergeMap` and observe they arrive together.

### Medium

Build a search input pipeline: `map` to the value, `debounceTime(300)`, `distinctUntilChanged`, then `switchMap` to a fake request `of(\`results for \${term}\`).pipe(delay(1000))`. Type several terms quickly and confirm only the last term's results appear.

### Challenging

Create a "Save" button whose clicks map to a fake 1-second request. Implement it three ways — `exhaustMap`, `concatMap`, and `switchMap` — and describe in comments what happens when you click three times rapidly under each: which requests run, which are ignored, and which are cancelled. State which one you'd ship for a save action and why.

# Further Reading

- [RxJS — switchMap](https://rxjs.dev/api/index/function/switchMap)
- [RxJS — mergeMap](https://rxjs.dev/api/index/function/mergeMap) and [concatMap](https://rxjs.dev/api/index/function/concatMap)
- [RxJS — exhaustMap](https://rxjs.dev/api/index/function/exhaustMap)
- [RxJS — Operators (higher-order)](https://rxjs.dev/guide/operators)
