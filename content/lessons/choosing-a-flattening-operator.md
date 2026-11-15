---
slug: choosing-a-flattening-operator
title: "Choosing between mergeMap, concatMap, switchMap, and exhaustMap"
topic: flattening-higher-order
kind: concept
summary: "Pick the right flattening operator by asking what should happen when a new outer value arrives while a previous inner Observable is still running."
tags: [rxjs, mergemap, concatmap, switchmap, exhaustmap]
related: [merge-map, concat-map, switch-map, exhaust-map]
---
> **In one line:** the four flattening operators differ in one decision — when a new outer value arrives while an inner stream is still running, do you **run in parallel** (`mergeMap`), **queue in order** (`concatMap`), **cancel the old one** (`switchMap`), or **ignore the new one** (`exhaustMap`)?

## The idea

Whenever you map an event to an async Observable, ask one question: **"What should happen if another outer event arrives before the current inner Observable has completed?"**

| Operator | Policy on overlap | Cancels in-flight inner? | Drops outer events? | Classic use cases |
|---|---|---|---|---|
| **`mergeMap`** | **Run in parallel** | No | No | Independent parallel fetches, firing analytics, per-item actions |
| **`concatMap`** | **Queue in FIFO order** | No | No | Sequential `POST`/`PUT` writes, ordered steps, sequential animations |
| **`switchMap`** | **Cancel previous, switch to latest** | **Yes** | No (aborts previous inner) | Typeahead search, route/tab parameter load, resetting a timer |
| **`exhaustMap`** | **Ignore new while busy** | No | **Yes** (drops new outer) | Login / form submit button, pull-to-refresh, non-overlapping poll |

```tree
What should happen when a new outer event arrives mid-flight?
  Only the latest matters (cancel the old inner stream)
    [switchMap] Typeahead search, route param fetch, timer reset @green
  Ignore new triggers until the current inner stream finishes
    [exhaustMap] Form submit, login button, non-overlapping poll @amber
  Every trigger must run, strictly one after another in order
    [concatMap] Sequential writes, ordered queue, chained animations @blue
  Every trigger must run immediately in parallel
    [mergeMap] Independent item fetches, fire-and-forget logs @primary
```

## See it

Watch all four operators side by side when outer values `1` and `2` arrive at `0ms` and `50ms`, and each inner request takes `100ms`:

```typescript
import { timer, take, of, delay, mergeMap, concatMap, switchMap, exhaustMap } from 'rxjs';

// Emits 0 at 0ms, and 1 at 50ms:
const triggers$ = timer(0, 50).pipe(take(2));
// Inner stream takes 100ms to emit:
const work$ = (id: number) => of(`done #${id}`).pipe(delay(100));

triggers$.pipe(mergeMap(work$)).subscribe((v) => console.log('mergeMap:', v));
// -> "done #0" (at 100ms), "done #1" (at 150ms) — ran concurrently

triggers$.pipe(concatMap(work$)).subscribe((v) => console.log('concatMap:', v));
// -> "done #0" (at 100ms), "done #1" (at 200ms) — #1 waited in queue for #0

triggers$.pipe(switchMap(work$)).subscribe((v) => console.log('switchMap:', v));
// -> "done #1" (at 150ms) — #0 was cancelled at 50ms!

triggers$.pipe(exhaustMap(work$)).subscribe((v) => console.log('exhaustMap:', v));
// -> "done #0" (at 100ms) — #1 at 50ms was ignored while #0 was busy!
```

## Go deeper

A helpful rule of thumb for CRUD operations:

- **Reads (GET)** triggered by changing criteria (search query, selected ID, active tab) almost always want **`switchMap`** so stale responses are cancelled.
- **Writes (POST / PUT / DELETE)** triggered by a single Submit button want **`exhaustMap`** (to prevent double submission), while writes from a stream of distinct edits want **`concatMap`** (to preserve edit order).
- **Independent parallel tasks** (fetching metadata for 5 uploaded files at once, where each file has its own progress bar) want **`mergeMap(fn, concurrencyLimit)`**.

## Watch out

- Never default to `mergeMap` (or `switchMap`) out of habit — using `switchMap` on a delete button cancels the first delete when the user clicks a second row, while using `mergeMap` on a search box causes out-of-order search results.

## Remember

- `mergeMap` = parallel; `concatMap` = sequential queue; `switchMap` = latest wins (cancels old); `exhaustMap` = first wins while busy (ignores new).
- Use `switchMap` for latest-state reads and `concatMap` or `exhaustMap` for mutations.
- All four operators subscribe to inner Observables automatically and flatten `Observable<Observable<T>>` into `Observable<T>`.
