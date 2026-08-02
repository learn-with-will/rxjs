---
id: lesson-22
slug: real-world-patterns-and-interop
title: "Real-World Patterns and Async Interop"
level: advanced
order: 22
duration: 34
tags:
  - rxjs
  - patterns
  - typeahead
  - polling
  - firstvaluefrom
  - interop
---

# Learning Objectives

By the end of this lesson you will be able to:

- Assemble a complete **typeahead** search pipeline.
- Implement **polling** that never overlaps requests and can be stopped.
- Apply **retry-with-backoff** and **caching** with `shareReplay`.
- Bridge Observables to Promises with `firstValueFrom` / `lastValueFrom`.
- Convert Promises to Observables and cancel `fetch` with `fromFetch`.
- Avoid the deprecated `toPromise()` and the empty-stream rejection pitfall.

# Why This Concept Exists

This is the capstone: the individual operators you've learned become **patterns** you'll reach for every day. Typeahead, polling, retry, and caching recur in nearly every app, and real codebases mix Observables with Promise-based APIs (`fetch`, `async/await`, libraries). Knowing the canonical shapes — and how to cross the Observable/Promise boundary cleanly — is what makes RxJS practical rather than academic.

# Concept Explanation

### Pattern 1 — Typeahead search

The full recipe: shape input, settle it, drop duplicates, then cancel stale requests with `switchMap`, recovering per request.

```ts
import { fromEvent, map, filter, debounceTime, distinctUntilChanged, switchMap, catchError, of } from 'rxjs';
import { ajax } from 'rxjs/ajax';

fromEvent(input, 'input').pipe(
  map((e) => (e.target as HTMLInputElement).value.trim()),
  filter((t) => t.length >= 2),
  debounceTime(300),
  distinctUntilChanged(),
  switchMap((term) =>
    ajax.getJSON(`/api/search?q=${encodeURIComponent(term)}`).pipe(catchError(() => of([]))),
  ),
);
```

### Pattern 2 — Polling without overlap

Drive requests on a clock, but don't let a slow request pile up. Use **`exhaustMap`** (ignore ticks while a request is in flight) or **`concatMap`** (queue them) — not `switchMap`, which would cancel an in-flight request every tick. Stop with `takeUntil`.

```ts
import { timer, exhaustMap, takeUntil } from 'rxjs';
import { ajax } from 'rxjs/ajax';

timer(0, 5000)
  .pipe(
    exhaustMap(() => ajax.getJSON('/api/status')), // skip a tick if still loading
    takeUntil(stopPolling$),
  )
  .subscribe(update);
```

### Pattern 3 — Retry with backoff

Handle transient failures with bounded, backed-off retries, then a fallback (Lesson 14):

```ts
import { timer, retry, catchError, of } from 'rxjs';

request$.pipe(
  retry({ count: 3, delay: (_, n) => timer(2 ** n * 500) }), // 1s, 2s, 4s
  catchError(() => of(FALLBACK)),
);
```

### Pattern 4 — Caching with `shareReplay`

Fetch once, share to all subscribers, replay to latecomers (Lesson 13). Safe for completing sources:

```ts
import { shareReplay } from 'rxjs';
import { ajax } from 'rxjs/ajax';

export const config$ = ajax.getJSON('/api/config').pipe(shareReplay(1));
```

### Interop — Observable ↔ Promise

- **Observable → Promise:** `firstValueFrom(obs)` resolves with the **first** value (then unsubscribes); `lastValueFrom(obs)` resolves with the **last** value on **completion**. Both **reject** with `EmptyError` if the source completes without emitting — pass `{ defaultValue }` to avoid that.

```ts
import { firstValueFrom, lastValueFrom, of } from 'rxjs';

const first = await firstValueFrom(of(1, 2, 3));                 // 1
const last = await lastValueFrom(of(1, 2, 3));                   // 3
const safe = await firstValueFrom(EMPTY$, { defaultValue: null }); // null, no throw
```

> `toPromise()` is **deprecated** (removed in RxJS 8). Use `firstValueFrom`/`lastValueFrom`.

- **Promise → Observable:** `from(promise)` (Lesson 2). And **`fromFetch`** wraps `fetch` as an Observable that **aborts** the request (via `AbortController`) when you unsubscribe:

```ts
import { fromFetch } from 'rxjs/fetch';
import { switchMap } from 'rxjs';

search$.pipe(
  switchMap((q) => fromFetch(`/api/search?q=${q}`)), // switchMap cancels -> fetch aborts
);
```

`lastValueFrom` on an **infinite** stream never resolves — only use it on streams that complete.

# Key Terminology

- **Typeahead** — search-as-you-type: settle input, then cancel stale requests.
- **Polling** — repeatedly requesting on a timer; use `exhaustMap`/`concatMap` to avoid overlap.
- **`firstValueFrom` / `lastValueFrom`** — convert an Observable to a Promise (first / last value).
- **`EmptyError`** — thrown/rejected when a stream expected to emit completes empty.
- **`fromFetch`** — an Observable wrapper around `fetch` with automatic cancellation.
- **`from(promise)`** — turns a Promise into an Observable.

# Syntax Overview

```ts
import { timer, exhaustMap, switchMap, retry, shareReplay, takeUntil,
         firstValueFrom, lastValueFrom, from } from 'rxjs';
import { fromFetch } from 'rxjs/fetch';

// Polling that won't overlap and can be stopped:
timer(0, 5000).pipe(exhaustMap(() => request$), takeUntil(stop$));

// Cached, retried request:
const data$ = request$.pipe(retry({ count: 3, delay: () => timer(1000) }), shareReplay(1));

// Interop:
const value = await firstValueFrom(data$);          // Observable -> Promise
const obs$ = from(fetch('/api'));                    // Promise -> Observable
fromFetch('/api/x');                                 // cancellable fetch as Observable
```

# Real World Analogy

These patterns are the **standard playbook plays** a team runs without thinking. Typeahead is the receptionist who waits for you to finish speaking, ignores you if you repeat yourself, and drops the previous caller the instant you ask something new. Polling with `exhaustMap` is a night watchman who checks the doors every five minutes but won't start a new round while still mid-round. Retry-with-backoff is knocking again after 1, then 2, then 4 minutes rather than pounding continuously. Caching is the front desk keeping a photocopy so the hundredth person asking gets the copy, not a fresh trip to the archive. And `firstValueFrom`/`fromFetch` are the bilingual staff who translate seamlessly between the Observable world and the Promise world.

# Examples

## Example 1 — Basic: await an Observable with firstValueFrom

```ts
import { firstValueFrom, of, delay } from 'rxjs';

async function loadUser() {
  const user = await firstValueFrom(of({ id: 1, name: 'Ada' }).pipe(delay(200)));
  console.log('got', user); // got { id: 1, name: 'Ada' }
  return user;
}
```

**Why this is good:** It shows the clean, modern bridge from Observable to `async/await` — no deprecated `toPromise()`, and `firstValueFrom` unsubscribes after the first value so nothing lingers.

## Example 2 — Real-world: non-overlapping polling that stops

```ts
import { timer, exhaustMap, map, retry, takeUntil, Subject } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const stop$ = new Subject<void>();

timer(0, 5000)
  .pipe(
    exhaustMap(() =>
      ajax.getJSON<{ online: number }>('/api/health').pipe(
        retry({ count: 2, delay: () => timer(1000) }), // survive a blip
      ),
    ),
    map((res) => res.online),
    takeUntil(stop$),
  )
  .subscribe((online) => console.log('online users:', online));

// Later: stop$.next(); stop$.complete();
```

**Why this is good:** It combines three patterns correctly — `exhaustMap` prevents overlapping polls, `retry` absorbs transient failures, and `takeUntil` gives a clean stop. Using `switchMap` here would risk cancelling a poll every 5 seconds before it finished.

## Example 3 — Best practice: a cached, cancellable data service

```ts
import { shareReplay, switchMap, catchError, of, firstValueFrom } from 'rxjs';
import { fromFetch } from 'rxjs/fetch';

// One cached config stream (completes, so shareReplay(1) is safe).
const config$ = fromFetch('/api/config').pipe(
  switchMap((res) => (res.ok ? res.json() : of({ fallback: true }))),
  catchError(() => of({ fallback: true })),
  shareReplay(1),
);

// Reactive consumers subscribe; imperative code can await it:
config$.subscribe((c) => console.log('reactive:', c));
async function getConfig() {
  return firstValueFrom(config$); // reuses the cached value, no extra request
}
```

**Why this is good:** It layers the patterns into a real service — `fromFetch` for a cancellable request, `catchError` for resilience, `shareReplay(1)` to cache, and `firstValueFrom` to serve imperative callers — all from one shared execution.

# Common Mistakes

- **Using `toPromise()`.** Deprecated and removed in RxJS 8. **Fix:** `firstValueFrom`/`lastValueFrom`.
- **`lastValueFrom` on an infinite stream.** It never resolves (no completion). **Fix:** ensure the source completes, or use `firstValueFrom`.
- **`firstValueFrom` on an empty stream.** Rejects with `EmptyError`. **Fix:** pass `{ defaultValue }`.
- **Polling with `switchMap`.** Cancels the in-flight request each tick, possibly starving slow requests. **Fix:** `exhaustMap` (skip) or `concatMap` (queue).
- **Forgetting to stop polling.** It runs forever. **Fix:** `takeUntil(stop$)` or a bounded operator.
- **Uncancellable fetches in a `switchMap`.** A plain `fetch` Promise isn't aborted on switch. **Fix:** use `fromFetch`, which aborts on unsubscribe.

# Best Practices

- Typeahead: `map → filter → debounceTime → distinctUntilChanged → switchMap(request.pipe(catchError))`.
- Polling: `timer(0, period) → exhaustMap(request) → takeUntil(stop$)`.
- Wrap transient-failure requests in `retry` with **backoff and a max count**; add a `catchError` fallback.
- Cache completing sources with `shareReplay(1)`; serve imperative callers with `firstValueFrom`.
- Prefer `fromFetch` over `from(fetch(...))` when you want the request cancelled on unsubscribe.
- Never use `toPromise()`; always handle the possibly-empty case in `firstValueFrom`/`lastValueFrom`.

# Summary

- **Typeahead** = shape + `debounceTime` + `distinctUntilChanged` + `switchMap` (with inner `catchError`).
- **Polling** uses `timer(0, period)` + `exhaustMap`/`concatMap` (never `switchMap`) + `takeUntil`.
- **Retry-with-backoff** (`retry({ count, delay })`) and **caching** (`shareReplay(1)`) harden real requests.
- **`firstValueFrom`/`lastValueFrom`** convert Observables to Promises; they reject with `EmptyError` on empty streams unless given a `defaultValue`.
- **`from(promise)`** and **`fromFetch`** convert the other way; `fromFetch` cancels via `AbortController`.
- **`toPromise()` is gone** — use the `*ValueFrom` functions, and only `lastValueFrom` on completing streams.

# Flash Cards

Q: What is the correct operator for the request step of a typeahead, and why?
A: `switchMap` — it cancels the previous request when a newer search term arrives, so stale responses can't overwrite fresh ones. Put a `catchError` inside it.

Q: Why use `exhaustMap` (or `concatMap`) instead of `switchMap` for polling?
A: `switchMap` would cancel the in-flight request on every tick, potentially never completing a slow request. `exhaustMap` skips ticks while busy; `concatMap` queues them.

Q: How do you convert an Observable to a Promise in RxJS 7, and what replaced `toPromise()`?
A: `firstValueFrom(obs)` (first value) or `lastValueFrom(obs)` (last value on completion). They replace the deprecated/removed `toPromise()`.

Q: What happens if you call `firstValueFrom` on a stream that completes without emitting?
A: The Promise rejects with an `EmptyError`. Pass `{ defaultValue }` to resolve with a fallback instead.

Q: Why can't you use `lastValueFrom` on an infinite stream?
A: `lastValueFrom` resolves only when the source completes; an infinite stream never completes, so the Promise never resolves.

Q: What advantage does `fromFetch` have over `from(fetch(url))`?
A: `fromFetch` aborts the underlying request (via AbortController) when you unsubscribe — so a `switchMap` truly cancels the network call, unlike a plain fetch Promise.

# Exercises

### Easy

Write an async function that uses `firstValueFrom` to await `of('hello').pipe(delay(300))` and logs the value. Then call `firstValueFrom(EMPTY)` and handle the rejected `EmptyError`, retrying with `{ defaultValue: 'none' }`.

### Medium

Implement polling: every 3 seconds, call a fake request `of(Date.now()).pipe(delay(500))` using `exhaustMap`, and stop after 10 seconds with `takeUntil(timer(10000))`. Log each result. Then switch the operator to `switchMap` and describe in a comment what could go wrong if requests were slower than the interval.

### Challenging

Build a cached data service: `fromFetch('/api/data')` → `switchMap(res => res.json())` → `catchError(() => of([]))` → `shareReplay(1)`. Subscribe twice reactively and also expose an `async getData()` using `firstValueFrom`. Confirm only one request fires. Then explain in a comment why `shareReplay(1)` is safe here and what would change for an infinite source.

# Further Reading

- [RxJS — firstValueFrom](https://rxjs.dev/api/index/function/firstValueFrom) and [lastValueFrom](https://rxjs.dev/api/index/function/lastValueFrom)
- [RxJS — fromFetch](https://rxjs.dev/api/fetch/fromFetch)
- [RxJS — ajax](https://rxjs.dev/api/ajax/ajax)
- [RxJS — shareReplay](https://rxjs.dev/api/index/function/shareReplay) and [retry](https://rxjs.dev/api/index/function/retry)
