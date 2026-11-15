---
slug: promise-interop-pitfalls
title: "Promise interop pitfalls: EmptyError, infinite hangs, and eager Promises"
topic: patterns-interop
kind: pitfall
summary: "Avoid the 3 bugs at the Promise-Observable boundary: EmptyError rejections, awaiting lastValueFrom on hot streams, and eager from(promise)."
tags: [rxjs, promises, firstvaluefrom, defer, pitfalls]
related: [first-value-from-and-last-value-from, from-fetch-and-abort, defer-per-subscriber]
---
> **In one line:** when crossing between Promises and Observables, guard `firstValueFrom`/`lastValueFrom` with `{ defaultValue }`, never `await lastValueFrom()` on a non-completing stream, and wrap Promise factories in `defer(() => ...)` instead of `from(fn())`.

## The idea

Promises and Observables have fundamentally different lifecycles:
- A **Promise** is **eager** (starts on creation), emits **exactly 1 value** (cannot complete with 0 values), and **cannot be cancelled**.
- An **Observable** is **lazy** (starts on `subscribe`), can emit **0, 1, or N values**, may **never complete**, and **can be cancelled**.

When converting between the two, watch out for three classic boundary bugs:

| Boundary direction | The pitfall | What goes wrong | The fix |
|---|---|---|---|
| **Observable → Promise** (`firstValueFrom` / `lastValueFrom`) | Stream completes with **0 emissions** (`EMPTY`, or `filter()` matched nothing) | Promise rejects with **`EmptyError: no elements in sequence`** | Pass `{ defaultValue: fallback }` as the 2nd argument |
| **Observable → Promise** (`lastValueFrom`) | Calling `await lastValueFrom(store$)` on a `BehaviorSubject` or `fromEvent` | Promise **hangs forever** because the source never calls `complete()` | Use **`firstValueFrom(store$)`** (or pipe `take(N)` before `lastValueFrom`) |
| **Promise → Observable** (`from(fetchData())`) | Passing an already-invoked Promise `from(fetchData())` instead of a factory | `fetchData()` runs **immediately** before `subscribe()`, and `retry()` reuses the same dead Promise! | Wrap in **`defer(() => fetchData())`** so a fresh Promise is created per subscription |

## See it

See all three fixes side by side:

```typescript
import { of, filter, BehaviorSubject, firstValueFrom, defer, retry } from 'rxjs';

// 1. Prevent EmptyError when a filtered stream emits 0 items:
async function findAdminName(names: string[]): Promise<string> {
  const admin$ = of(...names).pipe(filter(n => n.startsWith('admin:')));
  // Without { defaultValue }, an array with no admin throws EmptyError!
  return firstValueFrom(admin$, { defaultValue: 'admin:default' });
}

// 2. Always use firstValueFrom (NOT lastValueFrom) on BehaviorSubjects:
const theme$ = new BehaviorSubject('dark');
async function readTheme() {
  return firstValueFrom(theme$); // Resolves immediately and unsubscribes!
}

// 3. Wrap Promise-returning functions in defer(() => ...), NOT from(fn()):
const lazyApi$ = defer(() => fetch('/api/status').then(r => r.json())).pipe(
  retry(2) // Calls fetch() fresh on each retry!
);
```

## Go deeper

What if you wrap an `async` function inside `defer(async () => ...)` and you also want it to abort when RxJS unsubscribes? Since `defer` only expects an `ObservableInput`, it cannot pass an `AbortSignal` into an `async` function on its own. If your async work supports `AbortSignal` (like `fetch(url, { signal })` or Axios), either use `fromFetch(url)` or write a tiny `new Observable(subscriber => { const ctrl = new AbortController(); myAsyncFn(ctrl.signal).then(...); return () => ctrl.abort(); })` wrapper so RxJS cancellation propagates all the way into the Promise!

## Watch out

- An unhandled `EmptyError` from `await firstValueFrom(obs$)` without `{ defaultValue }` or `try/catch` will reject the outer `async` function and trigger an unhandled Promise rejection.
- Passing an `async` callback to `map(async x => ...)` produces an `Observable<Promise<T>>` instead of `Observable<T>` — always use `switchMap`, `concatMap`, or `mergeMap` when your callback returns a Promise!

## Remember

- Always pass `{ defaultValue }` to `firstValueFrom` and `lastValueFrom` if the source Observable can complete empty.
- Never call `lastValueFrom` on an infinite stream (`BehaviorSubject`, `Subject`, `fromEvent`, `interval`).
- Convert Promise-returning functions into Observables with `defer(() => promiseFn())`, never `from(promiseFn())`.
