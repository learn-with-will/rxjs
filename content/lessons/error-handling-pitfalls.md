---
slug: error-handling-pitfalls
title: "Common RxJS error-handling pitfalls"
topic: errors-cleanup
kind: pitfall
summary: "Avoid the four classic error traps: putting catchError before retry, returning a raw array instead of of([]), retrying a hot stream, and eager throwError."
tags: [rxjs, errors, catcherror, retry, throwerror, pitfall]
related: [catch-error-recovery, retry-and-backoff, catching-errors-inside-switch-map, empty-never-and-throw-error]
---
> **In one line:** watch out for the four classic error bugs: **`catchError` before `retry`**, **`catchError(() => [])` instead of `of([])`**, **retrying a hot stream or `from(promise)`**, and **calling `throwError(err)` without a factory**.

## The idea

Even when you know `catchError` and `retry`, four subtle mistakes can silently break your error recovery logic:

| Pitfall | Broken code | Why it fails | Fix |
|---|---|---|---|
| **1. `catchError` above `retry`** | `.pipe(catchError(...), retry(2))` | `catchError` swallows the error into a normal completion, so `retry` never sees an error! | Put **`retry(2)` above `catchError(...)`** |
| **2. Returning `[]` in `catchError`** | `catchError(() => [])` | `[]` is an Iterable, so RxJS treats it as `from([])` = `EMPTY` (0 emissions)! | Return **`of([])`** to emit an empty array |
| **3. Retrying `from(fetch(...))`** | `from(fetch(url)).pipe(retry(2))` | `fetch(url)` already ran once at creation; retrying re-awaits the same rejected Promise! | Wrap in **`defer(() => fetch(url))`** or `fromFetch` |
| **4. Eager `throwError(new Error())`** | `throwError(new Error('x'))` | Deprecated in RxJS 7; captures stack trace at creation time and shares one Error instance | Pass a factory: **`throwError(() => new Error('x'))`** |

## See it

Compare the broken vs fixed versions of Pitfalls 1, 2, and 3:

```typescript
import { defer, of, throwError, retry, catchError } from 'rxjs';

let calls = 0;
const coldApi$ = defer(() => throwError(() => new Error(`Fail #${++calls}`)));

// Before (bugs #1 and #2): catchError is BEFORE retry(2), and returns raw []!
coldApi$
  .pipe(
    catchError(() => []), // Swallows error AND emits 0 values (from([]) = EMPTY)!
    retry(2),             // Never reached because catchError already recovered!
  )
  .subscribe((items) => console.log('Before (never logs!):', items));

// After: retry(2) FIRST on a cold defer() stream, then catchError returning of([])!
calls = 0;
coldApi$
  .pipe(
    retry(2),                 // Runs 1 initial + 2 retries = 3 calls total
    catchError(() => of([])), // Emits [] as fallback after all retries fail
  )
  .subscribe((items) => console.log('After (3 attempts):', items)); // []
```

## Go deeper

Why does Pitfall #3 (`from(fetch(url)).pipe(retry(2))`) catch so many developers off guard? Because `fetch(url)` is called **eagerly** before `from(...)` even receives the Promise! Once a JavaScript `Promise` rejects, it is permanently rejected — subscribing to `from(sameRejectedPromise)` three times just reads the same rejection three times without ever making a second HTTP call. Wrapping the call in `defer(() => fetch(url))` (or using `fromFetch(url)`) makes the Promise creation lazy so each retry invokes `fetch(url)` fresh.

## Watch out

- Audit any `catchError(() => [])` or `catchError(() => 'fallback')` in your codebase — in JavaScript, both arrays `[]` and strings `'fallback'` are `Iterable`, so `catchError` will iterate their elements one by one unless you wrap them in `of([])` or `of('fallback')`!
- Always place `retry` **before** `catchError` in the `.pipe()` chain.

## Remember

- Order matters: place `retry(...)` **above** `catchError(...)` so retries happen before fallback recovery.
- Always wrap fallback values in `of(value)` inside `catchError` — returning raw `[]` or `'str'` treats them as iterables!
- `retry` only re-executes cold streams — wrap eager Promises in `defer(() => ...)`.
