---
slug: retry-and-backoff
title: "Retrying failed streams with retry() and backoff"
topic: errors-cleanup
kind: api
summary: "retry(count) or retry({ count, delay, resetOnSuccess }) resubscribes to a cold source when it errors, supporting fixed or exponential backoff in RxJS 7.3+."
tags: [rxjs, retry, backoff, errors, rxjs-7]
related: [catch-error-recovery, errors-are-terminal, error-handling-pitfalls]
since: "7.3"
---
> **In one line:** `retry({ count, delay })` catches an upstream error and **re-subscribes to the cold source from the beginning**, optionally waiting with a fixed or exponential backoff delay between attempts.

## The idea

Network requests and WebSockets occasionally fail due to transient blips. Because a cold Observable creates a fresh execution every time you subscribe to it, **retrying** in RxJS simply means **catching the error and subscribing to the upstream Observable again**.

- **`retry(2)`** allows **1 initial attempt + up to 2 retries = 3 total attempts** immediately back-to-back. If the 3rd attempt still errors, the error is forwarded downstream.
- **`retry({ count, delay, resetOnSuccess })` (RxJS 7.3+):** replaces the deprecated `retryWhen` operator with a clean configuration object:
  - `count`: maximum number of retry attempts before giving up.
  - `delay`: either a number in milliseconds (`delay: 1000`) or a function `(error, retryCount) => ObservableInput` returning a timer (for exponential backoff or conditional retries!).
  - `resetOnSuccess: true`: resets the retry counter back to `0` whenever the source successfully emits a value (essential for long-lived WebSockets!).

| Config | Total attempts (1 initial + retries) | Delay between attempts |
|---|---|---|
| `retry(2)` | 3 attempts total | Immediate (0ms) |
| `retry({ count: 3, delay: 1000 })` | 4 attempts total | Fixed 1,000ms pause |
| `retry({ count: 3, delay: (err, n) => timer(n * 1000) })` | 4 attempts total | Linear/exponential backoff |

## See it

Here is how to implement exponential backoff that also bails out immediately without retrying on `404 Not Found` errors:

```typescript
import { defer, throwError, timer, retry, catchError, of } from 'rxjs';

let attempt = 0;
const flakyRequest$ = defer(() =>
  ++attempt < 3
    ? throwError(() => ({ status: 503, message: 'Unavailable' }))
    : of('Loaded on attempt #3!'),
);

flakyRequest$
  .pipe(
    retry({
      count: 3,
      delay: (err: { status: number }, retryCount) => {
        if (err.status >= 400 && err.status < 500) return throwError(() => err);
        return timer(Math.pow(2, retryCount - 1) * 100); // 100ms, 200ms, 400ms
      },
    }),
    catchError(() => of('Fallback after retries exhausted')),
  )
  .subscribe((res) => console.log('Result:', res));
```

## Go deeper

Look at how `retry` and `catchError` work together in that pipeline:

1. **`retry({ count: 3, ... })` goes first:** it tries up to 3 retries with backoff.
2. **`catchError(...)` goes after `retry`:** only if all retries are exhausted (or the `delay` function rethrows a 4xx error) does the error reach `catchError`, which provides the final fallback value!

If you accidentally put `catchError` *before* `retry`, `catchError` would swallow the error on the first failure and `retry` would never see an error to retry!

> **Note:** `retryWhen` is deprecated in RxJS 7.3+ and scheduled for removal in v8 — always use `retry({ count, delay })` instead.

## Watch out

- `retry` only re-runs work if the upstream Observable is **cold** (such as `defer`, `ajax`, `fromFetch`, or `new Observable`). Retrying a hot `Subject` just resubscribes to an already-dead Subject!
- Always place `retry(...)` **before** `catchError(...)` in `.pipe()`.

## Remember

- `retry(n)` runs 1 initial attempt plus up to `n` retries (total `n + 1` attempts) before forwarding the error.
- Use `retry({ count, delay: (err, retryCount) => timer(...) })` in RxJS 7.3+ for exponential backoff and conditional retries.
- Place `retry` **above** `catchError` so `catchError` acts as the final safety net after retries fail.
