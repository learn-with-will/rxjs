---
slug: timeout-operator
title: "Enforcing time limits with timeout()"
topic: errors-cleanup
kind: api
summary: "timeout(ms) or timeout({ first, each, with }) errors with TimeoutError (or switches to a fallback stream via with) if emissions do not arrive in time."
tags: [rxjs, timeout, timeouterror, errors, rxjs-7]
related: [catch-error-recovery, retry-and-backoff, race-first-wins]
since: "7.0"
---
> **In one line:** `timeout(ms)` (or `timeout({ first, each, with })`) aborts the upstream subscription if a value doesn't arrive within the deadline — either throwing a **`TimeoutError`** or switching to a fallback Observable via **`with`**.

## The idea

A hung HTTP request or silent WebSocket can leave a user staring at a spinner forever. **`timeout`** sets a deadline on how long you are willing to wait for emissions.

In **RxJS 7+**, `timeout` accepts either a simple millisecond number or a rich configuration object:

- **`timeout(3000)`**: if more than `3000` ms pass before the first emission (or between any two subsequent emissions), unsubscribe upstream and error with **`TimeoutError`**.
- **`timeout({ first: 5000, each: 2000, with: () => fallback$ })`**:
  - `first`: deadline in ms (or a `Date`) for the **very first** emission to arrive.
  - `each`: deadline in ms between **subsequent** emissions after the first.
  - `with`: a factory `(info) => ObservableInput` that replaces the default `TimeoutError` with a custom fallback stream or custom error (replacing the deprecated `timeoutWith` operator!).

| Call | What it enforces | On timeout |
|---|---|---|
| `timeout(2000)` | Max 2,000ms before 1st value and between values | Emits `TimeoutError` |
| `timeout({ first: 1000 })` | Max 1,000ms for the 1st value only | Emits `TimeoutError` |
| `timeout({ first: 1000, with: () => of(cached) })` | Max 1,000ms for 1st value | Switches cleanly to `of(cached)` |

## See it

```typescript
import { timer, map, timeout, TimeoutError, catchError, of } from 'rxjs';

const slowApi$ = timer(2000).pipe(map(() => 'Live Server Data'));

// 1. Default TimeoutError caught downstream:
slowApi$
  .pipe(
    timeout(500),
    catchError((err) =>
      err instanceof TimeoutError ? of('Fallback after TimeoutError') : of('Other error'),
    ),
  )
  .subscribe((v) => console.log('Example 1:', v));

// 2. RxJS 7+ config with `first` and `with` (no catchError needed!):
slowApi$
  .pipe(
    timeout({ first: 500, with: () => of('Cached Fallback via timeout({ with })') }),
  )
  .subscribe((v) => console.log('Example 2:', v));
```

## Go deeper

Why is `timeout({ first: 3000, with: () => of(fallback) })` so clean in RxJS 7+?

1. When the 500ms deadline expires, `timeout` **automatically unsubscribes** from `slowApi$` (aborting the underlying `fromFetch` or `ajax` request!).
2. If you use `with: () => of(fallback)`, it switches straight to the fallback Observable without wrapping and unwrapping an error through `catchError` (which might otherwise catch unrelated non-timeout errors!).
3. You can also combine `timeout(1000)` **followed by** `retry(2)` so each individual attempt gets a 1,000ms deadline before triggering a retry!

## Watch out

- Place `timeout(1000)` **before** `retry(2)` if you want each retry attempt to have its own 1-second deadline; placing `timeout` *after* `retry` puts a single deadline on the entire retry sequence combined.
- The old `timeoutWith(ms, fallback$)` operator is deprecated in RxJS 7+ — use `timeout({ each: ms, with: () => fallback$ })` instead.

## Remember

- `timeout(ms)` unsubscribes upstream and errors with `TimeoutError` if `ms` elapses without an emission.
- Use `timeout({ first, each, with })` in RxJS 7+ to configure initial vs ongoing deadlines and fallback streams.
- Check `err instanceof TimeoutError` when handling timeouts inside `catchError`.
