---
slug: empty-never-and-throw-error
title: "Special streams: EMPTY, NEVER, and throwError()"
topic: creating-observables
kind: api
summary: "EMPTY completes immediately with no values, NEVER never emits or completes, and throwError(() => err) errors immediately using an error factory."
tags: [rxjs, empty, never, throwerror, creation]
related: [of-and-from, catch-error-recovery, defer-per-subscriber]
---
> **In one line:** `EMPTY` completes immediately with zero values, `NEVER` stays silent forever, and `throwError(() => new Error(...))` errors immediately using a factory function.

## The idea

Sometimes a branch of your pipeline needs an Observable that emits **no values at all** — either to finish quietly, to wait forever in a test, or to signal an immediate failure. RxJS provides three minimal streams for those three cases:

| Source | `next` emissions | Terminal notification | Typical use |
|---|---|---|---|
| **`EMPTY`** | None | `complete` immediately | "Do nothing and finish" branch inside `switchMap` or `catchError` |
| **`NEVER`** | None | None (stays open forever) | Testing timeouts or keeping a stream open indefinitely |
| **`throwError(() => err)`** | None | `error` immediately | Raising a domain error in a pipe or rethrowing inside `catchError` |

Notice that `EMPTY` and `NEVER` are **constant** Observable instances (not functions you call), whereas `throwError` is a function. In RxJS 7+, `throwError` takes an **error factory** — `() => new Error('boom')` — rather than the error object directly.

## See it

```typescript
import { EMPTY, NEVER, throwError } from 'rxjs';

// 1. EMPTY: no next, immediate complete
EMPTY.subscribe({
  next: () => console.log('never runs'),
  complete: () => console.log('EMPTY completed immediately'),
});

// 2. throwError: pass a factory function in RxJS 7+
// Before (deprecated): throwError(new Error('boom'))
// After (RxJS 7+):
const fail$ = throwError(() => new Error('invalid token'));

fail$.subscribe({
  error: (err: Error) => console.error('Caught:', err.message),
});

// 3. NEVER: emits nothing and never completes
const sub = NEVER.subscribe(() => console.log('never runs'));
sub.unsubscribe();
```

## Go deeper

Why does `throwError` require a factory function `() => new Error(...)` instead of `new Error(...)` directly? In JavaScript, `new Error()` captures the stack trace at the moment the `Error` object is **created**. If you created the error once when defining the Observable, every subscriber and every retry would receive a stale stack trace pointing to definition time instead of subscription time. Calling the factory on each `subscribe()` produces a fresh `Error` with an accurate stack trace and timestamp:

```typescript
import { of, mergeMap, EMPTY, throwError } from 'rxjs';

of(10, -1, 20)
  .pipe(
    mergeMap((n) => {
      if (n === 0) return EMPTY; // skip silently
      if (n < 0) return throwError(() => new Error(`Negative: ${n}`));
      return of(n * 2);
    }),
  )
  .subscribe({
    next: console.log,
    error: (e: Error) => console.error('Stopped:', e.message),
  });
```

## Watch out

- Passing an error instance directly — `throwError(new Error('x'))` — is deprecated in RxJS 7 and removed in RxJS 8; always pass a factory `throwError(() => new Error('x'))`.
- `EMPTY` is a constant (`EMPTY`), not a function (`empty()` is deprecated and removed).

## Remember

- `EMPTY` emits nothing and completes immediately — ideal as a no-op fallback in `catchError`.
- `NEVER` emits nothing and never completes or errors.
- `throwError(() => new Error('...'))` takes an error factory and errors immediately on subscribe.
