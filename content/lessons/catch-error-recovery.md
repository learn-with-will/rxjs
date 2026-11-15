---
slug: catch-error-recovery
title: "Recovering from errors with catchError()"
topic: errors-cleanup
kind: api
summary: "catchError((err, caught$) => recovery$) intercepts an upstream error and switches to a recovery Observable: a fallback value, EMPTY, or throwError."
tags: [rxjs, catcherror, errors, recovery, fallback]
related: [errors-are-terminal, retry-and-backoff, catching-errors-inside-switch-map, empty-never-and-throw-error]
---
> **In one line:** `catchError((err, caught$) => recovery$)` intercepts an upstream error and **switches to whatever recovery Observable your callback returns**.

## The idea

Because the upstream Observable that errored has already terminated, `catchError` cannot "resume" the broken execution at the line that threw. Instead, `catchError(selector)` catches the error notification and asks your `selector(err, caught$)` function to **return a replacement Observable** for downstream subscribers to switch to.

That gives you four standard recovery strategies depending on what Observable you return:

| Strategy | What to return from `catchError` | Downstream effect |
|---|---|---|
| **1. Emit fallback value(s)** | `return of(defaultData)` | Emits `defaultData` and **completes cleanly** |
| **2. Swallow & complete silently** | `return EMPTY` | Emits no fallback value and **completes cleanly** |
| **3. Log & rethrow (or wrap error)** | `return throwError(() => new CustomError(err))` | Forwards the new error downstream |
| **4. Retry the source** | `return caught$` *(2nd parameter)* | Resubscribes to the upstream pipeline from scratch (prefer `retry(n)`!) |

## See it

Here are the first three strategies in action:

```typescript
import { throwError, of, EMPTY, catchError } from 'rxjs';

const failing$ = throwError(() => new Error('Network 503'));

// 1. Replace error with a safe fallback value (of([])):
failing$
  .pipe(catchError(() => of([])))
  .subscribe((items) => console.log('Fallback items:', items)); // []

// 2. Complete silently with EMPTY (no next emission):
failing$
  .pipe(catchError(() => EMPTY))
  .subscribe({ complete: () => console.log('Swallowed via EMPTY') });

// 3. Wrap and rethrow a domain error with throwError(() => ...):
failing$
  .pipe(catchError((e: Error) => throwError(() => new Error(`Wrapped: ${e.message}`))))
  .subscribe({ error: (e: Error) => console.log('Rethrown:', e.message) });
```

## Go deeper

Notice two crucial details in the snippet above:

1. **`catchError` must return an `ObservableInput`** (an Observable like `of(fallback)`, `EMPTY`, `throwError(...)`, or a Promise) — returning a raw value like `return []` instead of `return of([])` is a mistake (or with arrays, `return []` is treated as `from([])`, which is `EMPTY` instead of emitting `[]`!).
2. When you recover with `of(fallback)` or `EMPTY`, the downstream subscriber sees a **normal `complete()`**, not an `error()`.

## Watch out

- Do **not** write `catchError(() => [])` when you want to emit an empty array as a fallback! Because an array is an `Iterable` (`ObservableInput`), RxJS unwraps `[]` like `from([])` (which emits **nothing** and completes!). Always write **`catchError(() => of([]))`** to emit the empty array itself.
- Never return `caught$` (the second parameter) unconditionally without a counter, or a synchronous error will cause an infinite synchronous retry loop!

## Remember

- `catchError((err) => recovery$)` replaces a failed stream with a new recovery Observable.
- Return `of(fallback)` to emit a default value, `EMPTY` to complete silently, or `throwError(() => err)` to rethrow.
- Always wrap fallback arrays in `of([])` so RxJS doesn't unwrap the array as an empty iterable.
