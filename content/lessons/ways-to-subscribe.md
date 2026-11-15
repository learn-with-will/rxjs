---
slug: ways-to-subscribe
title: "Ways to call subscribe()"
topic: observables-subscriptions
kind: api
summary: "Pass subscribe() a full Observer object, a partial Observer object, or a single next callback function; separate positional callbacks are deprecated."
tags: [rxjs, subscribe, observer, deprecation]
related: [observer-callbacks, subscription-and-unsubscribe, unsubscribe-vs-complete-pitfall]
---
> **In one line:** pass `subscribe()` either an **Observer object** `{ next, error, complete }` (full or partial) or a **single `next` function** — never three separate positional functions.

## The idea

Every callback on an Observer is optional. In RxJS 7+, `observable.subscribe()` accepts your handlers in three clean ways:

| Form | Example | When to use |
|---|---|---|
| **Full Observer object** | `source$.subscribe({ next, error, complete })` | When you care about values, errors, and completion |
| **Partial Observer object** | `source$.subscribe({ error: console.error })` | When you only need one or two of the callbacks |
| **Single `next` function** | `source$.subscribe((v) => console.log(v))` | Quick shorthand when you only handle `next` |
| **No arguments** | `source$.subscribe()` | When side effects are already handled upstream in `tap` |

> **Watch out:** Passing **two or three positional functions** — `subscribe(nextFn, errorFn, completeFn)` — is **deprecated in RxJS 7** and removed in RxJS 8 because it was easy to pass `null` or `undefined` in the wrong slot. Always use the Observer object when you need `error` or `complete`.

## See it

```typescript
import { of, EMPTY } from 'rxjs';

// Before (deprecated in RxJS 7 — do NOT use positional error/complete args):
// of(1, 2).subscribe(
//   (n) => console.log(n),
//   (err) => console.error(err),
//   () => console.log('done'),
// );

// After: 1. Full or partial Observer object (preferred)
of(1, 2).subscribe({
  next: (n) => console.log('value:', n),
  error: (err) => console.error('error:', err),
  complete: () => console.log('done'),
});

// After: 2. Shorthand when you only need next
of(1, 2).subscribe((n) => console.log('shorthand:', n));

// After: 3. Partial observer watching only completion
EMPTY.subscribe({
  complete: () => console.log('EMPTY finished'),
});
```

## Go deeper

Using a partial Observer object is much clearer than positional arguments when you only care about `error` or `complete`. With the old positional signature, watching only `complete` forced you to write `source$.subscribe(undefined, undefined, () => console.log('done'))`. With an Observer object, you simply write `source$.subscribe({ complete: () => console.log('done') })`, which is self-documenting at every call site.

```typescript
import { throwError } from 'rxjs';

// Partial observer with only an error handler:
throwError(() => new Error('timeout')).subscribe({
  error: (err: Error) => console.warn('Handled:', err.message),
});
```

## Watch out

- `subscribe(fn1, fn2, fn3)` is deprecated; wrap multiple callbacks in `{ next, error, complete }`.
- Calling `subscribe((v) => ...)` without an `error` handler means an unexpected error will go unhandled unless caught earlier in `.pipe(catchError(...))`.

## Remember

- Pass `subscribe()` an Observer object `{ next, error, complete }` or a single `next` function.
- All three properties on the Observer object are optional (partial Observer).
- The multi-argument positional form `subscribe(next, error, complete)` is deprecated in RxJS 7+.
