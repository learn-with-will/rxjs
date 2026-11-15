---
slug: default-if-empty-and-throw-if-empty
title: "Handling empty streams: defaultIfEmpty() and throwIfEmpty()"
topic: errors-cleanup
kind: api
summary: "defaultIfEmpty(val) emits a fallback value if the source completes without emitting anything; throwIfEmpty(factory?) errors with EmptyError instead."
tags: [rxjs, defaultifempty, throwifempty, empty, emptyerror]
related: [empty-never-and-throw-error, fork-join, empty-error-and-take-one]
---
> **In one line:** if a source completes with **zero emissions**, **`defaultIfEmpty(val)`** emits `val` before completing, whereas **`throwIfEmpty()`** turns that silent empty completion into an error.

## The idea

Sometimes a stream completes normally without ever emitting a `next` value — for example `EMPTY`, a filtered stream where no item matched `filter(pred)`, or a `takeUntil` that fired before the first emission.

If the source emits **at least one value**, both `defaultIfEmpty` and `throwIfEmpty` do nothing at all and simply forward every value and completion through. Their difference appears **only when the source completes empty**:

| Operator | If source emits `'A', 'B'` then completes | If source completes **empty** (0 values) |
|---|---|---|
| *(none)* | Emits `'A', 'B'`, then completes | Emits nothing; completes immediately |
| **`defaultIfEmpty('none')`** | Emits `'A', 'B'`, then completes | Emits **`'none'`**, then completes |
| **`throwIfEmpty()`** | Emits `'A', 'B'`, then completes | Errors with **`EmptyError`** (or custom factory error) |

## See it

```typescript
import { of, filter, defaultIfEmpty, throwIfEmpty } from 'rxjs';

const numbers$ = of(1, 3, 5);

// 1. defaultIfEmpty: emit a fallback value if filter() drops everything
numbers$
  .pipe(
    filter((n) => n % 2 === 0), // Drops 1, 3, 5 -> stream completes empty!
    defaultIfEmpty(-1),         // Steps in on empty completion and emits -1
  )
  .subscribe((v) => console.log('defaultIfEmpty:', v)); // -1

// 2. throwIfEmpty: fail loudly if a stream completes with zero values
numbers$
  .pipe(
    filter((n) => n > 100),     // Drops 1, 3, 5 -> stream completes empty!
    throwIfEmpty(() => new Error('No numbers > 100 found!')),
  )
  .subscribe({
    next: (v) => console.log('next:', v),
    error: (err: Error) => console.log('throwIfEmpty error:', err.message),
  });
// Logs: "throwIfEmpty error: No numbers > 100 found!"
```

## Go deeper

Where is `defaultIfEmpty` a lifesaver in real-world code? Inside **`forkJoin`**!

Remember that `forkJoin({ user: user$, badge: badge$ })` requires **every** input stream to emit at least one value before completing — if `badge$` completes empty (`EMPTY`), the entire `forkJoin` completes without emitting `user`! Piping optional streams through `badge$.pipe(defaultIfEmpty(null))` guarantees that `badge$` emits `null` instead of completing empty, so `forkJoin` always resolves with `{ user, badge: null }`.

```typescript
import { of, EMPTY, forkJoin, defaultIfEmpty } from 'rxjs';

forkJoin({
  user: of('Ada'),
  badge: EMPTY.pipe(defaultIfEmpty(null)), // Prevents EMPTY from cancelling forkJoin!
}).subscribe((res) => console.log('forkJoin result:', res));
// Logs: { user: 'Ada', badge: null }
```

## Watch out

- Neither `defaultIfEmpty` nor `throwIfEmpty` ever fires on a stream that **never completes** (like `NEVER` or an open `Subject`) — they inspect the stream at the moment `complete()` arrives.
- Don't confuse `defaultIfEmpty(fallback)` (handles an **empty completion**) with `catchError(() => of(fallback))` (handles an **error**).

## Remember

- `defaultIfEmpty(fallback)` emits `fallback` if and only if the source completes without emitting any `next` value.
- `throwIfEmpty(factory?)` throws `EmptyError` (or the error from `factory()`) if the source completes empty.
- Wrap optional streams inside `forkJoin` with `.pipe(defaultIfEmpty(null))` so an empty stream doesn't short-circuit `forkJoin`.
