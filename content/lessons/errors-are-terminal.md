---
slug: errors-are-terminal
title: "Why uncaught errors terminate RxJS streams"
topic: errors-cleanup
kind: concept
summary: "By the Observable contract next* (error | complete)?, an error is a terminal signal that tears down the upstream subscription and closes the stream."
tags: [rxjs, errors, contract, lifecycle, catcherror]
related: [observable-contract-grammar, catch-error-recovery, catching-errors-inside-switch-map]
---
> **In one line:** in RxJS, an **`error` notification is terminal** — the instant an Observable errors, its producer tears down and **no further `next` values can ever flow through that subscription**.

## The idea

In synchronous JavaScript, if one iteration of a `for` loop throws an uncaught exception, the entire loop aborts immediately unless you put a `try / catch` **inside** the loop body.

Observables follow the exact same rule via the Observable contract `next* (error | complete)?`:

1. When a producer calls `subscriber.error(err)` — or when any callback inside `map`, `filter`, or `tap` throws a synchronous exception — RxJS catches it and turns it into an **`error` notification** traveling downstream.
2. As that `error` travels down `.pipe()`, every operator along the way **unsubscribes from its upstream source** and runs teardown.
3. If no `catchError` operator intercepts the error before `subscribe()`, your Observer's `error(err)` callback runs (or an unhandled error is thrown) and `subscription.closed` becomes `true`.
4. Even if you caught the error at the bottom of the pipeline, **the upstream subscription that errored is already dead and torn down**!

```sequence
actor src: Source Stream | emits 1, then errors @primary
actor m: map(x => x * 10) | torn down on error! @red
actor sub: Subscriber | receives error, stream is CLOSED @amber
src -> m: next(1)
m -> sub: next(10)
src -> m: error(new Error('500'))
m -> sub: error(Error: 500) -> teardown runs, subscription.closed = true!
```

## See it

Watch how throwing inside `map` on the second item immediately stops the stream and prevents item `3` from ever being processed:

```typescript
import { of, map, finalize } from 'rxjs';

of(1, 2, 3)
  .pipe(
    map((n) => {
      if (n === 2) {
        throw new Error('Boom on 2!');
      }
      return n * 10;
    }),
    finalize(() => console.log('Teardown ran immediately on error')),
  )
  .subscribe({
    next: (v) => console.log('next:', v),
    error: (err: Error) => console.log('error caught in subscribe:', err.message),
    complete: () => console.log('complete (never runs when stream errors!)'),
  });
// Output:
// next: 10
// error caught in subscribe: Boom on 2!
// Teardown ran immediately on error
// (Note: 3 is never emitted, and complete() never runs!)
```

## Go deeper

Look closely at what passing an `error: (err) => ...` callback to `.subscribe({ next, error })` does vs what it does **not** do:

- Passing `error` in `subscribe()` prevents the error from crashing the global console as an unhandled exception and lets you show a toast or error message.
- It does **not** keep the stream alive! Once `error(err)` in `subscribe()` runs, that subscription is permanently closed. To recover inside the pipeline or keep an outer event stream alive across failed network requests, you need `catchError` and `retry`.

## Watch out

- Always handle errors (via `catchError` in `.pipe()` or `error` in `.subscribe()`) — an Observable that errors with neither handler throws an **unhandled asynchronous exception** in RxJS 7+.
- Never assume a stream will keep emitting after its `error` callback has fired.

## Remember

- Errors in RxJS are **terminal**: an error stops the stream, runs teardown, and prevents `complete()` and any further `next()` emissions.
- Any exception thrown inside an operator callback (`map`, `filter`, `tap`) is automatically converted into an `error` notification.
- Handling `error` inside `subscribe()` logs or reports the failure, but the subscription is still closed.
