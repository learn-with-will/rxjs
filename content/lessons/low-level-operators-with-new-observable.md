---
slug: low-level-operators-with-new-observable
title: "Writing low-level custom operators with new Observable()"
topic: custom-operators-batching
kind: api
summary: "Build a custom operator from scratch using new Observable(subscriber => ...) when built-in operators cannot express your state or timing logic."
tags: [rxjs, custom-operators, observable, subscriber, teardown]
related: [operator-function-shape, composing-custom-operators-with-pipe, custom-operator-pitfalls]
---
> **In one line:** when you write a low-level operator with `(source$) => new Observable(subscriber => ...)`, you must fulfill four responsibilities: forward `next`/`error`/`complete`, keep state inside the `new Observable` callback, catch callback errors, and return the upstream teardown.

## The idea

Occasionally you need custom per-subscription state, custom timer coordination, or a specialized emission rule that would be awkward or slow to assemble from 6 built-in operators. In those cases, you return `(source$: Observable<T>) => new Observable<R>(subscriber => { ... })`.

Every low-level operator follows the exact same four-part skeleton:

```sequence
actor down: Downstream Subscriber @green
actor custom: new Observable | custom operator @primary
actor up: source$ | Observable<T> @blue
down -> custom: 1. subscribe()
note custom: 2. Allocate fresh per-subscription state @amber
custom -> up: 3. const sub = source$.subscribe({...})
up -> custom: next(v) / error(e) / complete()
custom -> down: subscriber.next(r) / error(e) / complete()
down -> custom: 4. unsubscribe()
custom -> up: return () => sub.unsubscribe()
```

## See it

Write a low-level `takeEveryNth(n)` operator that emits every `n`-th item (0-indexed: 0, n, 2n, ...) with fresh state per subscriber and full error/teardown handling:

```typescript
import { Observable, MonoTypeOperatorFunction, of } from 'rxjs';

export function takeEveryNth<T>(n: number): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) =>
    new Observable<T>(subscriber => {
      // 1. Per-subscription state MUST live inside new Observable()!
      let index = 0;
      // 2. Subscribe to upstream and forward all 3 notifications:
      const sub = source$.subscribe({
        next(value) {
          if (index++ % n === 0) subscriber.next(value);
        },
        error(err) { subscriber.error(err); },
        complete() { subscriber.complete(); },
      });
      // 3. Return teardown so unsubscribing downstream unsubscribes upstream!
      return () => sub.unsubscribe();
    });
}

of('a', 'b', 'c', 'd', 'e').pipe(takeEveryNth(2)).subscribe(console.log); // 'a', 'c', 'e'
```

## Go deeper

What if your low-level operator accepts a user-supplied callback function (like `project(value)` or `predicate(value)`) and that callback throws a synchronous exception inside `next(value)`? Wrap the user callback in a `try { ... } catch (err) { subscriber.error(err); }` block! When `subscriber.error(err)` runs, the Observable contract automatically triggers your returned teardown function `() => sub.unsubscribe()`, cleanly shutting down the upstream source and forwarding the error to any downstream `catchError` or `retry` operator in the pipeline.

## Watch out

- Never omit `error(err) { subscriber.error(err); }` or `complete() { subscriber.complete(); }` — leaving them out causes errors to become unhandled exceptions or causes downstream `last()` / `toArray()` operators to hang forever!
- Always return `() => sub.unsubscribe()` (or `return sub;` directly, since `new Observable` accepts a `Subscription` return value as its teardown!).

## Remember

- Declare mutable state **inside** `new Observable(subscriber => { ... })`, never outside it.
- Always forward `error(err)` and `complete()` to `subscriber`.
- Always return `() => sub.unsubscribe()` (or `return sub`) so downstream unsubscription tears down the upstream source.
