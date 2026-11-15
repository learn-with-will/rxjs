---
slug: nested-subscribe-anti-pattern
title: "Anti-pattern: nesting subscribe() inside subscribe()"
topic: flattening-higher-order
kind: pitfall
summary: "Calling subscribe() inside another subscribe() callback breaks cancellation and error handling; flatten with switchMap, concatMap, or mergeMap instead."
tags: [rxjs, pitfall, subscribe, switchmap, anti-pattern]
related: [what-is-a-higher-order-observable, choosing-a-flattening-operator, subscription-and-purity-anti-patterns]
---
> **In one line:** **never call `subscribe()` inside another `subscribe()` callback** — compose the streams in a single `.pipe()` using `switchMap`, `concatMap`, `mergeMap`, or `exhaustMap`.

## The idea

When developers first need to use the output of one Observable (`routeParams$`) to start a second Observable (`fetchUser$(id)`), it is tempting to nest a second `.subscribe()` inside the first `.subscribe()` callback — the RxJS equivalent of callback hell.

Why is nested `subscribe()` one of the worst bugs in RxJS?

1. **Broken unsubscription (memory leaks):** unsubscribing from the outer `Subscription` does **not** unsubscribe from the inner `Subscription`s you spawned inside the callback!
2. **Race conditions:** if `routeParams$` emits `id = 1` and then `id = 2`, the inner subscription for `id = 1` is never cancelled, so if `id = 1` resolves slower than `id = 2`, it overwrites the UI with stale data.
3. **Broken error & operator composition:** errors in the inner subscription don't flow through the outer pipeline, and you can't pipe downstream operators onto a single stream.

| Problem | Nested `subscribe()` | Flattened with `switchMap` |
|---|---|---|
| Single `sub.unsubscribe()` cleans up inner stream? | **No** (inner leaks!) | **Yes** |
| Cancels stale request when outer emits again? | **No** (race condition!) | **Yes** |
| Errors & values flow through one pipeline? | **No** | **Yes** |

## See it

```typescript
import { Subject, of, delay, switchMap } from 'rxjs';

const userId$ = new Subject<number>();
const fetchUser$ = (id: number) => of({ id, name: `User ${id}` }).pipe(delay(100));

// Before (anti-pattern): nested subscribe() leaks inner subscriptions and races!
const badSub = userId$.subscribe((id) => {
  fetchUser$(id).subscribe((user) => {
    console.log('nested (racy):', user);
  });
});

// After (idiomatic): flatten with switchMap into a single pipeline and single subscribe()
const goodSub = userId$
  .pipe(
    switchMap((id) => fetchUser$(id)),
  )
  .subscribe((user) => {
    console.log('flattened (safe):', user);
  });

badSub.unsubscribe();
goodSub.unsubscribe(); // cancels both userId$ AND any in-flight fetchUser$!
```

## Go deeper

What if your inner callback needs **both** the outer value `id` and the inner result `user`? You still don't need to nest `subscribe()`! Inside `switchMap((id) => ...)`, `id` is right there in lexical scope, so you can pipe `map` onto the inner Observable to combine them:

```typescript
import { of, switchMap, map } from 'rxjs';

of(42)
  .pipe(
    switchMap((id) =>
      of({ name: 'Ada' }).pipe(
        map((profile) => ({ id, ...profile })), // both id and profile are in scope!
      ),
    ),
  )
  .subscribe(console.log); // { id: 42, name: 'Ada' }
```

## Watch out

- Whenever you see `.subscribe(` inside another `.subscribe(` (or inside `tap`), refactor it to `switchMap`, `concatMap`, `mergeMap`, or `exhaustMap`.
- Keep exactly **one** `.subscribe()` at the very end of the pipeline where the final side effect happens.

## Remember

- Never nest `subscribe()` inside another `subscribe()` or inside `tap()`.
- Nested subscriptions break teardown, leak inner streams, and create out-of-order race conditions.
- Use `switchMap`, `concatMap`, `mergeMap`, or `exhaustMap` to keep a single flat pipeline.
