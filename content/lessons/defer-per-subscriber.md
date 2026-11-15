---
slug: defer-per-subscriber
title: "Lazy per-subscriber creation with defer()"
topic: creating-observables
kind: api
summary: "defer(factory) waits until subscribe() is called to invoke your factory and build a fresh Observable for each subscriber."
tags: [rxjs, defer, lazy, creation, promises]
related: [of-and-from, empty-never-and-throw-error, cold-and-unicast-by-default]
---
> **In one line:** `defer(() => factory())` delays building the inner Observable until subscription time, giving every subscriber (and every `retry`) a brand-new source.

## The idea

When you write `const now$ = of(Date.now())` or `const rand$ = of(Math.random())`, JavaScript evaluates `Date.now()` or `Math.random()` **once**, right when that line runs. The resulting number is baked into `of(...)`, so every subscriber — even one that subscribes ten minutes later — receives the exact same stale value. The same problem happens with `from(fetch('/api'))`: `fetch('/api')` starts the HTTP request immediately before anyone subscribes, and later subscribers just share that one settled Promise.

`defer(factory)` fixes this by moving the work from **definition time** to **subscription time**. You pass `defer` a zero-argument factory function that returns an `ObservableInput` (an Observable, Promise, array, or iterable). `defer` does nothing until someone calls `.subscribe()`, at which point it calls your factory to create a **fresh** source for that subscriber.

```sequence
actor sub1: Subscriber A
actor sub2: Subscriber B
actor def: defer(factory) | Observable @primary
actor fac: Factory | () => of(Math.random()) @blue
sub1 -> def: subscribe()
def -> fac: calls factory() #1
fac --> sub1: emits 0.42 | fresh value
sub2 -> def: subscribe()
def -> fac: calls factory() #2
fac --> sub2: emits 0.87 | new fresh value
```

## See it

Compare eager argument evaluation in `of()` with per-subscriber evaluation in `defer()`:

```typescript
import { of, defer } from 'rxjs';

// Before: Math.random() runs ONCE when eager$ is defined
const eager$ = of(Math.random());
eager$.subscribe((n) => console.log('eager A:', n)); // e.g. 0.51
eager$.subscribe((n) => console.log('eager B:', n)); // 0.51 (same number!)

// After: defer() runs the factory on EVERY subscribe()
const lazy$ = defer(() => of(Math.random()));
lazy$.subscribe((n) => console.log('lazy A:', n));   // e.g. 0.42
lazy$.subscribe((n) => console.log('lazy B:', n));   // e.g. 0.87 (fresh number!)
```

## Go deeper

Because `defer` accepts any `ObservableInput`, wrapping a Promise factory — `defer(() => fetch('/api/user'))` — is the idiomatic way to turn a Promise-returning function into a truly lazy, retryable Observable. If you pipe `retry(2)` onto `defer(() => fetch('/api'))`, each retry resubscribes to `defer`, which calls `fetch('/api')` again. With `from(fetch('/api'))`, `retry(2)` would merely re-read the same already-rejected Promise.

```typescript
import { defer, retry } from 'rxjs';

// Each subscribe (and each retry) invokes fetch() fresh:
const user$ = defer(() => fetch('/api/user').then((r) => r.json())).pipe(
  retry(2),
);

user$.subscribe({
  next: (user) => console.log('loaded user:', user),
  error: (err) => console.error('failed after retries:', err),
});
```

Think of `defer` as the bridge between imperative setup and lazy streams: anything you place inside its callback is guaranteed to wait until subscription time and run independently for each subscriber.

## Watch out

- Do not put "run once for the whole app" setup inside a `defer` factory — the factory runs again on **every** subscription.
- `from(myPromise())` calls `myPromise()` immediately; wrap it as `defer(() => myPromise())` so the Promise is only created on subscribe.

## Remember

- `of(expr)` evaluates `expr` once at creation time; `defer(() => of(expr))` evaluates it on each subscribe.
- Use `defer(() => fetch(...))` to make Promise-based work lazy and retryable.
- Every subscriber to a `defer` Observable triggers a fresh call to the factory.
