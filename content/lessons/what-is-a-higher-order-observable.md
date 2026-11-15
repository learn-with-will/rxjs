---
slug: what-is-a-higher-order-observable
title: "What is a higher-order Observable?"
topic: flattening-higher-order
kind: concept
summary: "A higher-order Observable is an Observable that emits other Observables (Observable<Observable<T>>), created when mapping each event to an async stream."
tags: [rxjs, higher-order, observable, flattening]
related: [merge-map, switch-map, nested-subscribe-anti-pattern, all-flattening-operators]
---
> **In one line:** a **higher-order Observable** is an `Observable<Observable<T>>` — an **outer stream** whose emitted values are themselves **inner streams** that must be flattened to get the underlying `T` values.

## The idea

Most Observables you start with are **first-order Observables**: `Observable<number>` emits numbers, and `Observable<MouseEvent>` emits mouse events.

Now suppose each click on a "Refresh" button should trigger an HTTP call `fetchUser$(): Observable<User>`. What happens if you pipe `clicks$` through plain `map(() => fetchUser$())`?

Because `map` simply emits whatever your callback returns, each click causes `map` to emit an **unsubscribed `Observable<User>` object**! Your stream now has the type **`Observable<Observable<User>>`** — a **higher-order Observable**:

- The **outer Observable** (`clicks$`) controls *when* new inner work is triggered.
- Each **inner Observable** (`fetchUser$()`) represents the asynchronous work kicked off by one outer emission.
- **Flattening** is the act of automatically subscribing to those inner Observables and forwarding their unwrapped `User` values onto a single first-order `Observable<User>` output stream.

```flow
outer: Outer Stream (clicks$) | emits click events @primary
ho: map(() => fetchUser$()) | emits Observable<User> objects! @amber
flat: Flattening Operator | subscribes to inner streams & unwraps User @blue
out: First-Order Output | emits User objects @green
outer -> ho: Click 1, Click 2
ho -> flat: Observable<User>, Observable<User>
flat -> out: User 1, User 2
```

## See it

Compare what plain `map` emits (an `Observable` instance, without ever starting the HTTP request!) versus what a flattening operator like `switchMap` emits:

```typescript
import { of, map, switchMap, delay } from 'rxjs';

// Simulated async request returning an inner Observable<string>:
const fetchUser$ = (id: number) => of(`User #${id}`).pipe(delay(100));

// 1. Plain map() produces Observable<Observable<string>> — prints "[object Object]"!
of(1, 2)
  .pipe(map((id) => fetchUser$(id)))
  .subscribe((innerObs) => console.log('map emitted:', innerObs));

// 2. switchMap() subscribes to the inner Observable and emits the unwrapped string:
of(1, 2)
  .pipe(switchMap((id) => fetchUser$(id)))
  .subscribe((user) => console.log('switchMap emitted:', user)); // "User #2"
```

## Go deeper

Because multiple outer values can arrive while previous inner Observables are still running, RxJS cannot guess **how** you want overlapping inner streams handled. Should they run in parallel? Wait in a queue? Cancel the previous inner stream? Ignore new outer events while busy?

Those four policies correspond to RxJS's four core flattening operators: **`mergeMap`**, **`concatMap`**, **`switchMap`**, and **`exhaustMap`**. Every single one of them is equivalent to `map(fn)` followed by its matching `*All` operator (`mergeAll`, `concatAll`, `switchAll`, `exhaustAll`).

## Watch out

- If your `subscribe` callback receives an `Observable` object instead of your data, you used `map` where you needed `switchMap`, `mergeMap`, `concatMap`, or `exhaustMap`.
- Remember that an inner Observable returned inside `map` is cold — until a flattening operator subscribes to it, the async work inside it does not even start!

## Remember

- A higher-order Observable has type `Observable<Observable<T>>`: an outer stream emitting inner streams.
- Using plain `map(x => asyncStream$(x))` creates a higher-order Observable without subscribing to the inner streams.
- Flattening operators (`mergeMap`, `concatMap`, `switchMap`, `exhaustMap`) subscribe to inner streams and unwrap their values into `Observable<T>`.
