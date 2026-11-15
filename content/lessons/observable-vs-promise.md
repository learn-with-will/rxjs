---
slug: observable-vs-promise
title: "Observable vs Promise"
topic: observables-subscriptions
kind: concept
summary: "Unlike a Promise, an Observable is lazy, can emit multiple values, is cancellable with unsubscribe(), and can deliver values synchronously or asynchronously."
tags: [rxjs, observable, promise, comparison]
related: [what-is-an-observable, push-vs-pull-grid, first-observable-new-observable]
---
> **In one line:** compared to a Promise, an Observable is **lazy**, emits **many** values, is **cancellable**, and can run **synchronously or asynchronously**.

## The idea

Promises and Observables both push values to consumers, which makes it tempting to think of an Observable as just a fancier Promise. In fact, they differ in four fundamental ways that affect how your code runs:

| Property | `Promise` | `Observable` |
|---|---|---|
| **Execution** | **Eager** — starts work the moment `new Promise(...)` is called | **Lazy** — does nothing until `.subscribe()` is called |
| **Values** | Resolves at most **one** value | Can emit **zero, one, or many** values over time |
| **Cancellation** | Not built-in once started | Cancellable at any time via `subscription.unsubscribe()` |
| **Timing** | `.then()` callbacks are **always asynchronous** (microtask queue) | Can deliver values **synchronously or asynchronously** |

Because an Observable is lazy, defining one is free — if nobody subscribes, no network call or timer is ever started. And because each subscription runs the producer function fresh, subscribing twice starts two independent runs.

## See it

Here you can see both laziness and synchronous delivery in action:

```typescript
import { Observable } from 'rxjs';

const eagerPromise = new Promise<number>((resolve) => {
  console.log('1. Promise executor runs immediately');
  resolve(42);
});

const lazyObservable$ = new Observable<number>((subscriber) => {
  console.log('3. Observable producer runs on subscribe');
  subscriber.next(42);
  subscriber.complete();
});

console.log('2. Before subscribe');
lazyObservable$.subscribe((n) => console.log('4. Observable value:', n));
eagerPromise.then((n) => console.log('5. Promise value (async microtask):', n));
```

Notice the console order: `1`, then `2`, then `3` and `4` synchronously during `subscribe()`, and finally `5` on the microtask queue after synchronous code finishes.

## Go deeper

A Promise is like ordering a single book online: the warehouse starts shipping the moment you place the order (eager), exactly one parcel arrives (single value), and you cannot call the van back mid-route. An Observable is like a magazine subscription: nothing ships until you subscribe, issues keep arriving over time, and cancelling stops future deliveries. For a simple one-shot operation that needs no cancellation, a Promise is fine; for events, streams, or cancellable work, reach for an Observable.

## Watch out

- Do not assume an Observable callback runs on the next tick like `.then()` — a synchronous Observable invokes `next` immediately on the same call stack.
- Creating an Observable without subscribing triggers no work at all.

## Remember

- Observables are lazy and run only when subscribed; Promises are eager.
- Observables emit zero to many values and can be cancelled with `unsubscribe()`.
- Promise handlers are always async, while Observables can be sync or async.
