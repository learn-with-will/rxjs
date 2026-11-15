---
slug: push-vs-pull-grid
title: "Push vs pull, single vs multiple"
topic: observables-subscriptions
kind: concept
summary: "Every value producer in JavaScript fits into a 2x2 grid of pull or push and single or multiple values; Observable fills the push-multiple cell."
tags: [rxjs, observable, push, pull, fundamentals]
related: [what-is-an-observable, observable-vs-promise, core-building-blocks-and-dollar-suffix]
---
> **In one line:** on the 2×2 grid of value producers, an Observable occupies the **push + multiple values** cell that Promises and Iterators leave empty.

## The idea

Every value producer in JavaScript can be classified by two questions:

1. **Who decides when a value is delivered?** In a **pull** system, the consumer asks for the next value when it is ready. In a **push** system, the producer decides when to hand a value over, and the consumer reacts whenever it arrives.
2. **How many values can it produce?** Either a **single** value or **multiple** values over the producer's lifetime.

Putting those two axes together gives a 2×2 grid that explains why Observables exist:

| Delivery | Single value | Multiple values |
|---|---|---|
| **Pull** (consumer decides) | `Function` | `Iterator` / `Generator` |
| **Push** (producer decides) | `Promise` | **`Observable`** |

A regular function is **pull + single**: you call it, and it returns one value immediately. An iterator or generator is **pull + multiple**: you call `next()` each time you want another item. A Promise is **push + single**: the async operation decides when to resolve, and it resolves with one value. An **Observable** fills the remaining fourth cell — **push + multiple** — pushing zero, one, or many values on the producer's schedule.

## See it

Compare pulling multiple values from an array iterator with having multiple values pushed to you by an Observable:

```typescript
import { Observable } from 'rxjs';

// Pull + multiple: the consumer pulls each value when it asks for it
const iterator = [10, 20, 30][Symbol.iterator]();
console.log(iterator.next().value); // 10
console.log(iterator.next().value); // 20

// Push + multiple: the producer pushes values whenever it wants
const pushed$ = new Observable<number>((subscriber) => {
  subscriber.next(10);
  subscriber.next(20);
  subscriber.next(30);
  subscriber.complete();
});

pushed$.subscribe((value) => console.log('pushed:', value));
```

With the iterator, your code drives the loop step by step. With the Observable, you hand over a callback and the producer pushes each value to you.

## Go deeper

That single difference — **push + multiple** instead of **push + single** — is why an Observable can model open-ended event streams like mouse clicks, WebSocket messages, or repeating timers, whereas a Promise can only model a one-shot async result. At the same time, because an Observable can also emit just one value and complete, it can represent everything a Promise represents too.

## Watch out

- You cannot "pull" or read a return value directly from `observable.subscribe()`; values only arrive inside the callback you pass.
- An iterator waits for you to call `.next()`, whereas an Observable pushes values at its own pace once subscribed.

## Remember

- Pull means the consumer requests values; push means the producer delivers them when ready.
- Functions and Iterators are pull; Promises and Observables are push.
- Observable is the push + multiple producer in JavaScript.
