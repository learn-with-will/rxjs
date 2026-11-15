---
slug: combine-latest
title: "Reactive derived state with combineLatest()"
topic: combining-joining
kind: api
summary: "combineLatest([a$, b$]) waits until every input stream has emitted at least once, then emits an array or object of the latest values whenever any input emits."
tags: [rxjs, combinelatest, combining, derived-state]
related: [combine-latest-waits-for-all-and-start-with, with-latest-from, combine-latest-vs-with-latest-from, combine-latest-object-form-and-with]
---
> **In one line:** `combineLatest([a$, b$])` waits until **every** source has emitted at least once, and from then on emits the **latest value from each source** whenever **any** source emits.

## The idea

Think of `combineLatest` as a **spreadsheet formula cell** like `=A1 * B1`. Once both `A1` and `B1` have a value, changing *either* `A1` *or* `B1` immediately recalculates the formula using the new value from the cell that changed alongside the most recent value from the other cell.

Here are the rules of `combineLatest([a$, b$, ...])`:

1. It subscribes to all input Observables simultaneously.
2. **Initial gate:** it emits **nothing** until **every single input Observable** has emitted at least one value.
3. **Ongoing emissions:** after that initial gate is satisfied, whenever **any** input emits a new value, `combineLatest` emits a tuple `[latestA, latestB, ...]` combining the new value with the cached latest values of all the other inputs.
4. **Completion:** it completes only when **all** input Observables have completed (or immediately if any input completes before ever emitting a single value!).

```sequence
actor a: price$ | emits 100, then 120 @blue
actor b: taxRate$ | emits 0.1, then 0.2 @amber
actor cl: combineLatest([price$, taxRate$]) | emits [price, tax] @primary
a -> cl: 100 (taxRate$ hasn't emitted yet -> waits)
b -> cl: 0.1 (all have emitted! -> emits [100, 0.1])
a -> cl: 120 (price$ updated -> emits [120, 0.1])
b -> cl: 0.2 (taxRate$ updated -> emits [120, 0.2])
```

## See it

```typescript
import { Subject, combineLatest, map } from 'rxjs';

const price$ = new Subject<number>();
const quantity$ = new Subject<number>();

combineLatest([price$, quantity$])
  .pipe(
    map(([price, qty]) => ({ price, qty, total: price * qty })),
  )
  .subscribe((order) => console.log('Order summary:', order));

price$.next(25);    // Nothing emitted yet (quantity$ has not emitted)
quantity$.next(2);  // Logs: { price: 25, qty: 2, total: 50 }
price$.next(30);    // Logs: { price: 30, qty: 2, total: 60 }
quantity$.next(4);  // Logs: { price: 30, qty: 4, total: 120 }
```

## Go deeper

What happens if one input completes after emitting a value (say `taxRate$ = of(0.1)`), while `price$` keeps emitting new prices over time? `combineLatest` remembers `0.1` as the latest value of `taxRate$` and **keeps emitting** `[newPrice, 0.1]` on every new `price$` emission until `price$` also completes! Only if an input completes **empty** (zero emissions, like `EMPTY`) does `combineLatest` complete immediately without ever emitting.

What if an input emits **multiple values synchronously** (like `of(1, 2, 3)`) when `combineLatest([of(1, 2, 3), of('a', 'b')])` subscribes? Because `of(1, 2, 3)` runs to completion synchronously during the first subscription before `of('a', 'b')` is subscribed, only `3` is cached when `'a'` and `'b'` emit — producing `[3, 'a']` and `[3, 'b']`.

## Watch out

- Always pass an **array** `combineLatest([a$, b$])` (or dictionary object `combineLatest({ a: a$, b: b$ })`) — passing rest arguments `combineLatest(a$, b$)` is deprecated in RxJS 7+.
- If two inputs update synchronously in the same routine, `combineLatest` will emit once for the first update and immediately again for the second (a transient intermediate emission).

## Remember

- `combineLatest([a$, b$])` emits only after **every** input has emitted at least once.
- After that, **any** input emission triggers a new combined emission with the latest values of all inputs.
- It completes only when **all** inputs have completed (or if an input completes empty).
