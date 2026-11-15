---
slug: group-by-operator
title: "Partitioning a stream by key with groupBy()"
topic: custom-operators-batching
kind: api
summary: "Demultiplex a single stream into keyed inner GroupedObservable<K, T> streams so you can debounce, scan, or throttle each key independently."
tags: [rxjs, groupby, groupedobservable, partitioning, higher-order]
related: [group-by-duration-and-memory-pitfall, filter-operator, what-is-a-higher-order-observable]
---
> **In one line:** `groupBy(keySelector)` splits a single `Observable<T>` into a higher-order stream of `GroupedObservable<K, T>` inner streams — one per unique key `group$.key` — so you can apply independent operators to each key in parallel.

## The idea

While `partition()` splits a stream into **two** static branches (true vs false), what if you have a stream of stock ticks for 50 ticker symbols, or edits across N document IDs, and you want to `debounceTime(500)` or `scan()` **per ticker symbol / per document ID** independently?

If you put `debounceTime(500)` on the single combined stream, an edit to Document B cancels the pending save for Document A! Instead, you pipe `groupBy(item => item.docId)`:
1. Whenever `groupBy` sees a key `K` for the **first time**, it creates an internal `Subject` and emits a new `GroupedObservable<K, T>` (which has a `.key: K` property) downstream.
2. Every subsequent item with that same key `K` is pushed into that existing `GroupedObservable<K, T>`.
3. You flatten the groups with **`mergeMap(group$ => ...)`** so all key groups run their pipelines concurrently in parallel!

```flow
src: Edits | { docId, text } @blue
gb: groupBy(e => e.docId) | partitions by key @primary
ga: group$ (key: 'A') | debounceTime(500) @green
gb2: group$ (key: 'B') | debounceTime(500) @amber
out: mergeMap | independent saves per docId @green
src -> gb
gb -> ga: docId === 'A'
gb -> gb2: docId === 'B'
ga -> out
gb2 -> out
```

## See it

Group an incoming order stream by `category` and maintain an independent running total for each category using `groupBy` + `mergeMap` + `scan`:

```typescript
import { of, groupBy, mergeMap, scan, map } from 'rxjs';

interface Order { category: string; amount: number; }

const orders$ = of<Order>(
  { category: 'Books', amount: 20 },
  { category: 'Tech',  amount: 100 },
  { category: 'Books', amount: 15 },
  { category: 'Tech',  amount: 50 },
);

orders$.pipe(
  groupBy(order => order.category),
  mergeMap(group$ =>
    group$.pipe(
      scan((total, order) => total + order.amount, 0),
      map(total => ({ category: group$.key, runningTotal: total }))
    )
  )
).subscribe(console.log);
// { category: 'Books', runningTotal: 20 }
// { category: 'Tech',  runningTotal: 100 }
// { category: 'Books', runningTotal: 35 }
// { category: 'Tech',  runningTotal: 150 }
```

## Go deeper

You can also pass an `element` selector in the options object `groupBy(keySelector, { element: item => item.amount })` so each `GroupedObservable<string, number>` emits just the extracted `amount` directly instead of the full `Order` object.

## Watch out

- **Never** flatten `groupBy()` with `switchMap` or `concatMap`! `switchMap` will immediately unsubscribe from Group A the moment Group B is created, and `concatMap` will block Group B until Group A completes. Always use **`mergeMap`** with `groupBy()`.
- By default, each `GroupedObservable` stays open until the upstream source completes — see the next lesson for how to close idle groups with `duration`.

## Remember

- `groupBy(keyFn)` emits a `GroupedObservable<K, T>` with a `group$.key` property for each distinct key `K`.
- Always flatten `groupBy` with `mergeMap(group$ => ...)` so all groups run concurrently.
- Use `groupBy` + `mergeMap` whenever you need per-entity `debounceTime`, `throttleTime`, `scan`, or `concatMap` queuing.
