---
slug: group-by-duration-and-memory-pitfall
title: "Preventing groupBy memory leaks with the duration selector"
topic: custom-operators-batching
kind: pitfall
summary: "Close inactive GroupedObservable groups automatically using the duration option so high-cardinality keys never leak memory."
tags: [rxjs, groupby, memory-leaks, duration, pitfalls]
related: [group-by-operator, what-causes-rxjs-memory-leaks, debounce-time]
---
> **In one line:** `groupBy()` keeps an internal `Map<K, Subject<T>>` of every key it has ever seen — on a long-lived stream with high-cardinality keys (like `userId` or `requestId`), you **must** pass a `duration` selector to evict idle groups.

## The idea

How does `groupBy(keyFn)` know whether a key `K` has already been seen? Internally, it stores a `Map<K, Subject<T>>`. By default, entries are only deleted from that `Map` when the **outer source completes**.

If your outer source is a long-lived WebSocket, server-sent event stream, or DOM event stream, and your key is high-cardinality (thousands of unique `userId`s, `sessionId`s, or `itemId`s over time), that internal `Map` and every inner `mergeMap` subscription will **grow forever and leak memory**!

To fix this, pass the `duration` option to `groupBy`:
```typescript
groupBy(keyFn, {
  duration: group$ => group$.pipe(debounceTime(10_000))
})
```
Whenever the Observable returned by `duration(group$)` emits or completes, `groupBy` automatically:
1. Calls `complete()` on that `group$` (tearing down its inner `mergeMap` pipeline!).
2. Deletes `group$.key` from the internal `Map` to free memory.
3. Creates a fresh `GroupedObservable` if that key ever appears again later.

| Key cardinality | Outer stream lifetime | Do you need `duration`? |
|---|---|---|
| Finite & small (e.g., 4 status enum values) | Any | Optional (at most 4 groups ever exist) |
| Finite dataset | Short-lived (completes right away) | No (all groups complete with outer stream) |
| **Unbounded / high-cardinality** (`userId`, `docId`, `symbol`) | **Long-lived** (WebSocket, UI events) | **Required!** Use `duration: g$ => g$.pipe(debounceTime(ms))` |

## See it

Evict idle per-document auto-save groups after 5 seconds of inactivity using `duration`:

```typescript
import { Subject, groupBy, debounceTime, mergeMap, tap } from 'rxjs';

interface DocEdit { docId: string; content: string; }
const edits$ = new Subject<DocEdit>();

edits$.pipe(
  groupBy(edit => edit.docId, {
    // Complete and evict any docId group after 5s of silence on that docId:
    duration: group$ => group$.pipe(debounceTime(5000)),
  }),
  mergeMap(group$ =>
    group$.pipe(
      debounceTime(500),
      tap({
        next: edit => console.log(`Saving ${group$.key}:`, edit.content),
        complete: () => console.log(`Evicted idle group: ${group$.key}`),
      })
    )
  )
).subscribe();
```

## Go deeper

Notice how the example above uses `debounceTime(500)` inside `mergeMap` to save rapid edits, and `debounceTime(5000)` inside `duration` to close and garbage-collect the group after 5 seconds of idle time. Always make the `duration` timeout **longer** than any inner `debounceTime` so the group doesn't complete before the debounced emission fires (though even if it does, `debounceTime` flushes its pending value on `complete()`).

## Watch out

- Never pass the legacy positional arguments `groupBy(keyFn, elementFn, durationFn)` in new RxJS 7+ code — positional `element` and `duration` parameters are deprecated in favor of the options object `groupBy(keyFn, { element, duration })`.
- If an inner `mergeMap` pipeline uses `take(1)`, completing the inner consumer also removes the group once the group has no remaining subscribers.

## Remember

- `groupBy` retains a `Subject` in an internal `Map` for every active key until the group or outer stream completes.
- Always pass `{ duration: group$ => group$.pipe(debounceTime(idleMs)) }` when grouping long-lived streams by dynamic IDs.
- When `duration` fires, the group completes cleanly and is garbage-collected from the internal `Map`.
