---
slug: of-and-from
title: "Creating streams with of() and from()"
topic: creating-observables
kind: api
summary: "of() emits each argument you pass in order and completes; from() converts an array, iterable, Promise, or Observable into a stream."
tags: [rxjs, of, from, creation]
related: [of-vs-from-pitfall, range-creator, empty-never-and-throw-error]
---
> **In one line:** `of(a, b, c)` synchronously emits the exact arguments you pass it and completes, while `from(input)` unpacks an array, iterable, or Promise into individual emissions.

## The idea

You rarely need to call `new Observable` by hand when you already have values in memory or a Promise in flight. RxJS ships two everyday creation functions for turning existing data into a stream:

- **`of(...values)`** takes any number of arguments, emits each argument **in order, synchronously**, and then immediately completes.
- **`from(input)`** takes a single `ObservableInput` — an array, an array-like object, any iterable (such as a `string`, `Set`, or generator), a `Promise`, or another `Observable` — and converts it into an Observable.

| Creator | Input | Emissions | Timing |
|---|---|---|---|
| `of(1, 2, 3)` | Separate arguments | `1`, then `2`, then `3`, then `complete` | Synchronous |
| `from([10, 20, 30])` | Array or iterable | `10`, then `20`, then `30`, then `complete` | Synchronous |
| `from('hi')` | String (iterable) | `'h'`, then `'i'`, then `complete` | Synchronous |
| `from(promise)` | Promise | The resolved value, then `complete` (or `error` on reject) | Asynchronous |

## See it

Here is how `of` and `from` turn arguments, arrays, strings, and Promises into streams that work with the exact same `.subscribe()` API:

```typescript
import { of, from } from 'rxjs';

of(1, 2, 3).subscribe((n) => console.log('of:', n));
// Logs synchronously: of: 1, of: 2, of: 3

from([10, 20, 30]).subscribe((n) => console.log('array:', n));
// Logs synchronously: array: 10, array: 20, array: 30

from('hi').subscribe((ch) => console.log('char:', ch));
// Logs synchronously: char: h, char: i

from(Promise.resolve('done')).subscribe((res) => console.log('promise:', res));
// Logs asynchronously on microtask queue: promise: done
```

## Go deeper

When you pass a Promise to `from(promise)`, RxJS attaches `.then()` when you subscribe. If the Promise resolves, the Observable emits that value once and completes; if the Promise rejects, the Observable emits an `error` notification.

```typescript
import { from } from 'rxjs';

const rejected$ = from(Promise.reject(new Error('network down')));

rejected$.subscribe({
  next: (v) => console.log('value:', v),
  error: (err: Error) => console.error('error:', err.message), // "network down"
});
```

Note that if you pass an already-created Promise (`from(fetch('/api'))`), the network call has already started eagerly — pair `from` with `defer` when you want the Promise created lazily per subscription.

## Watch out

- Passing a scheduler as the last argument to `of` or `from` (such as `of(1, 2, asyncScheduler)`) is deprecated in RxJS 7; use `scheduled([1, 2], asyncScheduler)` instead.
- `from` iterates strings character by character — use `of('hello')` when you want to emit the whole string as one value.

## Remember

- `of(a, b, c)` emits each argument as a separate synchronous value, then completes.
- `from(input)` iterates arrays/iterables and unwraps Promises into a stream.
- Both complete automatically once their values (or resolved Promise) have been emitted.
