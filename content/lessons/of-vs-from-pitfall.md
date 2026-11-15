---
slug: of-vs-from-pitfall
title: "of([1, 2, 3]) vs from([1, 2, 3])"
topic: creating-observables
kind: pitfall
summary: "Passing an array to of() emits the whole array as a single value; passing it to from() iterates the array and emits each item separately."
tags: [rxjs, of, from, pitfall, arrays]
related: [of-and-from, range-creator, map-operator]
---
> **In one line:** `of([1, 2, 3])` emits **one value** (the array itself), whereas `from([1, 2, 3])` iterates the array and emits **three separate numbers**.

## The idea

The single most common creation mistake in RxJS is mixing up `of([1, 2, 3])` and `from([1, 2, 3])`. They look almost identical at a glance, but they produce completely different streams and TypeScript types:

- **`of(arg)`** never inspects or unpacks its arguments. If you pass it one argument — even an array — it emits that one argument as a single `next` notification and completes. Its type is `Observable<number[]>`.
- **`from(arg)`** expects a collection or Promise to unpack. When you pass it an array, it loops over the elements and emits each one in turn. Its type is `Observable<number>`.

| Call | Return type | How many `next` emissions? | What `next` receives |
|---|---|---|---|
| `of([1, 2, 3])` | `Observable<number[]>` | **1** | `[1, 2, 3]` |
| `of(1, 2, 3)` | `Observable<number>` | **3** | `1`, then `2`, then `3` |
| `from([1, 2, 3])` | `Observable<number>` | **3** | `1`, then `2`, then `3` |

## See it

```typescript
import { of, from } from 'rxjs';

// Before: accidentally wrapping an array in of() when you wanted each item
of([1, 2, 3]).subscribe((value) => {
  console.log('of([1, 2, 3]):', value);
});
// Logs ONCE: of([1, 2, 3]): [1, 2, 3]

// After: use from() to emit each element of an array one by one
from([1, 2, 3]).subscribe((value) => {
  console.log('from([1, 2, 3]):', value);
});
// Logs THREE times:
// from([1, 2, 3]): 1
// from([1, 2, 3]): 2
// from([1, 2, 3]): 3
```

## Go deeper

Both behaviours are genuinely useful — you just have to pick the one that matches your downstream pipeline:

- Reach for **`of(items)`** when your subscriber or UI component wants the **entire array at once** (for example, returning a fallback empty array `of([])` inside `catchError`, or feeding a table component that renders a list).
- Reach for **`from(items)`** when your pipeline wants to process **one item at a time** (for example, piping each user ID into `mergeMap` to fetch its details).

```typescript
import { of, from, map } from 'rxjs';

// Downstream expects the full array to read .length:
of(['a', 'b', 'c'])
  .pipe(map((arr) => arr.length))
  .subscribe((len) => console.log('list length:', len)); // 3

// Downstream transforms each string individually:
from(['a', 'b', 'c'])
  .pipe(map((item) => item.toUpperCase()))
  .subscribe((item) => console.log('item:', item)); // A, B, C
```

## Watch out

- Don't write `from(1, 2, 3)` — `from` takes a single iterable or Promise, not rest arguments, and will fail to compile.
- Don't write `of('abc')` if you want each character, and don't write `from('abc')` if you want the whole string `'abc'` as one emission.

## Remember

- `of([1, 2, 3])` emits one array (`Observable<number[]>`).
- `from([1, 2, 3])` emits three numbers (`Observable<number>`).
- Use `of([])` when downstream expects a list, and `from(list)` when downstream processes items one by one.
