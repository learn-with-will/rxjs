---
slug: operator-order-matters
title: "Why operator order matters in .pipe()"
topic: pipes-transforming
kind: technique
summary: "Because each operator in .pipe() feeds the next, swapping their order can change both the emitted values and how much work the pipeline performs."
tags: [rxjs, pipe, operators, performance, filter, map]
related: [chaining-with-pipe, map-operator, filter-operator]
---
> **In one line:** operators in `.pipe()` run sequentially from top to bottom, so **changing their order can change the output values** and **filtering before mapping avoids wasted work**.

## The idea

Because each stage in `source$.pipe(op1, op2)` only sees what the previous stage emitted, two pipelines with the exact same operators in a different order can behave very differently:

1. **Correctness:** if `map` changes the values that `filter` tests, `filter` then `map` tests the *original* values, whereas `map` then `filter` tests the *transformed* values. Similarly, `filter` then `take(2)` gives you the first two **matching** items, while `take(2)` then `filter` only inspects the first two items total and may emit fewer than two!
2. **Performance:** placing a cheap `filter` **before** an expensive `map` (or network call) drops unwanted items early so the expensive step only runs on survivors.

| Pipeline order | Input `1, 2, 3, 4` | What happens | Output |
|---|---|---|---|
| `filter(isEven)` → `take(2)` | `1, 2, 3, 4` | Keeps evens (`2, 4`), takes first 2 | `2, 4` |
| `take(2)` → `filter(isEven)` | `1, 2, 3, 4` | Takes first 2 (`1, 2`), keeps evens | `2` (only one!) |

## See it

See how swapping `filter` and `map` changes how many times `expensiveTransform` runs:

```typescript
import { of, filter, map } from 'rxjs';

function expensiveTransform(n: number): number {
  console.log('transforming:', n);
  return n * 10;
}

// Good order: filter FIRST so expensiveTransform only runs on even numbers (2 and 4)
of(1, 2, 3, 4)
  .pipe(
    filter((n) => n % 2 === 0),
    map((n) => expensiveTransform(n)),
  )
  .subscribe((result) => console.log('result:', result));
// Logs "transforming: 2", "result: 20", "transforming: 4", "result: 40"
```

And see how swapping `filter` and `map` can change the **actual values** when the predicate depends on the number:

```typescript
import { range, filter, map } from 'rxjs';

// 1. Keep multiples of 3 (3, 6, 9), then add 1 -> 4, 7, 10
range(1, 9)
  .pipe(
    filter((n) => n % 3 === 0),
    map((n) => n + 1),
  )
  .subscribe((n) => console.log('filter then map:', n));

// 2. Add 1 first (2..10), then keep multiples of 3 -> 3, 6, 9
range(1, 9)
  .pipe(
    map((n) => n + 1),
    filter((n) => n % 3 === 0),
  )
  .subscribe((n) => console.log('map then filter:', n));
```

## Watch out

- Putting `take(n)` *before* `filter(...)` caps the raw input count rather than the filtered output count — put `filter` before `take(n)` when you want "`n` matching items".
- Putting `debounceTime` *after* an HTTP `switchMap` debounces the responses instead of the keystrokes — always debounce **before** firing the request.

## Remember

- Each operator in `.pipe()` only sees what the operator above it emitted.
- Swapping `map`, `filter`, or `take` can produce completely different outputs.
- Filter and debounce **early** in the pipeline so expensive downstream work runs only when needed.
