---
slug: distinct-until-changed
title: "Suppressing consecutive duplicates with distinctUntilChanged()"
topic: rate-limiting-time
kind: api
summary: "distinctUntilChanged(comparator?) suppresses values that match the immediately preceding emitted value (using === by default), while allowing later repeats."
tags: [rxjs, distinctuntilchanged, deduplication, filtering]
related: [distinct-until-key-changed-and-objects, debounce-time, typeahead-search-pattern]
---
> **In one line:** `distinctUntilChanged()` drops a value only if it equals the **immediately previous emitted value** — it is a consecutive deduplicator, not a global `Set`.

## The idea

Suppose a user types `"react"` in a search box, presses a non-character key like `Shift` or `ArrowLeft`, or quickly types a space and backspaces it before `debounceTime` fires. The input's trimmed string is still `"react"`. Re-running your search query or re-rendering your component for the exact same value is pure waste.

**`distinctUntilChanged(comparator?)`** remembers only the **last value it emitted downstream**:

- The very first value from the source is **always emitted**.
- For each subsequent value `curr`, it compares `prev` and `curr` (using `===` by default, or your custom `comparator(prev, curr)` function returning `true` when they should be treated as equal).
- If they are equal, `curr` is **dropped**. If they differ, `curr` is **emitted** and becomes the new `prev`.

Crucially, if a value changes from `1` to `2` and later back to `1`, the second `1` **is emitted** because it differs from `2`.

```flow
src: Source | 1, 1, 2, 2, 2, 1, 3 @primary
duc: distinctUntilChanged() | compares with previous emitted value @blue
out: Output | 1, 2, 1, 3 (note: second 1 is kept!) @green
src -> duc: 1, 1, 2, 2, 2, 1, 3
duc -> out: 1, 2, 1, 3
```

## See it

```typescript
import { of, distinctUntilChanged } from 'rxjs';

// 1. Default (===) comparison on primitives:
of(1, 1, 2, 2, 2, 1, 3)
  .pipe(distinctUntilChanged())
  .subscribe((n) => console.log('default:', n));
// Logs: 1, 2, 1, 3  (only consecutive duplicates are dropped)

// 2. Custom comparator: case-insensitive string comparison
of('RxJS', 'rxjs', 'RXJS', 'Angular', 'angular')
  .pipe(
    distinctUntilChanged(
      (prev, curr) => prev.toLowerCase() === curr.toLowerCase(),
    ),
  )
  .subscribe((word) => console.log('case-insensitive:', word));
// Logs: "RxJS", "Angular"
```

## Go deeper

Look carefully at the custom comparator `(prev, curr) => boolean`: returning **`true`** means *"these two values are the **same**, so **suppress** `curr`"*; returning **`false`** means *"they are different, so **emit** `curr`"*. When `curr` is suppressed, `prev` stays unchanged as the last value that actually made it through.

> **Tip:** What if you actually want to drop **all** duplicates ever seen across the entire life of the stream (not just consecutive ones)? RxJS has `distinct()`, which stores every seen value in an internal `Set` — use it with care on long-lived streams so the `Set` doesn't grow without bound.

## Watch out

- `distinctUntilChanged()` on `1, 2, 1` emits `1, 2, 1` — it only checks the immediately preceding value, not earlier history.
- Do not invert the custom comparator: returning `true` means **equal (drop)**, not "keep" like `filter`!

## Remember

- `distinctUntilChanged()` drops a value only when it matches the most recently emitted value.
- By default it compares values with `===`.
- Pass a custom `(prev, curr) => boolean` comparator when equality depends on custom logic (return `true` when equal).
