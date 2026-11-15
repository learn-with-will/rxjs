---
slug: immutable-accumulators-and-tap-pitfalls
title: "Transformation pitfalls: mutating scan state and misusing tap"
topic: pipes-transforming
kind: pitfall
summary: "Mutating an array or object accumulator in scan() leaks a shared reference across emissions, and returning a value from tap() does not transform the stream."
tags: [rxjs, scan, tap, map, pitfall, immutability]
related: [scan-running-accumulator, tap-for-side-effects, distinct-until-changed]
---
> **In one line:** never mutate the accumulator inside `scan` (return a new object or array instead), and never try to transform values by returning from `tap` (use `map` instead).

## The idea

When building transformation pipelines with `map`, `tap`, and `scan`, three subtle mistakes cause bugs that compile cleanly in TypeScript:

| Mistake | Why it breaks | Fix |
|---|---|---|
| **Mutating `acc` in `scan`** (`acc.push(x); return acc;`) | Every emission points to the **same array reference**, breaking history snapshots and `distinctUntilChanged()` | Return a new reference: `[...acc, x]` or `{ ...acc, count: acc.count + 1 }` |
| **Returning a transformed value in `tap`** (`tap(x => x * 2)`) | `tap` ignores your callback's return value and forwards the original `x` | Use `map(x => x * 2)` to change values |
| **Putting side effects inside `map`** | Makes a pure projection impure; side effects re-run unexpectedly per subscriber | Keep `map` pure; move logging or analytics into `tap` |

## See it

See how mutating the array accumulator in `scan` corrupts saved snapshots because every emitted value is the exact same array reference:

```typescript
import { of, scan } from 'rxjs';

// Before (bug): mutating the accumulator array in place
const snapshotsBad: number[][] = [];
of(1, 2, 3)
  .pipe(
    scan((acc: number[], n) => {
      acc.push(n); // Mutates the same array in place!
      return acc;
    }, []),
  )
  .subscribe((arr) => snapshotsBad.push(arr));

console.log('Bad snapshots:', snapshotsBad);
// [[1, 2, 3], [1, 2, 3], [1, 2, 3]] — all three point to the mutated final array!

// After (correct): return a brand-new array on every step
const snapshotsGood: number[][] = [];
of(1, 2, 3)
  .pipe(scan((acc: number[], n) => [...acc, n], []))
  .subscribe((arr) => snapshotsGood.push(arr));

console.log('Good snapshots:', snapshotsGood);
// [[1], [1, 2], [1, 2, 3]] — each emission is an independent immutable snapshot
```

## Go deeper

Immutable updates in `scan` also matter whenever downstream code compares the previous emission to the current one — for example with `distinctUntilChanged()`, React state setters, or Angular OnPush inputs. If `scan` mutates `state` in place and returns the same object reference, `prev === curr` is `true`, so `distinctUntilChanged()` thinks nothing changed and drops the update!

## Watch out

- `acc.push(x)`, `acc.sort()`, and `acc.count++` all mutate in place; use spread syntax (`[...acc, x]`, `{ ...acc, count: acc.count + 1 }`) inside `scan` and `reduce`.
- If you pass `scan(fn, [])` a mutable seed array and mutate it inside `fn`, a second subscriber to that pipeline will start with the already-mutated seed array!

## Remember

- Always return a new object or array reference from `scan` and `reduce` accumulators.
- Mutating an accumulator in place breaks snapshot history and reference checks like `distinctUntilChanged`.
- Use `map` to transform values and `tap` only for side effects.
