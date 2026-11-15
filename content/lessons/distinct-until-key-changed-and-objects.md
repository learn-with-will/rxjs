---
slug: distinct-until-key-changed-and-objects
title: "Object deduplication and distinctUntilKeyChanged()"
topic: rate-limiting-time
kind: pitfall
summary: "Default distinctUntilChanged() uses === reference equality and lets fresh object literals through; use distinctUntilKeyChanged(key) or a key selector."
tags: [rxjs, distinctuntilchanged, distinctuntilkeychanged, objects, pitfall]
related: [distinct-until-changed, immutable-accumulators-and-tap-pitfalls, scan-running-accumulator]
---
> **In one line:** default `distinctUntilChanged()` compares object **references** (`===`), so two newly created objects `{ id: 1 }` and `{ id: 1 }` are treated as **different** unless you compare by key or custom comparator.

## The idea

In JavaScript, `{ id: 1 } === { id: 1 }` is `false` because each object literal allocates a new reference in memory. That means putting bare `distinctUntilChanged()` on a stream that emits freshly constructed objects (from `map`, `JSON.parse`, or an immutable state store) will **never deduplicate anything** — every object has a new reference!

RxJS gives you three clean ways to deduplicate object streams by their contents:

| Approach | Example | Best when |
|---|---|---|
| **`distinctUntilKeyChanged('id')`** | Compares `prev.id === curr.id` | You want to deduplicate by a single property name |
| **`distinctUntilChanged(comparator)`** | `distinctUntilChanged((a, b) => a.x === b.x && a.y === b.y)` | You need to compare multiple fields |
| **`distinctUntilChanged(comparator, keySelector)`** | `distinctUntilChanged(undefined, u => u.id)` | You want to compute a derived key per emission |

## See it

Compare bare `distinctUntilChanged()` (broken on fresh objects) with `distinctUntilKeyChanged('id')`:

```typescript
import { of, distinctUntilChanged, distinctUntilKeyChanged } from 'rxjs';

const updates = [
  { id: 1, name: 'Ada' },
  { id: 1, name: 'Ada' }, // new object reference with identical id & name
  { id: 2, name: 'Alan' },
];

// Before (bug): default === sees two different object references, so id: 1 emits twice!
of(...updates)
  .pipe(distinctUntilChanged())
  .subscribe((u) => console.log('bare distinctUntilChanged:', u.id)); // 1, 1, 2

// After (Option 1): compare a single property with distinctUntilKeyChanged
of(...updates)
  .pipe(distinctUntilKeyChanged('id'))
  .subscribe((u) => console.log('distinctUntilKeyChanged:', u.id)); // 1, 2

// After (Option 2): custom comparator checking multiple fields
of(...updates)
  .pipe(distinctUntilChanged((a, b) => a.id === b.id && a.name === b.name))
  .subscribe((u) => console.log('custom comparator:', u.id)); // 1, 2
```

## Go deeper

What is the flip side of reference equality? If you **mutate** the same object in place (`state.count++; subject.next(state)`) and pass that mutated reference into bare `distinctUntilChanged()`, `prev === curr` is **`true`** (because both point to the exact same object in memory!), so `distinctUntilChanged()` will **silently drop** your update! That is why RxJS state pipelines pair **immutable object updates** (`{ ...state, count: state.count + 1 }`) with `distinctUntilKeyChanged` or `map(s => s.count), distinctUntilChanged()`.

## Watch out

- Bare `distinctUntilChanged()` on a stream of fresh object literals (`map(x => ({ value: x }))`) never drops duplicates because `prev === curr` is always `false`.
- Bare `distinctUntilChanged()` on a mutated object reference (`obj.count++; next(obj)`) drops *every* update because `prev === curr` is always `true`.

## Remember

- Default `distinctUntilChanged()` uses `===`, which checks reference identity for objects and arrays.
- Use `distinctUntilKeyChanged('prop')` when deduplicating objects by a single property.
- Use `distinctUntilChanged((a, b) => ...)` when comparing multiple fields on emitted objects.
