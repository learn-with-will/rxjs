---
slug: scan-running-accumulator
title: "Accumulating state over time with scan()"
topic: pipes-transforming
kind: api
summary: "scan((acc, value) => nextAcc, seed) maintains state across emissions and emits the updated accumulator after every source value."
tags: [rxjs, scan, state, accumulator, transformation]
related: [map-operator, reduce-vs-scan, immutable-accumulators-and-tap-pitfalls]
---
> **In one line:** `scan((acc, curr) => nextAcc, seed)` works like `Array.prototype.reduce`, except it **emits the running accumulated value after every single source emission**.

## The idea

`map` transforms each value in isolation — it has no memory of previous values. When you need **state that evolves over time** (a click counter, a running total, a shopping cart, or a Redux-style state reducer), you reach for **`scan(accumulator, seed)`**.

Here is how `scan` works on each emission when you provide an initial `seed`:

1. Before the first source emission, the internal accumulator starts at `seed`.
2. When the first source value `v1` arrives, `scan` calls `acc1 = accumulator(seed, v1, 0)` and **immediately emits `acc1`**.
3. When `v2` arrives, `scan` calls `acc2 = accumulator(acc1, v2, 1)` and **immediately emits `acc2`**, and so on for the life of the stream.

```flow
e1: Click #1 | +1 @primary
s1: scan((count) => count + 1, 0) | emits 1 @blue
e2: Click #2 | +1 @primary
s2: scan state = 1 | emits 2 @green
e1 -> s1: seed 0 + 1
s1 -> s2: carries state 1
e2 -> s2: state 1 + 1
```

## See it

Here `scan` turns discrete numbers into a running sum, and turns action objects into an evolving balance:

```typescript
import { of, scan } from 'rxjs';

// 1. Running sum starting from seed 0:
of(1, 2, 3, 4)
  .pipe(scan((sum, n) => sum + n, 0))
  .subscribe((sum) => console.log('running sum:', sum));
// Logs: 1, then 3, then 6, then 10

// 2. Mini state machine (Redux-style reducer over time):
type Action = { type: 'add' | 'remove'; amount: number };

of<Action>({ type: 'add', amount: 10 }, { type: 'add', amount: 5 }, { type: 'remove', amount: 3 })
  .pipe(
    scan(
      (state, action) => ({
        balance: action.type === 'add' ? state.balance + action.amount : state.balance - action.amount,
      }),
      { balance: 0 },
    ),
  )
  .subscribe((s) => console.log('balance:', s.balance));
// Logs: 10, then 15, then 12
```

## Go deeper

What happens if you omit the `seed` argument (`scan((a, b) => a + b)`)? Without a seed, the **first source value** is emitted as-is and used as the initial accumulator, and the `accumulator` function only starts running on the **second** emission. Providing an explicit `seed` is almost always better: it works uniformly from the very first emission and lets the accumulator have a different TypeScript type (such as an object or array) from the incoming events.

## Watch out

- Never mutate the accumulator in place (`acc.push(item); return acc;`) — always return a new object or array (`[...acc, item]`) so downstream operators like `distinctUntilChanged` see a new reference.
- `scan` does **not** emit the initial `seed` before the first source value arrives; chain `startWith(seed)` after `scan` if subscribers need the initial state immediately on subscribe.

## Remember

- `scan(accumulator, seed)` updates state on every emission and emits each intermediate result.
- Always pass an explicit `seed` when the state type differs from the event type.
- Return a new object or array from the accumulator function to keep state snapshots immutable.
