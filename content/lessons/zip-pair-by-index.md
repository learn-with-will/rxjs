---
slug: zip-pair-by-index
title: "Lockstep pairing by index with zip()"
topic: combining-joining
kind: api
summary: "zip(a$, b$) pairs the 1st emission of each stream, then the 2nd, then the 3rd in lockstep, buffering faster streams and completing when the shortest completes."
tags: [rxjs, zip, combining, joining, backpressure]
related: [combine-latest, merge-and-concat, joining-stream-pitfalls, pipeable-with-operators]
---
> **In one line:** `zip(a$, b$)` pairs emissions **strictly by index** — 1st with 1st, 2nd with 2nd, 3rd with 3rd — like the teeth of a zipper, and completes as soon as the shortest stream runs out of pairs.

## The idea

How does `zip(a$, b$)` differ from `combineLatest([a$, b$])`?

- **`combineLatest`** reuses the *latest* value whenever *any* stream emits.
- **`zip`** never reuses a value! It waits until **every** input has produced its **0th** emission and emits `[a0, b0]`; then it waits until **every** input has produced its **1st** emission and emits `[a1, b1]`, and so on in strict lockstep.

Because `zip` pairs by index, if `a$` emits 10 values per second while `b$` emits only 1 value per second, `zip` **buffers `a$`'s unmatched values in memory** waiting for `b$`'s matching indices to arrive. And as soon as any input **completes** and its buffer of unmatched values is exhausted, no more complete pairs can ever be formed — so `zip` **immediately completes** and unsubscribes from all the other inputs!

```sequence
actor a: names$ | "Ada", "Alan", "Grace" (3 items) @blue
actor b: scores$ | 98, 95 (2 items + complete) @amber
actor z: zip(names$, scores$) | pairs by index 0, 1 @primary
a -> z: "Ada" (index 0)
b -> z: 98 (index 0) -> emits ["Ada", 98]
a -> z: "Alan" (index 1), "Grace" (index 2 buffered)
b -> z: 95 (index 1) + complete() -> emits ["Alan", 95] + complete()!
```

## See it

Notice that `"Grace"` (index 2) is never emitted because `scores$` completed after index 1:

```typescript
import { of, zip } from 'rxjs';

const names$ = of('Ada', 'Alan', 'Grace');
const scores$ = of(98, 95); // Only 2 items!

zip(names$, scores$).subscribe({
  next: ([name, score]) => console.log(`${name}: ${score}`),
  complete: () => console.log('zip completed when shortest stream finished'),
});
// Output:
// Ada: 98
// Alan: 95
// zip completed when shortest stream finished
```

You can also pair an array of items with `interval(500)` using `zip` to drip-feed synchronous items out one every 500ms:

```typescript
import { from, interval, zip, map } from 'rxjs';

zip(from(['Ready', 'Set', 'Go!']), interval(500))
  .pipe(map(([word]) => word))
  .subscribe((word) => console.log(word));
// Emits "Ready" at 500ms, "Set" at 1000ms, "Go!" at 1500ms, then completes!
```

## Go deeper

Look at that second snippet: `zip(from(['Ready', 'Set', 'Go!']), interval(500))`. Even though `interval(500)` is an infinite stream, `from(['Ready', 'Set', 'Go!'])` completes after 3 items — so once index 2 (`"Go!"`) is paired at 1500ms, `zip` automatically **completes and unsubscribes from `interval(500)`**!

## Watch out

- Do not use `zip` on two streams that emit at permanently different rates (e.g. `mousemove` at 60 Hz zipped with `click` at 0.1 Hz) — the fast stream's unmatched values accumulate in an unbounded internal buffer and leak memory!
- Like `merge` and `concat`, `zip(a$, b$)` takes rest arguments (or an array), and has a pipeable counterpart `zipWith(b$)`.

## Remember

- `zip(a$, b$)` pairs the `i`-th emission of `a$` with the `i`-th emission of `b$` in strict lockstep.
- Faster streams have their unmatched values buffered in memory until slower streams catch up.
- `zip` completes as soon as any input completes and has no buffered values left to pair.
