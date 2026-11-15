---
slug: which-filtering-operators-complete
title: "Which filtering operators complete the stream?"
topic: filtering-limiting
kind: concept
summary: "Distinguish early completers (take, takeWhile, first), post-completion emitters (takeLast, last), and pass-through filters (filter, skip)."
tags: [rxjs, filtering, completion, take, filter]
related: [filter-operator, take-bounding-streams, take-while-and-inclusive, take-last-and-skip]
---
> **In one line:** filtering operators fall into three groups: **early completers** (`take`, `takeWhile`, `first`), **post-completion emitters** (`takeLast`, `last`), and **pass-through filters** (`filter`, `skip`).

## The idea

Knowing whether an operator **completes the stream on its own** or **waits for the upstream source to complete** is critical for preventing both memory leaks and hung pipelines. Every filtering operator belongs to one of three buckets:

```tree
How does the operator handle completion?
  Completes early on its own (bounds infinite streams)
    [take(n)] After n values | unsubscribes source @green
    [takeWhile] On first false | unsubscribes source @green
    [first] On first match | unsubscribes source @green
  Waits for source to complete first (hangs on infinite streams)
    [takeLast(n)] Buffers last n | emits on complete @amber
    [last] Keeps last match | emits on complete @amber
  Passes source completion through (never stops source early)
    [filter] Drops non-matches | stays open with source @blue
    [skip(n)] Drops first n | stays open with source @blue
```

## See it

See how the three categories behave on an infinite source (`interval`) vs a finite source (`of`):

```typescript
import { interval, of, take, filter, takeLast } from 'rxjs';

// 1. Early completer: take(2) stops interval(100) after 2 values — safe!
interval(100)
  .pipe(take(2))
  .subscribe({
    next: (n) => console.log('take(2):', n),
    complete: () => console.log('take(2) completed and stopped interval'),
  });

// 2. Post-completion emitter: takeLast(2) needs a completing source (or take before it)
of(10, 20, 30, 40)
  .pipe(takeLast(2))
  .subscribe((n) => console.log('takeLast(2):', n)); // 30, 40

// 3. Combining them safely: bound interval with take(5) FIRST, then takeLast(2)
interval(100)
  .pipe(
    take(5),     // bounds the infinite stream to 0, 1, 2, 3, 4 and completes
    takeLast(2), // now receives the completion and emits 3, 4!
  )
  .subscribe((n) => console.log('bounded takeLast:', n));
```

## Go deeper

Look at example 3 above: `interval(100).pipe(take(5), takeLast(2))`. Because `take(5)` is an **early completer**, placing it before `takeLast(2)` turns the infinite `interval` into a finite stream of 5 items, which allows the **post-completion emitter** `takeLast(2)` to fire! If you reversed their order (`takeLast(2)` then `take(5)`), `takeLast(2)` would sit directly on `interval(100)` waiting for completion that never arrives.

## Watch out

- Pass-through operators (`filter`, `skip`, `skipWhile`) never stop an infinite source — you still need `take`, `takeWhile`, `takeUntil`, or `unsubscribe()` to avoid a leak.
- Post-completion operators (`takeLast`, `last`, `reduce`, `toArray`) never emit on an infinite source unless an early-completing operator sits **above** them in `.pipe()`.

## Remember

- **Early completers** (`take`, `takeWhile`, `first`, `takeUntil`) stop the stream and unsubscribe from infinite sources.
- **Post-completion emitters** (`takeLast`, `last`) emit only after the source completes.
- **Pass-through filters** (`filter`, `skip`) drop values selectively but only complete when the source does.
