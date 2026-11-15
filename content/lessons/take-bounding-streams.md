---
slug: take-bounding-streams
title: "Bounding streams with take(n)"
topic: filtering-limiting
kind: api
summary: "take(count) emits at most the first count values from the source and then immediately completes and unsubscribes from the source."
tags: [rxjs, take, completion, filtering, memory-leaks]
related: [filter-operator, take-while-and-inclusive, empty-error-and-take-one]
---
> **In one line:** `take(n)` forwards the first `n` values and then **immediately completes the stream**, automatically unsubscribing from the source.

## The idea

Infinite streams like `interval(1000)` or `fromEvent(button, 'click')` never complete on their own. `take(count)` is the simplest way to turn an infinite stream into a **finite, self-cleaning stream**:

1. It forwards each incoming value until `count` values have been emitted.
2. On the `count`-th emission, right after forwarding that value, `take(count)` sends a **`complete()`** notification downstream and **unsubscribes** from the upstream source.
3. If the upstream source completes early after fewer than `count` values, `take(count)` simply forwards those values and completes without error.

```sequence
actor src: interval(1000) | infinite source
actor tk: take(3) | operator @primary
actor sub: Subscriber | consumer @green
src -> tk: next(0)
tk -> sub: next(0) (1 of 3)
src -> tk: next(1)
tk -> sub: next(1) (2 of 3)
src -> tk: next(2)
tk -> sub: next(2) (3 of 3)
tk -> sub: complete() | stream finished!
tk -> src: unsubscribe() | clears interval
```

## See it

Here `take(3)` bounds an infinite `interval`, and `take(1)` creates a self-removing one-shot click handler:

```typescript
import { interval, fromEvent, take } from 'rxjs';

// 1. Bound an infinite interval to 3 ticks, then auto-complete:
interval(500)
  .pipe(take(3))
  .subscribe({
    next: (n) => console.log('tick:', n),
    complete: () => console.log('3 ticks done — interval torn down!'),
  });
// Logs: tick: 0, tick: 1, tick: 2, 3 ticks done — interval torn down!

// 2. React to the very first click only, then auto-remove the DOM listener:
const button = document.querySelector('#confirm')!;
fromEvent(button, 'click')
  .pipe(take(1))
  .subscribe(() => console.log('Confirmed once; listener removed'));
```

## Go deeper

Notice how clean `fromEvent(button, 'click').pipe(take(1))` is: as soon as the user clicks once, `take(1)` emits the event, completes, and runs `fromEvent`'s teardown (`removeEventListener`) automatically. You don't need to store the `Subscription` or call `unsubscribe()` manually (unless the component can unmount *before* that first click happens).

What happens if the upstream source has fewer than `n` items — say `of('a', 'b').pipe(take(5))`? `take(5)` simply emits `'a'` and `'b'` and forwards the upstream `complete` notification without throwing any error. In other words, `take(n)` means **"at most `n` emissions"**, making it completely safe to use on streams that might finish early or even be empty.

## Watch out

- Calling `take(0)` (or a negative number) completes immediately on subscribe without emitting any values or even subscribing to the source.
- If a component using `fromEvent(...).pipe(take(1))` unmounts *before* the user ever clicks, the subscription is still waiting for that 1 click — pair it with `takeUntil(destroy$)` when the target outlives the component.

## Remember

- `take(n)` emits the first `n` values and then immediately completes and unsubscribes upstream.
- If the source completes with fewer than `n` values, `take(n)` completes cleanly without erroring.
- `take(1)` is the standard idiom for a safe, one-shot subscription.
