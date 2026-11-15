---
slug: asserting-subscriptions-with-marbles
title: "Asserting subscription lifecycles with expectSubscriptions()"
topic: schedulers-testing
kind: api
summary: "Verify that operators subscribe at the right frame (^) and clean up inner or upstream streams (!) using expectSubscriptions()."
tags: [rxjs, testscheduler, expectsubscriptions, testing, memory-leaks]
related: [cold-hot-and-expect-observable, testing-time-based-operators, marble-testing-pitfalls]
---
> **In one line:** every `cold()` and `hot()` mock Observable tracks a `.subscriptions` log that you can assert with `expectSubscriptions(source$.subscriptions).toBe('^---!')` to prove your pipeline unsubscribes on time.

## The idea

Checking what values an Observable *emits* with `expectObservable()` is only half the story. What if a refactor accidentally removes `takeUntil(destroy$)` or replaces `switchMap` with `mergeMap`, leaving upstream or inner subscriptions open? The output values might still look right in a simple test while leaking resources in production!

Every `cold()` and `hot()` stream created inside `testScheduler.run()` exposes a `.subscriptions` property. You pass that property to `expectSubscriptions(source$.subscriptions).toBe(...)` using subscription marble syntax:
- **`^`**: A subscriber subscribed at this frame.
- **`!`**: The subscriber unsubscribed at this frame (either explicitly via `take` / `takeUntil` / `switchMap`, or because the source completed/errored).
- **`(^!)`**: Subscribed and unsubscribed synchronously within the same frame.
- **Array of strings `['^--!', '---^--!']`**: Asserts multiple subscriptions to the same cold or hot source (for example, during `retry(1)` or `repeat(2)`).

> **Tip:** Asserting `expectSubscriptions` is the single best way to write a regression test proving that `take(1)`, `takeUntil(destroy$)`, or `switchMap` actually tore down an expensive upstream stream!

## See it

Prove both that `takeUntil(stop$)` unsubscribes from a hot source at the exact millisecond `stop$` emits, and that `retry(1)` creates two sequential subscriptions to a cold source:

```typescript
import { TestScheduler } from 'rxjs/testing';
import { takeUntil, retry } from 'rxjs';

const testScheduler = new TestScheduler((a, e) => expect(a).toEqual(e));

testScheduler.run(({ cold, hot, expectObservable, expectSubscriptions }) => {
  // 1. Verify takeUntil unsubscribes at frame 5:
  const source$ = hot('--a--b--c--d--|');
  const stop$   = hot('-----s--|');
  const sub     =     '^----!';

  expectObservable(source$.pipe(takeUntil(stop$))).toBe('--a--|');
  expectSubscriptions(source$.subscriptions).toBe(sub);

  // 2. Verify retry(1) makes two sequential subscriptions to a cold source:
  const flaky$ = cold('--a--#');
  expectObservable(flaky$.pipe(retry(1))).toBe('--a----a--#');
  expectSubscriptions(flaky$.subscriptions).toBe([
    '^----!',       // 1st attempt: 0ms to 5ms
    '-----^----!',  // 2nd attempt (retry): 5ms to 10ms
  ]);
});
```

## Go deeper

Why does `expectSubscriptions` accept an array of marble strings `['^----!', '-----^----!']` in the `retry(1)` test? Because every time an operator subscribes to a `cold()` or `hot()` test Observable, `TestScheduler` pushes a new `SubscriptionLog` entry into `flaky$.subscriptions`. Passing an array asserts the exact start (`^`) and end (`!`) frame of each individual subscription in order.

## Watch out

- When a source completes (`|`) or errors (`#`) naturally, its subscription also ends at that frame — so a cold stream `'--a--|'` has subscription marble `'^----!'`.
- An infinite stream that is never unsubscribed has subscription marble `'^'` (no `!`).

## Remember

- Use `expectSubscriptions(source$.subscriptions).toBe('^---!')` to assert when a stream is subscribed (`^`) and unsubscribed (`!`).
- Pass an array of subscription marble strings `['^--!', '---^--!']` when testing operators that subscribe multiple times (`retry`, `repeat`, `switchMap`).
- Always pair `expectObservable` with `expectSubscriptions` when testing teardown and cancellation logic.
