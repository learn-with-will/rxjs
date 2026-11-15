---
slug: virtual-time-and-test-scheduler
title: "Virtual time and TestScheduler.run()"
topic: schedulers-testing
kind: concept
summary: "Replace slow, flaky wall-clock timers in unit tests with deterministic virtual time using RxJS TestScheduler.run()."
tags: [rxjs, testscheduler, testing, virtual-time, marbles]
related: [what-is-a-scheduler, marble-diagram-syntax, cold-hot-and-expect-observable]
---
> **In one line:** `TestScheduler.run()` hijacks `asyncScheduler` inside its callback and replaces wall-clock `setTimeout` ticks with an instant, deterministic virtual clock where 1 character equals 1 virtual millisecond.

## The idea

Why is testing `debounceTime(300)` or `retry({ delay: 2000 })` with real timers painful?
1. **Wall-clock tests are slow:** Waiting 2 real seconds per test makes a 200-test suite take minutes.
2. **Real timers are flaky:** OS CPU throttling on busy CI runners causes `setTimeout(..., 300)` to fire at `314ms`, breaking assertions randomly.

RxJS solves both problems with `TestScheduler` from `'rxjs/testing'`. When you call `testScheduler.run(helpers => { ... })`, RxJS intercepts all default `asyncScheduler` operators (`debounceTime`, `delay`, `interval`, `timer`, `timeout`, `auditTime`, `throttleTime`) and binds them to a synchronous **virtual clock** that executes 10 hours of virtual stream time in under 2 milliseconds of real CPU time.

| Testing approach | Speed for a 5-second stream | Timing accuracy | How it works |
|---|---|---|---|
| Real `setTimeout` + `done()` | 5,000ms real time | Flaky (±15ms jitter) | Waits on the OS macrotask queue |
| `TestScheduler.run()` | **< 2ms real time** | **Exact to 1ms** | Steps through a synchronous virtual priority queue frame by frame |

## See it

Instantiate `TestScheduler` with your test runner's deep-equality assertion (`expect(actual).toEqual(expected)` in Vitest or Jest) and wrap each test inside `testScheduler.run()`:

```typescript
import { TestScheduler } from 'rxjs/testing';
import { delay } from 'rxjs';
import { describe, it, expect, beforeEach } from 'vitest';

describe('Marble testing with TestScheduler', () => {
  let testScheduler: TestScheduler;

  beforeEach(() => {
    testScheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });
  });

  it('shifts emissions by 3ms in virtual time', () => {
    testScheduler.run(({ cold, expectObservable }) => {
      const source$ = cold('--a--b--|');
      const expected =     '-----a--b--|';
      expectObservable(source$.pipe(delay(3))).toBe(expected);
    });
  });
});
```

## Go deeper

What makes `testScheduler.run()` so ergonomic in RxJS 6.5+? Before `run()` was introduced, you had to manually pass `testScheduler` as the last argument to every single operator (`debounceTime(30, testScheduler)`) and each `-` character represented `10ms`. Inside `testScheduler.run()`, RxJS temporarily overrides `asyncScheduler.delegate` so every time-based operator uses virtual time automatically without changing your production code, and each character in a marble string represents exactly **1 virtual millisecond**.

## Watch out

- Synchronous assertions inside `testScheduler.run()` do **not** run immediately when `expectObservable(...).toBe(...)` is called — they are queued and verified when the `run()` callback exits and flushes the virtual clock.
- Never reuse a single `TestScheduler` instance across multiple tests without re-instantiating it in `beforeEach`.

## Remember

- Import `TestScheduler` from `'rxjs/testing'` and pass `(actual, expected) => expect(actual).toEqual(expected)` to its constructor.
- Always wrap marble tests in `testScheduler.run(({ cold, hot, expectObservable, expectSubscriptions, flush }) => { ... })`.
- Inside `run()`, every `asyncScheduler`-backed operator uses virtual time automatically and 1 marble character equals 1ms.
