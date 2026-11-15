---
slug: marble-testing-pitfalls
title: "Marble testing pitfalls: Promises, sync groups, and flush()"
topic: schedulers-testing
kind: pitfall
summary: "Avoid the 3 classic TestScheduler traps: unvirtualized Promise microtasks, 4ms synchronous group frame shifts, and asserting side effects before flush()."
tags: [rxjs, testscheduler, pitfalls, promises, testing]
related: [virtual-time-and-test-scheduler, marble-diagram-syntax, asserting-subscriptions-with-marbles]
---
> **In one line:** `TestScheduler` only controls RxJS schedulers — it **cannot** virtualize native `Promise` microtasks, its synchronous groups `(ab)` shift subsequent frames by 1ms per character, and side-effect assertions must run after `flush()`.

## The idea

Even experienced developers hit three common traps when writing RxJS marble tests:

1. **Native Promises ignore `TestScheduler`:** `from(Promise.resolve('a'))` or `switchMap(async () => ...)` schedules work on the JavaScript engine's native **microtask queue**, not on RxJS's `asyncScheduler`. Because `TestScheduler.run()` executes synchronously inside a single turn of the event loop, the Promise microtask never resolves before `run()` finishes!
2. **Synchronous group width shifts later frames:** Writing `(ab)` emits `a` and `b` at the same millisecond, *but* because `(ab)` is 4 characters long, it advances the virtual clock by **4ms**. Forgetting those extra 3ms throws off every marble after the group.
3. **Asserting `vi.fn()` spies before `flush()`:** Calling `expectObservable(obs$).toBe(...)` only *schedules* the virtual subscription. If you assert `expect(mySpy).toHaveBeenCalled()` on the very next line inside `run()`, the virtual clock hasn't run yet! You must call `flush()` first (or assert after `run()` returns).

| Pitfall | Symptom in test output | Fix |
|---|---|---|
| `from(Promise)` or `async` callback inside `run()` | Expected `'a'`, received `''` (empty timeline) | Mock the dependency to return `cold('(a\|)')` or `of('a')` in marble tests, or test real Promises with `async/await` + `firstValueFrom` |
| Frame mismatch after `(ab)` or `(a\|)` | Expected frame `5ms`, actual frame `8ms` | Pad the other timeline with spaces or dashes to match the character width of `(ab)` |
| Spy assertion fails inside `run()` | `expect(spy).toHaveBeenCalled()` is `0` | Call `flush()` from the `run({ flush })` helpers before asserting on external spies |

## See it

See how calling `flush()` executes queued virtual timelines so spy assertions succeed, and how to pad timelines beneath a synchronous group `(ab)`:

```typescript
import { TestScheduler } from 'rxjs/testing';
import { tap, concatWith } from 'rxjs';
import { vi, expect } from 'vitest';

const testScheduler = new TestScheduler((a, e) => expect(a).toEqual(e));

testScheduler.run(({ cold, expectObservable, flush }) => {
  const spy = vi.fn();

  // '(ab)' is 4 chars wide (0ms..3ms), so 'c' after '-' lands at 5ms!
  const first$  = cold('(ab)-c|').pipe(tap(spy));
  const second$ = cold('       -d|');
  expectObservable(first$.pipe(concatWith(second$))).toBe('(ab)-c-d|');

  // Without flush(), spy has NOT been called yet because the virtual clock hasn't run!
  flush();
  expect(spy).toHaveBeenCalledTimes(3); // 'a', 'b', 'c'
});
```

## Go deeper

What if your code genuinely uses `async / await` or `from(fetch(...))` and you cannot replace the inner source with `cold()`? Don't force `TestScheduler` onto native Promise code! Instead, write a standard `async` test using `firstValueFrom` or `lastValueFrom` (and your test runner's `vi.useFakeTimers()` if timers are involved). Use `TestScheduler` for pure Observable and operator pipelines, and `async / await` with `firstValueFrom` for Promise integration tests.

## Watch out

- Never mix legacy outside-`run()` marble helpers (`10ms` per dash) with inside-`run()` helpers (`1ms` per dash).
- Calling `flush()` manually runs all queued `expectObservable` and `expectSubscriptions` assertions immediately.

## Remember

- `TestScheduler` cannot virtualize native `Promise` or `async/await` microtasks — mock async dependencies with `cold()` in marble tests.
- Every character inside `(ab|)` advances virtual time by 1ms; align parallel marble strings with spaces to prevent off-by-N frame bugs.
- Call `flush()` before checking `vi.fn()` spies or external state mutations inside `testScheduler.run()`.
