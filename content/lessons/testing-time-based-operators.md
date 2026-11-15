---
slug: testing-time-based-operators
title: "Testing debounceTime, switchMap, and retry in virtual time"
topic: schedulers-testing
kind: pattern
summary: "Write fast, deterministic marble tests for debounceTime, throttleTime, switchMap cancellation, and delayed retries."
tags: [rxjs, testscheduler, debouncetime, switchmap, testing]
related: [virtual-time-and-test-scheduler, marble-diagram-syntax, asserting-subscriptions-with-marbles]
---
> **In one line:** combine time progression syntax (`300ms`) with `cold()` inner streams to verify rate-limiting operators, `switchMap` cancellation, and backoff retries in milliseconds.

## The idea

Real-world RxJS pipelines combine time-based rate limiting (`debounceTime`, `throttleTime`) with higher-order flattening (`switchMap`, `exhaustMap`) and delayed error recovery (`retry({ delay })`). In `testScheduler.run()`, you can test all of these together by mapping each outer emission to a `cold()` inner Observable.

When tracing `debounceTime(300)` in a marble test, keep one rule in mind:
- After an input emission `a`, if the next emission `b` arrives in **fewer than 300ms**, `a` is dropped and the 300ms timer resets from `b`.
- If **300ms of silence** elapses (or the source completes `|` early!), the debounced value is emitted on that frame.

| Operator under test | Input marble pattern | Expected output marble pattern | Why |
|---|---|---|---|
| `debounceTime(3)` | `-a-b---c---\|` | `-------b---(c\|)` | `a` is replaced by `b`; `b` waits 3ms; `c` flushes on `\|` |
| `throttleTime(3)` | `-a-b-c-d---\|` | `-a-----d---\|` | `a` emits immediately, then silences 3ms (`b`, `c` dropped) |
| `switchMap(() => cold('--x\|'))` | `-a-b----\|` | `-----x--\|` | First inner `--x\|` is cancelled at `b`; second emits `x` 2ms after `b` |

## See it

Test a typeahead pipeline with `debounceTime(200)` and `switchMap` cancellation using explicit millisecond progression:

```typescript
import { TestScheduler } from 'rxjs/testing';
import { debounceTime, switchMap } from 'rxjs';

const testScheduler = new TestScheduler((a, e) => expect(a).toEqual(e));

testScheduler.run(({ cold, hot, expectObservable }) => {
  // User types 'r', then 50ms later 'rx' (resets 200ms debounce),
  // waits 200ms so 'rx' fires, then types 'rxjs' 300ms later
  const input$ = hot('a 50ms b 200ms ---- 300ms c|', { a: 'r', b: 'rx', c: 'rxjs' });

  // Simulated 50ms HTTP request for each debounced query:
  const results$ = input$.pipe(
    debounceTime(200),
    switchMap(q => cold('50ms (r|)', { r: `Result: ${q}` }))
  );

  // 'a' (0ms) is dropped when 'b' arrives at 51ms.
  // 'b' emits after 200ms debounce (at 251ms) + 50ms HTTP = 301ms!
  expectObservable(results$).toBe('301ms r 300ms (s|)', {
    r: 'Result: rx',
    s: 'Result: rxjs',
  });
});
```

## Go deeper

Notice how `cold('50ms (r|)')` emits its value `r` and completes `|` on the **exact same frame** at 50ms using `(r|)`. Real HTTP requests (`fetch`, `HttpClient`, `ajax`) always emit their single response and complete synchronously in the same tick, so modeling mock HTTP responses as `cold('50ms (r|)')` (rather than `cold('50ms r|')` which completes 1ms after emitting) accurately mirrors production behavior.

## Watch out

- Remember that `debounceTime` flushes any pending value **immediately** when the source completes (`|`), producing a synchronous group `(c|)` if completion arrives before the debounce window finishes.
- Remember that `a` itself takes `1ms`, so `'a 50ms b'` places `a` at `0ms`, advances `1ms` for `a` + `50ms` wait = `b` at `51ms`.

## Remember

- Model single-emission HTTP calls as `cold('50ms (r|)', { r: data })` so emission and completion happen in the same frame.
- Account for the 1ms frame width of emission tokens when calculating exact millisecond offsets.
- Combine `hot()` outer events with `cold()` inner requests to verify `switchMap`, `concatMap`, and `exhaustMap` timing.
