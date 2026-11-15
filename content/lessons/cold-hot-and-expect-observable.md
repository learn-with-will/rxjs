---
slug: cold-hot-and-expect-observable
title: "Creating mock streams with cold(), hot(), and expectObservable()"
topic: schedulers-testing
kind: api
summary: "Simulate per-subscriber streams with cold(), shared streams with hot() and ^, and assert rich values or errors with expectObservable()."
tags: [rxjs, testscheduler, cold, hot, expectobservable]
related: [virtual-time-and-test-scheduler, marble-diagram-syntax, asserting-subscriptions-with-marbles]
---
> **In one line:** `cold(marbles, values?, error?)` starts its timeline when subscribed, `hot(marbles, values?, error?)` runs from frame 0 and marks the test's subscription point with `^`, and `expectObservable(obs$).toBe(marbles, values?, error?)` asserts the output.

## The idea

Inside `testScheduler.run(({ cold, hot, expectObservable }) => ...)`, you create mock input Observables with `cold()` or `hot()` and assert the resulting output stream with `expectObservable()`:

- **`cold(marbles, values?, error?)`**: Simulates a **cold** Observable (like `ajax()`, `of()`, or `from()`). Its marble timeline starts at the exact frame a subscriber subscribes to it. It cannot contain a `^` marker.
- **`hot(marbles, values?, error?)`**: Simulates a **hot** shared source (like DOM clicks, a `Subject`, or a WebSocket). Its timeline is already running from the start of the test, and the caret `^` marks **frame 0** where `expectObservable` subscribes to the pipeline under test. Anything before `^` happened before subscription and is missed!
- **Mapping rich payloads and errors:** Marble characters are single letters (`a`, `b`, `c`), so you pass a `values` dictionary `{ a: { id: 1 }, b: { id: 2 } }` as the second argument and an optional custom error value as the third argument (replacing the default `'error'` string when `#` fires).

```sequence
actor hot: hot(--a--^--b--c--) | shared stream @amber
actor sub: expectObservable | test subscriber @primary
note hot: 'a' emitted before ^ | missed by subscriber @neutral
hot -> sub: ^ subscribe at frame 0
hot -> sub: 'b' at +3ms
hot -> sub: 'c' at +6ms
hot -> sub: complete() at +9ms
```

## See it

Compare `cold()` vs `hot()` with a `values` lookup object and a custom `Error` instance:

```typescript
import { TestScheduler } from 'rxjs/testing';
import { map } from 'rxjs';

const testScheduler = new TestScheduler((a, e) => expect(a).toEqual(e));

testScheduler.run(({ cold, hot, expectObservable }) => {
  // 1. Hot stream with ^ subscription marker and rich value mapping:
  // 'a' happens before '^', so the subscriber only sees 'b' and 'c'
  const clicks$ = hot('--a--^--b--c--|', { a: 1, b: 2, c: 3 });
  const doubled$ = clicks$.pipe(map(n => n * 10));

  expectObservable(doubled$).toBe('   ---b--c--|', { b: 20, c: 30 });

  // 2. Cold stream with a custom error object on '#':
  const apiError = new Error('503 Service Unavailable');
  const failing$ = cold('--a--#', { a: 'ok' }, apiError);
  expectObservable(failing$).toBe('--a--#', { a: 'ok' }, apiError);
});
```

## Go deeper

What if you need to test how your operator behaves when the subscriber unsubscribes early (for example, when a component unmounts at `5ms`)? `expectObservable(obs$, subscriptionMarbles)` accepts a second string argument! Writing `expectObservable(interval$, '^----!').toBe('--a-b')` subscribes at frame 0 (`^`) and automatically unsubscribes at frame 5 (`!`), letting you test infinite streams without hanging the virtual clock.

## Watch out

- If you omit the `values` object, each marble character emits its own literal string (`'a'` emits `"a"`, `'1'` emits `"1"` as a string, not the number `1`).
- If your stream emits `undefined` or `null`, you **must** pass `{ a: undefined }` in the `values` map.

## Remember

- Use `cold()` for per-subscriber sources (HTTP requests) and `hot()` with `^` for shared sources (DOM events, Subjects).
- Pass a `values` dictionary `{ a: payloadA, b: payloadB }` as the 2nd argument to `cold`, `hot`, and `toBe`.
- Pass a custom error value as the 3rd argument when testing `#` error notifications.
