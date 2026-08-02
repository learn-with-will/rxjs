---
id: lesson-18
slug: marble-testing
title: "Marble Testing with the TestScheduler"
level: advanced
order: 18
duration: 34
tags:
  - rxjs
  - testing
  - testscheduler
  - marbles
  - virtual-time
---

# Learning Objectives

By the end of this lesson you will be able to:

- Explain why the `TestScheduler` makes time-based streams testable and deterministic.
- Read and write marble-diagram syntax: `-`, values, `|`, `#`, `()`, `^`, `!`.
- Set up a `TestScheduler` and use its `run()` helpers.
- Assert emissions with `expectObservable(...).toBe(...)` and a values map.
- Assert subscription timing with `expectSubscriptions(...)`.
- Test operators like `map`, `debounceTime`, and `switchMap` without real timers.

# Why This Concept Exists

How do you test that `debounceTime(300)` emits after the right delay, or that `switchMap` cancels a request? With real timers, tests are slow and flaky. The **`TestScheduler`** replaces real time with **virtual time**: delays pass instantly and deterministically, and streams are described as compact **marble diagrams**. It turns "wait and hope" into "assert exactly".

# Concept Explanation

### Virtual time and the TestScheduler

`TestScheduler` runs scheduled work against a **virtual clock** you control, so a `5000 ms` delay completes immediately in the test. You give it an **assertion function** so it can compare actual vs expected notifications using your test runner's deep-equality check.

```ts
import { TestScheduler } from 'rxjs/testing';

const testScheduler = new TestScheduler((actual, expected) => {
  // Framework-agnostic: use your runner's deep-equal.
  expect(actual).toEqual(expected);
});
```

### The `run()` helper

Do all marble work inside `testScheduler.run(...)`. It provides helpers and **auto-virtualises** the async schedulers, so operators like `debounceTime` (which default to `asyncScheduler`) are automatically driven by virtual time.

```ts
testScheduler.run((helpers) => {
  const { cold, hot, expectObservable, expectSubscriptions } = helpers;
  // ... build streams, assert ...
});
```

Inside `run()`, **1 frame = 1 millisecond**, and each character in a marble diagram advances the clock by one frame.

### Marble syntax

- `-` — 1 frame passes (1 ms in `run()`).
- `a`, `b`, `1` — a value emission at that frame (mapped via a values object).
- `|` — completion.
- `#` — error.
- `( )` — group emissions that occur in the **same** frame, e.g. `(ab|)`.
- `^` — in a **hot** observable, the subscription point (frame 0); time before it is negative.
- `!` — in a **subscription** diagram, the moment of unsubscription.
- whitespace — ignored, used only to visually align diagrams.
- Time progression — write durations like `'10ms a'` or `'5s b'` for longer gaps.

### Cold and hot factories

- `cold(marble, values?, error?)` — a cold Observable that starts when the code under test subscribes.
- `hot(marble, values?, error?)` — a hot Observable; `^` marks frame 0.

```ts
const source$ = cold('-a-b-|', { a: 1, b: 2 }); // emits 1, then 2, then completes
```

### Assertions

- `expectObservable(obs).toBe(marble, values?, error?)` — asserts what `obs` emits and when.
- `expectSubscriptions(source.subscriptions).toBe(subMarble)` — asserts when `source` was subscribed (`^`) and unsubscribed (`!`).

# Key Terminology

- **TestScheduler** — schedules RxJS work on a controllable virtual clock (`rxjs/testing`).
- **Virtual time** — simulated time that advances instantly and deterministically.
- **Marble diagram** — a string describing a stream's emissions over frames.
- **Frame** — the unit of virtual time; 1 frame = 1 ms inside `run()`.
- **`cold` / `hot`** — factories for cold/hot test Observables.
- **`expectObservable` / `expectSubscriptions`** — assert emissions / subscription timing.
- **Values map** — an object mapping marble characters to real emitted values.

# Syntax Overview

```ts
import { TestScheduler } from 'rxjs/testing';
import { map, debounceTime } from 'rxjs';

const scheduler = new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

scheduler.run(({ cold, expectObservable, expectSubscriptions }) => {
  const source$ = cold('-a-b-c-|', { a: 1, b: 2, c: 3 });
  const expected =     '-a-b-c-|';
  expectObservable(source$.pipe(map((x) => x))).toBe(expected, { a: 1, b: 2, c: 3 });

  const subs =         '^------!';
  expectSubscriptions(source$.subscriptions).toBe(subs);
});
```

# Real World Analogy

The TestScheduler is a **film editor's timeline with a scrub bar**. Instead of watching a two-hour movie in real time to check a scene at 1:47:00, you drag the playhead there instantly. Marble diagrams are the **storyboard**: each cell is a frame, a letter is "something happens here", `|` is "the end", `#` is "the reel snaps". You lay the *expected* storyboard beside what the stream *actually* produced and check they match frame-for-frame — no waiting, no flakiness, total control over time.

# Examples

## Example 1 — Basic: testing map

```ts
import { TestScheduler } from 'rxjs/testing';
import { map } from 'rxjs';

const scheduler = new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

it('doubles each value', () => {
  scheduler.run(({ cold, expectObservable }) => {
    const source$ = cold('-a-b-c|', { a: 1, b: 2, c: 3 });
    const result$ = source$.pipe(map((x) => x * 10));

    expectObservable(result$).toBe('-a-b-c|', { a: 10, b: 20, c: 30 });
  });
});
```

**Why this is good:** It shows the full loop — a cold source, an operator under test, and an `expectObservable` assertion with a values map. The emission *timing* (`-a-b-c|`) is preserved because `map` doesn't change timing, only values.

## Example 2 — Real-world: testing debounceTime with virtual time

```ts
import { TestScheduler } from 'rxjs/testing';
import { debounceTime } from 'rxjs';

const scheduler = new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

it('emits only the last value after a quiet gap', () => {
  scheduler.run(({ cold, expectObservable }) => {
    //                 a arrives, b 2ms later, then a long quiet gap
    const source$ = cold('a-b---------|', { a: 'a', b: 'b' });
    const result$ = source$.pipe(debounceTime(5)); // 5 frames = 5 ms

    // Only 'b' survives, emitted 5ms after b arrives; completion passes through.
    expectObservable(result$).toBe('------b-----|', { b: 'b' });
  });
});
```

**Why this is good:** `debounceTime` is time-based and painful to test with real timers. Inside `run()` the async scheduler is virtualised, so the 5 ms debounce resolves instantly and deterministically — and the marble makes the timing explicit and reviewable.

## Example 3 — Best practice: asserting subscriptions (cancellation)

```ts
import { TestScheduler } from 'rxjs/testing';
import { switchMap } from 'rxjs';

const scheduler = new TestScheduler((actual, expected) => expect(actual).toEqual(expected));

it('switchMap unsubscribes the previous inner when a new value arrives', () => {
  scheduler.run(({ cold, hot, expectObservable, expectSubscriptions }) => {
    const inner1 =  cold('  a-a-a-a'); // long-running inner
    const inner2 =  cold('       b-b|');
    const source =  hot('  -x---y---|', { x: inner1, y: inner2 });
    const expected =     '  -a-a-b-b|';

    expectObservable(source.pipe(switchMap((i) => i))).toBe(expected);
    // inner1 is subscribed at x and UNSUBSCRIBED (!) when y arrives:
    expectSubscriptions(inner1.subscriptions).toBe(' -^---!');
  });
});
```

**Why this is good:** It verifies not just *what* was emitted but *that the previous inner was cancelled* — using `expectSubscriptions` and the `!` unsubscribe marker. Testing subscription timing is how you prove `switchMap`'s cancellation behaviour.

# Common Mistakes

- **Using real timers/delays in tests.** Slow and flaky. **Fix:** use `TestScheduler.run()`, which virtualises time.
- **Calling helpers outside `run()`.** `cold`, `hot`, and the `expect*` helpers only exist inside the `run()` callback. **Fix:** put all marble code inside it.
- **Forgetting the assertion function.** Without it, `TestScheduler` can't compare results. **Fix:** pass `(actual, expected) => expect(actual).toEqual(expected)` to the constructor.
- **Misaligning diagrams.** Whitespace is for alignment and is ignored; a stray dash shifts timing. **Fix:** align the expected diagram under the source and count frames carefully.
- **Ignoring the values map.** `-a-b-|` alone emits the strings `'a'`/`'b'`. **Fix:** supply `{ a: 1, b: 2 }` to map marbles to real values.

# Best Practices

- Do all marble testing inside **`testScheduler.run()`** so schedulers are auto-virtualised.
- Assert **both** emissions (`expectObservable`) and, where behaviour depends on it, **subscription timing** (`expectSubscriptions`).
- Use a **values map** for anything beyond single characters; keep marbles readable.
- Remember **1 frame = 1 ms** inside `run()`, and use `'Nms'`/`'Ns'` progression for larger gaps.
- Use the `time('--|')` helper to derive durations (e.g., to feed a debounce) instead of hard-coding frame counts.

# Summary

- The **`TestScheduler`** runs streams on a **virtual clock**, making time-based tests instant and deterministic.
- Do everything inside **`run()`**, which provides `cold`, `hot`, `expectObservable`, `expectSubscriptions`, `time` and auto-virtualises async schedulers.
- **Marble syntax:** `-` (frame), letters (values), `|` (complete), `#` (error), `()` (same-frame group), `^`/`!` (subscribe/unsubscribe).
- Inside `run()`, **1 frame = 1 ms**; use time-progression syntax for larger delays.
- Assert emissions with **`expectObservable(...).toBe(marble, values)`** and subscriptions with **`expectSubscriptions`**.
- Provide an **assertion function** to the constructor and a **values map** for non-trivial values.

# Flash Cards

Q: What problem does the TestScheduler solve?
A: It replaces real time with a controllable virtual clock, so time-based operators (debounceTime, delay, interval) can be tested instantly and deterministically instead of with slow, flaky real timers.

Q: Inside `testScheduler.run()`, how much virtual time does one frame represent?
A: 1 millisecond. Each character in a marble diagram advances the clock by one frame (1 ms).

Q: What do the marble symbols `|`, `#`, and `()` mean?
A: `|` is completion, `#` is an error, and `()` groups emissions that occur in the same frame (e.g. `(ab|)`).

Q: Which helper asserts a stream's emissions, and how do you map marble letters to real values?
A: `expectObservable(obs).toBe(marble, values)` — the `values` object maps letters to real values, e.g. `{ a: 1, b: 2 }`.

Q: How do you assert that an operator unsubscribed from a source at a certain time?
A: Use `expectSubscriptions(source.subscriptions).toBe('^---!')`, where `^` is the subscribe frame and `!` is the unsubscribe frame.

Q: Why must marble helpers like `cold` and `expectObservable` be used inside `run()`?
A: `run()` supplies those helpers and virtualises the schedulers; outside it they aren't available and time wouldn't be controlled.

# Exercises

### Easy

Set up a `TestScheduler` with your test runner's deep-equal. Inside `run()`, create `cold('-a-b|', { a: 1, b: 2 })`, pipe it through `map(x => x + 1)`, and assert with `expectObservable(...).toBe('-a-b|', { a: 2, b: 3 })`.

### Medium

Test `filter(x => x % 2 === 0)` on `cold('-a-b-c-d|', { a: 1, b: 2, c: 3, d: 4 })`. Write the expected marble that keeps only `b` and `d` at their original frames (odd values become gaps). Verify it passes.

### Challenging

Test that `debounceTime(3)` on `cold('a-b-c-------|')` emits only `c` after the quiet gap. Compute the expected emission frame and write the marble. Then, using `expectSubscriptions`, assert the source's subscription window. Explain in a comment how virtual time made this deterministic.

# Further Reading

- [RxJS — Testing (Marble testing)](https://rxjs.dev/guide/testing/marble-testing)
- [RxJS — TestScheduler](https://rxjs.dev/api/testing/TestScheduler)
- [RxJS — Testing internals](https://rxjs.dev/guide/testing/internal-test)
- [ReactiveX — Testing operators](https://rxjs.dev/guide/testing/marble-testing#api)
