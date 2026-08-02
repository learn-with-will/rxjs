---
id: lesson-08
slug: time-based-operators
title: "Rate-Limiting Time: debounceTime, throttleTime, and distinctUntilChanged"
level: intermediate
order: 08
duration: 26
tags:
  - rxjs
  - debouncetime
  - throttletime
  - distinctuntilchanged
  - time
  - operators
---

# Learning Objectives

By the end of this lesson you will be able to:

- Wait for a stream to go quiet before reacting, using `debounceTime`.
- Rate-limit a busy stream with `throttleTime`, and know its leading-edge default.
- Emit the latest value per window with `auditTime` and sample periodically with `sampleTime`.
- Drop consecutive duplicates with `distinctUntilChanged` (and by key).
- Choose the right tool for typeahead, scroll, resize, and button-spam problems.
- Avoid the classic object-reference pitfall with `distinctUntilChanged`.

# Why This Concept Exists

Some streams are *too* chatty. A text input fires on every keystroke; `scroll`, `resize`, and `mousemove` fire dozens of times a second. Reacting to each event — firing an API call, recomputing layout — is wasteful and janky. Time-based operators throttle that firehose down to something reasonable: react only when the user pauses, or at most once per interval, or only when the value actually changed.

# Concept Explanation

### `debounceTime` — wait for silence

`debounceTime(dueTime)` holds each value and only emits it once `dueTime` milliseconds have passed **with no new emission**. If another value arrives first, the timer resets and the earlier value is dropped. You always get the **latest** value of a burst, after things settle.

```text
source:            a-b-c------d---|
debounceTime(3):   --------c------(d|)   (emits the last of each quiet-terminated burst)
```

This is the classic "the user stopped typing" detector.

### `throttleTime` — at most once per window

`throttleTime(duration)` emits a value, then **ignores** further values for `duration` ms, then allows the next one. By default it emits on the **leading edge** (`{ leading: true, trailing: false }`) — the first value of each window, not the last.

```text
source:            a-b-c-d-e-f-g|
throttleTime(3):   a---d---g----|   (first of each window; middles dropped)
```

You can opt into a trailing emission with the config object: `throttleTime(300, asyncScheduler, { leading: true, trailing: true })`.

### `auditTime` and `sampleTime` — emit the latest, differently timed

- **`auditTime(duration)`** — when a value arrives it starts a timer, and when the timer fires it emits the **most recent** value (trailing edge). Good for "act on the latest, but no more than once per window."
- **`sampleTime(period)`** — on a fixed clock every `period` ms, emit the **most recent** value seen (or nothing if none arrived). Good for periodic snapshots of a fast stream.

### `distinctUntilChanged` — skip consecutive repeats

`distinctUntilChanged()` emits a value only when it **differs from the previously emitted value**, using `===` by default. It drops *consecutive* duplicates (not all duplicates ever seen).

```ts
import { of, distinctUntilChanged } from 'rxjs';

of(1, 1, 2, 2, 2, 3, 1)
  .pipe(distinctUntilChanged())
  .subscribe(console.log); // 1, 2, 3, 1  (note the final 1 is emitted — not consecutive)
```

For objects, pass a comparator or use `distinctUntilKeyChanged`:

```ts
import { of, distinctUntilKeyChanged } from 'rxjs';

of({ id: 1 }, { id: 1 }, { id: 2 })
  .pipe(distinctUntilKeyChanged('id'))
  .subscribe((v) => console.log(v.id)); // 1, 2
```

> `distinct()` (no "UntilChanged") drops **all** values ever seen by keeping them in a Set — which can grow unbounded on long streams. Prefer `distinctUntilChanged` unless you truly need global de-duplication.

# Key Terminology

- **`debounceTime(ms)`** — emits the latest value after a quiet gap of `ms`.
- **`throttleTime(ms)`** — emits at most once per `ms` window; leading-edge by default.
- **`auditTime(ms)`** — emits the most recent value at the *end* of each window (trailing edge).
- **`sampleTime(ms)`** — emits the most recent value on a fixed periodic clock.
- **`distinctUntilChanged(compare?)`** — drops values equal to the previously emitted one.
- **`distinctUntilKeyChanged(key)`** — same, comparing a chosen object key.
- **Leading / trailing edge** — whether the first or last value of a window is emitted.

# Syntax Overview

```ts
import { fromEvent, map, debounceTime, throttleTime, auditTime, sampleTime, distinctUntilChanged } from 'rxjs';

const input = document.querySelector<HTMLInputElement>('#q')!;
const text$ = fromEvent(input, 'input').pipe(map((e) => (e.target as HTMLInputElement).value));

text$.pipe(debounceTime(300), distinctUntilChanged()); // typeahead: settle, then only if changed
fromEvent(window, 'scroll').pipe(throttleTime(200));    // at most every 200ms (leading)
fromEvent(window, 'mousemove').pipe(auditTime(100));    // latest position per 100ms window
fromEvent(window, 'mousemove').pipe(sampleTime(1000));  // one sample per second
```

# Real World Analogy

These operators are different ways to handle an **eager crowd at a door**. `debounceTime` is a nervous host who only opens the door once nobody has knocked for a few seconds — if knocks keep coming, they keep waiting. `throttleTime` is a turnstile that admits the first person, then locks for a fixed interval no matter how many push behind. `auditTime` waits out the interval and then admits whoever is *currently* at the front. `sampleTime` is a supervisor who glances at the door on a regular schedule and notes whoever happens to be standing there. And `distinctUntilChanged` is a doorkeeper who only bothers announcing a guest when it's a *different* guest from the one just announced.

# Examples

## Example 1 — Basic: typeahead input (debounce + distinct)

```ts
import { fromEvent, map, debounceTime, distinctUntilChanged } from 'rxjs';

const input = document.querySelector<HTMLInputElement>('#search')!;

fromEvent(input, 'input')
  .pipe(
    map((e) => (e.target as HTMLInputElement).value.trim()),
    debounceTime(300),        // wait until typing pauses
    distinctUntilChanged(),   // ignore if the settled text didn't change
  )
  .subscribe((query) => console.log('search:', query));
```

**Why this is good:** It's the canonical pairing. `debounceTime` avoids firing on every keystroke; `distinctUntilChanged` avoids re-searching when the settled text is identical (e.g., the user typed then deleted back to the same word).

## Example 2 — Real-world: prevent double-submit with throttleTime

```ts
import { fromEvent, throttleTime } from 'rxjs';

const saveBtn = document.querySelector('#save')!;

fromEvent(saveBtn, 'click')
  .pipe(throttleTime(2000)) // ignore extra clicks for 2s after the first
  .subscribe(() => console.log('saving…'));
```

**Why this is good:** `throttleTime`'s leading-edge default fires immediately on the first click (responsive) and then swallows rapid repeat clicks — exactly what you want to stop accidental double submissions.

## Example 3 — Best practice: throttle a scroll handler with auditTime

```ts
import { fromEvent, auditTime, map } from 'rxjs';

fromEvent(window, 'scroll')
  .pipe(
    auditTime(100),                       // at most ~10 updates/sec, using the latest
    map(() => window.scrollY),
  )
  .subscribe((y) => updateScrollIndicator(y));

function updateScrollIndicator(y: number) {
  /* cheap DOM update */
}
```

**Why this is good:** Scroll fires far too often to handle each event. `auditTime(100)` guarantees at most one update per 100 ms and, unlike leading-edge throttle, emits the **latest** scroll position at the end of each window — so the indicator lands on where the user actually is.

# Common Mistakes

- **Confusing debounce with throttle.** Debounce waits for a *pause* (great for typing); throttle *rate-limits* (great for scroll/clicks). **Fix:** "settle then act" → debounce; "no more than once per interval" → throttle.
- **`distinctUntilChanged` not de-duping objects.** New object literals are never `===`, so nothing is dropped. **Fix:** map to a primitive first, pass a comparator, or use `distinctUntilKeyChanged`.
- **Assuming `throttleTime` emits the last value of a burst.** By default it emits the **first** (leading edge). **Fix:** enable `{ trailing: true }`, or use `auditTime` for the latest.
- **Reaching for `distinct` when you meant `distinctUntilChanged`.** `distinct` remembers every value forever (unbounded memory). **Fix:** use `distinctUntilChanged` for consecutive de-duplication.
- **Debouncing after an expensive operator.** Put `debounceTime` *before* the API call so you don't start work you'll discard. **Fix:** order it early in the pipe.

# Best Practices

- Standard typeahead recipe: `map → debounceTime(~250–400) → distinctUntilChanged` (then `switchMap` in a later lesson).
- Use `throttleTime`/`auditTime` for high-frequency UI events (`scroll`, `resize`, `mousemove`); pick `auditTime` when you need the latest value.
- Always de-dupe object streams with a comparator or `distinctUntilKeyChanged`, never bare `distinctUntilChanged`.
- Keep durations human: ~300 ms feels instant for search; too long feels laggy.
- Prefer `distinctUntilChanged` over `distinct` unless you genuinely need to remember every prior value.

# Summary

- **`debounceTime`** emits the latest value only after the stream goes quiet — ideal for "done typing".
- **`throttleTime`** emits at most once per window, **leading-edge by default**; enable trailing if you need the last value.
- **`auditTime`** emits the most recent value at the end of each window; **`sampleTime`** emits on a fixed periodic clock.
- **`distinctUntilChanged`** drops consecutive duplicates using `===`; use a comparator or `distinctUntilKeyChanged` for objects.
- **`distinct`** removes all repeats ever seen but keeps them in memory — use sparingly.
- Match the tool to the problem: pause-detection (debounce) vs. rate-limiting (throttle/audit/sample).

# Flash Cards

Q: What is the difference between `debounceTime` and `throttleTime`?
A: `debounceTime` waits for a quiet gap and then emits the latest value (good for "done typing"). `throttleTime` emits at most once per interval (good for rate-limiting scroll/clicks).

Q: By default, does `throttleTime` emit the first or the last value of each window?
A: The first — it is leading-edge by default (`{ leading: true, trailing: false }`). Enable `trailing` (or use `auditTime`) to get the latest value.

Q: Why might `distinctUntilChanged()` fail to drop duplicate objects like `{id:1}, {id:1}`?
A: It compares with `===` by default, and two distinct object literals are never `===`. Use a comparator or `distinctUntilKeyChanged('id')`.

Q: What does `distinctUntilChanged` compare each value against?
A: The previously *emitted* value — so it drops only consecutive duplicates, not every duplicate ever seen.

Q: Which operator emits the most recent value at the END of each time window?
A: `auditTime` (trailing edge). `throttleTime`'s default is the leading edge, and `sampleTime` emits on a fixed periodic clock.

Q: What is the standard typeahead operator recipe (before flattening)?
A: `map` to the text, then `debounceTime(~300)`, then `distinctUntilChanged()` — settle, then only continue if the value changed.

# Exercises

### Easy

Attach to a text input's `input` event, map to the value, and log it through `debounceTime(400)`. Type quickly and confirm only the final settled value logs per burst.

### Medium

Wire a button's `click` through `throttleTime(1000)` and log a timestamp on each emission. Click rapidly and confirm at most one log per second, firing immediately on the first click. Then add `{ trailing: true }` and describe the difference.

### Challenging

Build a stream of `{ id, name }` objects that includes consecutive duplicates by `id`. Show that `distinctUntilChanged()` fails to de-dupe them, then fix it two ways: with `distinctUntilChanged((a, b) => a.id === b.id)` and with `distinctUntilKeyChanged('id')`. Confirm both emit only when the `id` changes.

# Further Reading

- [RxJS — debounceTime](https://rxjs.dev/api/index/function/debounceTime)
- [RxJS — throttleTime](https://rxjs.dev/api/index/function/throttleTime)
- [RxJS — auditTime](https://rxjs.dev/api/index/function/auditTime) and [sampleTime](https://rxjs.dev/api/index/function/sampleTime)
- [RxJS — distinctUntilChanged](https://rxjs.dev/api/index/function/distinctUntilChanged) and [distinctUntilKeyChanged](https://rxjs.dev/api/index/function/distinctUntilKeyChanged)
