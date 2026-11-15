---
slug: debounce-time
title: "Waiting for a pause with debounceTime()"
topic: rate-limiting-time
kind: api
summary: "debounceTime(dueTime) waits for dueTime ms of silence after each source emission before forwarding the latest value; new emissions reset the timer."
tags: [rxjs, debouncetime, rate-limiting, time, search]
related: [throttle-time, debounce-vs-throttle-choice, typeahead-search-pattern]
---
> **In one line:** `debounceTime(ms)` waits for `ms` milliseconds of **complete silence** from the source and then emits only the most recent value.

## The idea

When a user types `"rxjs"` into a search input, the browser fires four `input` events within a couple hundred milliseconds (`"r"`, `"rx"`, `"rxj"`, `"rxjs"`). Firing an HTTP request on every keystroke wastes bandwidth and hammers your server for intermediate strings the user wasn't done typing.

**`debounceTime(dueTime)`** solves this by acting like an elevator door that resets its close timer every time someone walks in:

1. When a value arrives from the source, `debounceTime` holds onto it and starts a `dueTime`-ms timer.
2. If **another** value arrives before the timer expires, the previous held value is **discarded** and the timer restarts from zero for the new value.
3. Only when `dueTime` ms pass with **no new emissions** does `debounceTime` emit the held value downstream.
4. If the source **completes** while a value is waiting in the timer, `debounceTime` flushes that pending value immediately and then completes.

```sequence
actor user: Search Input | rapid keystrokes
actor db: debounceTime(300) | resets timer on each key @primary
actor out: Subscriber | receives settled query @green
user -> db: "r" (starts 300ms timer)
user -> db: "rx" after 80ms (drops "r", restarts timer)
user -> db: "rxjs" after 90ms (drops "rx", restarts timer)
db --> out: 300ms of silence passes -> emits "rxjs"
```

## See it

Here we simulate rapid keystrokes at 0ms, 100ms, and 200ms, followed by a pause and one more keystroke at 700ms:

```typescript
import { Subject, debounceTime } from 'rxjs';

const keystrokes$ = new Subject<string>();

keystrokes$
  .pipe(debounceTime(300))
  .subscribe((query) => console.log('Search API call for:', query));

keystrokes$.next('r');    // t = 0ms   (timer starts)
setTimeout(() => keystrokes$.next('rx'), 100);   // t = 100ms (resets timer)
setTimeout(() => keystrokes$.next('rxjs'), 200); // t = 200ms (resets timer)
// At t = 500ms (300ms after 'rxjs'), logs: "Search API call for: rxjs"
```

## Go deeper

What if a source emits continuously every 100ms forever (like `interval(100)`) and you pipe it through `debounceTime(300)`? Because 300ms of silence **never** happens, `debounceTime(300)` will stay **completely silent forever**! That is called **debounce starvation**, and it is the main reason you should not use `debounceTime` for continuous pointer-move or scroll streams where you want periodic updates *during* the motion.

> **Tip:** When the quiet period depends on the value itself, `debounce(value => timer(ms))` lets you return a custom duration Observable per emission.

## Watch out

- Do not use `debounceTime(300)` if the source emits faster than every 300ms without ever pausing — the timer will reset endlessly and emit nothing until the burst stops.
- Always place `debounceTime` **before** `switchMap` in a search pipeline so you debounce the keystrokes, not the HTTP responses.

## Remember

- `debounceTime(ms)` emits the latest value only after `ms` milliseconds of silence.
- Every new emission before the timer expires drops the previous pending value and restarts the clock.
- If the source completes while a value is pending, that final value is flushed before `complete`.
