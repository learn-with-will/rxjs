---
slug: throttle-time
title: "Capping emission rate with throttleTime()"
topic: rate-limiting-time
kind: api
summary: "throttleTime(duration, scheduler?, config?) emits immediately by default and then silences the stream for duration ms; configure leading and trailing edges."
tags: [rxjs, throttletime, rate-limiting, leading, trailing]
related: [debounce-time, audit-time-and-sample-time, debounce-vs-throttle-choice]
---
> **In one line:** by default, `throttleTime(ms)` emits the **first value immediately** and then ignores all source values for the next `ms` milliseconds.

## The idea

Where `debounceTime` waits for silence *after* a burst, **`throttleTime(duration, scheduler?, config?)`** limits how frequently values can pass through **during** a burst — at most once per `duration` window.

By default, `throttleTime(ms)` uses `{ leading: true, trailing: false }`:

1. When the throttle window is closed and a value arrives, it **emits that value immediately** (the **leading** edge) and opens a cooldown window for `duration` ms.
2. Any values that arrive while that cooldown window is open are **ignored**.
3. Once `duration` ms have elapsed, the window closes and the next incoming value can fire immediately again.

You can customize the third argument `{ leading: boolean, trailing: boolean }` to control whether values fire at the start of the window, the end of the window, or both:

| Config `{ leading, trailing }` | When a burst `A, B, C` arrives inside one `duration` window |
|---|---|
| `{ leading: true, trailing: false }` *(default)* | Emits `A` immediately; drops `B` and `C` |
| `{ leading: false, trailing: true }` | Waits until the window ends, then emits `C` (same as `auditTime`) |
| `{ leading: true, trailing: true }` | Emits `A` immediately **and** emits `C` when the window ends |

## See it

Compare default leading-only throttling with `{ leading: true, trailing: true }` (which guarantees your UI also receives the final scroll position when the user stops):

```typescript
import { fromEvent, throttleTime, asyncScheduler } from 'rxjs';

// 1. Default ({ leading: true, trailing: false }): great for double-click protection
const saveBtn = document.querySelector('#save')!;
fromEvent(saveBtn, 'click')
  .pipe(throttleTime(1000))
  .subscribe(() => console.log('Save clicked (ignores clicks for 1s)'));

// 2. Leading + trailing: emit immediately AND emit the final position after cooldown
fromEvent(window, 'scroll')
  .pipe(
    throttleTime(200, asyncScheduler, { leading: true, trailing: true }),
  )
  .subscribe(() => console.log('scrollY:', window.scrollY));
```

## Go deeper

Notice that to pass the `config` object as the third argument to `throttleTime(200, asyncScheduler, { leading: true, trailing: true })`, you pass `asyncScheduler` (or `undefined`) as the second argument. Why is `{ leading: true, trailing: true }` so important for scroll or resize handlers? With default `trailing: false`, if the user scrolls at 0ms (emitted) and then stops scrolling at 80ms, the 80ms event is dropped inside the 200ms cooldown window — leaving your UI stuck at the 0ms position! Enabling `trailing: true` ensures that final 80ms position is emitted when the 200ms window closes.

## Watch out

- Default `throttleTime(ms)` has `trailing: false`, so the last event in a burst is dropped unless it happens to land after the cooldown window expires.
- Unlike `debounceTime`, `throttleTime` never starves on a continuous high-frequency stream: it guarantees regular emissions every `duration` ms.

## Remember

- Default `throttleTime(ms)` emits on the **leading** edge and ignores values for `ms` milliseconds.
- Pass `{ leading: true, trailing: true }` when you need both immediate feedback and the final value after a burst.
- Use `throttleTime` for button spam protection, scroll listeners, and continuous drag events.
