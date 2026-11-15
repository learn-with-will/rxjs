---
slug: debounce-vs-throttle-choice
title: "Choosing a rate-limiting operator"
topic: rate-limiting-time
kind: concept
summary: "Match each UI timing problem to the right operator: debounceTime for pauses, throttleTime for cooldowns, auditTime for trailing bursts, and sampleTime."
tags: [rxjs, debouncetime, throttletime, audittime, sampletime]
related: [debounce-time, throttle-time, audit-time-and-sample-time, distinct-until-changed]
---
> **In one line:** ask **when** you want values during a burst: **only after silence** (`debounceTime`), **immediately then cooldown** (`throttleTime`), **at the end of a cooldown window** (`auditTime`), or **on a fixed clock** (`sampleTime`).

## The idea

The four time-based rate limiters all thin out noisy streams, but they answer different UI timing questions:

```tree
Which rate-limiting operator fits your UI event?
  Wait until the user stops for N ms
    [debounceTime(N)] Typeahead search, auto-save draft, resize finished @green
  React immediately on the first event, then ignore for N ms
    [throttleTime(N)] Prevent double-click submit, rate-limit button spam @blue
  Give steady updates during continuous motion + final value
    [throttleTime(N, s, { leading: true, trailing: true })] Scroll progress, drag slider @primary
  Wait N ms after burst starts, then emit the latest value
    [auditTime(N)] Batch rapid state changes into one trailing update @amber
  Sample on a fixed periodic clock every N ms
    [sampleTime(N)] Telemetry dashboard, game loop position snapshot @neutral
```

| Scenario | Best operator | Why |
|---|---|---|
| Search input autocomplete | `debounceTime(300)` + `distinctUntilChanged()` | Wait until typing pauses and text actually changed |
| "Pay Now" button click | `throttleTime(1000)` | Fire on the very first click immediately; ignore frantic extra clicks |
| Infinite-scroll / sticky header | `throttleTime(100, asyncScheduler, { leading: true, trailing: true })` | Update smoothly while scrolling and never miss the final stop position |
| Live stock ticker UI | `sampleTime(500)` | Refresh the screen at a steady 2 Hz cadence |

## See it

Here is the classic search-input front end combining `map`, `debounceTime`, and `distinctUntilChanged` in the right order:

```typescript
import { fromEvent, map, debounceTime, distinctUntilChanged } from 'rxjs';

const searchInput = document.querySelector<HTMLInputElement>('#search')!;

const cleanSearch$ = fromEvent(searchInput, 'input').pipe(
  map((e) => (e.target as HTMLInputElement).value.trim()),
  debounceTime(300),        // 1. Wait for 300ms pause in typing
  distinctUntilChanged(),   // 2. Skip if trimmed text equals the last emitted query
);

cleanSearch$.subscribe((query) => console.log('Trigger search for:', query));
```

## Go deeper

Why should `debounceTime(300)` go **before** `distinctUntilChanged()` in that search pipeline? Suppose the query is currently `"rxjs"`, and the user types `"rxjs!"` and immediately presses Backspace within 100ms so the box reads `"rxjs"` again:

- If `debounceTime(300)` is **first**, it waits 300ms, sees `"rxjs"`, and passes it to `distinctUntilChanged()`, which compares `"rxjs" === "rxjs"` and emits **nothing**!
- If `distinctUntilChanged()` were **first**, it would see `"rxjs!"` (different) and then `"rxjs"` (different from `"rxjs!"`), so after 300ms `"rxjs"` would be emitted a second time!

Similarly, for window scroll or drag handlers, remember that you can combine `throttleTime(100, asyncScheduler, { leading: true, trailing: true })` with `distinctUntilChanged()` so that if the leading and trailing scroll positions happen to be the same pixel offset, the duplicate trailing emission is cleanly skipped.

## Watch out

- Put `debounceTime` **before** `distinctUntilChanged` so rapid type-and-backspace edits that return to the previous value are suppressed.
- Do not use `debounceTime` on `scroll` or `mousemove` if the UI should visually track the movement while it happens — use `throttleTime` or `auditTime` instead.

## Remember

- Use `debounceTime` when you only want the final settled value after a pause.
- Use `throttleTime` when you want immediate reaction on the leading edge and bounded frequency during continuous events.
- Pair `debounceTime` → `distinctUntilChanged` (in that order) for user text inputs.
