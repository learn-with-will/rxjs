---
slug: non-overlapping-polling-pattern
title: "Smart, non-overlapping HTTP polling with timer and exhaustMap"
topic: patterns-interop
kind: pattern
summary: "Build resilient HTTP polling that never overlaps slow requests and automatically pauses when the browser tab is hidden."
tags: [rxjs, polling, exhaustmap, timer, pattern]
related: [interval-and-timer, exhaust-map, take-until-destroy-pattern]
---
> **In one line:** combine `timer(0, 5000)` (for an immediate first fetch) with `exhaustMap` (to skip ticks while a slow request is still in flight) and `visibilitychange` (to pause polling when the browser tab is in the background).

## The idea

Naive polling with `setInterval` or `interval(5000).pipe(mergeMap(() => fetchStatus$()))` has three production flaws:
1. **No initial emission:** `interval(5000)` waits 5 full seconds before firing the very first request. Using **`timer(0, 5000)`** fires immediately at `0ms` and every `5000ms` thereafter.
2. **Request pile-up on slow networks:** If the server slows down and takes 8 seconds to respond, a 5-second `mergeMap` piles up overlapping requests, while `switchMap` aborts the 8-second request at 5s so *no* request ever finishes! Using **`exhaustMap`** ignores new timer ticks while a request is already in flight.
3. **Wasted battery & server load in background tabs:** Polling while `document.hidden === true` wastes bandwidth. With `fromEvent(document, 'visibilitychange')`, you can automatically pause polling when hidden and immediately resume when the user focuses the tab!

| Flattening operator in `timer(0, 5000)` | What happens if request takes 7s (longer than 5s period)? | Verdict for polling |
|---|---|---|
| **`exhaustMap`** | Ignores the 5s tick; waits for the 7s request to finish, then catches the 10s tick | **Best** — zero overlap, never starves slow responses |
| **`switchMap`** | Aborts the in-flight request at 5s and starts a new one (which also gets aborted at 10s!) | **Broken** on slow networks (livelock) |
| **`mergeMap`** | Runs multiple concurrent requests in parallel | **Dangerous** — stacks up requests against a struggling server |

## See it

Build a visibility-aware, non-overlapping HTTP poller that pauses when the browser tab is hidden and resumes immediately when visible:

```typescript
import { timer, fromEvent, startWith, map, distinctUntilChanged, switchMap, exhaustMap, catchError, EMPTY } from 'rxjs';
import { fromFetch } from 'rxjs/fetch';

const isTabVisible$ = fromEvent(document, 'visibilitychange').pipe(
  startWith(null),
  map(() => document.visibilityState === 'visible'),
  distinctUntilChanged()
);

export const liveDashboard$ = isTabVisible$.pipe(
  // Outer switchMap starts timer(0, 5000) when visible, and cancels it when hidden!
  switchMap(visible =>
    visible
      ? timer(0, 5000).pipe(
          exhaustMap(() =>
            fromFetch('/api/metrics', { selector: r => r.json() }).pipe(
              catchError(() => EMPTY) // Keep timer alive on transient errors!
            )
          )
        )
      : EMPTY
  )
);
```

## Go deeper

Notice how the outer `isTabVisible$` uses `switchMap`, while the inner `timer(0, 5000)` uses `exhaustMap`!
- When `visible` becomes `false`, the outer `switchMap` switches to `EMPTY`, immediately unsubscribing from `timer(0, 5000)` and aborting any in-flight fetch.
- When `visible` becomes `true` again, `switchMap` subscribes to a fresh `timer(0, 5000)`, firing an immediate `0ms` refresh and resuming the 5-second non-overlapping `exhaustMap` loop.

## Watch out

- Always catch errors inside `exhaustMap` and return `EMPTY` (or a fallback value) so a single transient 502 error doesn't kill the `timer(0, 5000)` loop.
- Remember to pipe `takeUntil(destroy$)` when subscribing to `liveDashboard$` in a component.

## Remember

- Use `timer(0, periodMs)` instead of `interval(periodMs)` so polling starts immediately at `0ms`.
- Use `exhaustMap` for the HTTP call so slow responses are neither aborted nor overlapped.
- Gate the polling timer with `visibilitychange` + outer `switchMap` to pause background tabs automatically.
