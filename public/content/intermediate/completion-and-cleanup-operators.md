---
id: lesson-15
slug: completion-and-cleanup-operators
title: "Completion and Cleanup: finalize, takeUntil, and timeout"
level: intermediate
order: 15
duration: 24
tags:
  - rxjs
  - takeuntil
  - finalize
  - timeout
  - cleanup
  - completion
---

# Learning Objectives

By the end of this lesson you will be able to:

- Complete a stream on a signal with `takeUntil`.
- Run guaranteed cleanup on complete, error, **and** unsubscribe with `finalize`.
- Guard slow sources with `timeout`, erroring or falling back.
- Handle empty streams with `defaultIfEmpty` and `throwIfEmpty`.
- Apply the `takeUntil(destroy$)` lifecycle pattern and know why `takeUntil` goes last.

# Why This Concept Exists

Streams need to **end** and to **clean up** — reliably, on every exit path. A component unmounts and its subscriptions must stop. A spinner must hide whether the request succeeded, failed, or was cancelled. A network call that hangs must not wait forever. These operators give you declarative control over *when a stream ends* and *what runs on the way out* — the difference between a tidy app and one that leaks subscriptions and spins forever.

# Concept Explanation

### `takeUntil` — complete when a notifier fires

`takeUntil(notifier$)` mirrors the source until `notifier$` emits its **first** value, at which point it **completes** the source (and unsubscribes).

```text
source:    --1--2--3--4-->
notifier:  --------X
takeUntil: --1--2--|        (completes when the notifier emits)
```

This is the backbone of declarative teardown: keep a subject that fires once when you want everything to stop.

```ts
import { interval, takeUntil, Subject } from 'rxjs';

const destroy$ = new Subject<void>();
interval(1000).pipe(takeUntil(destroy$)).subscribe(console.log);

// Later, on teardown:
destroy$.next();     // completes the interval stream
destroy$.complete();
```

### `finalize` — a guaranteed "finally"

`finalize(callback)` runs its callback exactly once when the stream **completes, errors, or is unsubscribed**. It's the RxJS `finally` — the place for cleanup that must always happen.

```ts
import { finalize } from 'rxjs';

source$.pipe(finalize(() => console.log('cleanup: hide spinner')));
// The callback fires whether the stream completed, errored, or was cancelled.
```

### `timeout` — fail fast on slowness

`timeout` errors with a `TimeoutError` if the source is too slow. RxJS 7 uses a config object:

- `timeout({ first: ms })` — allow `ms` for the **first** emission.
- `timeout({ each: ms })` — allow `ms` **between** emissions.
- `timeout({ each: ms, with: () => fallback$ })` — switch to a fallback Observable instead of erroring.

```ts
import { timeout, of } from 'rxjs';
import { ajax } from 'rxjs/ajax';

ajax.getJSON('/api/slow').pipe(timeout({ each: 5000 })); // TimeoutError after 5s of silence
ajax.getJSON('/api/slow').pipe(timeout({ each: 5000, with: () => of({ stale: true }) })); // fallback
```

> The old `timeoutWith` is deprecated — use `timeout({ with })`.

### `defaultIfEmpty` and `throwIfEmpty`

If a source **completes without emitting**, you often need to react:

- `defaultIfEmpty(value)` — emit `value` if the source was empty, then complete.
- `throwIfEmpty(errorFactory?)` — error if the source was empty.

```ts
import { EMPTY, defaultIfEmpty, throwIfEmpty } from 'rxjs';

EMPTY.pipe(defaultIfEmpty('none')).subscribe(console.log);  // 'none'
EMPTY.pipe(throwIfEmpty(() => new Error('no data'))).subscribe({ error: (e) => console.log(e.message) });
```

### Why `takeUntil` goes last

Place `takeUntil` as the **last** operator in a pipe. If operators that keep inner subscriptions (like `combineLatest` or `mergeMap`) sit *after* `takeUntil`, those inner subscriptions can outlive the notifier and leak. Putting `takeUntil` last ensures it governs the entire chain's teardown.

# Key Terminology

- **`takeUntil(notifier$)`** — completes the source when the notifier first emits.
- **`finalize(fn)`** — runs `fn` once on complete, error, or unsubscribe (the RxJS `finally`).
- **`timeout(config)`** — errors (or falls back) when the source is too slow.
- **`TimeoutError`** — the error `timeout` throws when the deadline passes.
- **`defaultIfEmpty(value)`** — emits `value` if the source completed empty.
- **`throwIfEmpty(factory?)`** — errors if the source completed empty.
- **`destroy$`** — a conventional Subject fired once to trigger `takeUntil` teardown.

# Syntax Overview

```ts
import { interval, of, Subject, takeUntil, finalize, timeout, defaultIfEmpty } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const destroy$ = new Subject<void>();

interval(1000).pipe(
  takeUntil(destroy$),                 // stop when destroy$ fires
);

ajax.getJSON('/api/data').pipe(
  timeout({ each: 8000 }),             // guard against a hung request
  finalize(() => hideSpinner()),       // always runs (success/error/cancel)
);

search$.pipe(defaultIfEmpty([]));      // never leave downstream without a value

function hideSpinner() {}
```

# Real World Analogy

These are the **closing-up routines of a shop**. `takeUntil` is the "we close when the manager rings the bell" rule — trading continues until that single signal, then the shutters come down. `finalize` is the end-of-shift checklist that runs no matter *how* the day ended — normal close, fire alarm, or early lockup: lights off, till counted, spinner hidden. `timeout` is the policy that if a supplier hasn't delivered within the agreed window, you stop waiting and either raise a complaint (error) or use the backup supplier (`with`). And `defaultIfEmpty` is stocking a "sorry, sold out" card so that even an empty shelf still shows the customer *something*.

# Examples

## Example 1 — Basic: stop a stream with takeUntil

```ts
import { interval, timer, takeUntil, map } from 'rxjs';

const stop$ = timer(3000); // fires once after 3 seconds

interval(500)
  .pipe(
    map((n) => n + 1),
    takeUntil(stop$), // complete when stop$ emits
  )
  .subscribe({ next: console.log, complete: () => console.log('stopped') });
// 1 2 3 4 5 6 stopped   (roughly — stops after ~3s)
```

**Why this is good:** A completely declarative stop condition — no manual `unsubscribe`, no stored Subscription. When `stop$` emits, the interval completes and cleans itself up.

## Example 2 — Real-world: a spinner that always hides with finalize

```ts
import { finalize, catchError, of } from 'rxjs';
import { ajax } from 'rxjs/ajax';

showSpinner();
ajax
  .getJSON('/api/report')
  .pipe(
    catchError(() => of({ error: true })),
    finalize(() => hideSpinner()), // runs on success, error, OR unsubscribe
  )
  .subscribe((result) => render(result));

function showSpinner() {}
function hideSpinner() {}
function render(r: unknown) {}
```

**Why this is good:** The spinner is guaranteed to hide on every exit path. Unlike putting `hideSpinner()` only in `next` or `complete`, `finalize` also covers the error and cancellation cases — no stuck spinners.

## Example 3 — Best practice: the takeUntil(destroy$) lifecycle pattern

```ts
import { fromEvent, interval, Subject, takeUntil, timeout, catchError, of } from 'rxjs';
import { ajax } from 'rxjs/ajax';

class Widget {
  private readonly destroy$ = new Subject<void>();

  init() {
    // Every long-lived subscription ends with takeUntil(this.destroy$), placed LAST.
    interval(1000).pipe(takeUntil(this.destroy$)).subscribe((n) => this.tick(n));

    ajax
      .getJSON('/api/data')
      .pipe(
        timeout({ each: 8000 }),
        catchError(() => of(null)),
        takeUntil(this.destroy$), // last operator
      )
      .subscribe((data) => this.render(data));
  }

  destroy() {
    this.destroy$.next();     // completes every subscription at once
    this.destroy$.complete();
  }

  tick(n: number) {}
  render(d: unknown) {}
}
```

**Why this is good:** One `destroy$` Subject tears down every subscription in the component with a single call — the standard, scalable pattern for lifecycle cleanup. `takeUntil` is placed **last** so it governs each entire pipeline.

# Common Mistakes

- **Placing `takeUntil` not last.** Operators after it (e.g., `mergeMap`, `combineLatest`) can keep inner subscriptions alive past the notifier. **Fix:** make `takeUntil` the final operator.
- **Assuming `finalize` runs only on completion.** It also runs on **error** and **unsubscribe**. **Fix:** rely on that — it's exactly why `finalize` is right for spinners and resource release.
- **A `notifier$` that never emits.** `takeUntil` then never completes the source. **Fix:** ensure the notifier emits (e.g., `destroy$.next()` in teardown).
- **Misreading `timeout` config.** `first` guards the first emission; `each` guards the gap between emissions. **Fix:** pick the field that matches your concern.
- **Ignoring empty streams.** Downstream code may expect a value that never comes. **Fix:** `defaultIfEmpty` (or `throwIfEmpty`) to make the empty case explicit.

# Best Practices

- Adopt **`takeUntil(destroy$)`** for component/service lifecycles, and place it **last** in every pipe.
- Use **`finalize`** for cleanup that must always run (hide spinner, close connection, release lock).
- Guard network calls with **`timeout`**, choosing `error` (default) or a `with` fallback deliberately.
- Make empty-completion explicit with **`defaultIfEmpty`** so downstream always receives something.
- Complete your `destroy$`/notifier Subjects after firing them to release their own subscribers.

# Summary

- **`takeUntil(notifier$)`** completes the source when the notifier first emits — the core teardown tool.
- **`finalize(fn)`** runs cleanup exactly once on **complete, error, or unsubscribe**.
- **`timeout`** errors with a `TimeoutError` (or falls back via `with`) when a source is too slow; `first` vs `each` choose the deadline type.
- **`defaultIfEmpty`/`throwIfEmpty`** handle sources that complete without emitting.
- The **`takeUntil(destroy$)`** pattern tears down many subscriptions with one signal.
- Put **`takeUntil` last** so it governs the whole pipeline and avoids leaks.

# Flash Cards

Q: What does `takeUntil(notifier$)` do?
A: It mirrors the source until the notifier emits its first value, then completes the source and unsubscribes — a declarative way to stop a stream.

Q: On which events does `finalize` run its callback?
A: On complete, error, AND unsubscribe — it's the RxJS `finally`, ideal for guaranteed cleanup like hiding a spinner.

Q: Why should `takeUntil` be the last operator in a pipe?
A: So it governs the entire chain's teardown. Operators after it (like mergeMap or combineLatest) could keep inner subscriptions alive past the notifier and leak.

Q: What is the difference between `timeout({ first })` and `timeout({ each })`?
A: `first` sets the deadline for the first emission; `each` sets the maximum allowed gap between consecutive emissions.

Q: How do you avoid erroring when a `timeout` deadline passes?
A: Provide a `with` factory returning a fallback Observable: `timeout({ each: ms, with: () => fallback$ })`.

Q: What does `defaultIfEmpty('none')` do to a source that completes without emitting?
A: It emits `'none'` and then completes, so downstream always receives a value even when the source was empty.

# Exercises

### Easy

Start an `interval(300)` and complete it with `takeUntil(timer(2000))`. Log each value and a `"done"` message on completion. Confirm it stops after about 2 seconds without any manual unsubscribe.

### Medium

Wrap a fake request `of('data').pipe(delay(400))` with a `finalize(() => console.log('cleanup'))` and a `catchError`. Subscribe, then in a separate run make the source error, and confirm `"cleanup"` logs in both the success and error cases. Then unsubscribe early and confirm it logs on cancellation too.

### Challenging

Build a `destroy$` Subject and two long-lived subscriptions (an `interval` and a `fromEvent` click) that each end with `takeUntil(destroy$)` placed last. Add a `timeout({ each: 1000, with: () => of('fallback') })` to a slow source. Call `destroy$.next()` and confirm all subscriptions complete together. Explain in a comment why moving `takeUntil` before `mergeMap` could leak.

# Further Reading

- [RxJS — takeUntil](https://rxjs.dev/api/index/function/takeUntil)
- [RxJS — finalize](https://rxjs.dev/api/index/function/finalize)
- [RxJS — timeout](https://rxjs.dev/api/index/function/timeout)
- [RxJS — defaultIfEmpty](https://rxjs.dev/api/index/function/defaultIfEmpty) and [throwIfEmpty](https://rxjs.dev/api/index/function/throwIfEmpty)
