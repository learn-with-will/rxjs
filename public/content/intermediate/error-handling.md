---
id: lesson-14
slug: error-handling
title: "Error Handling: catchError, retry, and throwError"
level: intermediate
order: 14
duration: 28
tags:
  - rxjs
  - catcherror
  - retry
  - throwerror
  - error-handling
---

# Learning Objectives

By the end of this lesson you will be able to:

- Explain why an error is a **terminal** notification that ends the stream.
- Recover from errors with `catchError` by returning a new Observable.
- Retry transient failures with `retry`, including **exponential backoff**.
- Raise your own errors with `throwError(() => …)`.
- Catch errors **inside** a flattening operator to keep an outer stream alive.
- Avoid the deprecated `retryWhen` in favour of `retry({ delay })`.

# Why This Concept Exists

In RxJS an error doesn't just get logged — it **terminates the stream**. One failed HTTP call inside a search pipeline can silently kill the whole search box forever. So error handling isn't optional garnish; it's structural. You need deliberate tools to **recover** (substitute a fallback), **retry** (try again, maybe with backoff), and **contain** errors so a single failure doesn't take down an entire stream.

# Concept Explanation

### Errors are terminal

When any operator callback throws, the producer calls `error`, or you use `throwError`, the stream sends an **`error`** notification and stops. No further `next` or `complete` follows, and the subscription is torn down. This is the contract from Lesson 4: `next* (error | complete)?`.

```ts
import { of, map } from 'rxjs';

of(1, 2, 0, 4)
  .pipe(map((n) => 10 / n), map((r) => (isFinite(r) ? r : (() => { throw new Error('÷0'); })())))
  .subscribe({ next: console.log, error: (e) => console.log('stopped:', e.message) });
// 10, 5, then "stopped: ÷0" — the 4 never gets processed
```

### `catchError` — substitute a new Observable

`catchError((err, caught) => Observable)` intercepts an error from **above** it in the pipe and **returns a new Observable** to continue with. What you return decides what happens next:

```ts
import { of, throwError, EMPTY, catchError } from 'rxjs';

// 1. Fallback value, then complete:
source$.pipe(catchError(() => of('fallback')));

// 2. Swallow the error and complete with nothing:
source$.pipe(catchError(() => EMPTY));

// 3. Rethrow (optionally transformed):
source$.pipe(catchError((err) => throwError(() => new Error(`wrapped: ${err.message}`))));
```

After `catchError` returns an Observable, the outer stream continues with **that** Observable's notifications; the original source is finished.

### `throwError` — create an erroring Observable

`throwError(() => new Error('boom'))` produces an Observable that emits **no** values and errors immediately. In RxJS 7 you pass an **error factory** (a function returning the error).

### `retry` — resubscribe on error

`retry(count)` catches an error and **resubscribes to the source** up to `count` times before giving up and forwarding the error. Because it resubscribes the (cold) source, it **re-runs** the producer — re-firing an HTTP request, for example.

```ts
import { retry } from 'rxjs';
import { ajax } from 'rxjs/ajax';

ajax.getJSON('/api/flaky').pipe(retry(2)); // up to 3 attempts total
```

### Retry with delay / exponential backoff

RxJS 7.5+ accepts a config object. `delay` can be milliseconds, or a **function** returning a notifier Observable — the pattern for exponential backoff:

```ts
import { timer, retry } from 'rxjs';
import { ajax } from 'rxjs/ajax';

ajax.getJSON('/api/flaky').pipe(
  retry({
    count: 3,
    delay: (error, retryCount) => timer(retryCount * 1000), // 1s, 2s, 3s
  }),
);
```

> `retryWhen` is **deprecated** in RxJS 7.8. Use `retry({ delay })` for custom retry timing instead.

### Contain errors inside flattening operators

This is the pattern that trips people up. If you put `catchError` **outside** a `switchMap`, the first inner error kills the whole outer stream (your search box dies). Put `catchError` **inside** the inner Observable so only that one request is recovered and the outer stream keeps flowing.

```ts
import { fromEvent, map, debounceTime, switchMap, catchError, of } from 'rxjs';
import { ajax } from 'rxjs/ajax';

search$.pipe(
  debounceTime(300),
  switchMap((term) =>
    ajax.getJSON(`/api/search?q=${term}`).pipe(
      catchError(() => of([])), // recover THIS request; outer stream survives
    ),
  ),
);
```

# Key Terminology

- **Terminal error** — an `error` notification that ends the stream permanently.
- **`catchError`** — replaces an errored stream with a new Observable (fallback, rethrow, or EMPTY).
- **`throwError(() => err)`** — an Observable that errors immediately (factory form in RxJS 7).
- **`retry(count | config)`** — resubscribes to the source on error, up to `count` times, optionally with a delay.
- **Backoff** — increasing the wait between retries (e.g., 1s, 2s, 4s).
- **Idempotent** — an operation safe to repeat; important when deciding whether to `retry`.

# Syntax Overview

```ts
import { of, throwError, EMPTY, catchError, retry, timer } from 'rxjs';

source$.pipe(catchError(() => of('fallback')));                 // recover with a value
source$.pipe(catchError(() => EMPTY));                          // swallow, complete
source$.pipe(catchError((e) => throwError(() => e)));           // rethrow
source$.pipe(retry(3));                                          // up to 3 more attempts
source$.pipe(retry({ count: 5, delay: (e, n) => timer(2 ** n * 100) })); // exp. backoff
throwError(() => new Error('boom'));                            // erroring Observable
```

# Real World Analogy

Think of a **delivery driver** hitting a locked gate (an error). `retry` is the driver circling back and trying again a few times — with backoff, they wait a little longer before each re-attempt so they don't hammer a gate that's clearly stuck. `catchError` is the fallback plan: leave the parcel at a neighbour's (`of(fallback)`), or quietly return it to the depot and move on (`EMPTY`), or escalate the problem to dispatch (`throwError`). And catching **inside** a `switchMap` is giving each individual delivery its own contingency plan, so one bad address doesn't make the driver abandon the entire route.

# Examples

## Example 1 — Basic: fallback with catchError

```ts
import { throwError, of, catchError } from 'rxjs';

const risky$ = throwError(() => new Error('network down'));

risky$
  .pipe(catchError((err) => of(`using cache (${err.message})`)))
  .subscribe({ next: console.log, complete: () => console.log('done') });
// using cache (network down) / done
```

**Why this is good:** It shows the core recovery move — turn an error into a graceful fallback value and a clean completion, so downstream consumers never see the failure.

## Example 2 — Real-world: retry with backoff, then fall back

```ts
import { timer, retry, catchError, of } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const data$ = ajax.getJSON('/api/report').pipe(
  retry({ count: 3, delay: (_, n) => timer(2 ** n * 500) }), // 1s, 2s, 4s
  catchError(() => of({ items: [], stale: true })),          // give up gracefully
);

data$.subscribe((report) => console.log(report));
```

**Why this is good:** Transient failures (a blip, a rate limit) get a few backed-off retries; if they all fail, `catchError` returns a safe, marked-stale fallback instead of crashing the stream — a robust real-world pattern.

## Example 3 — Best practice: contain errors inside switchMap

```ts
import { fromEvent, map, debounceTime, distinctUntilChanged, switchMap, catchError, of } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const input = document.querySelector<HTMLInputElement>('#search')!;

fromEvent(input, 'input')
  .pipe(
    map((e) => (e.target as HTMLInputElement).value.trim()),
    debounceTime(300),
    distinctUntilChanged(),
    switchMap((term) =>
      ajax.getJSON(`/api/search?q=${encodeURIComponent(term)}`).pipe(
        catchError(() => of([])), // one failed request -> empty results, stream lives on
      ),
    ),
  )
  .subscribe((results) => render(results));

function render(r: unknown) {}
```

**Why this is good:** The `catchError` is **inside** the `switchMap`, so a single failed request yields empty results but the outer search stream keeps working. Move that `catchError` outside and the first error would permanently kill the search box.

# Common Mistakes

- **Letting an error kill a long-lived stream.** An unhandled error on a search/UI stream ends it forever. **Fix:** `catchError` (usually inside a flattening operator) to recover.
- **`catchError` outside `switchMap`/`mergeMap`.** It catches once, then the outer stream is dead. **Fix:** put `catchError` on the **inner** Observable to isolate failures.
- **Returning a non-Observable from `catchError`.** The handler must return an Observable. **Fix:** return `of(fallback)`, `EMPTY`, or `throwError(() => …)`.
- **Using deprecated `retryWhen`.** **Fix:** use `retry({ count, delay })` with a notifier for backoff.
- **Blindly retrying non-idempotent requests.** Retrying a `POST` may double-charge a card. **Fix:** only retry idempotent operations, or make them idempotent.

# Best Practices

- Recover **inside** flattening operators so one failure doesn't end the outer stream.
- Use `retry` with a **max count** and **backoff** for transient errors; never retry infinitely by default.
- Return a meaningful fallback (`of(cachedValue)`, `of([])`) or `EMPTY` to keep pipelines alive.
- Rethrow with `throwError(() => …)` when an error is genuinely unrecoverable, and let a boundary handle it.
- Only retry **idempotent** operations; be cautious with writes/payments.
- Put unavoidable cleanup in `finalize` (next lesson), which runs on error too.

# Summary

- An **error** is terminal: it ends the stream and unsubscribes.
- **`catchError`** replaces the errored stream with a new Observable — a fallback value, `EMPTY`, or a rethrow.
- **`throwError(() => err)`** creates an immediately-erroring Observable (factory form).
- **`retry(count)`** resubscribes to the source on error; `retry({ count, delay })` enables backoff.
- Place `catchError` **inside** `switchMap`/`mergeMap` to keep an outer stream alive after an inner failure.
- **`retryWhen` is deprecated**; use `retry({ delay })`. Only retry **idempotent** operations.

# Flash Cards

Q: What happens to a stream when an operator callback throws?
A: The stream emits a terminal `error` notification and stops — no further next/complete, and the subscription is torn down. Errors must be handled with catchError/retry.

Q: What must a `catchError` handler return, and what are the common choices?
A: It must return an Observable. Common choices: `of(fallback)` to recover with a value, `EMPTY` to swallow and complete, or `throwError(() => err)` to rethrow.

Q: Why place `catchError` INSIDE a `switchMap` for a search box?
A: So a single failed request is recovered without ending the outer stream. Outside the switchMap, the first error would permanently kill the search pipeline.

Q: How do you add exponential backoff to retries in RxJS 7?
A: Use the config form: `retry({ count, delay: (error, retryCount) => timer(...) })`, returning a notifier whose delay grows with retryCount.

Q: Which retry operator is deprecated in RxJS 7.8, and what replaces it?
A: `retryWhen` is deprecated; use `retry({ delay })` with a notifier function for custom retry timing.

Q: Why be careful retrying a POST request?
A: POSTs are often non-idempotent — retrying could duplicate the effect (e.g., double-charge). Only retry idempotent operations, or make them idempotent.

# Exercises

### Easy

Create `throwError(() => new Error('fail'))` and use `catchError` to recover with `of('default value')`. Subscribe with next/error/complete handlers and confirm you get the default value and a completion, never an error.

### Medium

Simulate a flaky source that errors the first two times and succeeds the third (use a counter in `defer`). Apply `retry(3)` and confirm it eventually succeeds. Then add `retry({ count: 3, delay: (_, n) => timer(n * 200) })` and observe the increasing gaps between attempts.

### Challenging

Build a search pipeline (`map → debounceTime → switchMap`) where the request sometimes errors. Put `catchError(() => of([]))` inside the `switchMap` and confirm the search keeps working after a failure. Then move the `catchError` outside the `switchMap`, trigger an error, and demonstrate that the search box stops responding — explaining why in a comment.

# Further Reading

- [RxJS — catchError](https://rxjs.dev/api/index/function/catchError)
- [RxJS — retry](https://rxjs.dev/api/index/function/retry)
- [RxJS — throwError](https://rxjs.dev/api/index/function/throwError)
- [RxJS — Error handling guide](https://rxjs.dev/guide/operators)
