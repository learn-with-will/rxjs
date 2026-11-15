---
slug: from-fetch-and-abort
title: "Abortable HTTP requests with fromFetch()"
topic: patterns-interop
kind: api
summary: "Wrap the native browser Fetch API in a cold, automatically abortable Observable using fromFetch() from 'rxjs/fetch'."
tags: [rxjs, fromfetch, fetch, abortcontroller, http]
related: [of-and-from, switch-map, typeahead-search-pattern]
---
> **In one line:** `fromFetch(url, { selector })` from `'rxjs/fetch'` wraps the native `fetch()` API in a **cold** Observable and wires up an `AbortController` automatically so unsubscribing (or `switchMap` switching) aborts the underlying network request in the browser.

## The idea

What happens if you write `from(fetch('/api/data'))`? Two bad things:
1. **It is hot and eager:** `fetch('/api/data')` starts the network request immediately when `from(fetch(...))` is called, before anyone subscribes, and `retry()` cannot re-run it.
2. **It cannot be aborted:** A native `Promise` has no teardown hook, so when `switchMap` or `takeUntil` unsubscribes, the browser continues downloading the response in the background.

`fromFetch(input, initWithSelector)` from `'rxjs/fetch'` fixes both problems out of the box:
- It is **cold and lazy**: `fetch()` is only called when a subscriber subscribes (so `retry()` re-issues a fresh HTTP request on every attempt!).
- It creates an **`AbortController`** per subscription and passes `signal` to `fetch()`. The instant downstream unsubscribes (`switchMap`, `takeUntil`, `timeout`), `controller.abort()` cancels the in-flight TCP/HTTP request!
- Its `{ selector: response => ... }` option lets you parse `response.json()` or `response.text()` inside the same operator without an extra `switchMap`.

> **Watch out:** Just like native `fetch()`, `fromFetch()` only errors on **network failures** (DNS, offline, CORS). HTTP `404` or `500` responses still resolve as `next(response)` with `response.ok === false` — so always check `if (!res.ok) throw new Error(...)` inside `selector` or `switchMap`!

## See it

Use `fromFetch` with a `selector` that checks `res.ok` and parses JSON, plus automatic `retry(2)` and `timeout(5000)`:

```typescript
import { retry, timeout, catchError, of, throwError } from 'rxjs';
import { fromFetch } from 'rxjs/fetch';

interface UserProfile { id: number; name: string; }

export function fetchUser$(id: number) {
  return fromFetch<UserProfile>(`/api/users/${id}`, {
    selector: response => {
      if (!response.ok) {
        // Turn 4xx/5xx HTTP status codes into an Observable error notification:
        return throwError(() => new Error(`HTTP ${response.status}`));
      }
      return response.json() as Promise<UserProfile>;
    },
  }).pipe(
    timeout(5000), // Aborts the fetch request if it takes > 5 seconds!
    retry(2),      // Re-runs a fresh fetch() up to 2 times on error!
    catchError(err => {
      console.error('Fetch failed permanently:', err);
      return of(null);
    })
  );
}
```

## Go deeper

Why does `selector` accept a function returning either a `Promise<T>` (like `response.json()`) or an `ObservableInput<T>` (like `throwError(...)`)? Because internally `fromFetch` flattens the return value of `selector(response)` with ` innerFrom()`! And while `response.json()` is still streaming the response body over the network, unsubscribing from `fromFetch` will still abort the body download mid-stream.

## Watch out

- Never forget to check `if (!response.ok)` — otherwise `retry()` and `catchError()` will never trigger on HTTP `500 Internal Server Error` or `404 Not Found` responses!
- Import `fromFetch` from `'rxjs/fetch'`, not from the root `'rxjs'` entry point.

## Remember

- Import `fromFetch` from `'rxjs/fetch'` whenever you want a cold, abortable Observable wrapper around native `fetch()`.
- Unsubscribing from `fromFetch` (via `switchMap`, `takeUntil`, or `timeout`) automatically triggers `AbortController.abort()`.
- Always inspect `if (!response.ok)` inside `selector` to convert 4xx/5xx HTTP statuses into RxJS errors.
