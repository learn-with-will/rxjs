---
slug: typeahead-search-pattern
title: "The canonical RxJS typeahead search pattern"
topic: patterns-interop
kind: pattern
summary: "Build a fault-tolerant, race-condition-free live search box in 6 operators: map, debounceTime, distinctUntilChanged, switchMap, and inner catchError."
tags: [rxjs, typeahead, switchmap, debouncetime, pattern]
related: [debounce-time, distinct-until-changed, switch-map, catching-errors-inside-switch-map]
---
> **In one line:** the canonical RxJS typeahead combines `debounceTime` (wait for typing pause), `distinctUntilChanged` (skip duplicate queries), `switchMap` (cancel stale in-flight requests), and an **inner** `catchError` (keep the input stream alive after network failures).

## The idea

Live search (typeahead / autocomplete) is the flagship showcase for reactive programming because an imperative implementation requires tracking `setTimeout` IDs, `previousQuery` strings, `AbortController` instances, and request sequence numbers. In RxJS, every requirement maps to a single declarative operator:

| Pipeline step | Operator | What problem it solves |
|---|---|---|
| **1. Normalize input** | `map(e => e.target.value.trim())` | Extracts and trims the query string from each `input` event |
| **2. Wait for pause** | `debounceTime(300)` | Avoids firing a network request on every keystroke while the user is actively typing |
| **3. Deduplicate** | `distinctUntilChanged()` | If the user types `'rx'`, adds `'x'`, and backspaces back to `'rx'` within 300ms, skips re-fetching `'rx'` |
| **4. Cancel stale HTTP** | `switchMap(q => ...)` | If `'rx'` is still in flight when `'rxjs'` fires, **aborts** `'rx'` so out-of-order responses never overwrite newer results |
| **5. Short-circuit empty** | `q.length < 2 ? of([]) : searchApi$(q)` | Clears results immediately without hitting the server when the input is cleared |
| **6. Isolate errors** | `searchApi$(q).pipe(catchError(() => of([])))` | Catches HTTP errors on the **inner** stream so the outer `input` listener never dies! |

```sequence
actor user: Input Events | DOM input @blue
actor pipe: debounceTime(300) | distinctUntilChanged() @primary
actor sw: switchMap(q => fetch$(q)) | inner catchError @green
user -> pipe: 'r', 'rx' (within 100ms)
note pipe: 300ms pause -> emits 'rx' @amber
pipe -> sw: Start GET /search?q=rx
user -> pipe: 'rxjs' -> 300ms pause
pipe -> sw: Abort GET /search?q=rx! Start GET /search?q=rxjs
sw -> user: Render results for 'rxjs'
```

## See it

Assemble the complete 6-step typeahead pipeline:

```typescript
import { fromEvent, map, debounceTime, distinctUntilChanged, switchMap, catchError, of } from 'rxjs';
import { fromFetch } from 'rxjs/fetch';

const searchInput = document.querySelector<HTMLInputElement>('#search')!;

const results$ = fromEvent(searchInput, 'input').pipe(
  map(e => (e.target as HTMLInputElement).value.trim()),
  debounceTime(300),
  distinctUntilChanged(),
  switchMap(query => {
    if (query.length < 2) return of([]); // Clear immediately on short/empty query
    return fromFetch(`/api/search?q=${encodeURIComponent(query)}`, {
      selector: res => res.json(),
    }).pipe(
      catchError(err => {
        console.error('Search failed, keeping input alive:', err);
        return of([]); // MUST be inside switchMap!
      })
    );
  })
);
```

## Go deeper

Look at the order of `debounceTime(300)` followed by `distinctUntilChanged()`. What if you swapped them (`distinctUntilChanged()` before `debounceTime(300)`)? If the user is currently viewing results for `'rx'`, types `'rxx'`, and immediately hits Backspace within 50ms to return to `'rx'`, putting `distinctUntilChanged()` *first* sees `'rx' -> 'rxx' -> 'rx'` as distinct and fires a redundant request for `'rx'`! Putting `debounceTime(300)` **before** `distinctUntilChanged()` only compares the settled post-debounce query against the last *actually searched* query.

## Watch out

- Never put `catchError` on the **outer** pipeline after `switchMap` — a single 500 response will complete the outer `fromEvent` stream and permanently break the search box until page reload.
- Always URL-encode user input with `encodeURIComponent(query)`.

## Remember

- Always order `map(trim)` → `debounceTime(300)` → `distinctUntilChanged()` → `switchMap(...)`.
- Return `of([])` inside `switchMap` when the query is empty or shorter than your minimum length.
- Place `catchError(() => of([]))` on the **inner** HTTP stream inside `switchMap`.
