---
slug: catching-errors-inside-switch-map
title: "Keeping outer streams alive: catchError inside switchMap()"
topic: errors-cleanup
kind: pattern
summary: "Placing catchError on the outer pipeline kills the event listener on the first error; place catchError inside the inner Observable to keep listening."
tags: [rxjs, catcherror, switchmap, errors, pattern]
related: [errors-are-terminal, catch-error-recovery, switch-map, typeahead-search-pattern]
---
> **In one line:** in a long-lived event pipeline (`clicks$` or `searchInput$`), **put `catchError` inside the inner Observable of `switchMap`** — putting it on the outer pipeline kills the event listener after the first failed request!

## The idea

This is the single most common production bug in RxJS applications.

Suppose you build a search box: `searchInput$.pipe(switchMap(q => fetchResults$(q)), catchError(() => of([])))`.

1. The user types `"rx"` and the network temporarily fails.
2. `fetchResults$("rx")` errors, and because `switchMap` has no inner `catchError`, the error escapes into the **outer pipeline**.
3. The outer `catchError` catches the error, emits `[]`, and **completes the outer pipeline** — which **unsubscribes from `searchInput$`**!
4. The user types again, and **the search box is completely dead** until they refresh the page!

To keep the outer `searchInput$` stream alive forever, catch the error **inside** the inner Observable passed to `switchMap`:

```flow
in: searchInput$ (Outer Stream) | stays ALIVE across errors! @primary
sm: switchMap(q => ...) | flattens inner request @blue
inner: fetch$(q).pipe(catchError(() => of([]))) | catches error & completes inner only @green
out: Subscriber | receives [] on error, then next search results! @amber
in -> sm: "rx" (fails), then "rxjs" (succeeds)
sm -> inner: runs inner pipeline per query
inner -> out: [] for "rx", then [results] for "rxjs"
```

## See it

Compare the broken outer `catchError` (dies after the first bad query) with the resilient inner `catchError` (recovers and processes the next query):

```typescript
import { Subject, of, throwError, switchMap, catchError } from 'rxjs';

const search$ = new Subject<string>();
const api$ = (q: string) =>
  q === 'bad' ? throwError(() => new Error('500')) : of([`Hit: ${q}`]);

// Before (BUG): catchError on OUTER pipe — search$ dies after 'bad'!
search$
  .pipe(
    switchMap((q) => api$(q)),
    catchError(() => of([])), // Completes the ENTIRE outer pipeline!
  )
  .subscribe((res) => console.log('Outer catch:', res));

// After (SAFE): catchError INSIDE switchMap on the inner api$(q) stream!
search$
  .pipe(
    switchMap((q) => api$(q).pipe(catchError(() => of([])))),
  )
  .subscribe((res) => console.log('Inner catch:', res));

search$.next('bad');  // Both log: []
search$.next('good'); // Outer is DEAD! Inner logs: ["Hit: good"]
```

## Go deeper

Why does the inner `catchError` keep `search$` alive? Because `switchMap` (and `mergeMap`, `concatMap`, `exhaustMap`) expects each **inner** Observable to emit values and complete, while the **outer** stream keeps running. When `api$('bad').pipe(catchError(() => of([])))` catches the error and returns `of([])`, `switchMap` simply sees a normal inner Observable that emitted `[]` and completed — so `switchMap` stays subscribed to `search$` waiting for `'good'`!

## Watch out

- Check every `switchMap`, `mergeMap`, `concatMap`, and `exhaustMap` in your codebase: if the outer stream is a user event or state stream that must stay alive, `catchError` (and `retry`) belong on the **inner** stream inside the flattening operator.
- If you return `EMPTY` from the inner `catchError(() => EMPTY)`, the failed request emits nothing downstream and the outer stream still stays alive for the next trigger.

## Remember

- An uncaught error from an inner Observable kills the entire outer flattening pipeline.
- Putting `catchError` on the outer pipeline unsubscribes from the outer event source and completes the stream.
- Put `catchError` (and `retry`) **inside** `switchMap(x => request$(x).pipe(retry(2), catchError(...)))` so the outer stream keeps listening.
