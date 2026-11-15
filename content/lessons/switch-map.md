---
slug: switch-map
title: "Cancelling stale work with switchMap()"
topic: flattening-higher-order
kind: api
summary: "switchMap(project) subscribes to the newest inner Observable and immediately unsubscribes from any previous in-flight inner Observable."
tags: [rxjs, switchmap, cancellation, flattening, search]
related: [what-is-a-higher-order-observable, merge-map, concat-map, exhaust-map]
---
> **In one line:** whenever a new outer value arrives, `switchMap(fn)` **immediately unsubscribes from the previous inner Observable** and switches to the new one.

## The idea

Imagine a user clicks "Profile A" and, while Profile A's slow network request is still in flight, clicks "Profile B". You no longer care about Profile A's response — in fact, if Profile A's slow response arrived *after* Profile B's fast response, displaying it would show the wrong user on screen!

**`switchMap(project)`** encodes **"only the latest outer event matters"**:

1. When outer value #1 arrives, `switchMap` subscribes to inner Observable #1.
2. If outer value #2 arrives while inner #1 is still running, `switchMap` **calls `unsubscribe()` on inner #1** (triggering its teardown, such as aborting an in-flight `fetch`) and immediately subscribes to inner #2.
3. Only emissions from the **currently active** inner Observable reach your subscriber.

```sequence
actor user: Outer Stream | emits "A", then "B"
actor sm: switchMap | keeps only latest inner stream @primary
actor r1: Inner "A" (slow) | cancelled when "B" arrives! @red
actor r2: Inner "B" (latest) | completes & emits @green
user -> sm: "A"
sm -> r1: subscribe()
user -> sm: "B" (arrives before "A" finishes)
sm -> r1: unsubscribe() (aborts stale request A!)
sm -> r2: subscribe()
r2 --> sm: "Results for B"
```

## See it

Here outer values `1`, `2`, and `3` arrive synchronously from `of(1, 2, 3)`, and each inner Observable has a `delay(100)`. Because `2` cancels `1` and `3` cancels `2` before their 100ms timer finishes, **only `3` completes**:

```typescript
import { of, switchMap, delay, finalize } from 'rxjs';

const fetchProfile$ = (id: number) =>
  of(`Profile #${id} data`).pipe(
    delay(100),
    finalize(() => console.log(`Inner stream #${id} torn down`)),
  );

of(1, 2, 3)
  .pipe(
    switchMap((id) => fetchProfile$(id)),
  )
  .subscribe((data) => console.log('Received:', data));
// Output:
// Inner stream #1 torn down  (cancelled immediately when 2 arrived)
// Inner stream #2 torn down  (cancelled immediately when 3 arrived)
// Received: Profile #3 data  (after 100ms)
// Inner stream #3 torn down
```

## Go deeper

Why didn't `of(1, 2, 3)` cancel `1` and `2` if the inner stream was synchronous (`of(id * 10)` without `delay`)? Because a synchronous inner Observable emits and completes **during `subscribe()`** before the next outer value `2` is even processed! `switchMap` only cancels an inner Observable when a new outer value arrives **while the previous inner Observable is still active**.

This automatic cancellation makes `switchMap` the default choice for **reads/queries** (search autocomplete, route parameter changes, filter dropdowns, and restarting intervals). Conversely, you should **never** use `switchMap` for non-idempotent **writes** (saving a form, deleting an item, charging a card) where cancelling an in-flight request would abort a mutation the user intended to complete.

## Watch out

- Never use `switchMap` for `POST`/`PUT`/`DELETE` write operations unless you intentionally want a newer trigger to abort the previous write mid-flight; use `concatMap`, `mergeMap`, or `exhaustMap` for writes.
- An uncaught error inside the inner Observable will still kill the outer `switchMap` pipeline unless you place `catchError` **inside** the inner stream (`switchMap(q => fetch$(q).pipe(catchError(...)))`).

## Remember

- `switchMap(fn)` cancels the previous active inner Observable whenever a new outer value arrives.
- Only one inner Observable is ever active at a time — the most recent one.
- Use `switchMap` for latest-wins reads (typeahead search, tab/route switches); avoid it for writes.
