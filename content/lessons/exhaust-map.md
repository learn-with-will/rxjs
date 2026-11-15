---
slug: exhaust-map
title: "Ignoring triggers while busy with exhaustMap()"
topic: flattening-higher-order
kind: api
summary: "exhaustMap(project) subscribes to the first inner Observable and completely ignores any new outer emissions until that active inner Observable completes."
tags: [rxjs, exhaustmap, flattening, form-submit, login]
related: [what-is-a-higher-order-observable, switch-map, concat-map, choosing-a-flattening-operator]
---
> **In one line:** `exhaustMap(fn)` runs the first inner Observable to completion and **drops any new outer emissions that arrive while that inner Observable is still active**.

## The idea

Suppose a user impatiently clicks the "Sign In" or "Submit Order" button three times while the server takes 800ms to respond:

- `mergeMap` would fire **three parallel** login/order requests!
- `concatMap` would queue **three sequential** login/order requests!
- `switchMap` would **abort** the first two requests and restart a third!

What you actually want is: *"Start the request on the first click, **ignore every extra click while that request is in flight**, and listen for clicks again only after the request finishes."* That is **`exhaustMap(project)`**:

1. When idle, an outer emission starts a new inner Observable.
2. While that inner Observable is active (not yet completed), **every new outer emission is silently dropped** — not queued, not subscribed.
3. As soon as the active inner Observable **completes**, `exhaustMap` becomes idle again and the next outer emission can start a new inner stream.

```sequence
actor btn: Login Button | 3 rapid clicks
actor em: exhaustMap | ignores triggers while busy @primary
actor api: login$() (500ms) | active inner request @green
btn -> em: Click #1 at 0ms (idle -> starts login$())
em -> api: subscribe()
btn -> em: Click #2 at 120ms (busy -> DROPPED!)
btn -> em: Click #3 at 250ms (busy -> DROPPED!)
api --> em: "200 OK" + complete() at 500ms (now idle again)
```

## See it

Here a source emits at 0ms, 100ms, 200ms, and 500ms, while each inner operation takes 300ms:

```typescript
import { timer, of, exhaustMap, delay, take } from 'rxjs';

const submitOrder$ = (clickNum: number) =>
  of(`Order from click #${clickNum} succeeded`).pipe(delay(300));

// Emits 0 at 0ms, 1 at 150ms, 2 at 300ms, 3 at 450ms:
timer(0, 150)
  .pipe(
    take(4),
    exhaustMap((clickNum) => submitOrder$(clickNum)),
  )
  .subscribe((result) => console.log(result));
// Click #0 starts at 0ms and runs until 300ms (Click #1 at 150ms is dropped!)
// Click #2 starts at 300ms and runs until 600ms (Click #3 at 450ms is dropped!)
```

## Go deeper

How does `exhaustMap` differ from `throttleTime(300)` on a Submit button? `throttleTime(300)` uses a hardcoded 300ms guess — if the network actually takes 900ms on a slow connection, a second click at 400ms still slips through! `exhaustMap` ties the cooldown period to the **actual completion of the inner Observable**, whether that request takes 50ms or 5,000ms. That also makes `exhaustMap` ideal for **non-overlapping polling** (`interval(5000).pipe(exhaustMap(() => fetchStatus$()))`), ensuring a new poll tick is skipped if the previous HTTP request is still in flight.

## Watch out

- Just like `concatMap`, the inner Observable inside `exhaustMap` **must eventually complete** — if it never completes, `exhaustMap` stays "busy" forever and ignores all future outer emissions!
- If the inner Observable can error, catch the error inside the inner pipe so it completes cleanly and unlocks `exhaustMap` for the user's next retry click.

## Remember

- `exhaustMap(fn)` starts an inner Observable when idle and ignores all outer emissions until that inner Observable completes.
- Unlike `throttleTime(ms)`, its lock duration matches the exact lifetime of the async operation.
- Use `exhaustMap` for login buttons, form submissions, pull-to-refresh, and non-overlapping polling.
