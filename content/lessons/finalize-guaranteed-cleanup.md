---
slug: finalize-guaranteed-cleanup
title: "Guaranteed cleanup with finalize()"
topic: errors-cleanup
kind: api
summary: "finalize(callback) runs its cleanup callback exactly once when the stream ends for any reason: normal completion, an error, or consumer unsubscription."
tags: [rxjs, finalize, cleanup, teardown, loading-spinner]
related: [unsubscribe-vs-complete-pitfall, take-until-notifier, tap-for-side-effects]
---
> **In one line:** `finalize(() => ...)` is the `try / finally` of RxJS — its callback runs **once** whether the stream **completes**, **errors**, or is **unsubscribed**.

## The idea

Suppose you set `isLoading = true` when starting an HTTP request, and you need to set `isLoading = false` when the request ends. Where should `isLoading = false` go?

- If you put it in `subscribe({ complete })`, it won't run when the request **errors** or when the user navigates away and **unsubscribes**!
- If you duplicate it in `complete` and `error`, it still won't run on **`unsubscribe()`** (for example when `switchMap` cancels a stale request)!

**`finalize(callback)`** solves all three exit paths in one place. Whenever the pipeline tears down — for **any** reason — `finalize` executes your callback once.

| How the stream ended | `complete` callback | `error` callback | `finalize` callback |
|---|---|---|---|
| Stream completed normally | Runs | No | **Runs** |
| Stream failed with an error | No | Runs | **Runs** |
| Consumer called `unsubscribe()` / cancelled by `switchMap` | No | No | **Runs** |

## See it

Here we pair `defer` (to turn the spinner **on** lazily when subscribed) with `finalize` (to turn the spinner **off** on complete, error, or cancel):

```typescript
import { defer, timer, map, finalize } from 'rxjs';

let isLoading = false;

const loadReport$ = defer(() => {
  isLoading = true;
  console.log('Spinner ON (isLoading =', isLoading, ')');
  return timer(500).pipe(map(() => 'Report Data'));
}).pipe(
  finalize(() => {
    isLoading = false;
    console.log('Spinner OFF via finalize (isLoading =', isLoading, ')');
  }),
);

// Even if we cancel after 150ms (before the 500ms timer finishes), finalize() runs!
const sub = loadReport$.subscribe((data) => console.log('Received:', data));
setTimeout(() => sub.unsubscribe(), 150);
// Output:
// Spinner ON (isLoading = true )
// Spinner OFF via finalize (isLoading = false )
```

## Go deeper

Look at why we turned the spinner on inside `defer(() => { isLoading = true; return ... })` (or using RxJS 7.3+'s `tap({ subscribe: () => { isLoading = true; }, finalize: () => { isLoading = false; } })`):

```typescript
import { of, delay, tap } from 'rxjs';

// RxJS 7.3+ tap() supports subscribe, unsubscribe, and finalize lifecycle hooks!
const tracked$ = of('payload').pipe(
  delay(100),
  tap({
    subscribe: () => console.log('Started subscription'),
    finalize: () => console.log('Ended (complete, error, or unsubscribe)'),
  }),
);
```

Both `finalize(fn)` and `tap({ subscribe, finalize })` guarantee that subscription setup and teardown stay symmetrically paired inside the pipeline.

## Watch out

- Do not set `isLoading = true` *outside* the Observable before calling `.subscribe()` if the stream might be subscribed to multiple times or retried — use `defer` or `tap({ subscribe })` so setup runs on each subscription.
- `finalize` cannot swallow an error or emit fallback values — it only runs a side-effect callback during teardown.

## Remember

- `finalize(() => ...)` runs on **complete**, **error**, and **unsubscribe** — just like `finally` in JavaScript.
- Use `finalize` for hiding loading spinners, closing resources, and recording duration metrics.
- In RxJS 7.3+, `tap({ subscribe, finalize })` lets you pair setup and cleanup hooks in one operator.
