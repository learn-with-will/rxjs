---
slug: unsubscribe-vs-complete-pitfall
title: "Unsubscribing does not trigger complete()"
topic: observables-subscriptions
kind: pitfall
summary: "Calling subscription.unsubscribe() cancels the stream and runs producer teardown, but it never invokes your Observer's complete() callback."
tags: [rxjs, unsubscribe, complete, pitfall, finalize]
related: [subscription-and-unsubscribe, observer-callbacks, finalize-guaranteed-cleanup]
---
> **In one line:** `unsubscribe()` and `complete()` both stop the stream and run producer teardown, but **calling `unsubscribe()` does not call your Observer's `complete` callback**.

## The idea

A common beginner bug is putting cleanup or UI-reset code (like hiding a loading spinner or logging "finished") inside the Observer's `complete` callback, and then wondering why it never runs when the subscription is cancelled with `unsubscribe()`.

In RxJS, **completion** and **cancellation** are two distinct events:

- **`complete`** means the **producer** decided the stream finished naturally and pushed a completion notification downstream to your Observer.
- **`unsubscribe()`** means the **consumer** walked away early and cancelled the subscription. Because the consumer initiated the stop, the producer simply runs its teardown function and does **not** notify the Observer's `complete` callback.

| How the stream ended | Producer teardown runs? | `finalize()` operator runs? | Observer `complete()` runs? |
|---|---|---|---|
| Producer called `complete()` | **Yes** | **Yes** | **Yes** |
| Producer called `error(err)` | **Yes** | **Yes** | **No** (`error` runs instead) |
| Consumer called `sub.unsubscribe()` | **Yes** | **Yes** | **No** |

## See it

Compare what happens when you cancel an `interval` with `unsubscribe()`:

```typescript
import { interval, finalize } from 'rxjs';

// Before: putting cleanup only in complete() — skipped on unsubscribe!
const badSub = interval(500).subscribe({
  next: (n) => console.log('tick:', n),
  complete: () => console.log('complete callback (NEVER RUNS on unsubscribe)'),
});

// After: use finalize() for cleanup that must run on complete, error, OR unsubscribe
const goodSub = interval(500)
  .pipe(finalize(() => console.log('finalize: runs even on unsubscribe!')))
  .subscribe((n) => console.log('tick:', n));

setTimeout(() => {
  badSub.unsubscribe();
  goodSub.unsubscribe();
}, 1200);
```

When `unsubscribe()` runs at 1.2 seconds, `badSub`'s `complete` callback is silent, whereas `goodSub`'s `finalize` callback runs reliably.

## Go deeper

Note that completing operators like `take(1)` or `takeUntil(destroy$)` work by **emitting a `complete` notification downstream** and unsubscribing upstream. So if a stream ends because `take(3)` reached 3 values, your Observer's `complete` callback *does* run — whereas if you call `sub.unsubscribe()` directly on the returned `Subscription`, `complete` does not run.

Why does RxJS make this distinction? Because `complete` represents a semantic domain event ("all items have been delivered" or "the download finished 100%"), whereas `unsubscribe()` means "the listener stopped caring before the stream finished." Mixing the two would cause code that reacts to "download complete!" to fire erroneously when a user navigates away and cancels the download mid-flight.

## Watch out

- Never rely on the Observer's `complete` callback to release a lock or hide a spinner if the stream can also error or be unsubscribed.
- Use the `finalize(() => ...)` operator in `.pipe()` whenever cleanup must run on all three exit paths (complete, error, and unsubscribe).

## Remember

- `complete` means the producer finished; `unsubscribe()` means the consumer cancelled.
- Calling `sub.unsubscribe()` runs producer teardown, but **not** the Observer's `complete` callback.
- Put guaranteed consumer-side cleanup in `finalize()`, not only in `complete()`.
