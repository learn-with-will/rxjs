---
slug: subscription-and-unsubscribe
title: "Subscriptions and unsubscribe()"
topic: observables-subscriptions
kind: api
summary: "subscribe() returns a Subscription handle representing the running execution; call subscription.unsubscribe() to cancel work and trigger teardown."
tags: [rxjs, subscription, unsubscribe, teardown, memory-leaks]
related: [observer-callbacks, unsubscribe-vs-complete-pitfall, grouping-subscriptions-with-add]
---
> **In one line:** `subscribe()` returns a **`Subscription`** handle whose `unsubscribe()` method stops the running execution, releases its resources, and flips `subscription.closed` to `true`.

## The idea

When you call `source$.subscribe(...)`, you start an active execution that may own real resources — a `setInterval` timer, a DOM event listener, an open WebSocket, or an in-flight network request. `subscribe()` returns a **`Subscription`** object so you hold the off-switch for that execution.

A `Subscription` has three key members:

| Member | Type | What it does |
|---|---|---|
| **`unsubscribe()`** | `() => void` | Cancels the execution, stops further notifications, and runs teardown |
| **`closed`** | `boolean` | `false` while active; becomes `true` after `unsubscribe()`, `complete`, or `error` |
| **`add(teardown)`** | `(t) => void` | Attaches a child Subscription or teardown function to be cleaned up together |

Calling `unsubscribe()` is **idempotent**: calling it a second or third time on an already-closed Subscription is a safe no-op.

## See it

Here is how to start an open-ended DOM event stream, inspect `closed`, and cancel it so the click listener is removed:

```typescript
import { interval } from 'rxjs';

const sub = interval(500).subscribe((tick) => {
  console.log('tick:', tick);
});

console.log('Initially closed?', sub.closed); // false

setTimeout(() => {
  sub.unsubscribe();
  console.log('Closed after unsubscribe?', sub.closed); // true
}, 1600);
```

After 1.6 seconds (ticks `0`, `1`, and `2`), `sub.unsubscribe()` stops the interval timer and sets `sub.closed` to `true`.

## Go deeper

When an Observable **completes** or **errors** on its own, RxJS automatically runs the teardown and marks `subscription.closed = true`. That means finite streams like `of(1, 2, 3)` clean themselves up immediately. You need `unsubscribe()` (or a completing operator like `takeUntil`) whenever a source is **open-ended** (`interval`, `fromEvent`, `Subject`) or when a component unmounts before a slow async operation finishes.

```typescript
import { of } from 'rxjs';

const finiteSub = of('done').subscribe(console.log);
// Synchronous of() has already completed and torn down:
console.log('Finite stream closed?', finiteSub.closed); // true
```

In component frameworks like React (`useEffect`) or Angular (`ngOnDestroy`), the standard lifecycle pattern is to start your subscription when the view mounts and return or invoke `sub.unsubscribe()` when the view unmounts. That single call guarantees that any underlying timer, `AbortController`, WebSocket connection, or DOM event listener created inside the Observable pipeline is released immediately rather than outliving the component on screen.

## Watch out

- Calling `source$.subscribe(...)` on an infinite stream and discarding the returned `Subscription` leaves you with no handle to stop it later.
- Do not assume garbage collection will clean up an active `fromEvent` or `interval` subscription — as long as the event target or timer is alive, it retains your subscriber callback in memory.

## Remember

- `subscribe()` returns a `Subscription` representing one running Observable execution.
- Call `sub.unsubscribe()` to cancel the stream and run its teardown logic.
- `sub.closed` is `true` once the stream has completed, errored, or been unsubscribed.
