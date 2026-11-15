---
slug: post-terminal-notifications-ignored
title: "Post-terminal notifications are ignored"
topic: observables-subscriptions
kind: concept
summary: "RxJS enforces the Observable contract through its Subscriber wrapper: any next, error, or complete calls after the first terminal event are silently ignored."
tags: [rxjs, contract, subscriber, complete, error]
related: [observable-contract-grammar, observable-execution-phases, producer-teardown-function]
---
> **In one line:** RxJS enforces the Observable contract automatically — once a producer calls `complete()` or `error()`, the `Subscriber` closes and silently ignores any later `next`, `error`, or `complete` calls.

## The idea

What happens if a buggy producer calls `subscriber.next(2)` *after* it already called `subscriber.complete()` or `subscriber.error()`?

You might worry that your Observer's `next` callback will fire after completion. It won't. When you pass an Observer to `observable.subscribe(observer)`, RxJS wraps your Observer in an internal **`Subscriber`** object before handing it to the producer function. That `Subscriber` acts as a guard enforcing `next* (error | complete)?`:

| Producer call | When the Subscriber is still open | After `complete()`, `error()`, or `unsubscribe()` |
|---|---|---|
| `subscriber.next(v)` | Delivered to Observer's `next` | **Silently ignored** |
| `subscriber.complete()` | Delivered to Observer's `complete`; closes Subscriber | **Silently ignored** |
| `subscriber.error(e)` | Delivered to Observer's `error`; closes Subscriber |Not delivered to Observer |

## See it

Even if a producer tries to emit more values and a second completion or error after terminating, your Observer never sees them:

```typescript
import { Observable } from 'rxjs';

const guarded$ = new Observable<number>((subscriber) => {
  subscriber.next(1);
  subscriber.complete(); // Terminal notification! Subscriber is now stopped.

  // Everything pushed below violates the contract and is ignored by Subscriber:
  subscriber.next(2);
  subscriber.complete();
});

guarded$.subscribe({
  next: (n) => console.log('next:', n),
  complete: () => console.log('complete'),
});
// Output:
// next: 1
// complete
// (2 is never logged, and complete only logs once)
```

The exact same guard applies after `subscriber.error()`:

```typescript
import { Observable } from 'rxjs';

const afterError$ = new Observable<number>((subscriber) => {
  subscriber.next(1);
  subscriber.error(new Error('failed')); // Terminal notification!
  subscriber.next(2);                    // Ignored
  subscriber.complete();                 // Ignored
});

afterError$.subscribe({
  next: (n) => console.log('next:', n),
  error: (e: Error) => console.log('error:', e.message),
  complete: () => console.log('complete'),
});
// Output: next: 1 / error: failed
```

## Go deeper

Notice that while `Subscriber` prevents post-terminal notifications from reaching your Observer, **the synchronous lines of code inside your producer function still execute** up to the `return` statement unless you return early. Inside custom producers, you can check `if (subscriber.closed) return;` before doing expensive work, or simply `return` right after calling `subscriber.complete()` or `subscriber.error()`.

## Watch out

- Calling `subscriber.complete()` does not magically `return` from your producer function — synchronous code written below `subscriber.complete()` still runs even though its `subscriber.next()` calls are ignored.
- Always release timers or listeners in the returned teardown function so background callbacks stop firing after termination.

## Remember

- RxJS wraps your Observer in a `Subscriber` that strictly enforces `next* (error | complete)?`.
- Any `next`, `error`, or `complete` call made after the first terminal event is ignored.
- Consumers never need to guard against receiving values after `complete` or `error`.
