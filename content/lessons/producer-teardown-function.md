---
slug: producer-teardown-function
title: "Writing producer teardown functions"
topic: observables-subscriptions
kind: technique
summary: "Return a cleanup function from new Observable's producer to release timers, sockets, or listeners; RxJS runs it exactly once on complete, error, or unsubscribe."
tags: [rxjs, teardown, observable, cleanup, memory-leaks]
related: [first-observable-new-observable, observable-execution-phases, subscription-and-unsubscribe]
---
> **In one line:** the function you **return** from a `new Observable` producer is its **teardown** (finalizer), and RxJS guarantees it runs **exactly once** when the execution ends by completion, error, or unsubscription.

## The idea

Whenever a custom Observable allocates a resource during subscription — starting a `setInterval`, opening a `WebSocket`, registering an `AbortController`, or attaching a DOM listener — it must also define how to release that resource.

You do that by **returning a teardown function** `() => { ... }` at the end of the producer. RxJS calls this teardown function **at most once** per subscription, triggered by whichever exit happens first:

1. The producer calls `subscriber.complete()`.
2. The producer calls `subscriber.error(err)` (or throws an exception).
3. The consumer calls `subscription.unsubscribe()` (or a downstream operator like `take(1)` unsubscribes).

```flow
sub: Subscription Active | resource allocated @primary
c: complete() | producer finished @green
e: error(err) | producer failed @red
u: unsubscribe() | consumer cancelled @amber
td: Teardown Function | runs once to release resource @blue
sub -> c
sub -> e
sub -> u
c -> td
e -> td
u -> td
```

## See it

Here is a simulated socket stream whose teardown closes the connection whether the consumer unsubscribes or the stream completes:

```typescript
import { Observable } from 'rxjs';

const socket$ = new Observable<string>((subscriber) => {
  console.log('Opening socket...');
  const id = setInterval(() => subscriber.next('ping'), 500);

  // Teardown function: runs once on complete, error, or unsubscribe
  return () => {
    clearInterval(id);
    console.log('Closing socket (teardown ran)');
  };
});

const sub = socket$.subscribe((msg) => console.log('received:', msg));

// Cancel after 1.2s — triggers the returned teardown function:
setTimeout(() => sub.unsubscribe(), 1200);
// Output:
// Opening socket...
// received: ping
// received: ping
// Closing socket (teardown ran)
```

## Go deeper

What if the stream completes on its own inside the producer? RxJS **still** runs the teardown function immediately upon `subscriber.complete()`, and calling `sub.unsubscribe()` afterward will not run the teardown a second time:

```typescript
import { Observable } from 'rxjs';

const finite$ = new Observable<number>((subscriber) => {
  subscriber.next(1);
  subscriber.complete(); // Immediately triggers teardown #1
  return () => console.log('Teardown ran exactly once');
});

const sub = finite$.subscribe();
sub.unsubscribe(); // Safe no-op: teardown already ran on complete()
```

## Watch out

- Never clean up resources only before calling `subscriber.complete()` — if the consumer calls `unsubscribe()` early, code before `complete()` is never reached, whereas the returned teardown function always runs.
- Keep teardown synchronous and idempotent: release the timer, listener, or controller and return.

## Remember

- Always pair resource setup in `new Observable` with a returned teardown function `() => { ... }`.
- Teardown runs **exactly once** on the first of `complete`, `error`, or `unsubscribe`.
- Once teardown has run, later `unsubscribe()` calls are safe no-ops.
