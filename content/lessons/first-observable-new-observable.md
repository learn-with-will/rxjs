---
slug: first-observable-new-observable
title: "Creating a stream with new Observable"
topic: observables-subscriptions
kind: api
summary: "new Observable(subscribeFn) defines a stream from a producer function that receives a subscriber and pushes next, error, or complete notifications."
tags: [rxjs, observable, subscriber, teardown]
related: [what-is-an-observable, observable-vs-promise, producer-teardown-function]
---
> **In one line:** `new Observable((subscriber) => { ... })` builds a stream from a producer function that pushes values with `subscriber.next()` and optionally returns a cleanup function.

## The idea

In everyday RxJS code you usually create streams with helper functions like `of`, `from`, or `fromEvent`. Under the hood, every one of them uses the `Observable` constructor. Writing `new Observable` yourself demystifies the whole library and is the tool you reach for when wrapping a custom callback or timer API.

You pass `new Observable<T>(fn)` a **producer function** (also called the subscribe function). That function does **not** run when you call `new Observable`. Instead, RxJS saves it and calls it each time someone calls `.subscribe()`, passing in a `subscriber` object. Inside the producer, you:

- Call `subscriber.next(value)` zero or more times to push values.
- Call `subscriber.complete()` when the stream has finished, or `subscriber.error(err)` if it failed.
- Optionally **return a teardown function** that cleans up resources (such as clearing a timer or removing an event listener) when the stream ends or the consumer unsubscribes.

```sequence
actor app: Consumer | calls subscribe()
actor obs: Observable | producer function @primary
actor timer: Resource | setInterval @blue
app -> obs: subscribe(observer) | starts execution
obs -> timer: setInterval(...) | starts producing
timer --> app: subscriber.next(date) | pushes each second
app -> obs: sub.unsubscribe() | cancels after 5s
obs -> timer: clearInterval(id) | teardown runs
```

## See it

Here is a custom clock Observable that emits the current `Date` every second and cleans up its interval when unsubscribed:

```typescript
import { Observable } from 'rxjs';

const clock$ = new Observable<Date>((subscriber) => {
  const id = setInterval(() => {
    subscriber.next(new Date());
  }, 1000);

  // Teardown: runs when the consumer unsubscribes
  return () => {
    clearInterval(id);
    console.log('clock timer cleared');
  };
});

const sub = clock$.subscribe((now) => console.log(now.toLocaleTimeString()));
setTimeout(() => sub.unsubscribe(), 3500);
```

After 3.5 seconds, `sub.unsubscribe()` stops the stream and executes the returned teardown function, clearing the interval so it cannot leak.

## Go deeper

What happens if you subscribe to `clock$` twice? Because `new Observable` saves the producer function and calls it on every `subscribe()`, each subscriber gets its **own** `setInterval` and its **own** teardown function. Unsubscribing one subscriber clears only its timer and leaves the other subscriber running untouched.

```typescript
const subA = clock$.subscribe((d) => console.log('A:', d.getSeconds()));
const subB = clock$.subscribe((d) => console.log('B:', d.getSeconds()));

// Cancelling A stops A's interval; B keeps ticking independently
subA.unsubscribe();
```

## Watch out

- If your producer starts an interval or attaches a DOM listener and does **not** return a teardown function, unsubscribing leaves the timer or listener running in the background.
- Keep the producer focused on producing values; put display logic or logging inside the subscriber (or `tap`).

## Remember

- Pass `new Observable` a producer function that receives `subscriber`.
- Push values with `subscriber.next(v)` and end with `complete()` or `error(e)`.
- Return a teardown function from the producer to release timers, sockets, or listeners.
