---
slug: what-is-an-observable
title: "What is an Observable?"
topic: observables-subscriptions
kind: concept
summary: "An Observable is a lazy, push-based collection of zero or more values delivered over time that can complete or error."
tags: [rxjs, observable, streams, fundamentals]
related: [push-vs-pull-grid, observable-vs-promise, first-observable-new-observable]
---
> **In one line:** an Observable represents a lazy, push-based stream of zero or more values over time that can eventually complete or error.

## The idea

JavaScript is full of things that produce values *over time*: mouse clicks, keystrokes, HTTP responses, WebSocket messages, timers, and animation frames. Without RxJS, you handle each one with a different tool — event listeners, callbacks, Promises, or `setInterval` — and each has its own rules for subscribing, cleaning up, error handling, and cancellation.

When you need to combine these sources — for example, waiting 300 ms after the user stops typing, cancelling the previous request, and retrying on failure — wiring callbacks together by hand becomes brittle and hard to maintain.

RxJS (Reactive Extensions for JavaScript) unifies all of these asynchronous and event-based sources under **one abstraction**: the **Observable**. Instead of returning a single value once like a normal function, an Observable can **push** zero, one, or many values to whoever is listening, either synchronously or spread out over time. When it finishes, it sends a `complete` signal, or if something goes wrong, it sends an `error`.

> **Tip:** RxJS documentation often draws streams as **marble diagrams**, where a horizontal line is time flowing left to right and each character on the line is an emitted value.

```text
clicks$:  ---c----c--c--------c---->
          (each "c" is one click; time flows left to right)
```

## See it

Once every source is an Observable, the same set of composable operators works on all of them. You stop writing *how* to wire callbacks and start declaring *what* should happen as values flow:

```typescript
import { Observable } from 'rxjs';

const colors$ = new Observable<string>((subscriber) => {
  subscriber.next('red');
  subscriber.next('green');
  subscriber.next('blue');
  subscriber.complete();
});

colors$.subscribe({
  next: (color) => console.log('color:', color),
  complete: () => console.log('no more colors'),
});
// Logs: color: red / color: green / color: blue / no more colors
```

Here `colors$` pushes three values in sequence and then announces completion.

## Go deeper

Think of an Observable like **subscribing to a magazine**. Defining the Observable is like the magazine existing — no issues land on your doormat until you actually subscribe. Once subscribed, issues (values) arrive on the publisher's schedule, not yours. The magazine may eventually end its run (`complete`), or the publisher may go out of business mid-run (`error`), and you can cancel your subscription whenever you like.

## Watch out

- Building an Observable does no work by itself; nothing runs until something calls `subscribe()`.
- Do not assume an Observable emits only once — it can emit zero, one, or thousands of values, and may never complete.

## Remember

- An Observable is a lazy, push-based stream of values delivered over time.
- It unifies events, timers, and async requests under one composable abstraction.
- Every stream can emit values (`next`), finish (`complete`), or fail (`error`).
