---
slug: cold-and-unicast-by-default
title: "Observables are cold and unicast by default"
topic: observables-subscriptions
kind: concept
summary: "A plain Observable is cold (unicast): every call to subscribe() runs the producer function independently, giving each subscriber its own separate execution."
tags: [rxjs, cold, unicast, observable, subscribe]
related: [observable-execution-phases, cold-vs-hot-observables, defer-per-subscriber]
---
> **In one line:** plain Observables are **cold (unicast)** — each `subscribe()` call triggers a brand-new, independent run of the producer function.

## The idea

When two parts of your code subscribe to the same plain Observable, do they share one execution or run it twice? By default, **they run it twice**.

A plain Observable has two defining properties:

1. **Lazy:** the producer function does not run until someone calls `subscribe()`.
2. **Unicast (cold):** every call to `subscribe()` invokes the producer function **again from scratch**, creating a separate execution for each subscriber.

That means if an Observable wraps `Math.random()`, a timer, or an HTTP request (`ajax('/api/users')`), subscribing to it in two components will generate **two different random numbers**, start **two separate timers**, or fire **two duplicate HTTP requests**.

```sequence
actor s1: Subscriber 1
actor s2: Subscriber 2
actor obs: Cold Observable | new Observable(producer) @primary
s1 -> obs: subscribe() #1
note obs: Producer runs (#1) | independent execution @blue
obs --> s1: next(0.13), complete()
s2 -> obs: subscribe() #2
note obs: Producer runs (#2) | fresh independent execution @amber
obs --> s2: next(0.88), complete()
```

## See it

Watch a subscription counter and `Math.random()` prove that two subscribers to the same Observable get two completely independent runs:

```typescript
import { Observable } from 'rxjs';

let runs = 0;
const cold$ = new Observable<string>((subscriber) => {
  runs++;
  const random = Math.random().toFixed(2);
  console.log(`Producer run #${runs}`);
  subscriber.next(`run #${runs} -> ${random}`);
  subscriber.complete();
});

cold$.subscribe((val) => console.log('Subscriber A got:', val));
cold$.subscribe((val) => console.log('Subscriber B got:', val));
// Output:
// Producer run #1
// Subscriber A got: run #1 -> 0.13
// Producer run #2
// Subscriber B got: run #2 -> 0.88
```

## Go deeper

Why is cold/unicast the default? Because it makes Observables predictable and composable: each subscriber gets the full sequence of values from the beginning, without worrying that an earlier subscriber already consumed them. When you *do* want multiple subscribers to share a single execution (such as one shared HTTP request or one shared WebSocket), you explicitly opt into **multicasting** with `share()`, `shareReplay()`, or a `Subject`.

## Watch out

- Subscribing twice to an HTTP Observable (or using Angular's `async` pipe twice in a template on the same cold stream) fires the HTTP request **twice** unless you multicast it.
- Do not put "run once for the whole app" side effects inside a cold Observable's producer — they will run once per subscriber.

## Remember

- Plain Observables are **cold** and **unicast** by default.
- Every `subscribe()` call runs the producer function again independently.
- To share a single execution across multiple subscribers, use multicasting (`share`, `shareReplay`, or `Subject`).
