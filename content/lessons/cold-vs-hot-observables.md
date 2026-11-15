---
slug: cold-vs-hot-observables
title: "Cold vs hot Observables: unicast vs multicast"
topic: subjects-multicasting
kind: concept
summary: "Cold Observables create a fresh producer per subscriber (unicast); hot Observables share an external producer across all subscribers (multicast)."
tags: [rxjs, cold, hot, unicast, multicast]
related: [cold-and-unicast-by-default, what-is-a-subject, share-operator, share-replay-for-caching]
---
> **In one line:** a **cold** Observable creates its producer *inside* `subscribe()` so every subscriber gets an independent execution, whereas a **hot** Observable broadcasts a producer that lives *outside* `subscribe()` to all subscribers at once.

## The idea

We saw in Topic 1 that Observables created with `new Observable`, `of`, `from`, `interval`, or `ajax` are **cold** by default. Now that you know how `Subject` works, we can define **cold vs hot** precisely:

- **Cold (unicast):** the data producer is **created inside** the subscription function. Subscribing twice starts **two separate producers** (two timers, two `fetch` calls), and each subscriber sees the full sequence from the beginning.
- **Hot (multicast):** the data producer **already exists outside** the subscription function (a `Subject`, a shared WebSocket, mouse movements on screen, or a stream piped through `share()`). Subscribing simply taps into the live broadcast, and late subscribers only see emissions from the moment they join.

| Dimension | Cold Observable | Hot Observable |
|---|---|---|
| **Analogy** | Streaming an on-demand movie (starts at 0:00 for you) | Tuning into a live TV broadcast (joins mid-show) |
| **Where producer lives** | Created fresh *inside* each `subscribe()` call | Exists *outside* `subscribe()`; shared by all |
| **3 subscribers =** | 3 independent executions (e.g. 3 HTTP requests) | 1 shared execution multicast to 3 observers |
| **Examples** | `of`, `from`, `interval`, `ajax`, `fromFetch`, `defer` | `Subject`, `BehaviorSubject`, `source$.pipe(share())` |

## See it

Compare a cold `Observable` (where the random number is generated inside `subscribe`, giving each subscriber a different number) with a hot `Observable` (where the random number is generated outside `subscribe` or pushed via a `Subject`, giving both subscribers the exact same number):

```typescript
import { Observable, Subject } from 'rxjs';

// 1. COLD: producer is created INSIDE the subscribe callback (unicast)
const coldRandom$ = new Observable<number>((subscriber) => {
  subscriber.next(Math.random());
  subscriber.complete();
});

coldRandom$.subscribe((n) => console.log('Cold A:', n)); // e.g. 0.418...
coldRandom$.subscribe((n) => console.log('Cold B:', n)); // e.g. 0.903... (different!)

// 2. HOT: producer lives OUTSIDE subscribe via a Subject (multicast)
const hotRandom$ = new Subject<number>();

hotRandom$.subscribe((n) => console.log('Hot A:', n));
hotRandom$.subscribe((n) => console.log('Hot B:', n));

hotRandom$.next(Math.random()); // Both Hot A and Hot B log the EXACT same number!
```

## Go deeper

What about `fromEvent(document, 'click')` — is it hot or cold? Strictly speaking, it is **warm** (a hybrid): the underlying producer (the user clicking the mouse in the real world) exists outside `subscribe()`, so late subscribers miss earlier clicks just like a hot stream. However, each call to `fromEvent(...).subscribe()` still registers its own separate `addEventListener` handler on the DOM node unless you multicast it with `share()`.

When you have a cold Observable (like an HTTP request `fetchUser$()`) and three components on screen all subscribe to it, coldness means the browser fires **three duplicate HTTP requests**! Turning that cold stream into a hot multicast stream so all three components share **one** execution is the job of `share()` and `shareReplay()`.

## Watch out

- Do not assume an Observable variable `const user$ = fetchUser$()` caches its result like a `Promise` — because `user$` is cold, every `.subscribe()` call triggers a brand-new network request unless you add `share()` or `shareReplay()`.
- Converting a cold stream to hot means late subscribers may miss early emissions unless you use a replay buffer (`shareReplay(1)`).

## Remember

- **Cold (unicast):** producer is created per subscription; N subscribers run N independent executions.
- **Hot (multicast):** producer is shared across subscriptions; all active subscribers receive the same emission simultaneously.
- Use `share()` or `shareReplay()` to convert a cold Observable into a hot multicast Observable.
