---
id: lesson-04
slug: the-observable-lifecycle-and-contract
title: "The Observable Lifecycle and Contract"
level: beginner
order: 04
duration: 24
tags:
  - rxjs
  - observable
  - contract
  - cold
  - lifecycle
  - teardown
---

# Learning Objectives

By the end of this lesson you will be able to:

- Describe the four things that happen during an Observable execution: create, subscribe, emit, tear down.
- State the Observable contract grammar `next* (error | complete)?`.
- Explain why Observables are **lazy** and **unicast (cold) by default**.
- Predict that notifications sent *after* a terminal event are ignored.
- Write a producer that returns a **teardown** function and know exactly when it runs.
- Explain why two subscriptions to the same Observable are independent.

# Why This Concept Exists

Operators, Subjects, error handling, multicasting — every advanced RxJS topic assumes you understand the *rules* an Observable follows. These rules are collectively called the **Observable contract**. Internalise them now and later behaviour ("why did my HTTP request fire twice?", "why did nothing happen after the error?") stops being mysterious and becomes predictable.

# Concept Explanation

### The execution has four phases

1. **Creation** — you build the Observable (via `new Observable` or a creation function). No work happens yet.
2. **Subscription** — `subscribe()` runs the producer function, passing it a `Subscriber`.
3. **Emission** — the producer pushes notifications: `next(value)` zero or more times, then optionally one `error(err)` or one `complete()`.
4. **Teardown** — cleanup runs once, triggered by the first terminal notification *or* by `unsubscribe()`.

### The contract grammar

An Observable may only deliver notifications in this sequence:

```text
next* (error | complete)?
```

That is: any number of `next` values, optionally followed by **exactly one** terminal notification — either `error` or `complete`, never both. This is the guarantee every operator and consumer relies on.

### Lazy and unicast (cold) by default

Two properties define a plain Observable:

- **Lazy:** the producer function runs only on `subscribe()`. An Observable you never subscribe to does nothing.
- **Unicast / cold:** each `subscribe()` runs the producer **again**, independently. Two subscribers do not share one execution — they each get their own. An Observable that fetches data will fetch **once per subscriber**.

```ts
import { Observable } from 'rxjs';

const random$ = new Observable<number>((subscriber) => {
  console.log('producer run');       // side effect: runs per subscribe
  subscriber.next(Math.random());
  subscriber.complete();
});

random$.subscribe((n) => console.log('A', n)); // producer run / A 0.13
random$.subscribe((n) => console.log('B', n)); // producer run / B 0.88
```

"producer run" prints **twice**, and A and B get **different** numbers — proof that the execution is not shared. (Sharing one execution across subscribers is *multicasting*, covered later with Subjects.)

### Terminal notifications are final — and enforced

Once `error` or `complete` fires, the Subscriber marks itself stopped and **ignores** any further `next`, `error`, or `complete` from the producer. The contract isn't just a guideline; RxJS enforces it for you.

```ts
const guarded$ = new Observable<number>((subscriber) => {
  subscriber.next(1);
  subscriber.complete();
  subscriber.next(2);      // ignored — the subscriber already stopped
  subscriber.complete();   // ignored
});

guarded$.subscribe({ next: console.log, complete: () => console.log('done') });
// 1 / done   — the 2 never arrives
```

### Teardown: exactly once, on the way out

The function you **return** from the producer is the teardown. It runs exactly once — when the stream completes, errors, or is unsubscribed — making it the right place to release timers, listeners, and sockets.

```ts
const ticks$ = new Observable<number>((subscriber) => {
  let n = 0;
  const id = setInterval(() => subscriber.next(n++), 1000);
  return () => {
    clearInterval(id);       // teardown
    console.log('torn down');
  };
});
```

# Key Terminology

- **Observable contract** — the rules an Observable must follow, summarised by `next* (error | complete)?`.
- **Observable execution** — the run started by a single `subscribe()` call.
- **Lazy** — no work until subscription.
- **Unicast / cold** — each subscription gets its own independent execution (its own producer run).
- **Subscriber** — the internal object RxJS gives the producer; it enforces the contract and delivers notifications to your Observer.
- **Teardown (finalizer)** — cleanup logic run once when the execution ends by completion, error, or unsubscription.
- **Terminal notification** — `error` or `complete`.

# Syntax Overview

```ts
import { Observable } from 'rxjs';

const source$ = new Observable<number>((subscriber) => {
  // --- emission phase ---
  subscriber.next(1);
  subscriber.next(2);
  subscriber.complete();          // terminal: nothing after this is delivered

  // --- teardown phase ---
  return () => {
    // runs once on complete / error / unsubscribe
  };
});

const sub = source$.subscribe({
  next: (v) => console.log(v),
  error: (e) => console.error(e),
  complete: () => console.log('complete'),
});

sub.unsubscribe(); // would also trigger teardown if the stream were still open
```

# Real World Analogy

An Observable is a **recipe**; subscribing is **cooking it**. Writing the recipe (creation) makes nothing to eat — it's lazy. Every time someone cooks it (subscribes), they buy their own ingredients and produce their own meal (unicast/cold): two cooks don't share one pan. Once the dish is **served** (complete) or the kitchen **catches fire** (error), cooking stops and you clean up the station exactly once (teardown) — and you certainly can't serve more food after the meal's already over (post-terminal notifications ignored).

# Examples

## Example 1 — Basic: proving cold/unicast execution

```ts
import { Observable } from 'rxjs';

let subscribes = 0;
const source$ = new Observable<number>((subscriber) => {
  subscribes++;
  subscriber.next(subscribes);
  subscriber.complete();
});

source$.subscribe((v) => console.log('sub1 got', v)); // sub1 got 1
source$.subscribe((v) => console.log('sub2 got', v)); // sub2 got 2
console.log('producer ran', subscribes, 'times');      // producer ran 2 times
```

**Why this is good:** The counter makes "runs per subscribe" undeniable. Understanding that each subscriber triggers a fresh execution explains duplicated HTTP calls and sets up the need for multicasting later.

## Example 2 — Real-world: teardown that always runs

```ts
import { Observable } from 'rxjs';

const connection$ = new Observable<string>((subscriber) => {
  console.log('open socket');
  const id = setInterval(() => subscriber.next('message'), 1000);
  return () => {
    clearInterval(id);
    console.log('close socket'); // teardown
  };
});

const sub = connection$.subscribe((msg) => console.log(msg));
setTimeout(() => sub.unsubscribe(), 3500);
// open socket / message / message / message / close socket
```

**Why this is good:** It models a real resource (a connection). The teardown pairs with the setup so the socket is always closed — whether the stream completes, errors, or is unsubscribed.

## Example 3 — Best practice: the contract protects your consumer

```ts
import { Observable } from 'rxjs';

const safe$ = new Observable<number>((subscriber) => {
  subscriber.next(1);
  subscriber.error(new Error('failed'));
  subscriber.next(2);              // ignored — already errored
  subscriber.complete();           // ignored — already terminated
});

safe$.subscribe({
  next: (v) => console.log('next', v),
  error: (e) => console.log('error', e.message),
});
// next 1 / error failed   — no "next 2", no completion
```

**Why this is good:** It demonstrates that even a buggy producer can't violate the contract from your Observer's point of view — RxJS guarantees nothing arrives after a terminal notification, so your handlers stay simple.

# Common Mistakes

- **Assuming subscribers share one execution.** By default they don't — the producer re-runs per subscribe (cold/unicast). **Fix:** if you need one shared run, multicast with `share`/`Subject` (a later lesson).
- **Putting a "run-once" side effect in the producer.** It runs on **every** subscribe. **Fix:** move one-time work outside the Observable, or multicast.
- **Expecting values after `complete`/`error`.** Nothing is delivered after a terminal notification. **Fix:** design around `next* (error | complete)?`.
- **Forgetting to return teardown for resources.** Timers and listeners leak on unsubscribe. **Fix:** always return a cleanup function from the producer.
- **Thinking `unsubscribe` and `complete` are the same.** Both trigger teardown, but only `complete` calls your `complete` handler.

# Best Practices

- Treat the producer function as **"runs once per subscription"** and keep it free of unintended shared side effects.
- Always pair resource setup with a **teardown** return so cleanup is guaranteed.
- Rely on the contract: your Observer never needs to guard against post-terminal notifications.
- When two parts of your app must react to the **same** execution, reach for multicasting rather than subscribing twice.
- Keep producers pure and deterministic where you can; push nondeterminism (random, `Date.now`) behind `defer` when freshness matters.

# Summary

- An Observable execution has four phases: **create → subscribe → emit → tear down**.
- The contract is **`next* (error | complete)?`** — many values, then at most one terminal notification.
- Observables are **lazy** (run on subscribe) and **cold/unicast** (each subscription is independent).
- The producer re-runs on **every** subscribe, so side effects and requests happen per subscriber.
- Notifications sent **after** a terminal event are ignored — RxJS enforces the contract.
- **Teardown** runs exactly once on complete, error, or unsubscribe; use it to release resources.

# Flash Cards

Q: What is the Observable contract grammar?
A: `next* (error | complete)?` — zero or more `next` values, then at most one terminal notification (`error` OR `complete`), and nothing after it.

Q: What does "cold" / "unicast" mean for a plain Observable?
A: Each `subscribe()` runs the producer independently, so subscribers do not share one execution — the work (and any request) happens once per subscriber.

Q: If a producer calls `next(2)` after it already called `complete()`, what does the subscriber receive?
A: Nothing — the subscriber has stopped, so notifications after a terminal event are ignored. RxJS enforces the contract.

Q: When does the teardown function returned by the producer run?
A: Exactly once, on the first of: completion, error, or unsubscription.

Q: Why might an HTTP-fetching Observable make the request twice?
A: Because it is cold/unicast — two subscriptions each run the producer, so each triggers its own request. Multicasting shares a single execution.

Q: Do `unsubscribe()` and `complete()` both run teardown?
A: Yes, both trigger teardown — but only `complete()` calls your Observer's `complete` handler; `unsubscribe()` does not.

# Exercises

### Easy

Write an Observable whose producer logs `"subscribed"` and emits one value, then completes. Subscribe to it twice and confirm from the console that `"subscribed"` appears twice — demonstrating cold/unicast behaviour.

### Medium

Create an Observable that starts a `setInterval` emitting a counter and returns a teardown that clears the interval and logs `"cleaned"`. Subscribe, then `unsubscribe()` after 3 seconds. Confirm the teardown runs. Then make a version that calls `complete()` after five ticks and confirm teardown still runs.

### Challenging

Write a producer that calls `next(1)`, then `complete()`, then `next(2)` and `error(new Error('late'))`. Subscribe with all three handlers. Predict the exact console output before running, then verify that only `next 1` and the completion appear — proving the contract ignores everything after the first terminal notification.

# Further Reading

- [RxJS — Observable (Observable execution)](https://rxjs.dev/guide/observable)
- [ReactiveX — The Observable Contract](https://reactivex.io/documentation/contract.html)
- [RxJS — Subscription](https://rxjs.dev/guide/subscription)
- [RxJS — Glossary and Semantics](https://rxjs.dev/guide/glossary-and-semantics)
