---
id: lesson-01
slug: what-is-rxjs
title: "What Is RxJS? Observables and Reactive Thinking"
level: beginner
order: 01
duration: 22
tags:
  - rxjs
  - observable
  - reactive
  - streams
  - fundamentals
---

# Learning Objectives

By the end of this lesson you will be able to:

- Explain what RxJS is and the problem reactive programming solves.
- Define an **Observable** as a lazy push collection of values over time.
- Place Functions, Promises, Iterators, and Observables on the pull/push and single/multiple grid.
- List the four ways an Observable differs from a Promise.
- Create a tiny Observable and subscribe to it.
- Recognise the `$` naming convention and the idea of a "stream".

# Why This Concept Exists

JavaScript is full of things that produce values *over time*: mouse clicks, keystrokes, HTTP responses, WebSocket messages, timers, animation frames. The language gives you a different tool for each one — event listeners, callbacks, Promises, `setInterval` — and each has its own rules for subscribing, cleaning up, error handling, and cancellation.

The moment you need to **combine** these — "search the API, but only after the user stops typing for 300 ms, cancel the previous request, and retry twice on failure" — that patchwork of callbacks becomes very hard to get right.

RxJS (Reactive Extensions for JavaScript) exists to unify all of these under **one abstraction**: the Observable. Once every source is an Observable, the same set of composable operators works on all of them. You stop writing *how* to wire callbacks together and start declaring *what* should happen as values flow.

# Concept Explanation

### An Observable is values over time

An **Observable** represents a collection of future values or events. Instead of returning one value like a function, it can **push** zero, one, or many values to whoever is listening — and it can do so synchronously or spread out over time. When it's finished it can **complete**, or it can signal an **error**.

You can picture a stream as a timeline, a notation RxJS calls a **marble diagram**:

```text
clicks$:  ---c----c--c--------c---->
              (each "c" is one click, time flows left to right)
```

### Push vs. pull, single vs. multiple

Every value producer in JavaScript fits into a 2×2 grid. **Pull** means the consumer decides when to read the next value; **push** means the producer decides when to hand it over.

|          | Single value        | Multiple values       |
| -------- | ------------------- | --------------------- |
| **Pull** | Function            | Iterator / Generator  |
| **Push** | Promise             | **Observable**        |

A Promise is a push system that resolves **one** value. An Observable is a push system that can deliver **many** — that single difference is why it can model event streams, not just one-shot async results.

### The core building blocks

- **Observable** — the stream definition; the thing you can subscribe to.
- **Observer** — the consumer: an object with `next`, `error`, and `complete` callbacks.
- **Subscription** — the running execution you get back from `subscribe()`; call `unsubscribe()` to cancel it.
- **Operators** — pure functions like `map` and `filter` that build new Observables from existing ones.
- **Subject** — a special Observable that can multicast to many Observers (a later lesson).

### A first Observable

```ts
import { Observable } from 'rxjs';

const hello$ = new Observable<string>((subscriber) => {
  subscriber.next('Hello');
  subscriber.next('RxJS');
  subscriber.complete();
});

hello$.subscribe((value) => console.log(value));
// Logs: "Hello" then "RxJS"
```

In practice you rarely call `new Observable` — creation functions like `of` and `fromEvent` (next lesson) build them for you — but seeing it once demystifies the whole library.

### Observable vs. Promise — four differences

1. **Lazy vs. eager.** A Promise starts its work the moment it's created. An Observable does **nothing** until you subscribe — subscribing is what runs the producer function above.
2. **Many vs. one.** A Promise settles with a single value. An Observable can emit any number of values before it completes.
3. **Cancellable.** `unsubscribe()` stops an Observable and runs its teardown. A Promise cannot be cancelled.
4. **Sync or async.** A Promise's `.then` is always delivered asynchronously (on the microtask queue). An Observable can emit synchronously (as `hello$` does) or asynchronously.

# Key Terminology

- **RxJS** — the JavaScript implementation of ReactiveX; a library for composing asynchronous and event-based programs with Observables.
- **Observable** — a lazy push collection of zero or more values, delivered over time, that can complete or error.
- **Observer** — the consumer object with `next` / `error` / `complete` handlers.
- **Subscription** — a running Observable execution; disposable via `unsubscribe()`.
- **Stream** — informal term for the sequence of values an Observable produces.
- **Emission** — a single value pushed by an Observable (a `next` notification).
- **Marble diagram** — a visual timeline of a stream's emissions used throughout RxJS docs.
- **`$` suffix** — a naming convention marking a variable as an Observable (e.g. `user$`).

# Syntax Overview

```ts
import { Observable } from 'rxjs';

// 1. An Observable is defined by a subscribe function that receives a subscriber.
const numbers$ = new Observable<number>((subscriber) => {
  subscriber.next(1);
  subscriber.next(2);
  subscriber.next(3);
  subscriber.complete(); // no more values will be sent

  // Optional teardown: returned function runs on complete, error, or unsubscribe.
  return () => console.log('cleaned up');
});

// 2. Nothing has run yet. Subscribing starts the execution.
const subscription = numbers$.subscribe({
  next: (n) => console.log('value', n),
  error: (err) => console.error(err),
  complete: () => console.log('done'),
});

// 3. You can cancel at any time (irrelevant here since it already completed).
subscription.unsubscribe();
```

# Real World Analogy

An Observable is like **subscribing to a magazine**. Defining the Observable is like the magazine *existing* — but no issues land on your doormat until you actually **subscribe**. Once subscribed, issues (values) arrive over time on the publisher's schedule, not yours (that's "push"). The magazine may eventually end its run (**complete**), or the publisher may go bankrupt mid-run (**error**). And you can **cancel** your subscription whenever you like and the deliveries stop. A Promise, by contrast, is like ordering a single book: it's already being shipped the moment you order, exactly one arrives, and you can't call it back.

# Examples

## Example 1 — Basic: A synchronous stream of values

```ts
import { Observable } from 'rxjs';

const colors$ = new Observable<string>((subscriber) => {
  subscriber.next('red');
  subscriber.next('green');
  subscriber.next('blue');
  subscriber.complete();
});

colors$.subscribe({
  next: (color) => console.log(color),
  complete: () => console.log('no more colors'),
});
// red / green / blue / no more colors  — all synchronously
```

**Why this is good:** It shows the whole lifecycle in miniature — multiple `next` values followed by a single `complete` — and proves that an Observable can be entirely synchronous, unlike a Promise.

## Example 2 — Real-world: Multiple values arriving over time

```ts
import { Observable } from 'rxjs';

// Emits the current time every second, forever, until unsubscribed.
const clock$ = new Observable<Date>((subscriber) => {
  const id = setInterval(() => subscriber.next(new Date()), 1000);
  // Teardown stops the timer when the consumer unsubscribes.
  return () => clearInterval(id);
});

const sub = clock$.subscribe((now) => console.log(now.toLocaleTimeString()));

// Later, stop receiving updates and clear the interval:
setTimeout(() => sub.unsubscribe(), 5000);
```

**Why this is good:** A Promise cannot express "a value every second." This is the Observable's home turf — many values over time — and the teardown function shows how cancellation prevents a leaked timer.

## Example 3 — Best practice: Laziness in action

```ts
import { Observable } from 'rxjs';

const effect$ = new Observable<number>((subscriber) => {
  console.log('producer running'); // side effect inside the Observable
  subscriber.next(42);
  subscriber.complete();
});

console.log('before subscribe');
// "producer running" has NOT printed yet — the Observable is lazy.

effect$.subscribe((n) => console.log('got', n));
// Now it prints: "producer running" then "got 42"
```

**Why this is good:** It makes laziness concrete. The producer only runs on `subscribe()`, which means an unused Observable costs nothing and each subscription gets its own fresh run — the foundation for everything in later lessons.

# Common Mistakes

- **Assuming an Observable runs when created.** It doesn't — the producer function only runs on `subscribe()`. **Fix:** remember that creating an Observable is free; subscribing is what does work.
- **Treating an Observable like a Promise that resolves once.** It can emit many values, and it may never complete. **Fix:** handle `next` for *each* value, and don't assume there's exactly one.
- **Forgetting to subscribe.** Building a beautiful pipeline and never subscribing means nothing happens. **Fix:** something must subscribe (directly, or a framework binding like Angular's `async` pipe).
- **Expecting a return value.** `observable.subscribe()` returns a `Subscription`, not the emitted data. **Fix:** read values inside the `next` callback, not from the return value.

# Best Practices

- Suffix Observable variables with `$` (`results$`, `clicks$`) so streams are obvious at a glance.
- Think in **streams**: ask "what are the values, and when do they arrive?" before reaching for an operator.
- Keep the producer function focused on producing; put display and other side effects in the subscriber (or in `tap`, a later lesson).
- Reach for Observables when you have events or multiple/cancellable async values; a plain Promise is still fine for a single one-shot request.
- Install the current major version with `npm install rxjs` (this course targets **RxJS 7+**).

# Summary

- **RxJS** unifies events, timers, and async results under a single abstraction, the **Observable**.
- An **Observable** is a *lazy*, *push*-based collection of *multiple* values over time that can **complete** or **error**.
- On the producer grid, an Observable is the **push + multiple** cell; a Promise is **push + single**.
- Compared to a Promise, an Observable is **lazy**, emits **many** values, is **cancellable**, and can be **synchronous or asynchronous**.
- The core pieces are the **Observable**, the **Observer** (`next`/`error`/`complete`), the **Subscription**, and **operators**.
- **Nothing runs until you subscribe**, and each subscription starts a fresh execution.

# Flash Cards

Q: In one sentence, what is an Observable?
A: A lazy, push-based collection of zero or more values delivered over time that can then complete or error.

Q: Which cell of the pull/push × single/multiple grid does an Observable occupy?
A: Push + multiple. (Promise is push + single; Function is pull + single; Iterator is pull + multiple.)

Q: Name two ways an Observable differs from a Promise.
A: An Observable is lazy (runs only on subscribe) and can emit many values; it is also cancellable and may be synchronous. A Promise is eager, single-value, and always async.

Q: When does the producer function passed to `new Observable(...)` actually run?
A: Only when something calls `subscribe()` — Observables are lazy, so creating one does no work.

Q: What does `observable.subscribe(...)` return, and how do you read the emitted values?
A: It returns a `Subscription` (used to `unsubscribe()`); you read values inside the `next` callback, not from the return value.

Q: What does the `$` at the end of a variable name like `user$` signify?
A: A naming convention indicating the variable holds an Observable (a stream).

# Exercises

### Easy

Create an Observable called `greeting$` that emits the three strings `"Hi"`, `"there"`, and `"friend"` and then completes. Subscribe to it and log each value plus a final `"done"` message.

### Medium

Write an Observable `countdown$` that, when subscribed, emits `3`, `2`, `1` one second apart and then completes. Use `setTimeout`/`setInterval` inside the producer, and return a teardown function that clears the timer. Subscribe and log each number.

### Challenging

Demonstrate laziness and independence: define one Observable whose producer logs `"start"` and emits a random number. Subscribe to it **twice**. Explain (in a comment) why `"start"` prints twice and why each subscriber can receive a different random number. Then create a version that unsubscribes after the first value.

# Further Reading

- [RxJS — Overview](https://rxjs.dev/guide/overview)
- [RxJS — Observable](https://rxjs.dev/guide/observable)
- [RxJS — Installation](https://rxjs.dev/guide/installation)
- [ReactiveX — Observable](https://reactivex.io/documentation/observable.html)
- [MDN — Using Promises](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/Using_promises)
