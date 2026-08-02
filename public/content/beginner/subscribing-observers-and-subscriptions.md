---
id: lesson-03
slug: subscribing-observers-and-subscriptions
title: "Subscribing: Observers and Subscriptions"
level: beginner
order: 03
duration: 22
tags:
  - rxjs
  - subscribe
  - observer
  - subscription
  - unsubscribe
---

# Learning Objectives

By the end of this lesson you will be able to:

- Subscribe to an Observable with a full or partial **Observer** object.
- Distinguish the three Observer callbacks: `next`, `error`, and `complete`.
- Explain what `subscribe()` returns and how to cancel it with `unsubscribe()`.
- Understand that `error` and `complete` are **terminal** and mutually exclusive.
- Group child subscriptions with `subscription.add()` for tidy teardown.
- Avoid the deprecated positional-callback form of `subscribe`.

# Why This Concept Exists

An Observable is only a *description* of work. **Subscribing** is what runs it — and running work means you now own a resource that must eventually be released. The Observer defines *what to do* with each value, and the Subscription is the *handle* that lets you stop the work. Getting subscribing and unsubscribing right is the difference between a clean app and one that leaks timers, listeners, and network calls.

# Concept Explanation

### The Observer: three callbacks

An **Observer** is an object with up to three handlers. All are optional:

```ts
const observer = {
  next: (value) => console.log('value:', value),
  error: (err) => console.error('error:', err),
  complete: () => console.log('complete'),
};
```

- **`next(value)`** is called once per emitted value — zero or more times.
- **`error(err)`** is called at most once, with the error, and then the stream is over.
- **`complete()`** is called at most once, with no argument, when the stream finishes successfully.

### Ways to subscribe

You can pass a full observer object, a partial one, or just a `next` function:

```ts
source$.subscribe({ next: (v) => console.log(v), error: console.error, complete: () => {} });
source$.subscribe({ next: (v) => console.log(v) }); // partial: only next
source$.subscribe((v) => console.log(v));           // shorthand: a next function
```

> Passing **separate positional callbacks** — `subscribe(nextFn, errorFn, completeFn)` — is **deprecated** in RxJS 7. Always use the observer object or a single `next` function.

### The Subscription and `unsubscribe`

`subscribe()` returns a **Subscription** — a handle to the running execution:

```ts
import { interval } from 'rxjs';

const sub = interval(1000).subscribe((n) => console.log(n));
// ...later
sub.unsubscribe(); // stops emissions and runs the Observable's teardown
```

`unsubscribe()` cancels the execution and triggers any teardown (clearing timers, removing event listeners). After it, no further `next`/`complete` will fire. A Subscription also exposes `closed: boolean`.

### Terminal notifications close the subscription

`error` and `complete` are **terminal** and **mutually exclusive** — a stream can do one or the other, never both, and never anything after. When either fires, RxJS **automatically** runs teardown and closes the subscription, so you don't need to unsubscribe manually for a stream that completes.

```text
next* (error | complete)?      ← the only legal grammar
```

Note: `unsubscribe()` does **not** call your `complete` handler — completion and cancellation are different events.

### Grouping subscriptions with `add`

A parent Subscription can adopt children with `add()`. Unsubscribing the parent unsubscribes every child too — a simple way to tear down several streams together.

```ts
const group = new Subscription();
group.add(stream1$.subscribe(/* ... */));
group.add(stream2$.subscribe(/* ... */));
group.unsubscribe(); // cancels both stream1 and stream2
```

# Key Terminology

- **Observer** — the consumer object with optional `next`, `error`, and `complete` methods.
- **`next` / `error` / `complete`** — the three notification types an Observable can send.
- **Subscription** — the disposable handle returned by `subscribe()`; has `unsubscribe()`, `add()`, and `closed`.
- **Teardown** — cleanup logic (the function returned by the producer) run on complete, error, or unsubscribe.
- **Terminal notification** — `error` or `complete`; after it, the stream sends nothing more.
- **Partial observer** — an observer that supplies only some of the three callbacks.

# Syntax Overview

```ts
import { interval, Subscription } from 'rxjs';

// Full observer object (preferred).
const sub: Subscription = interval(1000).subscribe({
  next: (n) => console.log('tick', n),
  error: (err) => console.error(err),
  complete: () => console.log('done'),
});

// Cancel when you no longer need it.
sub.unsubscribe();
console.log(sub.closed); // true

// Group several subscriptions and tear them all down at once.
const bag = new Subscription();
bag.add(interval(1000).subscribe(console.log));
bag.add(interval(500).subscribe(console.log));
bag.unsubscribe(); // stops both
```

# Real World Analogy

Subscribing is like following a **podcast**. The Observer is your set of reactions: `next` is listening to each new episode, `complete` is the host announcing the show has ended, and `error` is the feed breaking. The Subscription is the "Following" status in your app — and `unsubscribe()` is tapping **Unfollow**, after which no new episodes download. If the show ends naturally (**complete**), you're automatically unfollowed; but note that unfollowing yourself is *not* the same as the show ending — the host never gets to say goodbye (`complete` doesn't run on `unsubscribe`).

# Examples

## Example 1 — Basic: a full observer

```ts
import { of } from 'rxjs';

of('a', 'b', 'c').subscribe({
  next: (letter) => console.log('next:', letter),
  error: (err) => console.error('error:', err),
  complete: () => console.log('complete'),
});
// next: a / next: b / next: c / complete
```

**Why this is good:** It shows all three callbacks and demonstrates the legal grammar — several `next`s followed by exactly one `complete`, with `error` never firing on a successful stream.

## Example 2 — Real-world: cancel an open-ended stream

```ts
import { fromEvent } from 'rxjs';

const button = document.querySelector('#save')!;
const clicks$ = fromEvent(button, 'click');

const sub = clicks$.subscribe(() => console.log('saved'));

// This stream never completes on its own, so we cancel it when the
// component goes away — otherwise the click listener leaks.
function onDestroy() {
  sub.unsubscribe();
}
```

**Why this is good:** `fromEvent` never completes, so it models the real need for `unsubscribe()`. Cancelling removes the DOM listener via the Observable's teardown — no leak.

## Example 3 — Best practice: one bag for many subscriptions

```ts
import { interval, fromEvent, Subscription } from 'rxjs';

const subscriptions = new Subscription();

subscriptions.add(interval(1000).subscribe((n) => console.log('timer', n)));
subscriptions.add(
  fromEvent(window, 'resize').subscribe(() => console.log('resized')),
);

// A single call tears everything down — no forgotten subscriptions.
function cleanup() {
  subscriptions.unsubscribe();
}
```

**Why this is good:** Collecting subscriptions in one parent means teardown is a single call, so it's hard to forget one. (Later you'll meet `takeUntil`, an even more declarative way to do this.)

# Common Mistakes

- **Never unsubscribing from an open-ended stream.** `interval`, `fromEvent`, and friends keep running forever. **Fix:** store the Subscription and call `unsubscribe()`, or use a completing operator like `take`/`takeUntil`.
- **Expecting `complete` to fire on `unsubscribe`.** It does not — cancellation is not completion. **Fix:** put shared cleanup in `finalize` (a later lesson), which runs on complete, error, *and* unsubscribe.
- **Using the deprecated positional form** `subscribe(next, error, complete)`. **Fix:** pass an observer object `{ next, error, complete }`.
- **Omitting an `error` handler.** An unhandled Observable error is re-thrown and can crash the surrounding context. **Fix:** always provide `error` when the stream can fail.
- **Discarding the Subscription** (`source$.subscribe(...)` with no reference) when you need to cancel later. **Fix:** keep the returned Subscription.

# Best Practices

- Prefer the **observer object** form; it's explicit and future-proof.
- Always handle **`error`** for streams that can fail — errors are terminal and must go somewhere.
- Keep every Subscription you might need to cancel; group them with `add()` or manage them with `takeUntil`.
- Remember the grammar `next* (error | complete)?` — design your handlers around it.
- Don't do cleanup only in `complete`; if the stream can be cancelled, use `finalize` so teardown always runs.

# Summary

- An **Observer** has up to three callbacks: `next` (per value), `error` (once, terminal), `complete` (once, terminal).
- You can subscribe with a full observer, a partial observer, or a single `next` function; **positional callbacks are deprecated**.
- `subscribe()` returns a **Subscription** with `unsubscribe()`, `add()`, and `closed`.
- `error` and `complete` are **mutually exclusive terminal** events; after either, the stream is over and teardown runs automatically.
- `unsubscribe()` cancels work and runs teardown but does **not** call `complete`.
- Group child subscriptions with `add()` so one `unsubscribe()` cleans up many.

# Flash Cards

Q: What are the three Observer callbacks and how often can each fire?
A: `next` (zero or more times, once per value), `error` (at most once, terminal), and `complete` (at most once, terminal).

Q: What does `subscribe()` return and what is it for?
A: A Subscription — a handle to the running execution — whose `unsubscribe()` cancels the work and runs teardown.

Q: Can an Observable both error and complete?
A: No. `error` and `complete` are mutually exclusive terminal notifications; a stream does at most one of them and nothing after.

Q: Does `unsubscribe()` trigger the `complete` callback?
A: No. Unsubscribing cancels the stream and runs teardown, but it does not call `complete` — cancellation and completion are different events.

Q: Which form of `subscribe` is deprecated in RxJS 7?
A: The positional-callbacks form `subscribe(next, error, complete)`. Use an observer object `{ next, error, complete }` (or a single `next` function) instead.

Q: How does `subscription.add(child)` help with cleanup?
A: It adopts the child subscription so that unsubscribing the parent also unsubscribes the child — one call tears down the whole group.

# Exercises

### Easy

Subscribe to `of(10, 20, 30)` with a full observer that logs each value in `next`, logs `"finished"` in `complete`, and logs errors in `error`. Confirm from the output that `complete` runs once and `error` never runs.

### Medium

Create an `interval(500)` subscription that logs the tick number. Using `setTimeout`, call `unsubscribe()` after 2 seconds. Log `subscription.closed` before and after unsubscribing to observe it flip from `false` to `true`.

### Challenging

Start two streams — `interval(1000)` and a `fromEvent(window, 'click')` — and collect both subscriptions into a single parent `Subscription` with `add()`. Provide a `cleanup()` function that unsubscribes the parent. Then explain in a comment why grouping is safer than tracking each subscription separately, and why neither stream would ever call `complete`.

# Further Reading

- [RxJS — Observer](https://rxjs.dev/guide/observer)
- [RxJS — Subscription](https://rxjs.dev/guide/subscription)
- [RxJS — Observable (subscribing)](https://rxjs.dev/guide/observable)
- [ReactiveX — Observable contract](https://reactivex.io/documentation/observable.html)
