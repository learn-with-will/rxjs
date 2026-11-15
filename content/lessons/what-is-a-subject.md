---
slug: what-is-a-subject
title: "What is a Subject? Dual Observer + multicast Observable"
topic: subjects-multicasting
kind: concept
summary: "A Subject is both an Observable and an Observer (with next, error, and complete) that multicasts every event to all currently active subscribers."
tags: [rxjs, subject, multicasting, hot, observer]
related: [behavior-subject, replay-subject, protecting-subjects-with-as-observable, cold-vs-hot-observables]
---
> **In one line:** a **`Subject<T>`** is both an **Observable** and an **Observer** that acts like a live event bus, **multicasting** each `next`, `error`, or `complete` call to all current subscribers simultaneously.

## The idea

A plain `new Observable(subscriber => ...)` is **unicast** and **cold**: the producer logic lives *inside* the constructor callback, each subscriber gets its own private execution, and you cannot push values into it from the outside.

A **`Subject<T>`** flips that around:

1. **It is an `Observable<T>`:** consumers can call `subject.pipe(...).subscribe(...)` to listen.
2. **It is an `Observer<T>`:** your code can imperatively call `subject.next(val)`, `subject.error(err)`, and `subject.complete()` from the outside — or even pass the `Subject` itself into `source$.subscribe(subject)`!
3. **It is hot and multicast:** a Subject maintains an internal list of active subscribers. When you call `subject.next(x)`, every subscriber currently listening receives `x` in that exact instant.
4. **No memory for latecomers:** a plain `Subject` stores no history — any subscriber that joins *after* `subject.next(x)` was called misses `x` completely.

```flow
p: Imperative Code | calls subject.next(42) @primary
s: Subject<number> | hot multicast event bus @amber
s1: Subscriber A (joined early) | receives 42 @green
s2: Subscriber B (joined early) | receives 42 @green
s3: Subscriber C (joined late) | misses 42! @red
p -> s: next(42)
s -> s1: 42
s -> s2: 42
```

## See it

```typescript
import { Subject } from 'rxjs';

const bus$ = new Subject<string>();

// Subscriber A joins first:
bus$.subscribe((msg) => console.log('Sub A:', msg));

bus$.next('Event 1'); // Sub A receives "Event 1"

// Subscriber B joins later (after Event 1 was already emitted):
bus$.subscribe((msg) => console.log('Sub B:', msg));

bus$.next('Event 2'); // BOTH Sub A and Sub B receive "Event 2"
bus$.complete();
```

## Go deeper

What happens if someone subscribes to a plain `Subject` **after** `bus$.complete()` or `bus$.error(err)` has already been called?

- Once a Subject has **completed**, any late subscriber immediately receives **`complete()`** on subscribe (and any future `bus$.next(...)` calls are silently ignored).
- Once a Subject has **errored**, any late subscriber immediately receives that same **`error(err)`** notification on subscribe.

Because a plain `Subject` only emits to subscribers who are already listening at the moment `next()` runs, it is ideal for **discrete action/event streams** (`logoutClicked$`, `toastNotification$`, `destroy$`), and wrong for **current state** where late subscribers need the latest value on subscribe (use `BehaviorSubject` for that!).

You can also use a `Subject<void>` when the event itself carries no payload and only the *timing* of the signal matters — in TypeScript, `new Subject<void>()` lets you call `destroy$.next()` with no arguments.

## Watch out

- Late subscribers to a plain `Subject` miss all values emitted before their `subscribe()` call.
- Do not use a `Subject` when a cold creation function like `fromEvent`, `of`, or `defer` already models the source cleanly.

## Remember

- A `Subject<T>` is both an `Observable` (`.subscribe()`) and an `Observer` (`.next()`, `.error()`, `.complete()`).
- It is **hot** and **multicasts** each emission to all currently active subscribers.
- Plain `Subject` holds no current value — late subscribers only see future emissions.
