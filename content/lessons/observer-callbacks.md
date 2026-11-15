---
slug: observer-callbacks
title: "The Observer: next, error, and complete"
topic: observables-subscriptions
kind: concept
summary: "An Observer is an object with optional next, error, and complete callbacks that receive the three kinds of notifications an Observable can push."
tags: [rxjs, observer, next, error, complete]
related: [ways-to-subscribe, subscription-and-unsubscribe, observable-contract-grammar]
---
> **In one line:** an **Observer** is the consumer object with `next`, `error`, and `complete` callbacks that define how you react to each notification from an Observable.

## The idea

An Observable is only a description of work. When you subscribe to it, you supply an **Observer** — an object with up to three handler methods, one for each kind of notification an Observable can push:

| Callback | Signature | How often it fires | Meaning |
|---|---|---|---|
| **`next`** | `(value: T) => void` | Zero, one, or many times | A new value was emitted by the stream |
| **`error`** | `(err: any) => void` | At most **once** (terminal) | The stream failed with `err` and is now stopped |
| **`complete`** | `() => void` | At most **once** (terminal, takes no argument) | The stream finished cleanly and will emit nothing more |

`error` and `complete` are **mutually exclusive** and **terminal**: a stream can either complete cleanly or fail with an error, never both, and no `next` values can ever arrive after either one fires.

## See it

Here is a full Observer object receiving values and the final completion notification from `of()`:

```typescript
import { of } from 'rxjs';

const observer = {
  next: (letter: string) => console.log('next:', letter),
  error: (err: unknown) => console.error('error:', err),
  complete: () => console.log('complete: stream finished'),
};

of('a', 'b', 'c').subscribe(observer);
// Output:
// next: a
// next: b
// next: c
// complete: stream finished
```

Because `of('a', 'b', 'c')` succeeds, `next` runs three times, `complete` runs once at the end, and `error` never runs.

## Go deeper

What happens if the Observable errors midway through? As soon as `error` fires, the execution stops and `complete` is never called:

```typescript
import { Observable } from 'rxjs';

const flaky$ = new Observable<number>((subscriber) => {
  subscriber.next(1);
  subscriber.error(new Error('disk full'));
});

flaky$.subscribe({
  next: (n) => console.log('got:', n),
  error: (e: Error) => console.error('failed:', e.message),
  complete: () => console.log('never runs'),
});
// Output: got: 1 / failed: disk full
```

Think of subscribing like following a podcast: `next` is listening to each new episode, `complete` is the host announcing the show's finale, and `error` is the feed breaking.

## Watch out

- `complete()` receives **no arguments** — the final value was already delivered via the last `next(value)` call.
- Omitting an `error` callback on a stream that can fail leaves the error unhandled, which RxJS rethrows asynchronously to the global error handler.

## Remember

- An Observer has three optional callbacks: `next(value)`, `error(err)`, and `complete()`.
- `next` can run any number of times; `error` and `complete` run at most once.
- `error` and `complete` are mutually exclusive terminal events.
