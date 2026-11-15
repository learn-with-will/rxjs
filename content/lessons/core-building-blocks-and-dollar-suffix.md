---
slug: core-building-blocks-and-dollar-suffix
title: "Core RxJS building blocks and the $ suffix"
topic: observables-subscriptions
kind: concept
summary: "RxJS programs are built from five pieces — Observable, Observer, Subscription, Operators, and Subject — and use a trailing $ to mark stream variables."
tags: [rxjs, fundamentals, naming, conventions]
related: [what-is-an-observable, observer-callbacks, subscription-and-unsubscribe]
---
> **In one line:** every RxJS program is built from five pieces — **Observable**, **Observer**, **Subscription**, **Operators**, and **Subject** — and by convention Observable variables end with `$`.

## The idea

Before diving into individual operators, it helps to know how the five core pieces of RxJS fit together and how RxJS code is named in the wild:

| Building block | Role in a stream |
|---|---|
| **`Observable`** | The stream definition — a lazy push producer of values over time |
| **`Observer`** | The consumer object with `next`, `error`, and `complete` callbacks |
| **`Subscription`** | The running execution returned by `subscribe()`; call `unsubscribe()` to cancel it |
| **`Operators`** | Pure functions (`map`, `filter`, `switchMap`) that transform a stream inside `.pipe()` |
| **`Subject`** | A special multicast Observable that can push values to many Observers at once |

In community code and official examples, variables holding an Observable almost always end with a trailing dollar sign (`$`), a style sometimes called **Finnish notation** — for example `clicks$`, `user$`, or `searchResults$`. TypeScript types already tell the compiler what a variable is, but the `$` suffix tells *human readers* at a glance that a variable is a stream you subscribe to or pipe, not a plain synchronous value.

## See it

Here are the building blocks working together in a small snippet:

```typescript
import { Observable, Subscription } from 'rxjs';

// 1. Observable (named with a trailing $)
const greetings$: Observable<string> = new Observable((subscriber) => {
  subscriber.next('Hello');
  subscriber.next('RxJS');
  subscriber.complete();
});

// 2. Observer object with callbacks
const observer = {
  next: (msg: string) => console.log('got:', msg),
  error: (err: unknown) => console.error('failed:', err),
  complete: () => console.log('stream complete'),
};

// 3. Subscription returned by subscribe()
const sub: Subscription = greetings$.subscribe(observer);
sub.unsubscribe();
```

## Go deeper

When reading unfamiliar RxJS code, trace the types: start at the source `foo$` (the Observable), follow its `.pipe(...)` stages (the Operators), and find where `.subscribe(...)` creates the `Subscription` and hands values to the `Observer`. Keeping the `$` suffix on stream variables — and leaving it off plain values and Subscriptions — makes that trace effortless.

## Watch out

- Do not put a `$` suffix on a `Subscription` variable (name it `sub` or `subscription`), or readers will mistake the handle for an Observable they can `.pipe()`.
- Remember that `greetings$.subscribe()` returns a `Subscription`, not another `Observable` and not the emitted data.

## Remember

- Observable defines the stream; Observer consumes it; Subscription cancels it.
- Operators transform streams purely, and Subjects multicast to multiple listeners.
- Suffix Observable variables with `$` (`users$`, `clicks$`) so streams stand out immediately.
