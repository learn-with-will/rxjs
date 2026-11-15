---
slug: observable-contract-grammar
title: "The Observable contract: next* (error | complete)?"
topic: observables-subscriptions
kind: concept
summary: "Every Observable obeys the grammar next* (error | complete)?: zero or more next notifications followed optionally by either one error or one complete."
tags: [rxjs, contract, observable, lifecycle]
related: [observable-execution-phases, post-terminal-notifications-ignored, observer-callbacks]
---
> **In one line:** the Observable contract states that a stream may emit **zero or more `next` values**, optionally followed by **either one `error` or one `complete`**, and nothing after that: `next* (error | complete)?`.

## The idea

Every operator, Subject, and consumer in RxJS relies on one foundational rule called the **Observable contract**. In regular-expression notation, the legal sequence of notifications from any Observable execution is:

```text
next* (error | complete)?
```

Breaking that tiny expression down piece by piece:

- **`next*`** — `next` can fire **zero, one, or any number of times** (even infinitely many times on an open-ended stream).
- **`(error | complete)?`** — the stream may optionally end with a single terminal notification: **either** `error` **or** `complete`, never both.
- **`?` (optional)** — a stream is not required to terminate at all; streams like `interval(1000)` or `fromEvent(document, 'click')` emit `next` values forever until unsubscribed.

```tree
What can a stream emit?
  Zero or more next(value)
    [Open] Keeps running | never terminates (interval, fromEvent) @blue
    [Done] One complete() | finished cleanly; stream closes @green
    [Fail] One error(err) | failed; stream closes @red
```

## See it

Here are four valid streams that each illustrate a different path through the `next* (error | complete)?` grammar:

```typescript
import { of, EMPTY, NEVER, throwError } from 'rxjs';

// 1. Three nexts, then one complete: next next next complete
of(1, 2, 3).subscribe({
  next: (n) => console.log('of next:', n),
  complete: () => console.log('of complete'),
});

// 2. Zero nexts, then one complete: complete
EMPTY.subscribe({ complete: () => console.log('EMPTY complete') });

// 3. Zero nexts, then one error: error
throwError(() => new Error('boom')).subscribe({
  error: (e: Error) => console.log('throwError:', e.message),
});

// 4. Zero nexts, zero terminal events: stays open until unsubscribed
const sub = NEVER.subscribe();
sub.unsubscribe();
```

## Go deeper

Because the contract guarantees that `error` and `complete` are mutually exclusive and final, your Observer handlers never have to defend against impossible states:

- If your `complete` handler ran, you know for certain no `error` will ever fire for that subscription.
- If your `error` handler ran, you know no further `next` values or `complete` signal will ever arrive from that subscription.

## Watch out

- Because an error terminates the stream according to `next* (error | complete)?`, an uncaught error inside a long-lived event pipeline permanently stops the pipeline unless recovered with `catchError`.
- Never write logic that expects `complete()` to run after `error()` — use `finalize()` when you want a callback that runs after *either* terminal notification.

## Remember

- The Observable contract grammar is `next* (error | complete)?`.
- A stream can emit zero or more `next` values, and may never terminate on its own.
- At most one terminal event (`error` OR `complete`) can ever fire, ending the stream permanently.
