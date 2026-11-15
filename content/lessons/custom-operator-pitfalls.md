---
slug: custom-operator-pitfalls
title: "Custom operator pitfalls: shared state and missing teardowns"
topic: custom-operators-batching
kind: pitfall
summary: "Spot and fix the 4 bugs that break custom RxJS operators: shared closure state, dropped teardown returns, swallowed completions, and uncaught callback throws."
tags: [rxjs, custom-operators, pitfalls, memory-leaks, teardown]
related: [low-level-operators-with-new-observable, composing-custom-operators-with-pipe, operator-function-shape]
---
> **In one line:** declaring state outside `new Observable()`, forgetting `return () => sub.unsubscribe()`, or dropping `complete()`/`error()` turns a custom operator into a silent memory leak or state-corruption bug.

## The idea

When reviewing or debugging a custom operator, check for these four classic mistakes:

| Bug | What goes wrong | The fix |
|---|---|---|
| **1. State declared outside `new Observable()`** | Two components subscribe to the same piped stream and share the same `count` or `buffer` variable, corrupting each other's emissions | Move `let count = 0` **inside** `new Observable(subscriber => { let count = 0; ... })` (or inside `defer()`) |
| **2. Missing `return () => sub.unsubscribe()`** | Downstream `take(1)` or `takeUntil(destroy$)` unsubscribes, but `source$` keeps running forever in the background | Always `return () => { sub.unsubscribe(); clearTimeout(id); };` at the end of `new Observable` |
| **3. Forgetting `subscriber.complete()` or `subscriber.error(err)`** | Downstream `toArray()`, `lastValueFrom()`, or `concatMap()` hangs forever waiting for completion | Always wire `error: err => subscriber.error(err)` and `complete: () => subscriber.complete()` |
| **4. Uncaught throw inside a user callback** | `fn(value)` throws synchronously inside `next()`, crashing the caller without notifying `subscriber.error(err)` | Wrap `fn(value)` in `try / catch (err) { subscriber.error(err); }` |

## See it

Compare a broken stateful operator that leaks state and subscriptions against the fixed version:

```typescript
import { Observable } from 'rxjs';

// ❌ Before: count is shared across all subscribers & upstream is never unsubscribed!
export function brokenRunningTotal() {
  let total = 0; // BUG 1: Shared across all subscribers!
  return (source$: Observable<number>) =>
    new Observable<number>(subscriber => {
      // BUG 2: No error/complete forwarding, no teardown returned!
      source$.subscribe(n => { total += n; subscriber.next(total); });
    });
}

// ✅ After: isolated per-subscriber state, full forwarding, and returned teardown
export function runningTotal() {
  return (source$: Observable<number>) =>
    new Observable<number>(subscriber => {
      let total = 0; // Fresh per subscriber!
      const sub = source$.subscribe({
        next: n => { total += n; subscriber.next(total); },
        error: err => subscriber.error(err),
        complete: () => subscriber.complete(),
      });
      return () => sub.unsubscribe();
    });
}
```

## Go deeper

Look at `runningTotal()` above — could we have written it without `new Observable()` at all? Yes! `scan((total, n) => total + n, 0)` already isolates `total` per subscription, forwards `error` and `complete`, and tears down upstream subscriptions automatically. That is why Rule #1 of custom operators is: **compose built-in operators with `pipe()` first**, and only write `new Observable()` when built-in operators cannot express the behavior.

## Watch out

- If your custom operator starts any auxiliary resource (a `setTimeout`, `AbortController`, or `addEventListener`), clean that up inside the returned teardown function right alongside `sub.unsubscribe()`.
- Test every custom operator with two sequential or concurrent subscribers in `TestScheduler` to verify state isolation and `expectSubscriptions` teardown.

## Remember

- Never allocate mutable per-stream state outside `new Observable(subscriber => ...)` or `defer(() => ...)`.
- Always forward all three notifications (`next`, `error`, `complete`) unless your operator intentionally filters or transforms them.
- Always return a teardown function that unsubscribes from `source$` and cleans up any timers or listeners.
