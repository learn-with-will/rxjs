---
slug: composing-custom-operators-with-pipe
title: "Composing custom operators with standalone pipe()"
topic: custom-operators-batching
kind: pattern
summary: "Build reusable, type-safe custom operators like filterNil(), debug(), and retryWithBackoff() by combining built-in operators with standalone pipe()."
tags: [rxjs, custom-operators, pipe, composition, typescript]
related: [operator-function-shape, standalone-pipe-for-reuse, low-level-operators-with-new-observable]
---
> **In one line:** 95% of custom operators should be built by composing existing operators with standalone `pipe(op1(), op2(), ...)` from `'rxjs'` — giving you automatic subscription forwarding and teardown for free.

## The idea

Before writing a low-level `new Observable(subscriber => ...)` operator from scratch, ask: *can I express this as a combination of built-in RxJS operators?* When you compose built-in operators using the standalone `pipe()` helper (or `source$ => source$.pipe(...)`), RxJS automatically handles:
- Forwarding `next`, `error`, and `complete` notifications
- Unsubscribing from inner and upstream streams on teardown
- Maintaining TypeScript type inference across every step

Here are three battle-tested custom operators you can drop into any project:

| Custom operator | Composed from | What it solves |
|---|---|---|
| **`filterNil<T>()`** | `filter((x): x is NonNullable<T> => x != null)` | Strips both `null` and `undefined` and narrows the TypeScript type to `NonNullable<T>` |
| **`debug<T>(tag)`** | `tap({ next, error, complete, subscribe, unsubscribe })` | Logs every lifecycle event of a stream stage with a label |
| **`retryBackoff(max, ms)`** | `retry({ count: max, delay: (e, i) => timer(ms * 2 ** (i - 1)) })` | Reusable exponential backoff policy across HTTP pipelines |

## See it

Implement `filterNil()` and `debug(tag)` using standalone `pipe()`:

```typescript
import { pipe, filter, tap, OperatorFunction, MonoTypeOperatorFunction, of } from 'rxjs';

// 1. Narrows Observable<T | null | undefined> -> Observable<T>
export function filterNil<T>(): OperatorFunction<T, NonNullable<T>> {
  return pipe(
    filter((value): value is NonNullable<T> => value !== null && value !== undefined)
  );
}

// 2. Logs all 5 lifecycle hooks with a single operator
export function debug<T>(tag: string): MonoTypeOperatorFunction<T> {
  return pipe(tap({
    subscribe: () => console.log(`[${tag}] subscribe`),
    next: val => console.log(`[${tag}] next:`, val),
    error: err => console.log(`[${tag}] error:`, err),
    complete: () => console.log(`[${tag}] complete`),
    unsubscribe: () => console.log(`[${tag}] unsubscribe`),
  }));
}

of<string | null>('Ada', null, 'Grace').pipe(
  filterNil(),
  debug('Names') // TypeScript knows val is strictly 'string' here!
).subscribe();
```

## Go deeper

What is the difference between returning `pipe(op1(), op2())` and returning `(source$) => source$.pipe(op1(), op2())`? Functionally they are identical because standalone `pipe(f, g)` returns a function `(x) => g(f(x))`. However, if your custom operator needs to create **per-subscription state** (like a counter or timestamp initialized fresh on every `subscribe()`), wrap it in `defer(() => { const state = ...; return source$.pipe(...); })` so multiple subscribers never collide!

## Watch out

- If your composed operator initializes mutable variables in the outer factory function (outside `defer`), multiple subscribers to the same piped Observable will accidentally share and corrupt that variable.
- Keep custom operators focused on a single concern so they remain composable.

## Remember

- Prefer standalone `pipe(op1(), op2())` over `new Observable()` whenever your logic can be expressed with existing operators.
- Use a TypeScript type predicate `(v): v is NonNullable<T> => v != null` inside `filterNil()` for automatic type narrowing.
- Wrap stateful composed operators in `defer(() => ...)` so state is isolated per subscription.
