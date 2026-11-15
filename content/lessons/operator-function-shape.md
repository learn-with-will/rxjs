---
slug: operator-function-shape
title: "The shape of an RxJS operator: OperatorFunction<T, R>"
topic: custom-operators-batching
kind: concept
summary: "Understand why every pipeable operator in RxJS is simply a higher-order function returning (source$: Observable<T>) => Observable<R>."
tags: [rxjs, custom-operators, operatorfunction, monotypeoperatorfunction, typescript]
related: [what-is-a-pipeable-operator, composing-custom-operators-with-pipe, low-level-operators-with-new-observable]
---
> **In one line:** a pipeable operator is just a function that takes configuration arguments and returns an `OperatorFunction<T, R>` — a pure function `(source$: Observable<T>) => Observable<R>`.

## The idea

There is no magic class hierarchy behind RxJS pipeable operators. When you write `source$.pipe(op1(), op2())`, `.pipe()` simply passes `source$` into the function returned by `op1()`, passes the resulting Observable into the function returned by `op2()`, and returns the final Observable.

RxJS exports two TypeScript interfaces from `'rxjs'` to type custom operators cleanly:

| TypeScript type | Function signature | When to use it | Built-in examples |
|---|---|---|---|
| **`OperatorFunction<T, R>`** | `(source$: Observable<T>) => Observable<R>` | Input type `T` and output type `R` can **differ** | `map<T, R>`, `switchMap<T, R>`, `scan<T, R>`, `bufferCount<T>` (`T` → `T[]`) |
| **`MonoTypeOperatorFunction<T>`** | `(source$: Observable<T>) => Observable<T>` | Input type `T` and output type `T` are **identical** | `tap<T>`, `delay<T>`, `take<T>`, `debounceTime<T>`, `retry<T>` |

```flow
factory: multiplyBy(factor) | operator factory @blue
opfn: OperatorFunction<number, number> | (source$) => Observable @primary
inobs: source$ | Observable<number> @neutral
outobs: result$ | Observable<number> @green
factory -> opfn: returns
inobs -> opfn: passed by .pipe()
opfn -> outobs: returns
```

## See it

Write both a type-changing `OperatorFunction<T, R>` and a type-preserving `MonoTypeOperatorFunction<T>`:

```typescript
import { Observable, OperatorFunction, MonoTypeOperatorFunction, map, tap, of } from 'rxjs';

// 1. Changes type from T to { value: T; timestamp: number } -> OperatorFunction<T, R>
export function withTimestamp<T>(): OperatorFunction<T, { value: T; timestamp: number }> {
  return (source$: Observable<T>) =>
    source$.pipe(map(value => ({ value, timestamp: Date.now() })));
}

// 2. Preserves type T unchanged -> MonoTypeOperatorFunction<T>
export function logWithPrefix<T>(prefix: string): MonoTypeOperatorFunction<T> {
  return (source$: Observable<T>) =>
    source$.pipe(tap(value => console.log(`[${prefix}]`, value)));
}

// 3. Chain both custom operators inside a standard .pipe() call:
of('login', 'purchase').pipe(
  logWithPrefix('RawEvent'),
  withTimestamp(),
  logWithPrefix('StampedEvent')
).subscribe();
```

## Go deeper

Why do we write `withTimestamp()` as a factory function `() => (source$) => ...` instead of passing `withTimestamp` directly to `.pipe(withTimestamp)`? Two reasons:
1. **Consistency:** Every built-in RxJS operator is called with parentheses (`first()`, `share()`, `withTimestamp()`), even when it takes zero arguments, and can later accept optional configuration arguments (such as a custom clock function `withTimestamp(clockFn)`) without breaking any existing call sites across your application.
2. **Per-pipeline state:** If an operator needs closure state or options, calling the factory function creates a fresh operator instance for each `.pipe()` call. Furthermore, TypeScript's generic inference across multi-step `.pipe(op1(), op2(), op3())` overloads is specifically designed around functions returning `OperatorFunction<T, R>`, ensuring that each operator's output type `R` automatically flows into the next operator's input type `T`.

## Watch out

- If an operator narrows a union type (like filtering out `null | undefined`), use `OperatorFunction<T, NonNullable<T>>` rather than `MonoTypeOperatorFunction<T>` so downstream operators get the narrowed type!
- Never subscribe to `source$` inside the outer factory function — only subscribe when the returned Observable is subscribed to (or compose via `source$.pipe(...)`).

## Remember

- Every pipeable operator is a function returning `(source$: Observable<T>) => Observable<R>`.
- Use `OperatorFunction<T, R>` when the emitted type changes (or narrows) and `MonoTypeOperatorFunction<T>` when `T` stays the same.
- Compose existing operators whenever possible before reaching for `new Observable()`.
