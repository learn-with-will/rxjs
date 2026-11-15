---
slug: what-is-a-pipeable-operator
title: "What is a pipeable operator?"
topic: pipes-transforming
kind: concept
summary: "A pipeable operator is a pure function from source Observable to transformed Observable, produced by calling a factory like map(fn)."
tags: [rxjs, operators, pipeable, operatorfunction]
related: [chaining-with-pipe, operators-are-pure, operator-function-shape]
---
> **In one line:** a **pipeable operator** is a function of type `(source: Observable<T>) => Observable<R>`, produced by calling an operator factory such as `map(fn)` or `filter(fn)`.

## The idea

RxJS has two distinct families of operators:

1. **Creation operators** — standalone functions like `of`, `from`, `fromEvent`, and `interval` that create a brand-new Observable from non-Observable inputs.
2. **Pipeable operators** — functions like `map`, `filter`, and `take` that transform an existing Observable into a new Observable inside `.pipe()`.

Look closely at how you use `map((x: number) => x * 2)`. `map` itself is an **operator factory**: when you call `map(fn)`, it returns the actual **pipeable operator** — a function whose TypeScript type is `OperatorFunction<T, R>`, which simply means `(source: Observable<T>) => Observable<R>`.

| Piece | Example | Type | Role |
|---|---|---|---|
| **Operator factory** | `map` | `(project: (v: T) => R) => OperatorFunction<T, R>` | Configures the behaviour |
| **Pipeable operator** | `map((x: number) => x * 2)` | `OperatorFunction<number, number>` | Takes an `Observable<number>`, returns a new `Observable<number>` |

## See it

Because a pipeable operator is just a function from `Observable` to `Observable`, you can even call it directly on a source (though in practice you always pass it to `.pipe()`):

```typescript
import { of, map, OperatorFunction } from 'rxjs';

// Calling the factory map(...) returns an OperatorFunction:
const double: OperatorFunction<number, number> = map((x: number) => x * 2);

const source$ = of(1, 2, 3);

// Calling the operator function directly vs passing it to .pipe():
const direct$ = double(source$);
const piped$ = source$.pipe(double);

direct$.subscribe((n) => console.log('direct:', n)); // 2, 4, 6
piped$.subscribe((n) => console.log('piped:', n));   // 2, 4, 6
```

## Go deeper

Why did RxJS design operators as standalone functions instead of methods on `Observable.prototype` (like `Array.prototype.map`)? Early versions of RxJS (v5 and earlier) patched hundreds of operator methods onto `Observable.prototype`. Bundlers could not tree-shake unused methods from a prototype, so importing one operator pulled in huge swathes of the library. Pipeable operators are plain imported functions: if your app only imports `map` and `filter`, your bundler drops every other operator at build time.

```typescript
import { of, first } from 'rxjs';

// Even parameterless operators like first() are factories — you must call first():
of('alpha', 'beta')
  .pipe(first())
  .subscribe((v) => console.log('first item:', v)); // "alpha"
```

This two-step curried design (factory call → `OperatorFunction`) also makes custom operators trivial to write: any function you write that accepts an `Observable<T>` and returns an `Observable<R>` can be dropped straight into `.pipe()` alongside RxJS's built-in operators.

## Watch out

- Inside `.pipe()`, you must **call** the operator factory: write `source$.pipe(map(x => x * 2))` or `source$.pipe(first())`, never `source$.pipe(map)` or `source$.pipe(first)`.
- Never chain operators as methods on the Observable (`source$.map(...).filter(...)`) — prototype-patched operators were removed in RxJS 6+.

## Remember

- Creation operators create streams from scratch; pipeable operators transform an existing stream.
- Calling a factory like `map(fn)` returns an `OperatorFunction<T, R>` of shape `(source$) => result$`.
- Always invoke the operator factory inside `.pipe(map(...))`.
