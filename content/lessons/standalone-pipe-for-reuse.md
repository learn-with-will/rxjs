---
slug: standalone-pipe-for-reuse
title: "Composing reusable pipelines with standalone pipe()"
topic: pipes-transforming
kind: technique
summary: "The standalone pipe(op1, op2, ...) function from 'rxjs' combines multiple operators into a single reusable OperatorFunction you can drop into any .pipe()."
tags: [rxjs, pipe, composition, custom-operators, reuse]
related: [chaining-with-pipe, what-is-a-pipeable-operator, composing-custom-operators-with-pipe]
---
> **In one line:** the standalone `pipe(op1, op2)` function composes several operators into **one reusable operator** without needing a source Observable up front.

## The idea

In addition to the `.pipe()` method on every Observable instance, RxJS exports a standalone **`pipe()` function** from `'rxjs'`.

While `source$.pipe(op1, op2)` immediately applies `op1` and `op2` to `source$` and returns an `Observable`, calling the standalone `pipe(op1, op2)` returns a single composed `OperatorFunction` that you can store in a variable and pass into any number of `.pipe()` calls later.

| Form | Imported from | Returns | Purpose |
|---|---|---|---|
| **`source$.pipe(op1, op2)`** | Method on `Observable` | `Observable<R>` | Transform a specific source stream right now |
| **`pipe(op1, op2)`** | `import { pipe } from 'rxjs'` | `OperatorFunction<T, R>` | Bundle a multi-step recipe into one reusable operator |

## See it

Here we bundle a two-step "keep positive numbers and double them" sequence with standalone `pipe()` and reuse it across two different sources:

```typescript
import { of, pipe, filter, map } from 'rxjs';

// Build one reusable operator with the standalone pipe() function:
const positiveDoubles = pipe(
  filter((n: number) => n > 0),
  map((n) => n * 2),
);

// Drop it into any source$.pipe(...) just like a built-in operator:
of(-3, 0, 4, 7)
  .pipe(positiveDoubles)
  .subscribe((n) => console.log('stream A:', n)); // 8, 14

of(10, -5, 20)
  .pipe(positiveDoubles)
  .subscribe((n) => console.log('stream B:', n)); // 20, 40
```

## Go deeper

You can also wrap standalone `pipe()` in a regular function to accept configuration parameters — which is the standard way to author custom operators in RxJS:

```typescript
import { of, pipe, map, filter, OperatorFunction } from 'rxjs';

const cleanStrings = (minLength: number): OperatorFunction<string, string> =>
  pipe(
    map((s) => s.trim()),
    filter((s) => s.length >= minLength),
  );

of('  a ', '  rxjs ', '   hi  ')
  .pipe(cleanStrings(3))
  .subscribe(console.log); // "rxjs"
```

## Watch out

- Annotate the input parameter type on the first operator inside standalone `pipe(filter((n: number) => ...))` so TypeScript knows the input type `T` of the composed operator.
- If a composed operator uses stateful operators (like `scan`), avoid sharing a single `const op = pipe(...)` instance only if you capture mutable state outside the operators — built-in operators create fresh state per subscription automatically.

## Remember

- Import standalone `pipe` from `'rxjs'` to combine multiple operators into one `OperatorFunction`.
- Pass the composed operator into `source$.pipe(myOperator)` just like any built-in operator.
- Wrap `pipe(...)` in a function `(arg) => pipe(...)` when your reusable pipeline needs parameters.
