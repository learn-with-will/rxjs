---
slug: chaining-with-pipe
title: "Chaining operators with .pipe()"
topic: pipes-transforming
kind: api
summary: "Every Observable has a .pipe(op1, op2, ...) method that chains pipeable operators top-to-bottom so each stage receives the output of the previous stage."
tags: [rxjs, pipe, operators, composition]
related: [what-is-a-pipeable-operator, operators-are-pure, operator-order-matters]
---
> **In one line:** `source$.pipe(a, b, c)` feeds `source$` through `a`, then `b`, then `c` in top-to-bottom order — the readable equivalent of `c(b(a(source$)))`.

## The idea

If a pipeable operator is a function `a(source$)`, chaining three operators by nesting function calls would look like `take(2)(map(n => n * 10)(filter(n => n % 2 === 0)(source$)))` — which you would have to read **inside-out, right-to-left**.

To make pipelines read in the natural order values flow, every Observable has a **`.pipe(...operators)`** method. You pass any number of pipeable operators as arguments, one per line, and values flow **top-to-bottom** through each stage:

```flow
src: of(1, 2, 3, 4, 5) | source Observable @primary
f: filter(n => n % 2 === 0) | keeps 2, 4 @blue
m: map(n => n * 10) | produces 20, 40 @blue
sub: subscribe(console.log) | logs 20, 40 @green
src -> f: 1, 2, 3, 4, 5
f -> m: 2, 4
m -> sub: 20, 40
```

## See it

```typescript
import { of, filter, map, take } from 'rxjs';

const result$ = of(1, 2, 3, 4, 5, 6).pipe(
  filter((n) => n % 2 === 0), // Stage 1: keep even numbers (2, 4, 6)
  map((n) => n * 10),         // Stage 2: multiply each by 10 (20, 40, 60)
  take(2),                    // Stage 3: take the first two and complete (20, 40)
);

// Nothing has run yet! Subscribing starts the pipeline:
result$.subscribe((n) => console.log('output:', n));
// Output:
// output: 20
// output: 40
```

## Go deeper

Here is a practical DOM example that turns raw `input` events from a search box into trimmed query strings of at least two characters:

```typescript
import { fromEvent, map, filter } from 'rxjs';

const input = document.querySelector<HTMLInputElement>('#search')!;

const query$ = fromEvent(input, 'input').pipe(
  map((event) => (event.target as HTMLInputElement).value.trim()),
  filter((text) => text.length >= 2),
);

query$.subscribe((text) => console.log('Search for:', text));
```

Notice how TypeScript tracks the type from stage to stage: `fromEvent` emits `Event`, `map` turns it into `string`, and `filter` receives `text: string` automatically.

What happens if you call `source$.pipe()` with zero arguments? It simply returns `source$` unchanged. And when a stream completes or errors partway through a `.pipe()` chain (for example when `take(2)` in the first snippet reaches its second value), the teardown signal travels **upward** from `take(2)` through `map` and `filter` all the way back to the source Observable, cleaning up every stage in the pipeline at once.

## Watch out

- Calling `source$.pipe(...)` only **builds a description** of the pipeline — no operator runs until something calls `.subscribe()` on the returned Observable.
- Write one operator per line inside `.pipe()` so each transformation stage is easy to scan and diff.

## Remember

- `source$.pipe(op1, op2, op3)` applies operators in order from top to bottom.
- It is identical to `op3(op2(op1(source$)))`, with full TypeScript type inference between stages.
- A piped Observable is still lazy — nothing runs until `.subscribe()` is called at the end.
