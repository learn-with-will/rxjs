---
slug: map-operator
title: "Reshaping values with map()"
topic: pipes-transforming
kind: api
summary: "map(project) applies a pure projection function to each emitted value and emits the transformed result, one-in and one-out."
tags: [rxjs, map, transformation, operators]
related: [tap-for-side-effects, scan-running-accumulator, chaining-with-pipe]
---
> **In one line:** `map((value, index) => newValue)` transforms every emitted value with a pure function and emits the result **one-for-one**.

## The idea

Raw streams rarely emit values in the exact shape your UI or downstream logic wants. A DOM `input` event arrives as an `Event` object when you want the trimmed text string; an API returns a full user record when you want `"${user.first} ${user.last}"`.

`map(project)` is the workhorse transformation operator. For every `next` value from the source, it calls `project(value, index)` and synchronously emits whatever `project` returns:

- **One-in, one-out:** `map` never drops values (use `filter` for that) and never changes the timing of emissions.
- **Pure:** the callback should compute and return a new value without side effects.
- **Pass-through for `error` and `complete`:** terminal signals pass straight through `map` untouched.

```flow
src: Source Stream | emits { first, last } @primary
m: map(u => u.first + ' ' + u.last) | one-in, one-out projection @blue
out: Output Stream | emits full name string @green
src -> m: { first: 'Ada', last: 'Lovelace' }
m -> out: 'Ada Lovelace'
```

## See it

Here `map` projects user objects into formatted display strings, and also uses the optional zero-based `index` second parameter:

```typescript
import { from, map } from 'rxjs';

const users = [
  { id: 1, first: 'Ada', last: 'Lovelace' },
  { id: 2, first: 'Alan', last: 'Turing' },
];

from(users)
  .pipe(
    map((user, index) => `#${index + 1}: ${user.first} ${user.last}`),
  )
  .subscribe((label) => console.log(label));
// Output:
// #1: Ada Lovelace
// #2: Alan Turing
```

## Go deeper

What happens if your `map` callback returns **another Observable** or a **Promise** (for example, `map(id => fetchUser(id))`)? Because `map` simply emits whatever your callback returns, the output becomes an `Observable<Observable<User>>` — a **higher-order Observable** — rather than the `User` data itself. Whenever your transformation is asynchronous or returns a stream, reach for a flattening operator like `switchMap`, `mergeMap`, or `concatMap` instead of plain `map`.

> **Tip:** RxJS 6 had a `mapTo(constant)` operator for emitting a constant on every event. In RxJS 7+, `mapTo` is deprecated — write `map(() => constant)` instead.

```typescript
import { fromEvent, map } from 'rxjs';

// Replace deprecated mapTo(1) with map(() => 1):
const plusOne$ = fromEvent(document, 'click').pipe(
  map(() => 1),
);
```

Because `map(() => value)` calls your arrow function fresh on each emission, it also avoids a subtle `mapTo` footgun where passing a mutable reference like `mapTo({ clicked: true })` shared the exact same object instance across every emission.

## Watch out

- Do not forget `return` in a multi-line `map((x) => { ... })` callback, or `map` will emit `undefined` for every value.
- Do not put logging, DOM writes, or state mutations inside `map` — keep `map` pure and use `tap` for side effects.

## Remember

- `map(fn)` transforms each emission one-in, one-out, just like `Array.prototype.map` over time.
- Keep the projection function pure and synchronous; use `switchMap` / `mergeMap` for async projections.
- Prefer `map(() => value)` over the deprecated `mapTo(value)`.
