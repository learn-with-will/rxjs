---
slug: combine-latest-object-form-and-with
title: "Named dictionary combineLatest({}) and combineLatestWith()"
topic: combining-joining
kind: api
summary: "Pass a dictionary object combineLatest({ user: user$, theme: theme$ }) to get named properties instead of positional tuples, or pipe with combineLatestWith()."
tags: [rxjs, combinelatest, combinelatestwith, typescript, viewmodel]
related: [combine-latest, combine-latest-waits-for-all-and-start-with, pipeable-with-operators]
since: "7.0"
---
> **In one line:** `combineLatest({ a: a$, b: b$ })` emits a **typed object `{ a, b }`** instead of a positional array tuple, and `combineLatestWith(b$)` is the pipeable version for `.pipe()`.

## The idea

When you combine four or five streams using the array form `combineLatest([user$, perms$, theme$, notifications$, locale$])`, destructuring the resulting 5-element tuple `([user, perms, theme, notifications, locale])` is fragile: it is easy to mix up the positional order when adding or removing a stream.

In RxJS 7+, `combineLatest` also accepts a **dictionary object** of Observables:

```text
combineLatest({ user: user$, theme: theme$ })  -->  Observable<{ user: User; theme: Theme }>
```

Each emission is a plain object with the exact same keys and fully inferred property types — a pattern often called a **reactive ViewModel stream (`vm$`)**.

And when you are already inside a `.pipe()` chain and want to combine the current stream with another Observable using `combineLatest` semantics, RxJS 7.2+ provides the pipeable **`combineLatestWith(...other$)`** operator.

| Form | Syntax | Emits |
|---|---|---|
| **Array form** | `combineLatest([a$, b$])` | Tuple `[a, b]` |
| **Dictionary form** | `combineLatest({ a: a$, b: b$ })` | Object `{ a, b }` |
| **Pipeable form** | `a$.pipe(combineLatestWith(b$))` | Tuple `[a, b]` |

## See it

```typescript
import { of, combineLatest, combineLatestWith, map } from 'rxjs';

const user$ = of({ name: 'Ada' });
const theme$ = of<'dark' | 'light'>('dark');
const unreadCount$ = of(3);

// 1. Dictionary form: emits a named object { user, theme, unreadCount }
const vm$ = combineLatest({
  user: user$,
  theme: theme$,
  unreadCount: unreadCount$,
});

vm$.subscribe(({ user, theme, unreadCount }) => {
  console.log(`${user.name} (${theme}) - ${unreadCount} unread`);
});

// 2. Pipeable combineLatestWith() inside an existing .pipe() chain:
user$
  .pipe(
    combineLatestWith(theme$),
    map(([user, theme]) => `${user.name} prefers ${theme} mode`),
  )
  .subscribe(console.log);
```

## Go deeper

Just like the array form, the dictionary form `combineLatest({ user: user$, theme: theme$ })` waits until **every property's Observable** has emitted at least once before emitting its first `{ user, theme }` object, and then emits a fresh object whenever any property's Observable emits.

> **Tip:** Passing an old third "result selector" function to `combineLatest([a$, b$], (a, b) => ...)` is deprecated in RxJS 7+. Pipe the result through `map(([a, b]) => ...)` or use the dictionary form instead.

## Watch out

- Passing an empty array `combineLatest([])` or empty object `combineLatest({})` completes immediately without emitting any values.
- In `combineLatestWith`, remember that **both** the source stream and the argument stream trigger new emissions — if only the source stream should trigger, use `withLatestFrom` instead.

## Remember

- Use `combineLatest({ key1: obs1$, key2: obs2$ })` to emit self-documenting objects instead of long positional tuples.
- Use `combineLatestWith(other$)` when you want `combineLatest` behavior inside an existing `.pipe()` chain.
- Avoid the deprecated resultSelector argument — chain `map(...)` in `.pipe()` instead.
