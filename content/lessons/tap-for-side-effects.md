---
slug: tap-for-side-effects
title: "Peeking and side effects with tap()"
topic: pipes-transforming
kind: api
summary: "tap() runs side effects such as logging or analytics for each notification while passing every value, error, and completion through unchanged."
tags: [rxjs, tap, debugging, side-effects, operators]
related: [map-operator, immutable-accumulators-and-tap-pitfalls, finalize-guaranteed-cleanup]
---
> **In one line:** `tap()` lets you inspect notifications or run side effects mid-pipeline while forwarding every value through **completely unchanged**.

## The idea

When a pipeline of four or five operators produces an unexpected result, how do you see what values look like between stage 2 and stage 3 without breaking the chain? Or where do you trigger a side effect like analytics or logging without polluting a pure `map`?

That is the job of **`tap`** (called `do` in older versions of RxJS). `tap` is a mirror: whatever comes in goes straight out to the next operator, and any value your `tap` callback returns is **ignored**.

You can call `tap` with either a single `next` function or an Observer object watching `next`, `error`, `complete`, `subscribe`, `unsubscribe`, and `finalize`:

| Call | What it observes |
|---|---|
| `tap((v) => console.log(v))` | Every `next` emission passing through that point in the pipe |
| `tap({ next, error, complete })` | Values as well as terminal `error` or `complete` notifications |
| `tap({ subscribe, unsubscribe, finalize })` | Lifecycle hooks when downstream subscribes or tears down (RxJS 7.3+) |

## See it

Place `tap` before and after `map` to watch values transform step by step without altering the stream:

```typescript
import { of, tap, map } from 'rxjs';

of(1, 2, 3)
  .pipe(
    tap((n) => console.log('before map:', n)),
    map((n) => n * 10),
    tap({
      next: (n) => console.log('after map:', n),
      complete: () => console.log('pipeline completed'),
    }),
  )
  .subscribe();
// Output:
// before map: 1 -> after map: 10
// before map: 2 -> after map: 20
// before map: 3 -> after map: 30
// pipeline completed
```

## Go deeper

Even though `tap` ignores your callback's return value, **throwing an error** inside `tap` is *not* ignored: if your `tap` callback throws an exception, the stream immediately transitions into an `error` state and stops, just like any other operator. Also remember that although `tap` cannot replace the reference being emitted, mutating a property on an object inside `tap((user) => { user.admin = true; })` mutates that object in place for downstream consumers — treat values inside `tap` as read-only!

## Watch out

- Returning a new value from `tap((n) => n * 2)` does **nothing** — `tap` ignores return values; use `map` to transform values.
- Do not mutate objects or arrays inside `tap`; use `tap` only to observe or trigger external side effects like logging.

## Remember

- `tap` executes side effects and passes every notification through unchanged.
- Pass an object `tap({ next, error, complete })` to inspect terminal events as well as values.
- `tap` ignores return values — if you want to change the emitted value, use `map`.
