---
slug: first-value-from-and-last-value-from
title: "Bridging Observables to async/await: firstValueFrom and lastValueFrom"
topic: patterns-interop
kind: api
summary: "Convert any Observable into a Promise in RxJS 7+ using firstValueFrom() or lastValueFrom() instead of the removed .toPromise() method."
tags: [rxjs, firstvaluefrom, lastvaluefrom, promises, async-await]
related: [observable-vs-promise, empty-error-and-take-one, promise-interop-pitfalls]
---
> **In one line:** `.toPromise()` was removed in RxJS 8 because its name hid *which* emission it resolved with — replace it with **`firstValueFrom(obs$)`** (resolves on the **1st** emission and unsubscribes immediately) or **`lastValueFrom(obs$)`** (waits for `complete()` and resolves with the **final** emission).

## The idea

When integrating RxJS streams into `async / await` functions (like route guards, CLI scripts, server actions, or `async` unit tests), RxJS 7+ provides two explicit functions with crystal-clear behavior:

| Function | When the returned Promise resolves | What happens to the subscription | Best for |
|---|---|---|---|
| **`firstValueFrom(obs$, config?)`** | Immediately upon the **first `next(value)`** notification | Automatically **unsubscribes** on that first emission (just like `take(1)`) | Reading the current value of a `BehaviorSubject`, waiting for the next single event, or awaiting a 1-shot HTTP stream |
| **`lastValueFrom(obs$, config?)`** | Only when the Observable calls **`complete()`** (resolving with the most recent `next` value) | Stays subscribed until `complete()` or `error()` | Multi-emission finite pipelines (e.g., `reduce()`, `toArray()`, or replacing legacy `.toPromise()`) |

Both functions accept an optional second argument `{ defaultValue: T }`. If the Observable completes without emitting a single value, passing `{ defaultValue }` resolves the Promise with `defaultValue` instead of rejecting with `EmptyError`!

```sequence
actor caller: async function | awaits Promise @green
actor fvf: firstValueFrom(source$) | takes 1st value @primary
actor src: source$ | emits a, b, c @blue
caller -> fvf: await firstValueFrom(source$)
fvf -> src: subscribe()
src -> fvf: next('a')
fvf -> src: unsubscribe() immediately!
fvf -> caller: Promise resolves with 'a'
```

## See it

Use `firstValueFrom` to snapshot a `BehaviorSubject` inside an `async` function, and `lastValueFrom` with `{ defaultValue }` on a finite stream:

```typescript
import { BehaviorSubject, EMPTY, of, firstValueFrom, lastValueFrom } from 'rxjs';

const currentUser$ = new BehaviorSubject<{ id: number } | null>({ id: 42 });

async function checkAuthGuard(): Promise<boolean> {
  // Resolves immediately with { id: 42 } and unsubscribes from currentUser$!
  const user = await firstValueFrom(currentUser$);
  return user !== null;
}

async function getFinalScore(): Promise<number> {
  // Waits for completion and returns 30; if stream were empty, returns defaultValue 0:
  const finalScore = await lastValueFrom(of(10, 20, 30), { defaultValue: 0 });
  const emptySafe  = await firstValueFrom(EMPTY, { defaultValue: -1 });
  console.log({ finalScore, emptySafe }); // { finalScore: 30, emptySafe: -1 }
  return finalScore;
}
```

## Go deeper

Why is `firstValueFrom(currentUser$)` safe on an infinite `BehaviorSubject`, whereas `lastValueFrom(currentUser$)` would freeze your `async` function forever? Because a `BehaviorSubject` never completes! `firstValueFrom` subscribes, receives the synchronous initial value on the first `next()`, unsubscribes immediately, and resolves the Promise. `lastValueFrom` waits for `complete()`, so calling `await lastValueFrom(currentUser$)` hangs forever.

## Watch out

- Never call `obs$.toPromise()` — it was deprecated in RxJS 7 and removed in RxJS 8.
- Always pass `{ defaultValue: ... }` when the source Observable might complete without emitting any values (for example, after a `filter()` or `takeWhile()`).

## Remember

- Use `firstValueFrom(obs$)` when you want the first emitted value and immediate unsubscription (safe on infinite streams like `BehaviorSubject`).
- Use `lastValueFrom(obs$)` when you want to wait for a finite stream to complete and return its final value.
- Pass `{ defaultValue }` as the second argument to prevent `EmptyError` rejections on empty streams.
