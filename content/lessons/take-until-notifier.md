---
slug: take-until-notifier
title: "Declarative cancellation with takeUntil()"
topic: errors-cleanup
kind: api
summary: "takeUntil(notifier$) forwards values from the source until notifier$ emits its first value, then immediately completes and unsubscribes upstream."
tags: [rxjs, takeuntil, cancellation, cleanup, memory-leaks]
related: [why-take-until-goes-last, take-until-destroy-pattern, finalize-guaranteed-cleanup]
---
> **In one line:** `source$.pipe(takeUntil(stop$))` mirrors `source$` until `stop$` emits **any `next` value**, at which point the stream **immediately completes and tears down**.

## The idea

Instead of storing `Subscription` objects and calling `sub.unsubscribe()` imperatively, **`takeUntil(notifier$)`** lets you express cancellation **declaratively** using another Observable as a stop signal:

- While `notifier$` is silent, every emission from `source$` passes straight through.
- The instant `notifier$` emits its **first `next` value** (even `undefined` from a `Subject<void>`), `takeUntil` sends a **`complete()`** notification downstream and **unsubscribes** from both `source$` and `notifier$`.

This lets you build rich, event-driven lifecycles without a single imperative `unsubscribe()` call — such as "track `mousemove` after `mousedown` **until `mouseup`**" or "keep all component streams alive **until `destroy$` emits**".

```sequence
actor src: interval(200) | infinite source @blue
actor stop: stop$ (notifier) | emits when user clicks Stop @amber
actor tu: takeUntil(stop$) | completes on first stop$ emission @primary
actor sub: Subscriber | receives complete()! @green
src -> tu: 0, 1, 2
tu -> sub: 0, 1, 2
stop -> tu: next() (stop signal!)
tu -> src: unsubscribe()
tu -> sub: complete()
```

## See it

Here are two classic uses of `takeUntil`: stopping a timer when a notifier emits, and building a drag-and-drop stream from `mousedown`, `mousemove`, and `mouseup`:

```typescript
import { interval, Subject, fromEvent, switchMap, map, takeUntil } from 'rxjs';

// 1. Stop an interval declaratively when stop$ emits:
const stop$ = new Subject<void>();
interval(200).pipe(takeUntil(stop$)).subscribe({
  next: (n) => console.log('tick:', n),
  complete: () => console.log('Stopped cleanly via takeUntil!'),
});
setTimeout(() => stop$.next(), 550); // Completes after ticks 0, 1!

// 2. Drag stream: on mousedown, listen to mousemove UNTIL mouseup!
const down$ = fromEvent<MouseEvent>(document, 'mousedown');
const move$ = fromEvent<MouseEvent>(document, 'mousemove');
const up$ = fromEvent<MouseEvent>(document, 'mouseup');

const drag$ = down$.pipe(
  switchMap(() =>
    move$.pipe(
      map((e) => ({ x: e.clientX, y: e.clientY })),
      takeUntil(up$), // Unsubscribes from mousemove the instant mouseup fires!
    ),
  ),
);
```

## Go deeper

Watch what happens if `notifier$` **completes without ever emitting a `next` value** (for example `EMPTY`): `takeUntil` only reacts to a **`next` emission** (or an `error`) from `notifier$`! If `notifier$` simply calls `complete()` without calling `next()`, `takeUntil` ignores the notifier's completion and `source$` keeps running! That is why when you trigger a `destroy$` Subject in cleanup, you must call **`destroy$.next()`** (followed by `destroy$.complete()`), never `destroy$.complete()` alone.

## Watch out

- Calling `destroy$.complete()` without calling `destroy$.next()` first does **not** trigger `takeUntil(destroy$)`! Always call `destroy$.next()` to fire the stop signal.
- Unlike `sub.unsubscribe()`, stopping a stream with `takeUntil(stop$)` **does** invoke your Observer's `complete()` callback because `takeUntil` emits a completion notification downstream.

## Remember

- `takeUntil(notifier$)` completes the stream and unsubscribes upstream the instant `notifier$` emits its first `next` value.
- It triggers on `notifier$`'s `next` (or `error`), **not** on `notifier$`'s `complete`.
- Always call both `destroy$.next()` and `destroy$.complete()` when firing a teardown Subject.
