---
slug: choosing-a-subject-variant
title: "Choosing between Subject, BehaviorSubject, ReplaySubject, and AsyncSubject"
topic: subjects-multicasting
kind: concept
summary: "Compare all four Subject variants by what they emit to late subscribers, whether they require a seed value, and what happens after complete()."
tags: [rxjs, subject, behaviorsubject, replaysubject, asyncsubject]
related: [what-is-a-subject, behavior-subject, replay-subject, async-subject]
---
> **In one line:** choose **`Subject`** for one-off events, **`BehaviorSubject`** for state with an initial value, **`ReplaySubject(N)`** for buffered history or seedless caching, and **`AsyncSubject`** for a single final result upon completion.

## The idea

Here is the complete comparison of all four Subject classes in RxJS:

| Variant | Needs seed? | Late subscriber (while open) gets | Late subscriber (after `complete()`) gets | Best for |
|---|---|---|---|---|
| **`Subject<T>`** | No | Nothing until next `.next()` | Only `complete()` | Action/event buses (`clicks$`, `destroy$`, `toasts$`) |
| **`BehaviorSubject<T>`** | **Yes** | The **1 current** value immediately | Only `complete()` (no value!) | Reactive state stores (`theme$`, `authState$`, `count$`) |
| **`ReplaySubject<T>(N, window?)`** | No | Up to the **last `N`** values immediately | The **last `N`** values + `complete()` | Seedless latest cache (`N=1`), recent event log (`N>1`) |
| **`AsyncSubject<T>`** | No | Nothing until `complete()` is called | The **1 final** value + `complete()` | One-shot async computation cache (Promise-like) |

```tree
Which Subject variant fits your use case?
  Discrete events where latecomers should NOT get past events
    [Subject<T>] Action bus, destroy$ notifier, UI notifications @blue
  Current state that always has a valid initial value
    [BehaviorSubject<T>(seed)] Store state, filter criteria, feature flags @green
  Latecomers need recent value(s), or there is no valid initial seed
    [ReplaySubject<T>(N)] Loaded config (N=1), recent chat/audit history (N=10) @primary
  Consumers only want the final result after complete()
    [AsyncSubject<T>] Heavy one-time calculation or initialization result @amber
```

## See it

Watch all four Subject types respond to the exact same sequence: `next(1)`, `next(2)`, a late subscriber joining, `next(3)`, and `complete()`:

```typescript
import { Subject, BehaviorSubject, ReplaySubject, AsyncSubject } from 'rxjs';

const s$ = new Subject<number>();
const b$ = new BehaviorSubject<number>(0);
const r$ = new ReplaySubject<number>(2);
const a$ = new AsyncSubject<number>();

for (const subj of [s$, b$, r$, a$]) {
  subj.next(1);
  subj.next(2);
}

// Late subscribers join AFTER 1 and 2 have been pushed:
s$.subscribe((v) => console.log('Subject:', v));         // (nothing yet)
b$.subscribe((v) => console.log('BehaviorSubject:', v)); // 2 (latest)
r$.subscribe((v) => console.log('ReplaySubject(2):', v));// 1, 2 (last two)
a$.subscribe((v) => console.log('AsyncSubject:', v));    // (nothing yet)

for (const subj of [s$, b$, r$, a$]) {
  subj.next(3);      // s$, b$, r$ all log 3; a$ still silent
  subj.complete();   // a$ now logs 3!
}
```

## Go deeper

One subtle distinction worth memorizing for interviews and debugging is **post-completion behavior**:

- If `b$ = new BehaviorSubject(10)` calls `b$.complete()`, a subscriber who subscribes *after* `complete()` receives **zero values** — just `complete`.
- If `r$ = new ReplaySubject(1)` emits `10` and calls `r$.complete()`, a subscriber who subscribes *after* `complete()` **still receives `10`** followed by `complete`.

That is why HTTP response caching in RxJS (`shareReplay(1)`) is built on `ReplaySubject(1)`, not `BehaviorSubject` — because HTTP Observables complete right after emitting their response!

## Watch out

- Don't use `BehaviorSubject<T | null>(null)` just because you don't have an initial value yet — if consumers shouldn't run until the first real `T` arrives, `ReplaySubject<T>(1)` avoids `null` checks everywhere.
- Don't use `ReplaySubject` or `BehaviorSubject` for one-off commands like "show alert toast" — otherwise every new component that subscribes later will immediately re-trigger the old toast!

## Remember

- `Subject` = live events only; `BehaviorSubject` = current state with seed; `ReplaySubject(N)` = last `N` values (even after complete); `AsyncSubject` = final value on complete.
- `ReplaySubject(1)` replays after completion; `BehaviorSubject` does not.
- Always encapsulate Subjects as private fields and expose `.asObservable()` to external consumers.
