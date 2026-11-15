---
slug: joining-stream-pitfalls
title: "Comparison and pitfalls of joining operators"
topic: combining-joining
kind: pitfall
summary: "Avoid the classic bugs of joining operators: forkJoin on non-completing streams, concat after an infinite stream, and zip between mismatched-rate streams."
tags: [rxjs, forkjoin, concat, zip, merge, pitfall]
related: [merge-and-concat, zip-pair-by-index, fork-join, race-first-wins]
---
> **In one line:** every joining operator has a signature failure mode — **`forkJoin` hangs if an input never completes**, **`concat` starves streams after an infinite first stream**, and **`zip` leaks memory when rates diverge**.

## The idea

Let's compare all six multi-stream operators side by side so you can pick the right one at a glance and spot their classic bugs in code review:

| Operator | Emits what? | Emits when? | Completes when? | Classic pitfall |
|---|---|---|---|---|
| **`combineLatest([a$, b$])`** | `[latestA, latestB]` | Any input emits (after all emitted once) | **All** inputs complete | Hangs silently if one input never emits its 1st value |
| **`a$.pipe(withLatestFrom(b$))`** | `[a, latestB]` | Only when **`a$`** emits (after `b$` emitted once) | When **`a$`** completes | Drops `a$` triggers before `b$` has emitted once |
| **`merge(a$, b$)`** | Single values `a` or `b` | Immediately as any input emits | **All** inputs complete | Interleaves values with no guaranteed order |
| **`concat(a$, b$)`** | All of `a$`, then all of `b$` | Sequentially | Last input completes | Never subscribes to `b$` if `a$` is infinite! |
| **`zip(a$, b$)`** | `[a_i, b_i]` by index | When all inputs reach index `i` | **Any** input completes (and buffer empties) | Unbounded memory buffer if `a$` is faster than `b$` |
| **`forkJoin([a$, b$])`** | `[lastA, lastB]` once | Only after **all** inputs complete | Immediately after its 1 emission | Never emits if any input doesn't complete (or is `EMPTY`) |

## See it

Here is the classic `forkJoin` + `BehaviorSubject` bug and how `take(1)` fixes it:

```typescript
import { BehaviorSubject, of, forkJoin, take } from 'rxjs';

const currentUserId$ = new BehaviorSubject<number>(42); // Open Subject: NEVER completes!
const config$ = of({ region: 'eu' });                  // Completes immediately

// Before (bug): forkJoin waits forever because BehaviorSubject never completes!
forkJoin({
  userId: currentUserId$,
  config: config$,
}).subscribe((res) => console.log('Before (never logs!):', res));

// After: bound the BehaviorSubject with take(1) so it emits 42 and completes!
forkJoin({
  userId: currentUserId$.pipe(take(1)),
  config: config$,
}).subscribe((res) => console.log('After (works!):', res));
// Logs: After (works!): { userId: 42, config: { region: 'eu' } }
```

## Go deeper

Notice how a single missing `take(1)` turned `forkJoin` into a silent hang! Whenever you pass a state stream (`BehaviorSubject`, store selector, or `fromEvent`) into an operator that waits for completion — namely **`forkJoin`**, **`concat`** (as an earlier argument), **`reduce`**, **`last`**, **`takeLast`**, or **`lastValueFrom`** — you **must** bound that infinite stream with `take(1)` (or `take(n)` / `takeWhile`) first.

## Watch out

- Check the argument style: `combineLatest([a$, b$])` and `forkJoin([a$, b$])` take an **array** (or dictionary object), whereas `merge(a$, b$)`, `concat(a$, b$)`, and `zip(a$, b$)` take **rest arguments**.
- If any input to `forkJoin` completes empty (`EMPTY`), `forkJoin` completes without emitting; guard optional streams with `defaultIfEmpty(null)`.

## Remember

- `forkJoin` requires every input to emit at least once and **complete** — wrap `BehaviorSubject` inputs with `.pipe(take(1))`.
- `concat(a$, b$)` requires `a$` to complete before `b$` ever starts.
- `zip(a$, b$)` requires inputs to emit at comparable rates so internal buffers don't grow unbounded.
