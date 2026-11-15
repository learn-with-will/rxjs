---
slug: fork-join
title: "Waiting for final values with forkJoin()"
topic: combining-joining
kind: api
summary: "forkJoin([a$, b$]) or forkJoin({ a: a$, b: b$ }) waits for all input streams to complete and emits one combined result of each input's final value."
tags: [rxjs, forkjoin, joining, http, promise-all]
related: [combine-latest, merge-and-concat, joining-stream-pitfalls]
---
> **In one line:** `forkJoin` is the Observable equivalent of `Promise.all` — it waits for **every input Observable to complete** and then emits **once** with each stream's **last** emitted value.

## The idea

When loading a dashboard page, you often need to fire three HTTP requests in parallel (`user$`, `orders$`, `stats$`) and render only after **all three have finished**.

- `combineLatest` emits every time any input emits.
- **`forkJoin`** stays silent while the inputs are running, waits until **every single input Observable has emitted at least one value and completed**, and then emits **one single array or dictionary object** containing the **final** value from each input, followed immediately by `complete`.

| Input behavior | What `forkJoin({ a: a$, b: b$ })` does |
|---|---|
| `a$` emits `1, 2` and completes; `b$` emits `'x', 'y'` and completes | Emits `{ a: 2, b: 'y' }` once (the **last** value of each) and completes |
| `a$` never completes (e.g. `interval` or open `Subject`) | **Never emits!** Waits forever for `a$` to complete |
| `a$` completes **empty** (e.g. `EMPTY`, 0 values) | Completes immediately **without emitting** any value |
| Any input errors | Immediately **errors** and unsubscribes from the other still-running inputs |

## See it

Just like `combineLatest`, `forkJoin` accepts either an array `forkJoin([a$, b$])` or a named dictionary object `forkJoin({ a: a$, b: b$ })`:

```typescript
import { of, delay, forkJoin } from 'rxjs';

// Simulated parallel HTTP requests (each emits once and completes):
const user$ = of({ id: 1, name: 'Ada' }).pipe(delay(120));
const roles$ = of(['admin', 'editor']).pipe(delay(80));
const multiValue$ = of('draft-1', 'draft-2', 'final-draft'); // emits 3 values then completes

forkJoin({
  user: user$,
  roles: roles$,
  latestDraft: multiValue$,
}).subscribe({
  next: (result) => console.log('All completed:', result),
  complete: () => console.log('forkJoin complete!'),
});
// At 120ms, logs once:
// All completed: { user: { id: 1, name: 'Ada' }, roles: ['admin', 'editor'], latestDraft: 'final-draft' }
// forkJoin complete!
```

## Go deeper

Look closely at what happens when an input to `forkJoin` completes without emitting any value (such as `EMPTY`), or when an input errors:

1. **Empty input:** if `roles$` completes with zero emissions, `forkJoin` cannot build a complete result object, so it immediately completes with **zero emissions** (or if you await it with `firstValueFrom(forkJoin(...))`, throws `EmptyError`). Use `defaultIfEmpty(fallback)` on any input that might legitimately complete empty.
2. **Error in one input:** if `roles$` errors at 80ms while `user$` is still running, `forkJoin` immediately forwards the error and **unsubscribes from `user$`**! To prevent one failed optional request from cancelling the others, attach `catchError(() => of(fallbackValue))` to the individual inner Observable before passing it into `forkJoin`.

## Watch out

- Never pass a non-completing stream (`fromEvent`, `interval`, `BehaviorSubject`) into `forkJoin` without bounding it first (e.g. `state$.pipe(take(1))`), or `forkJoin` will hang forever!
- Put `catchError` on individual input streams inside `forkJoin` if you want partial results when one request fails.

## Remember

- `forkJoin([a$, b$])` or `forkJoin({ a: a$, b: b$ })` waits for **all** inputs to complete, then emits their **final** values once.
- Every input passed to `forkJoin` must emit at least one value and **complete**.
- If any input errors, `forkJoin` immediately errors and cancels all remaining in-flight inputs.
