---
slug: pipeable-with-operators
title: "Pipeable joining operators: mergeWith, concatWith, zipWith, raceWith"
topic: combining-joining
kind: api
summary: "In RxJS 7+, use mergeWith, concatWith, zipWith, raceWith, and combineLatestWith to join additional streams inside an existing .pipe() chain."
tags: [rxjs, mergewith, concatwith, zipwith, racewith, rxjs-7]
related: [merge-and-concat, zip-pair-by-index, race-first-wins, importing-operators-in-rxjs-7]
since: "7.0"
---
> **In one line:** when you are already inside `source$.pipe(...)`, use the **`*With`** operators — `mergeWith`, `concatWith`, `zipWith`, `raceWith`, and `combineLatestWith` — instead of the deprecated pipeable `merge`/`concat`/`zip`/`race`.

## The idea

In RxJS 6, there were two different functions named `merge`: a creation function `merge(a$, b$)` in `'rxjs'` and a pipeable operator `merge(b$)` in `'rxjs/operators'`. Having identical names for two different signatures caused constant auto-import confusion.

In **RxJS 7+**, the naming is crystal clear:

- **Creation functions** (called at the top level to start a pipeline) use the base names: `merge`, `concat`, `zip`, `race`, `combineLatest`.
- **Pipeable operators** (called inside `source$.pipe(...)` to join another stream onto the current pipeline) end in **`With`**:

| Creation function (top-level) | Pipeable operator (inside `.pipe()`) | Behavior |
|---|---|---|
| `merge(a$, b$)` | `a$.pipe(mergeWith(b$))` | Interleave emissions from `a$` and `b$` |
| `concat(a$, b$)` | `a$.pipe(concatWith(b$))` | Emit all of `a$`, then subscribe to `b$` |
| `zip(a$, b$)` | `a$.pipe(zipWith(b$))` | Pair emissions `[a_i, b_i]` by index |
| `race(a$, b$)` | `a$.pipe(raceWith(b$))` | Mirror whichever of `a$` or `b$` emits first |
| `combineLatest([a$, b$])` | `a$.pipe(combineLatestWith(b$))` | Emit `[latestA, latestB]` on any update |

And every single one of them is imported straight from `'rxjs'`!

## See it

```typescript
import { of, concatWith, mergeWith, zipWith } from 'rxjs';

// 1. concatWith: append a trailing stream after the current pipeline completes
of('Loading...', 'Fetching data...')
  .pipe(concatWith(of('Done!')))
  .subscribe((msg) => console.log('concatWith:', msg));
// -> "Loading...", "Fetching data...", "Done!"

// 2. mergeWith: blend another event source right inside .pipe()
const localSaves$ = of('saved-locally');
const autoSaves$ = of('auto-saved');

localSaves$
  .pipe(mergeWith(autoSaves$))
  .subscribe((e) => console.log('mergeWith:', e));

// 3. zipWith: pair each value with its index partner from another stream
of('Ada', 'Alan')
  .pipe(zipWith(of(101, 102)))
  .subscribe(([name, id]) => console.log('zipWith:', id, name));
```

## Go deeper

When should you prefer the creation function (`merge(a$, b$)`) versus the pipeable operator (`a$.pipe(mergeWith(b$))`)?

- Prefer the **creation function** `merge(a$, b$, c$)` when `a$`, `b$`, and `c$` are symmetric peers at the start of a pipeline — listing all peers at the same indentation level reads most naturally.
- Prefer the **pipeable `*With` operator** when you are already several steps deep inside `source$.pipe(map(...), filter(...), mergeWith(extra$))` and want to fold in another stream without wrapping the whole expression inside an outer `merge(...)` call.

## Watch out

- The old pipeable `merge`, `concat`, `zip`, and `race` imports from `'rxjs/operators'` are deprecated in RxJS 7 and removed in RxJS 8 — always use `mergeWith`, `concatWith`, `zipWith`, and `raceWith`.
- Don't confuse `combineLatestWith(b$)` (both `a$` and `b$` trigger) with `withLatestFrom(b$)` (only `a$` triggers).

## Remember

- Top-level creation functions: `merge`, `concat`, `zip`, `race`, `combineLatest`.
- Pipeable operators inside `.pipe()`: `mergeWith`, `concatWith`, `zipWith`, `raceWith`, `combineLatestWith`.
- Import both families directly from `'rxjs'`.
