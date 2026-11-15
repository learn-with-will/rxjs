---
slug: combine-latest-vs-with-latest-from
title: "combineLatest() vs withLatestFrom(): which one triggers?"
topic: combining-joining
kind: concept
summary: "Use combineLatest when all inputs are peers that should recalculate output on any change; use withLatestFrom when one event stream should sample passive state."
tags: [rxjs, combinelatest, withlatestfrom, comparison, combining]
related: [combine-latest, with-latest-from, combine-latest-waits-for-all-and-start-with]
---
> **In one line:** use **`combineLatest`** when **every** input should trigger a recalculation, and use **`withLatestFrom`** when **only one trigger stream** should fire while the others supply passive context.

## The idea

Confusing `combineLatest` and `withLatestFrom` is one of the most frequent sources of subtle bugs in RxJS applications — either firing side effects on every state change (when `combineLatest` was used instead of `withLatestFrom`) or failing to update derived UI when a secondary filter changes (when `withLatestFrom` was used instead of `combineLatest`).

Compare their behavior across every dimension:

| Question | `combineLatest([a$, b$])` | `a$.pipe(withLatestFrom(b$))` |
|---|---|---|
| **Kind of operator** | Creation function | Pipeable operator on `a$` |
| **Does `a$` trigger an emission?** | **Yes** (once `b$` has emitted) | **Yes** (once `b$` has emitted) |
| **Does `b$` trigger an emission?** | **Yes** (once `a$` has emitted) | **No** — `b$` only updates its cached value |
| **If `a$` emits before `b$` has emitted?** | Waits; emits when `b$` first emits | **Drops** that `a$` emission; waits for next `a$` |
| **When does it complete?** | When **both** `a$` and `b$` complete | As soon as **`a$`** completes |

## See it

Watch the exact same sequence of events (`a = 1`, `b = 'X'`, `b = 'Y'`, `a = 2`) flow through both operators:

```typescript
import { Subject, combineLatest, withLatestFrom } from 'rxjs';

const a$ = new Subject<number>();
const b$ = new Subject<string>();

combineLatest([a$, b$]).subscribe(([a, b]) =>
  console.log('combineLatest:', a, b),
);

a$.pipe(withLatestFrom(b$)).subscribe(([a, b]) =>
  console.log('withLatestFrom:', a, b),
);

a$.next(1);   // Neither emits yet (b$ hasn't emitted; withLatestFrom drops 1!)
b$.next('X'); // combineLatest emits: 1, "X" (withLatestFrom stays silent!)
b$.next('Y'); // combineLatest emits: 1, "Y" (withLatestFrom stays silent!)
a$.next(2);   // BOTH emit: 2, "Y"
```

## Go deeper

Look closely at what happened when `a$.next(1)` was followed by `b$.next('X')`:

- **`combineLatest([a$, b$])`** remembered `1` from `a$`, so as soon as `b$` emitted `'X'`, `combineLatest` immediately emitted `[1, 'X']`.
- **`a$.pipe(withLatestFrom(b$))`** dropped `1` because `b$` wasn't ready yet, and ignored `b$.next('X')` and `b$.next('Y')` because `b$` is not a trigger — it didn't emit until `a$.next(2)` arrived!

So the decision rule is simple:

- **Derived value / live filter / computed view model** (`price$ * qty$`, `items$ + filter$ + sort$`): use **`combineLatest`**.
- **Action / event sampling current state** (`submitClick$` reading `formValue$`, `request$` reading `authToken$`): use **`withLatestFrom`**.

## Watch out

- Never combine an action trigger (`deleteClick$`) with live state (`selectedItem$`) using `combineLatest` — changing `selectedItem$` after the first click will immediately fire another delete!
- Remember that `withLatestFrom` drops primary triggers that arrive before all secondary streams have emitted at least once.

## Remember

- `combineLatest([a$, b$])`: **symmetric peers** — any stream emitting triggers a new combined output.
- `a$.pipe(withLatestFrom(b$))`: **primary trigger + passive state** — only `a$` triggers output.
- Use `combineLatest` for derived state and `withLatestFrom` for events that read current state.
