---
slug: unsubscription-strategies
title: "Comparing the 5 RxJS unsubscription strategies"
topic: patterns-interop
kind: concept
summary: "Choose the right cleanup strategy for any context: framework bindings, take(1)/first(), takeUntil(destroy$), Subscription.add(), or sub.unsubscribe()."
tags: [rxjs, unsubscribe, takeuntil, lifecycle, architecture]
related: [what-causes-rxjs-memory-leaks, take-until-destroy-pattern, grouping-subscriptions-with-add]
---
> **In one line:** prefer **framework auto-binding** (Angular `async` pipe / `toSignal`, React `useEffect` return) or **declarative `takeUntil(destroy$)`** over manual imperative `sub.unsubscribe()` tracking.

## The idea

There are five standard ways to manage subscription lifetimes in an RxJS codebase, ranked from most automatic to most manual:

| Strategy | How it works | Best for | Boilerplate |
|---|---|---|---|
| **1. Framework binding** | React `useEffect(() => { const s = obs$.subscribe(...); return () => s.unsubscribe(); }, [])`, or Angular `async` pipe / `toSignal(obs$)` | UI components rendering a single stream | **Lowest** (0 manual teardown fields) |
| **2. Declarative `takeUntil(destroy$)`** (or `takeUntilDestroyed()`) | Pipe `takeUntil(this.destroy$)` as the last operator on every subscription in a class | Class components, services, or controllers with 2+ streams | **Low** (1 shared subject per class) |
| **3. Self-bounding operators** (`take(1)`, `first()`, `takeWhile`) | Completes automatically as soon as the condition is met | One-shot reads of a `BehaviorSubject` or waiting for a single event | **Zero** teardown code |
| **4. Composite `Subscription.add()`** | `this.subs.add(s1$.subscribe(...));` then `this.subs.unsubscribe()` on destroy | Imperative modules grouping multiple subscriptions | **Medium** |
| **5. Single `sub.unsubscribe()`** | Hold `const sub = obs$.subscribe(...)` and call `sub.unsubscribe()` | Starting and stopping a single toggleable task (like a timer) | **High** if repeated for 5 streams |

> **Tip:** If you only need to read the **current snapshot** of a `BehaviorSubject` once (for example, inside a button click handler), don't leave an open subscription — either read `store$.getValue()` or pipe `store$.pipe(take(1))`, or better yet, combine it declaratively inside the click pipeline using `withLatestFrom(store$)`!

## See it

Compare how React `useEffect` cleanup and declarative `take(1)` eliminate manual subscription tracking:

```typescript
import { fromEvent, BehaviorSubject, take } from 'rxjs';

const authToken$ = new BehaviorSubject<string>('jwt-xyz');

// Strategy 1: React useEffect auto-cleanup (return () => sub.unsubscribe())
function mountWindowResizeListener(onWidth: (w: number) => void) {
  const sub = fromEvent(window, 'resize').subscribe(() => onWidth(window.innerWidth));
  return () => sub.unsubscribe(); // Returned directly from useEffect!
}

// Strategy 3: One-shot read from a long-lived BehaviorSubject using take(1)
// Completes and unsubscribes synchronously on the very first emission!
authToken$.pipe(take(1)).subscribe(token => {
  console.log('One-time token snapshot:', token);
});
```

## Go deeper

Why is declarative `takeUntil(destroy$)` preferred over `this.subs.add(...)` in multi-stream classes? Because with `takeUntil(this.destroy$)`, the cleanup rule sits **visually right inside the `.pipe()` chain** of the stream itself, and when the stream completes naturally earlier, its subscription is already cleaned up rather than lingering inside a parent `Subscription` container until the class is destroyed.

## Watch out

- Using `take(1)` on a stream that might *never* emit while the component is mounted still keeps the subscription alive until either 1 item arrives or the component is destroyed — combine `take(1)` with `takeUntil(destroy$)` if the emission isn't guaranteed!
- Never assign multiple `.subscribe()` calls to the same `this.sub = ...` variable without unsubscribing the previous one first; the overwritten subscription is orphaned and leaked.

## Remember

- In React hooks, return `() => sub.unsubscribe()` directly from `useEffect`.
- In Angular templates, prefer the `async` pipe or `toSignal(obs$)`; in imperative Angular code, use `takeUntilDestroyed()`.
- For framework-agnostic classes with multiple subscriptions, standardize on `takeUntil(this.destroy$)` as the final operator.
