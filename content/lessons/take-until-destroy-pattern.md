---
slug: take-until-destroy-pattern
title: "The takeUntil(destroy$) lifecycle pattern"
topic: patterns-interop
kind: pattern
summary: "Implement the canonical destroy$ Subject teardown pattern for classes and components, and always call both next() and complete() on destroy."
tags: [rxjs, takeuntil, destroy, lifecycle, memory-leaks]
related: [take-until-notifier, why-take-until-goes-last, unsubscription-strategies]
---
> **In one line:** create `private readonly destroy$ = new Subject<void>()`, place `takeUntil(this.destroy$)` as the **last** operator in every `.pipe()`, and call both `this.destroy$.next()` and `this.destroy$.complete()` on unmount.

## The idea

Whenever a class (a UI controller, custom element, WebSocket manager, or framework component) manages multiple RxJS subscriptions that should all end when the instance is destroyed, the **`destroy$` Subject pattern** is the universal standard across the RxJS ecosystem:

1. **Declare the notifier:** `private readonly destroy$ = new Subject<void>();`
2. **Pipe `takeUntil(this.destroy$)` last:** Append `takeUntil(this.destroy$)` as the **final operator** before `.subscribe(...)` on every stream.
3. **Fire and complete on teardown:** In your `destroy()` / `ngOnDestroy()` / `disconnectedCallback()` method, call:
   ```typescript
   this.destroy$.next();
   this.destroy$.complete();
   ```

Why must you call **both** `this.destroy$.next()` **and** `this.destroy$.complete()`?
- **`this.destroy$.next()`** is what actually triggers `takeUntil(this.destroy$)`! (`takeUntil` ignores `complete()` on its notifier and only reacts to `next()` emissions.)
- **`this.destroy$.complete()`** releases any remaining internal observer references on `destroy$` itself and prevents accidental future subscriptions after destruction.

> **Watch out:** Calling `this.destroy$.complete()` alone without `this.destroy$.next()` does **nothing** to `takeUntil(this.destroy$)` — all your subscriptions will stay open!

## See it

Here is the complete, production-ready `destroy$` pattern in a framework-agnostic controller (and its modern Angular 16+ `takeUntilDestroyed()` one-liner equivalent):

```typescript
import { Subject, interval, fromEvent, takeUntil } from 'rxjs';

export class DashboardController {
  private readonly destroy$ = new Subject<void>();

  init() {
    interval(5000).pipe(
      takeUntil(this.destroy$) // Always LAST in .pipe()!
    ).subscribe(() => this.refreshMetrics());

    fromEvent(window, 'online').pipe(
      takeUntil(this.destroy$)
    ).subscribe(() => this.refreshMetrics());
  }

  refreshMetrics() { console.log('Refreshing...'); }

  destroy() {
    this.destroy$.next();     // 1. Triggers all takeUntil(this.destroy$) operators
    this.destroy$.complete(); // 2. Closes the Subject itself
  }
}
```

## Go deeper

If you are using **Angular 16+**, `@angular/core/rxjs-interop` ships `takeUntilDestroyed()` which creates and wires the internal `DestroyRef` notifier for you automatically — so you just write `.pipe(takeUntilDestroyed())` inside your constructor (or `.pipe(takeUntilDestroyed(this.destroyRef))` inside other methods) without declaring `destroy$` manually!

What if a class can be **stopped and restarted** multiple times during its lifetime (like `startPolling()` / `stopPolling()`)? Then **do not** call `this.stop$.complete()` inside `stopPolling()`! Calling `this.stop$.next()` cancels all active `takeUntil(this.stop$)` streams while keeping `this.stop$` alive for the next `startPolling()` call. Only call `.complete()` when the class instance itself is permanently destroyed.

## Watch out

- Calling *only* `this.destroy$.complete()` without `this.destroy$.next()` does **not** trigger `takeUntil(this.destroy$)` — all your subscriptions will stay alive and leak!
- Always place `takeUntil(this.destroy$)` **at the very end** of `.pipe()` (except right before `share()`/`shareReplay({ refCount: true })` if intentionally sharing a bounded source).

## Remember

- `takeUntil(notifier$)` only fires when `notifier$` emits a **value** via `.next()`, not when it completes.
- Always call `this.destroy$.next()` followed by `this.destroy$.complete()` in your teardown hook.
- Place `takeUntil(this.destroy$)` as the final operator before `.subscribe()`.
