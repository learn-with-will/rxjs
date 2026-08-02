---
id: lesson-21
slug: memory-management-and-anti-patterns
title: "Memory Management and Anti-Patterns"
level: advanced
order: 21
duration: 28
tags:
  - rxjs
  - memory-leaks
  - takeuntil
  - unsubscribe
  - anti-patterns
---

# Learning Objectives

By the end of this lesson you will be able to:

- Recognise the common causes of RxJS memory leaks.
- Apply the main unsubscription strategies: manual, `takeUntil`, and auto-completing operators.
- Avoid the nested-`subscribe` anti-pattern by flattening.
- Prevent the `shareReplay` refCount leak.
- Prefer declarative composition and framework bindings over manual subscribing.
- Keep pipelines pure by putting side effects in `tap`.

# Why This Concept Exists

Every `subscribe()` on a non-completing stream is a resource you now own — a listener, a timer, a network connection — and if you never unsubscribe, it lives forever. In long-running apps (SPAs especially), leaked subscriptions pile up: retained memory, duplicated work, and callbacks firing on components that no longer exist (often throwing errors). This lesson gathers the disciplines that keep RxJS code leak-free and idiomatic.

# Concept Explanation

### What leaks, and why

A subscription leaks when its source **never completes** and you **never unsubscribe**. The usual culprits:

- Infinite sources: `interval`, `fromEvent`, `timer(x, y)`, WebSocket streams, long-lived Subjects.
- Forgotten manual subscriptions whose `Subscription` was discarded.
- `shareReplay(1)` (refCount:false) on an infinite source (Lesson 13).

Finite sources that **complete** (a single HTTP request, `of`, `take(1)`) clean themselves up — no action needed.

### Strategy 1 — auto-completing operators

If the stream naturally needs only a bounded number of values, let it complete itself: `take(n)`, `first()`, `takeWhile(...)`. A completed stream tears down automatically.

```ts
import { fromEvent, take } from 'rxjs';
fromEvent(button, 'click').pipe(take(1)).subscribe(handleOnce); // self-cleans after 1 click
```

### Strategy 2 — `takeUntil(destroy$)`

For long-lived subscriptions tied to a lifecycle, complete them all with one signal. Keep a `destroy$` Subject, pipe **`takeUntil(destroy$)` last**, and fire it on teardown.

```ts
import { Subject, interval, takeUntil } from 'rxjs';

const destroy$ = new Subject<void>();
interval(1000).pipe(takeUntil(destroy$)).subscribe(tick);

function onDestroy() {
  destroy$.next();
  destroy$.complete();
}
```

### Strategy 3 — manual unsubscribe (grouped)

Store subscriptions and unsubscribe explicitly, ideally grouped in one parent `Subscription`:

```ts
import { Subscription } from 'rxjs';
const subs = new Subscription();
subs.add(stream1$.subscribe(/* */));
subs.add(stream2$.subscribe(/* */));
// teardown:
subs.unsubscribe();
```

### Strategy 4 — let the framework subscribe

The best subscription is often the one you never write. Angular's **`async` pipe** subscribes and unsubscribes automatically; Angular 16+ offers **`takeUntilDestroyed()`**. Binding a stream to the view is safer than a manual `subscribe`.

### Anti-pattern: nested `subscribe`

Subscribing inside a subscribe loses cancellation and error propagation and leaks inner subscriptions (Lesson 9). Flatten instead.

```ts
// ANTI-PATTERN
outer$.subscribe((x) => inner$(x).subscribe((y) => use(y)));

// FIX — flatten with the right strategy
outer$.pipe(switchMap((x) => inner$(x))).subscribe(use);
```

### Anti-pattern: manual subscribe for pure transformation

If you `subscribe` only to push values into another Subject or variable, you've stepped outside the stream. Prefer composing operators and subscribing once at the edge.

### Keep pipelines pure

Side effects belong in `tap`, not in `map`/`filter` callbacks. Impure operators are hard to test and reason about, and can hide subscription-order bugs.

# Key Terminology

- **Memory leak** — a subscription (and its resources) that is never released.
- **`takeUntil(destroy$)`** — the lifecycle-scoped completion pattern.
- **Auto-completing operator** — `take`, `first`, `takeWhile`; completes and self-tears-down.
- **Nested subscribe** — subscribing inside another subscribe; an anti-pattern.
- **`async` pipe / `takeUntilDestroyed`** — Angular helpers that manage subscription lifetime.
- **Zombie subscription** — one still firing callbacks after its owner is destroyed.

# Syntax Overview

```ts
import { Subject, interval, fromEvent, switchMap, takeUntil, take, finalize } from 'rxjs';

const destroy$ = new Subject<void>();

// 1. Auto-complete when bounded:
fromEvent(el, 'click').pipe(take(1)).subscribe(handleOnce);

// 2. Lifecycle scope (takeUntil LAST):
interval(1000).pipe(takeUntil(destroy$)).subscribe(tick);

// 3. Flatten instead of nesting:
search$.pipe(switchMap((q) => request(q)), takeUntil(destroy$)).subscribe(render);

// 4. Guaranteed cleanup on any exit:
stream$.pipe(finalize(() => release()), takeUntil(destroy$)).subscribe();

function onDestroy() { destroy$.next(); destroy$.complete(); }
```

# Real World Analogy

Subscriptions are **utility accounts**. A completing stream is a one-off purchase — pay once, done. An infinite stream is a **recurring subscription**: gym, streaming service, phone plan. If you move house and never cancel, you keep paying forever for something no one uses — that's a leak. `takeUntil(destroy$)` is setting every recurring account to auto-cancel the day you move out; `take(1)` is a strictly one-time pass; and the `async` pipe is a concierge who cancels everything in your name when you check out. Nested subscribe is signing up for a service *through* another service so you forget it even exists — impossible to cancel cleanly.

# Examples

## Example 1 — Basic: the leak and its fix

```ts
import { interval, Subject, takeUntil } from 'rxjs';

// LEAK: nothing ever stops this interval.
interval(1000).subscribe((n) => console.log('leaking', n));

// FIX: scope it to a lifecycle signal.
const destroy$ = new Subject<void>();
interval(1000).pipe(takeUntil(destroy$)).subscribe((n) => console.log('safe', n));

// When the owner is destroyed:
function destroy() {
  destroy$.next();
  destroy$.complete(); // stops the safe interval; the leaking one runs forever
}
```

**Why this is good:** Side by side, the leak and the fix make the discipline obvious: an infinite source must be bounded by a completion signal, or it runs forever with no way to stop it.

## Example 2 — Real-world: one destroy$ for a whole component

```ts
import { Subject, interval, fromEvent, switchMap, takeUntil } from 'rxjs';
import { ajax } from 'rxjs/ajax';

class Dashboard {
  private readonly destroy$ = new Subject<void>();

  init() {
    interval(5000)
      .pipe(switchMap(() => ajax.getJSON('/api/stats')), takeUntil(this.destroy$))
      .subscribe((stats) => this.render(stats));

    fromEvent(window, 'resize')
      .pipe(takeUntil(this.destroy$))
      .subscribe(() => this.relayout());
  }

  destroy() {
    this.destroy$.next();
    this.destroy$.complete(); // tears down BOTH subscriptions at once
  }

  render(s: unknown) {}
  relayout() {}
}
```

**Why this is good:** A single `destroy$` cleans up every subscription the component owns, no matter how many. It scales — adding a new subscription just means adding `takeUntil(this.destroy$)` (last), with nothing to remember on teardown.

## Example 3 — Best practice: flatten, don't nest; and finalize

```ts
import { fromEvent, switchMap, catchError, of, finalize, takeUntil, Subject } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const destroy$ = new Subject<void>();

fromEvent(document.querySelector('#load')!, 'click')
  .pipe(
    switchMap(() =>
      ajax.getJSON('/api/data').pipe(
        catchError(() => of(null)),
        finalize(() => hideSpinner()), // runs on complete/error/cancel of each request
      ),
    ),
    takeUntil(destroy$), // last operator
  )
  .subscribe((data) => render(data));

function hideSpinner() {}
function render(d: unknown) {}
```

**Why this is good:** It combines the disciplines — flattening with `switchMap` instead of nesting, per-request cleanup with `finalize`, and lifecycle teardown with `takeUntil` placed last. This is what production-grade RxJS looks like.

# Common Mistakes

- **Never unsubscribing infinite streams.** They leak listeners/timers. **Fix:** `takeUntil(destroy$)`, `take(n)`, or manual unsubscribe.
- **Nested `subscribe`.** Loses cancellation/errors and leaks inners. **Fix:** flatten with `switchMap`/`mergeMap`/`concatMap`.
- **`takeUntil` not last.** Operators after it can keep inner subscriptions alive. **Fix:** make it the final operator.
- **`shareReplay(1)` on an infinite source.** Leaks via refCount:false. **Fix:** `shareReplay({ bufferSize: 1, refCount: true })` or complete the source.
- **Manual `subscribe` where a binding exists.** More to leak, more to maintain. **Fix:** use the framework's async binding (e.g., Angular `async` pipe).
- **Side effects in `map`/`filter`.** Impure and surprising. **Fix:** move them to `tap`.

# Best Practices

- Prefer **declarative** teardown: `take`/`first` for bounded streams, `takeUntil(destroy$)` for lifecycles.
- Use **one `destroy$`** per component/service and place `takeUntil` **last** in every pipe.
- **Flatten** rather than nest; the flattening operator manages inner subscriptions for you.
- Let the **framework subscribe** when it can (Angular `async` pipe / `takeUntilDestroyed`).
- Guard cleanup with **`finalize`**, and keep operator callbacks **pure** (side effects in `tap`).
- Remember: streams that **complete** need no manual cleanup — the discipline is for the infinite ones.

# Summary

- Leaks come from **infinite streams that are never unsubscribed** (and the `shareReplay` refCount default).
- Bounded needs should **auto-complete** (`take`, `first`, `takeWhile`); lifecycle-scoped ones use **`takeUntil(destroy$)`** placed **last**.
- Group manual subscriptions in one `Subscription`, or better, let the **framework** manage them.
- **Nested `subscribe`** is an anti-pattern — flatten with a `*Map` operator.
- Keep operators **pure**; put side effects in **`tap`** and guaranteed cleanup in **`finalize`**.
- Completing streams clean themselves up; discipline is only required for the infinite ones.

# Flash Cards

Q: What is the fundamental cause of an RxJS memory leak?
A: A subscription to a source that never completes, which is never unsubscribed — so its listeners/timers/connections live forever.

Q: What are the main unsubscription strategies?
A: Auto-completing operators (take/first/takeWhile), the takeUntil(destroy$) pattern, grouped manual unsubscribe, and letting the framework manage it (e.g., Angular's async pipe).

Q: Why is nested subscribe an anti-pattern, and what's the fix?
A: It loses cancellation and error propagation and leaks inner subscriptions. Flatten with switchMap/mergeMap/concatMap instead.

Q: Do streams that complete (like a single HTTP request) need manual unsubscription?
A: No — a completing stream tears itself down automatically. Manual cleanup is only needed for infinite streams.

Q: Where should side effects go, and where should they NOT go?
A: Side effects belong in `tap` (and cleanup in `finalize`). They should not be inside `map`/`filter` callbacks, which must stay pure.

Q: How does one `destroy$` Subject simplify component cleanup?
A: Every subscription pipes `takeUntil(destroy$)` (last), so a single `destroy$.next()` completes them all at once — nothing to track individually.

# Exercises

### Easy

Write an `interval(500)` subscription that leaks, then fix it with `takeUntil(destroy$)`. Fire `destroy$.next()` after 2 seconds and confirm the safe version stops while the leaking one would continue.

### Medium

Refactor a nested-subscribe snippet — `clicks$.subscribe(() => request$().subscribe(render))` — into a single flattened pipeline using `switchMap`, ending with `takeUntil(destroy$)` placed last. Explain in a comment what the nested version leaked.

### Challenging

Build a component-like class with three long-lived subscriptions (`interval`, `fromEvent`, and a polled request via `switchMap`) all scoped by one `destroy$`. Add `finalize` to the request for spinner cleanup. Call `destroy()` and verify all three stop. Then deliberately move `takeUntil` before the `switchMap` in one pipe and describe why that reintroduces a potential leak.

# Further Reading

- [RxJS — Subscription (unsubscribing)](https://rxjs.dev/guide/subscription)
- [RxJS — takeUntil](https://rxjs.dev/api/index/function/takeUntil)
- [RxJS — finalize](https://rxjs.dev/api/index/function/finalize)
- [Angular — takeUntilDestroyed](https://angular.dev/api/core/rxjs-interop/takeUntilDestroyed)
