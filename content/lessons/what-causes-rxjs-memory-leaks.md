---
slug: what-causes-rxjs-memory-leaks
title: "What causes RxJS memory leaks (and which streams are safe)"
topic: patterns-interop
kind: concept
summary: "Identify the 4 long-lived stream sources that cause memory leaks in UI components and distinguish them from finite streams that auto-clean up."
tags: [rxjs, memory-leaks, subscriptions, architecture, cleanup]
related: [subscription-and-unsubscribe, unsubscription-strategies, share-replay-refcount-leak]
---
> **In one line:** a memory leak happens when a short-lived consumer (like a UI component) subscribes to a **long-lived producer** (like `window` events, `interval`, a global `BehaviorSubject`, or `shareReplay(1)`) without unsubscribing on unmount.

## The idea

Why does forgetting to unsubscribe keep an entire unmounted component tree in memory? Because the reference chain runs **from producer to subscriber**:
`Long-lived Producer` → holds `Subscriber` callback → closure captures `this` / component state → **Garbage Collector cannot free the component!**

Not every Observable is infinite. Knowing which sources complete automatically and which stay alive indefinitely tells you where leaks happen:

| Source category | Examples | Completes on its own? | Leak risk if not unsubscribed |
|---|---|---|---|
| **1. Global DOM / Event emitters** | `fromEvent(window, 'resize')`, `fromEvent(document, 'keydown')` | **Never** | **Critical** — `window` outlives every component |
| **2. Unbounded timers** | `interval(1000)`, `timer(0, 5000)` | **Never** | **Critical** — `setInterval` runs forever |
| **3. Long-lived Subjects / Stores** | Singleton `BehaviorSubject`, WebSocket `Subject` | **Never** (until app shutdown) | **Critical** — service retains every dead component's observer in its internal list |
| **4. `shareReplay(1)` (default)** | `obs$.pipe(shareReplay(1))` (`refCount: false`) | Only if upstream completes | **High** — keeps upstream subscription alive at 0 subscribers |
| **5. Finite synchronous / single-shot** | `of(1, 2)`, `from([1, 2])`, `firstValueFrom(http$)` | **Yes** (completes & auto-unsubscribes) | **Low** (though unsubscribing cancels in-flight HTTP work and prevents stale-setState warnings) |

```flow
win: Long-lived Producer | window or BehaviorSubject @red
sub: Active Subscriber | in internal observers[] list @amber
comp: Unmounted UI Component | closure captures state and DOM @red
win -> sub: retains reference
sub -> comp: closure retains
```

## See it

Contrast a global store subscription that leaks every time a component mounts/unmounts against one that cleans up:

```typescript
import { BehaviorSubject } from 'rxjs';

// Singleton app-wide state store (lives for the entire browser session):
export const themeStore$ = new BehaviorSubject<'light' | 'dark'>('dark');

class LeakyHeaderWidget {
  // ❌ Leaky: when the widget is destroyed, themeStore$ still holds this callback!
  init() {
    themeStore$.subscribe(theme => this.render(theme));
  }
  render(theme: string) { console.log('Header:', theme); }
}

class SafeHeaderWidget {
  // ✅ Safe: store the Subscription (or use takeUntil) and unsubscribe on destroy
  private sub = themeStore$.subscribe(theme => this.render(theme));
  render(theme: string) { console.log('Header:', theme); }
  destroy() { this.sub.unsubscribe(); }
}
```

## Go deeper

What about a single-shot HTTP request? Even though an HTTP Observable emits once and calls `complete()` (which triggers auto-unsubscription), what if the user navigates away while a slow 3-second request is still in flight? If you don't unsubscribe on unmount, the HTTP request isn't aborted (`AbortController`) and its `next` callback will still run 3 seconds later on a destroyed component! For that reason, treat **all** component-level `.subscribe()` calls uniformly: always clean them up on unmount.

## Watch out

- Never assume a custom service method `userService.getUser()` is finite just because it looks like an HTTP call — tomorrow a teammate might refactor it to return a cached `BehaviorSubject` or `shareReplay({ bufferSize: 1, refCount: true })` stream that never completes!
- Leaving zombie subscriptions alive doesn't just leak RAM; their `next()` callbacks keep executing CPU work and mutating stale state in the background.

## Remember

- Memory leaks occur because **producers hold references to subscribers**, preventing garbage collection of the subscriber's closure.
- The 4 classic leak sources are global `fromEvent`, `interval`/`timer`, shared service `Subject`s, and `shareReplay(1)` (`refCount: false`).
- Always clean up component subscriptions on unmount, even if a stream appears finite today.
