---
id: lesson-13
slug: multicasting-hot-and-cold
title: "Multicasting: Hot vs Cold, share, and shareReplay"
level: intermediate
order: 13
duration: 30
tags:
  - rxjs
  - hot
  - cold
  - share
  - sharereplay
  - multicast
---

# Learning Objectives

By the end of this lesson you will be able to:

- Define **cold** (unicast) vs **hot** (multicast) Observables precisely.
- Share one execution across many subscribers with `share()`.
- Cache and replay values to late subscribers with `shareReplay()`.
- Explain and avoid the **`shareReplay` refCount leak**.
- Place `share`/`shareReplay` correctly in a pipeline.
- Recognise the deprecated `multicast`/`publish` operators and their modern replacements.

# Why This Concept Exists

You already know plain Observables are **cold**: subscribe twice and the producer runs twice — two HTTP requests, two WebSocket connections, two of everything. That's wasteful when several parts of your app want the **same** result. **Multicasting** shares a single execution among all subscribers. Getting it right (and knowing the one dangerous default) is essential for performant, leak-free apps.

# Concept Explanation

### Cold vs hot

- **Cold / unicast:** the Observable creates its producer **inside** each subscription. Every subscriber gets an independent run. (`of`, `from`, `ajax`, and most Observables are cold.)
- **Hot / multicast:** the producer is **shared**. All subscribers observe the same execution and the same values. (A `Subject`, or any Observable made hot with `share`.)

Multicasting turns a cold Observable hot by routing its single execution through a Subject that fans values out to every subscriber.

### `share()` — one execution, reference-counted

`share()` multicasts the source. The first subscriber triggers a single subscription to the source; additional subscribers attach to that same execution. By default `share()` is **reference-counted**: when the last subscriber leaves (refCount → 0) it unsubscribes from the source, and a later subscriber restarts it.

```ts
import { interval, tap, share, take } from 'rxjs';

const shared$ = interval(1000).pipe(
  tap((n) => console.log('source produced', n)), // proof of one execution
  share(),
);

shared$.subscribe((n) => console.log('A', n));
setTimeout(() => shared$.subscribe((n) => console.log('B', n)), 2500);
// "source produced" logs ONCE per tick, not once per subscriber.
```

A plain `share()` uses a `Subject`, so it has **no replay** — a late subscriber (B) only sees values from the moment it joins.

### `shareReplay()` — multicast *and* replay

`shareReplay(bufferSize)` multicasts and **replays** the last `bufferSize` values to new/late subscribers. It's the standard way to **cache** a result so everyone reuses it.

```ts
import { shareReplay } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const config$ = ajax.getJSON('/api/config').pipe(shareReplay(1));

config$.subscribe((c) => console.log('reader 1', c)); // triggers ONE request
config$.subscribe((c) => console.log('reader 2', c)); // reuses cached value
// A subscriber added minutes later still gets the replayed config, no new request.
```

### The `shareReplay` refCount leak (important)

In RxJS 7, `shareReplay(1)` uses **`refCount: false`** by default. That means once it subscribes to the source, it **keeps the source subscription alive even after all subscribers leave**. For a source that **completes** (like an HTTP request) this is fine — that's the caching behaviour you want. But for an **infinite** source (like `interval` or a WebSocket), it **leaks**: the source keeps running forever with no subscribers.

```ts
// LEAK: interval never completes, refCount:false keeps it running with 0 subscribers.
const bad$ = interval(1000).pipe(shareReplay(1));

// SAFE: refCount:true unsubscribes from the source when the last subscriber leaves.
const good$ = interval(1000).pipe(shareReplay({ bufferSize: 1, refCount: true }));
```

**Rule of thumb:** `shareReplay(1)` is fine for **completing** sources (HTTP). For **infinite** sources, use `shareReplay({ bufferSize: 1, refCount: true })` (or ensure the source completes).

### Placement matters

Everything **above** `share`/`shareReplay` in the pipe is shared; operators **below** run per subscriber. Put the multicast operator right after the expensive work you want to share once.

```ts
source$.pipe(
  expensiveOperator(), // shared (runs once)
  shareReplay(1),
  map((x) => x),       // runs per subscriber (cheap projection)
);
```

### Deprecated multicasting operators

`multicast`, `publish`, `publishReplay`, `publishBehavior`, `publishLast`, and `refCount` are **deprecated** in RxJS 7. Use `share`, `shareReplay`, or the lower-level `connectable`/`connect` instead.

# Key Terminology

- **Cold / unicast** — producer per subscription; independent executions.
- **Hot / multicast** — one shared execution observed by all subscribers.
- **`share()`** — reference-counted multicast via a Subject; no replay.
- **`shareReplay(n)`** — multicast that replays the last `n` values to new subscribers.
- **refCount** — whether the multicast unsubscribes from the source when subscriber count hits zero.
- **`connectable` / `connect`** — lower-level primitives for manual multicast control.

# Syntax Overview

```ts
import { interval, share, shareReplay, take } from 'rxjs';
import { ajax } from 'rxjs/ajax';

interval(1000).pipe(share());                                  // hot, no replay, refCounted
ajax.getJSON('/api/config').pipe(shareReplay(1));               // cache a completing source
interval(1000).pipe(shareReplay({ bufferSize: 1, refCount: true })); // safe infinite multicast

// Custom share config (RxJS 7):
interval(1000).pipe(share({ resetOnRefCountZero: true }));
```

# Real World Analogy

A cold Observable is **cooking a private meal for each guest** — five guests, five separate dinners. Multicasting is **catering one buffet** everyone shares: the kitchen cooks once (`share`). A plain `share` buffet only lets you eat what's on the table *right now* — arrive late and you've missed earlier courses. `shareReplay` is a buffet with a **warming tray** that keeps the last dishes out, so latecomers can still serve themselves. The `refCount` leak is leaving the buffet running all night after everyone's gone home — fine if the kitchen was going to close anyway (a completing source), wasteful if it means the stove burns forever (an infinite source).

# Examples

## Example 1 — Basic: prove one execution with share

```ts
import { defer, of, tap, share, delay } from 'rxjs';

let requests = 0;
const source$ = defer(() => {
  requests++;
  return of(`response ${requests}`).pipe(delay(100));
});

const shared$ = source$.pipe(share());
shared$.subscribe((v) => console.log('A:', v));
shared$.subscribe((v) => console.log('B:', v));
// One request; A and B receive the same 'response 1'.
// Without share(), the counter would reach 2 — two separate executions.
```

**Why this is good:** The request counter makes multicasting concrete: `share()` collapses two subscriptions into a single source execution, exactly what you want for an expensive operation.

## Example 2 — Real-world: cache an HTTP config with shareReplay(1)

```ts
import { shareReplay, map } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// A single, cached config stream reused across the app.
export const config$ = ajax.getJSON<{ apiBase: string }>('/api/config').pipe(
  shareReplay(1), // safe: the request completes, so no leak
);

// Anywhere in the app:
config$.subscribe((c) => console.log('base:', c.apiBase)); // triggers the request once
config$.pipe(map((c) => c.apiBase)).subscribe(console.log); // reuses the cached response
```

**Why this is good:** The request fires once and its result is replayed to every current and future subscriber. Because the HTTP source **completes**, `shareReplay(1)`'s default `refCount: false` is exactly the caching behaviour you want — no leak.

## Example 3 — Best practice: multicast an infinite source safely

```ts
import { interval, map, shareReplay } from 'rxjs';

// A shared clock several components read from.
const clock$ = interval(1000).pipe(
  map(() => new Date()),
  shareReplay({ bufferSize: 1, refCount: true }), // unsubscribe source when nobody listens
);

const sub1 = clock$.subscribe((t) => console.log('widget1', t.toLocaleTimeString()));
const sub2 = clock$.subscribe((t) => console.log('widget2', t.toLocaleTimeString()));
// Both read one shared interval. When both unsubscribe, the interval stops.
```

**Why this is good:** For an infinite source, `refCount: true` ties the source's lifetime to its subscribers — the interval runs while at least one component listens and stops when they all leave, avoiding the classic `shareReplay` leak.

# Common Mistakes

- **`shareReplay(1)` on an infinite source.** With the default `refCount: false`, the source runs forever after subscribers leave. **Fix:** `shareReplay({ bufferSize: 1, refCount: true })`, or complete the source.
- **Expecting `share()` to replay.** A plain `share()` has no memory; late subscribers miss earlier values. **Fix:** use `shareReplay(n)` when latecomers need past values.
- **Putting `share` in the wrong place.** Only operators **above** it are shared. **Fix:** place `share`/`shareReplay` immediately after the expensive work you want to run once.
- **Forgetting that refCounted shares restart.** After the last subscriber leaves, the next one re-runs the source. **Fix:** if you need a one-time cache, use `shareReplay(1)` (refCount:false) on a completing source.
- **Using deprecated `publish`/`multicast`/`refCount`.** **Fix:** use `share`/`shareReplay` (or `connectable`/`connect`).

# Best Practices

- For **completing** sources you want to cache (HTTP), use `shareReplay(1)`.
- For **infinite** sources you want to share, use `shareReplay({ bufferSize: 1, refCount: true })` or `share()`.
- Place the multicast operator **right after** the work to share; keep cheap per-subscriber projections below it.
- Prefer `share`/`shareReplay` over the deprecated `publish*`/`multicast` family.
- When in doubt about lifetime, choose the **refCounted** form so the source stops when nobody is listening.

# Summary

- **Cold** Observables run a producer per subscriber; **hot** Observables share one execution.
- **Multicasting** turns cold into hot via a Subject; `share()` does this, reference-counted, with **no replay**.
- **`shareReplay(n)`** multicasts and replays the last `n` values to new/late subscribers — ideal for caching.
- In RxJS 7, `shareReplay(1)` defaults to **`refCount: false`**, which **leaks on infinite sources**; use `{ bufferSize: 1, refCount: true }` there.
- Operators **above** `share`/`shareReplay` are shared; those **below** run per subscriber.
- The `publish*`/`multicast`/`refCount` operators are deprecated — prefer `share`/`shareReplay`.

# Flash Cards

Q: What is the difference between a cold and a hot Observable?
A: Cold (unicast) runs the producer independently per subscriber; hot (multicast) shares one execution so all subscribers see the same values.

Q: Does `share()` replay past values to a late subscriber?
A: No. A plain `share()` uses a Subject with no memory, so late subscribers only see values emitted after they join. Use `shareReplay(n)` to replay.

Q: What is the `shareReplay` refCount leak, and when does it bite?
A: In RxJS 7, `shareReplay(1)` defaults to refCount:false, keeping the source subscribed even with zero subscribers. On an infinite source (interval, WebSocket) that runs forever — a leak. Fix with `{ bufferSize: 1, refCount: true }`.

Q: Why is `shareReplay(1)` safe for an HTTP request but risky for `interval`?
A: The HTTP source completes, so keeping it "alive" is harmless caching. interval never completes, so refCount:false keeps it running with no listeners.

Q: In a pipe, which operators are shared by `share`/`shareReplay` — those above or below it?
A: Those above it run once (shared); operators placed below it run per subscriber.

Q: Which multicasting operators are deprecated in RxJS 7?
A: multicast, publish, publishReplay, publishBehavior, publishLast, and refCount. Use share/shareReplay (or connectable/connect) instead.

# Exercises

### Easy

Create a cold Observable using `defer` that increments and logs a counter, then emits it. Subscribe twice and confirm the counter reaches 2. Add `share()` and confirm it stays at 1 — proving one shared execution.

### Medium

Wrap a fake HTTP call `of({ ok: true }).pipe(delay(300))` with `shareReplay(1)`. Subscribe immediately and again after 1 second. Confirm only one "request" happens and both subscribers receive the value, including the late one. Explain why refCount doesn't matter here.

### Challenging

Take `interval(500)` and pipe it through `shareReplay(1)`. Subscribe two observers, then unsubscribe both, and show (with a `tap` log in the source) that the interval keeps running — the leak. Fix it with `shareReplay({ bufferSize: 1, refCount: true })` and confirm the interval stops once both unsubscribe.

# Further Reading

- [RxJS — Subject (multicasting)](https://rxjs.dev/guide/subject)
- [RxJS — share](https://rxjs.dev/api/index/function/share)
- [RxJS — shareReplay](https://rxjs.dev/api/index/function/shareReplay)
- [RxJS — connectable](https://rxjs.dev/api/index/function/connectable) and [connect](https://rxjs.dev/api/index/function/connect)
