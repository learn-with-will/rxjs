---
slug: share-placement-and-deprecated-multicast
title: "Where to place share() in .pipe() (and deprecated multicast APIs)"
topic: subjects-multicasting
kind: technique
summary: "Operators above share() run once per shared execution while operators below share() run once per subscriber; replace multicast/publish/refCount with share()."
tags: [rxjs, share, sharereplay, multicast, pipe, rxjs-7]
related: [share-operator, share-replay-for-caching, operator-order-matters]
since: "7.0"
---
> **In one line:** everything **above `share()`** in `.pipe()` runs **once** for the whole group, while everything **below `share()`** runs **separately for each subscriber**.

## The idea

Where you place `share()` or `shareReplay()` inside a `.pipe()` chain determines which work is shared and which work is duplicated per subscriber:

```flow
src: Cold Source (fetch / timer) | runs ONCE @primary
m1: map / tap ABOVE share() | runs ONCE for all subscribers @blue
sh: share() | multicast boundary @amber
m2: map / tap BELOW share() | runs N TIMES (once per subscriber!) @red
src -> m1: 1x execution
m1 -> sh: 1x execution
sh -> m2: multiplied per subscriber
```

- Put expensive transformations (`map(parseJson)`, `switchMap(fetchData)`), logging `tap`s, and deduplication **above `share()`** so they execute only once per emission.
- Only put subscriber-specific operators (like a subscriber's own `takeUntil(componentDestroy$)`) **below `share()`**.

## See it

Watch how the `tap` above `share()` runs once, while the `tap` below `share()` runs twice for two subscribers:

```typescript
import { of, map, tap, share } from 'rxjs';

const pipeline$ = of(10).pipe(
  tap(() => console.log('1. ABOVE share(): runs ONCE')),
  map((n) => n * 2),
  share(),
  tap(() => console.log('2. BELOW share(): runs PER SUBSCRIBER')),
);

// Wait! On synchronous of(10), Sub 1 completes immediately before Sub 2 subscribes,
// or on async streams both run the BELOW tap:
pipeline$.subscribe((v) => console.log('Sub A:', v));
pipeline$.subscribe((v) => console.log('Sub B:', v));
```

And if you encounter legacy RxJS 6 multicasting operators in older codebases, here is how they map to RxJS 7+:

| Deprecated RxJS 6 API (removed in v8) | Modern RxJS 7+ replacement |
|---|---|
| `multicast(() => new Subject()), refCount()` | `share()` |
| `publish(), refCount()` | `share()` |
| `publishReplay(1), refCount()` | `shareReplay({ bufferSize: 1, refCount: true })` |
| `publishBehavior(seed), refCount()` | `share({ connector: () => new BehaviorSubject(seed) })` |
| ` publishLast(), refCount()` | `share({ connector: () => new AsyncSubject() })` |

## Go deeper

Look at what happens if you accidentally place `takeUntil(componentADestroy$)` **above** `shareReplay({ bufferSize: 1, refCount: false })` on a shared service stream: when Component A unmounts, `takeUntil` completes the shared stream upstream, killing it for Component B and Component C! Always place `share()` / `shareReplay()` at the end of the shared service pipeline, and let each consuming component attach its own `takeUntil(this.destroy$)` **below** `share()` when subscribing.

Similarly, if you place `retry(2)` **above** `share()`, all subscribers share the retry attempts together as one group; if you place `retry(2)` **below** `share()`, an upstream error tears down the shared Subject and each subscriber triggers its own retry sequence. In almost all cases, `retry` and `catchError` belong **above** `share()` so the shared stream recovers once for all listeners.

## Watch out

- Never put a single component's `takeUntil(destroy$)` **above** a shared `share()` or `shareReplay()` stream — it will complete the shared stream for every other consumer.
- Do not use `multicast`, `publish`, `publishReplay`, `publishBehavior`, `publishLast`, or `refCount` in RxJS 7+ code — `share({...})` and `shareReplay({...})` replace all of them.

## Remember

- Operators **above** `share()` run once per shared upstream emission; operators **below** `share()` run once per subscriber.
- Place shared transformations above `share()` and per-consumer `takeUntil` below `share()`.
- Replace all legacy `multicast`/`publish*`/`refCount` chains with `share()` or `shareReplay()`.
