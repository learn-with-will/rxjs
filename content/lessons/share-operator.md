---
slug: share-operator
title: "Multicasting live streams with share()"
topic: subjects-multicasting
kind: api
summary: "share(config?) multicasts a cold source through a Subject with reference counting, connecting on the first subscriber and disconnecting on the last."
tags: [rxjs, share, multicasting, refcount, hot]
related: [cold-vs-hot-observables, share-replay-for-caching, share-placement-and-deprecated-multicast]
since: "7.0"
---
> **In one line:** `share()` turns a cold Observable into a **reference-counted hot stream** — subscribing upstream when the subscriber count goes `0 → 1` and unsubscribing upstream when it drops `1 → 0`.

## The idea

Suppose two UI widgets both subscribe to a live WebSocket or polling stream `prices$`. Without multicasting, both widgets open their own separate WebSocket connection.

Piping `prices$` through **`share()`** inserts an internal `Subject` between the cold upstream source and your downstream subscribers, managed by automatic **reference counting**:

1. **`0 → 1` subscribers (connect):** when the **first** subscriber subscribes to the shared stream, `share()` subscribes once to the cold upstream source.
2. **`1 → 2 → N` subscribers (multicast):** while the upstream subscription is active, additional subscribers simply attach to the internal `Subject` — **no new upstream subscription** is created!
3. **`N → … → 0` subscribers (disconnect):** when the **last** subscriber unsubscribes (or the upstream source completes/errors), `share()` automatically unsubscribes from the upstream source and resets its internal `Subject`.
4. **Resubscribe later:** if a new subscriber joins after the count hit `0` (or after completion), `share()` starts a fresh upstream execution.

```flow
src: Cold Source (interval / socket) | 1 single execution @primary
sh: share() | Subject + refCount (0->1 connect, 1->0 teardown) @amber
s1: Subscriber 1 | joins at t=0ms @green
s2: Subscriber 2 | joins at t=250ms (shares execution!) @green
src -> sh: single upstream subscription
sh -> s1: broadcasts values
sh -> s2: broadcasts values
```

## See it

Watch the upstream `tap` log only once per tick even though two subscribers are listening simultaneously:

```typescript
import { interval, tap, take, share } from 'rxjs';

const sharedTicks$ = interval(200).pipe(
  tap((n) => console.log(`[Upstream executed once for tick ${n}]`)),
  take(3),
  share(),
);

// Both subscribers join together and share ONE upstream interval:
sharedTicks$.subscribe((n) => console.log('Sub A:', n));
sharedTicks$.subscribe((n) => console.log('Sub B:', n));
// Output:
// [Upstream executed once for tick 0] -> Sub A: 0, Sub B: 0
// [Upstream executed once for tick 1] -> Sub A: 1, Sub B: 1
// [Upstream executed once for tick 2] -> Sub A: 2, Sub B: 2
```

## Go deeper

In **RxJS 7+**, `share(config?)` accepts a configuration object that makes it the **one universal multicasting operator**:

```typescript
import { ReplaySubject, timer, share } from 'rxjs';

// Customizing share() in RxJS 7+:
const customShared$ = source$.pipe(
  share({
    connector: () => new ReplaySubject(1), // use ReplaySubject instead of plain Subject
    resetOnError: true,
    resetOnComplete: false,
    resetOnRefCountZero: () => timer(1000), // grace period before disconnecting!
  }),
);
```

Notice `resetOnRefCountZero: () => timer(1000)`: if a user switches tabs so component A unmounts and component B mounts 50ms later, waiting 1,000ms before tearing down the upstream connection avoids disconnecting and immediately reconnecting when the subscriber count briefly touches `0`!

## Watch out

- Plain `share()` uses a plain `Subject` under the hood — a subscriber that joins *after* an emission has already happened will **not** receive that past emission (use `shareReplay` or `share({ connector: () => new ReplaySubject(1) })` when latecomers need the latest value).
- Once the upstream source completes, a new subscriber to plain `share()` will trigger a fresh upstream subscription.

## Remember

- `share()` multicasts one upstream execution across all active subscribers using reference counting.
- It connects upstream on `0 → 1` subscribers and disconnects upstream on `1 → 0` subscribers.
- In RxJS 7+, pass a config object to `share({ connector, resetOnRefCountZero, ... })` for custom replay or delayed teardown.
