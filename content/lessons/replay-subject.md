---
slug: replay-subject
title: "Buffering history with ReplaySubject"
topic: subjects-multicasting
kind: api
summary: "ReplaySubject(bufferSize?, windowTime?) records up to bufferSize recent emissions and replays them in order to every late subscriber, even after completion."
tags: [rxjs, replaysubject, caching, buffer, subject]
related: [what-is-a-subject, behavior-subject, async-subject, share-replay-for-caching]
---
> **In one line:** `new ReplaySubject<T>(bufferSize?, windowTime?)` records the last `bufferSize` values (within an optional `windowTime` in ms) and **replays them synchronously to any new subscriber**.

## The idea

What if you want late subscribers to receive the most recent value(s), **or** you don't have a meaningful initial seed value before the first real event arrives, **or** you need to replay the last *N* values instead of just 1?

That is **`ReplaySubject<T>(bufferSize?, windowTime?)`**:

- **No initial seed required:** unlike `BehaviorSubject`, a `new ReplaySubject<User>(1)` starts empty. If a subscriber joins before the first `next()`, it simply waits until the first value arrives; if it joins *after* `next(user)`, it immediately receives `user`.
- **Configurable buffer size:** `new ReplaySubject(3)` keeps a sliding window of the **3 most recent** values and replays all 3 in order to every new subscriber. (Omitting `bufferSize` buffers *every* value ever emitted!)
- **Configurable time window:** `new ReplaySubject(10, 5000)` replays up to the last 10 values emitted within the **last 5,000ms**.
- **Replays even after `complete()`:** unlike `BehaviorSubject`, a completed `ReplaySubject` **still replays its buffered values** to late subscribers before sending `complete()`!

| Feature | `BehaviorSubject(seed)` | `ReplaySubject(1)` |
|---|---|---|
| Requires initial seed value? | **Yes** | **No** (starts empty until first `next`) |
| Emits on subscribe before 1st `next()`? | **Yes** (emits `seed`) | **No** (waits for first real value) |
| Replays buffered value(s) after `complete()`? | **No** | **Yes** |

## See it

```typescript
import { ReplaySubject } from 'rxjs';

// Keep a rolling history of the last 2 emissions:
const history$ = new ReplaySubject<string>(2);

history$.next('Page A');
history$.next('Page B');
history$.next('Page C'); // 'Page A' is evicted (bufferSize is 2)
history$.complete();

// Late subscriber joins AFTER complete() — still gets the last 2 values + complete!
history$.subscribe({
  next: (page) => console.log('Replayed:', page),
  complete: () => console.log('Replay finished'),
});
// Output:
// Replayed: Page B
// Replayed: Page C
// Replay finished
```

## Go deeper

Look at the comparison table between `BehaviorSubject` and `ReplaySubject(1)` above. When a user profile hasn't been fetched yet, forcing `new BehaviorSubject<User | null>(null)` pollutes every downstream operator with `| null` checks. If you use `new ReplaySubject<User>(1)` instead, the type is a clean `Observable<User>`: early subscribers wait until the profile arrives, and late subscribers immediately receive the cached `User`! (Under the hood, RxJS's `shareReplay(1)` operator uses a `ReplaySubject(1)`.)

## Watch out

- Calling `new ReplaySubject()` with **no arguments** defaults to `bufferSize = Infinity`, retaining every value ever emitted in memory for the life of the Subject — always pass an explicit `bufferSize` like `1` or `5`.
- Do not store large object graphs in a large `ReplaySubject` buffer without a size or `windowTime` cap.

## Remember

- `new ReplaySubject(bufferSize, windowTime?)` replays the last `bufferSize` values to every new subscriber.
- Unlike `BehaviorSubject`, `ReplaySubject(1)` needs no initial dummy seed and still replays after `complete()`.
- Always specify an explicit `bufferSize` so the replay buffer stays bounded.
