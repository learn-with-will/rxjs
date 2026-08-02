---
id: lesson-11
slug: joining-streams
title: "Joining Streams: merge, concat, zip, forkJoin, and race"
level: intermediate
order: 11
duration: 28
tags:
  - rxjs
  - merge
  - concat
  - zip
  - forkjoin
  - race
---

# Learning Objectives

By the end of this lesson you will be able to:

- Interleave streams as values arrive with `merge`.
- Run streams strictly in sequence with `concat`.
- Pair values by index with `zip`.
- Wait for several streams to finish and collect their final values with `forkJoin`.
- Pick the first stream to respond with `race`.
- Choose the right joining operator and avoid their completion pitfalls.

# Why This Concept Exists

The previous lesson combined the *latest* values of streams. But there are other ways to join streams: sometimes you want to interleave two event sources, run tasks one after another, pair values positionally, gather the results of several parallel requests, or take whichever source answers first. Each of these has a dedicated operator, and the differences come down to **timing** and **completion**.

# Concept Explanation

### `merge` — interleave concurrently

`merge(a$, b$, …)` subscribes to all sources at once and emits every value as it arrives, interleaved. It completes only when **all** sources complete.

```text
a$:    --1----2----3--|
b$:    ----A----B-----|
merge: --1-A--2-B--3--|
```

### `concat` — one after another, in order

`concat(a$, b$, …)` subscribes to the **first** source and, only when it **completes**, moves to the next. Values come out strictly in source order.

```text
a$:     --1--2--|
b$:             --A--B--|
concat: --1--2----A--B--|
```

The catch: if an earlier source **never completes** (e.g., `interval`), the later sources **never start**.

### `zip` — pair by index

`zip(a$, b$, …)` waits until every source has produced its *n*-th value, then emits them together as a tuple `[a_n, b_n, …]`. It's positional, not time-based.

```text
a$:  --1------2------3--|
b$:  ----A--------B---------C--|
zip: ----[1,A]----[2,B]----([3,C]|)
```

Because it pairs by index, a fast source's extra values are **buffered** until the slow source catches up — a potential memory concern with mismatched rates.

### `forkJoin` — the RxJS `Promise.all`

`forkJoin([a$, b$, …])` (or the object form) subscribes to all sources and waits for **every** one to **complete**, then emits a single array/object of each source's **last** value.

```text
a$:       --1--2--3|
b$:       ----A------B|
forkJoin: -----------([3,B]|)
```

Key rules: every source must **complete** (so it's ideal for HTTP requests, wrong for `interval`); if any source completes **without emitting**, `forkJoin` completes without emitting; if any source **errors**, `forkJoin` errors.

```ts
import { forkJoin } from 'rxjs';
import { ajax } from 'rxjs/ajax';

forkJoin({
  user: ajax.getJSON('/api/user'),
  posts: ajax.getJSON('/api/posts'),
  settings: ajax.getJSON('/api/settings'),
}).subscribe(({ user, posts, settings }) => render(user, posts, settings));
```

### `race` — first to emit wins

`race(a$, b$, …)` subscribes to all sources and, as soon as **one emits its first value**, unsubscribes from the rest and mirrors only the winner.

```text
a$:   ------1--2--3
b$:   --A--B--C
race: --A--B--C          (b emitted first, so b wins; a is dropped)
```

### The pipeable siblings

Each has a pipeable `*With` form for chaining on an existing source: `mergeWith`, `concatWith`, `zipWith`, `raceWith`. And `startWith(...values)` prepends seed values — effectively a `concat` of a synchronous source in front.

# Key Terminology

- **`merge`** — concurrent interleaving; completes when all sources complete.
- **`concat`** — sequential; each source runs to completion before the next.
- **`zip`** — index-based pairing into tuples; buffers faster sources.
- **`forkJoin`** — waits for all to complete, emits each source's last value once (like `Promise.all`).
- **`race`** — mirrors whichever source emits first; unsubscribes the losers.
- **`*With` operators** — pipeable versions (`mergeWith`, `concatWith`, `zipWith`, `raceWith`).

# Syntax Overview

```ts
import { merge, concat, zip, forkJoin, race, of, interval, take } from 'rxjs';

merge(clicksA$, clicksB$);                 // interleave two event streams
concat(of('first'), of('second'));          // 'first' then 'second' (each completes)
zip(names$, ages$);                          // [name0, age0], [name1, age1], ...
forkJoin([req1$, req2$, req3$]);             // [last1, last2, last3] after all complete
race(fromCache$, fromNetwork$);              // whichever answers first

// Non-completing sources break some of these:
concat(interval(1000), of('never runs'));    // second source never starts
forkJoin([interval(1000)]);                   // never emits — interval never completes
```

# Real World Analogy

Think of joining runners' data. **`merge`** is a live feed from several races at once — you hear every result the instant it happens, all mixed together. **`concat`** is running heats back-to-back: heat two can't start until heat one finishes. **`zip`** is a three-legged race pairing partners by position — partner A's first result waits for partner B's first result before the pair counts. **`forkJoin`** is the medal ceremony: it waits until *every* race has finished, then reads out each race's final winner in one go. **`race`** (aptly named) is a photo-finish bet — you back all lanes, and only the first to cross the line pays out; the rest are torn up.

# Examples

## Example 1 — Basic: merge two button streams

```ts
import { fromEvent, merge, map } from 'rxjs';

const up$ = fromEvent(document.querySelector('#up')!, 'click').pipe(map(() => +1));
const down$ = fromEvent(document.querySelector('#down')!, 'click').pipe(map(() => -1));

merge(up$, down$).subscribe((delta) => console.log('change:', delta));
// Emits +1 or -1 as either button is clicked, in real time.
```

**Why this is good:** Two independent event sources feed one handler. `merge` interleaves them so you react to whichever fires, without caring about order — the natural fit for combining event streams.

## Example 2 — Real-world: parallel requests with forkJoin

```ts
import { forkJoin } from 'rxjs';
import { ajax } from 'rxjs/ajax';

// Load everything a page needs, in parallel, and render once all are ready.
forkJoin({
  profile: ajax.getJSON('/api/profile'),
  notifications: ajax.getJSON('/api/notifications'),
}).subscribe({
  next: ({ profile, notifications }) => renderPage(profile, notifications),
  error: (err) => showError(err), // if any request fails, forkJoin errors
});

function renderPage(a: unknown, b: unknown) {}
function showError(e: unknown) {}
```

**Why this is good:** `forkJoin` is the RxJS `Promise.all`: fire independent requests concurrently and act once **all** have completed with their final values. The object form gives you labelled, typed results.

## Example 3 — Best practice: race a request against a timeout fallback

```ts
import { race, timer, map } from 'rxjs';
import { ajax } from 'rxjs/ajax';

const data$ = ajax.getJSON('/api/slow');
const timeout$ = timer(3000).pipe(map(() => ({ fallback: true })));

race(data$, timeout$).subscribe((result) => console.log('used:', result));
// If the request answers within 3s it wins; otherwise the timer's fallback wins.
```

**Why this is good:** `race` gives a clean "first responder wins" pattern. Whichever of the network or the timer emits first is mirrored and the other is cancelled — a lightweight fallback strategy. (`timeout`, a later lesson, is the dedicated tool for erroring on slowness.)

# Common Mistakes

- **`concat` after a non-completing source.** `concat(interval(1000), other$)` never reaches `other$`. **Fix:** ensure earlier sources complete (e.g., add `take`), or use `merge` if order doesn't matter.
- **`forkJoin` with a source that never completes.** It will never emit. **Fix:** only give `forkJoin` completing sources (HTTP, `of`, `take(n)`).
- **`forkJoin` silently emitting nothing.** If any source completes with **no** value, the whole result is empty. **Fix:** guarantee each source emits, or provide defaults.
- **`zip` buffering unbounded.** A fast source paired with a slow one buffers the surplus. **Fix:** avoid `zip` across very mismatched rates; consider `combineLatest` or `withLatestFrom`.
- **Confusing `zip`, `combineLatest`, and `forkJoin`.** They pair-by-index, take-latest, and take-final respectively. **Fix:** pick by whether you need positional, latest, or final values.

# Best Practices

- Use **`forkJoin`** for parallel one-shot requests where you need all results together (it mirrors `Promise.all`).
- Use **`concat`** when operations must happen in a strict sequence and each completes.
- Use **`merge`** to combine independent event streams where order doesn't matter.
- Use **`zip`** only when values genuinely correspond by position and rates are similar.
- Use **`race`** for "first responder wins" fallbacks; reach for `timeout` when you specifically want to error on slowness.

# Summary

- **`merge`** interleaves sources concurrently and completes when all do.
- **`concat`** runs sources in order; a non-completing source blocks the rest.
- **`zip`** pairs values by index into tuples and buffers faster sources.
- **`forkJoin`** waits for all sources to **complete**, then emits each one's **last** value (like `Promise.all`).
- **`race`** mirrors the first source to emit and cancels the others.
- Every one has a pipeable `*With` sibling; choose by timing and completion needs.

# Flash Cards

Q: How is `forkJoin` like `Promise.all`?
A: It runs all sources in parallel, waits for every one to complete, then emits a single array/object of each source's last value — and errors if any source errors.

Q: What breaks if the first source passed to `concat` never completes?
A: The later sources never start. concat only subscribes to the next source after the current one completes, so a source like `interval` blocks everything after it.

Q: How does `zip` combine values, and what is its memory risk?
A: It pairs values by index into tuples, emitting the n-th tuple once every source has an n-th value. A faster source's surplus values are buffered until the slow source catches up.

Q: What does `race` do with the sources that don't emit first?
A: It unsubscribes from them and mirrors only the first source to emit — a "first responder wins" pattern.

Q: Which joining operator interleaves values from all sources as they arrive?
A: `merge` — it subscribes concurrently and emits each value the moment it arrives, completing only when all sources complete.

Q: If a source in `forkJoin` completes without ever emitting, what does forkJoin emit?
A: Nothing — forkJoin completes without emitting, because it needs a final value from every source.

# Exercises

### Easy

Use `concat(of(1, 2, 3), of(4, 5, 6))` and log the values, confirming they come out in order 1–6. Then swap to `merge` and note that, because both `of` sources are synchronous, the ordering may differ.

### Medium

Simulate two requests with `of('user').pipe(delay(1000))` and `of('posts').pipe(delay(500))`. Combine them with `forkJoin` (object form) and log the combined result. Confirm it emits once, after ~1000ms, with both final values. Then make one source `interval(1000)` and explain why `forkJoin` now never emits.

### Challenging

Implement a cache-vs-network race: `cache$ = of(cachedValue).pipe(delay(50))` and `network$ = ajaxLike$.pipe(delay(400))`. Use `race` to take whichever answers first and log the winner. Then discuss in a comment how this differs from `merge` (which would emit both) and from `concat` (which would wait for the first to complete).

# Further Reading

- [RxJS — merge](https://rxjs.dev/api/index/function/merge) and [concat](https://rxjs.dev/api/index/function/concat)
- [RxJS — zip](https://rxjs.dev/api/index/function/zip)
- [RxJS — forkJoin](https://rxjs.dev/api/index/function/forkJoin)
- [RxJS — race](https://rxjs.dev/api/index/function/race)
