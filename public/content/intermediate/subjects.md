---
id: lesson-12
slug: subjects
title: "Subjects: Subject, BehaviorSubject, ReplaySubject, AsyncSubject"
level: intermediate
order: 12
duration: 28
tags:
  - rxjs
  - subject
  - behaviorsubject
  - replaysubject
  - multicast
---

# Learning Objectives

By the end of this lesson you will be able to:

- Explain why a Subject is **both an Observable and an Observer**.
- Multicast values to many subscribers with a plain `Subject`.
- Hold and read current state with `BehaviorSubject`.
- Replay past values to late subscribers with `ReplaySubject`.
- Emit only a final value with `AsyncSubject`.
- Protect a Subject by exposing it as a read-only Observable.

# Why This Concept Exists

Plain Observables are **unicast**: each subscriber gets its own private execution. But often you need the opposite — one source, **many** listeners, all sharing the same values (a state store, an event bus, a WebSocket feed). You also sometimes need to **push** values imperatively from outside the Observable. A **Subject** solves both: it's a multicasting Observable you can feed by hand. The four Subject variants differ in what they remember and replay.

# Concept Explanation

### A Subject is an Observable *and* an Observer

You can subscribe to a Subject (it's an Observable) **and** call `next`, `error`, `complete` on it (it's an Observer). Every value you push is multicast to **all** current subscribers.

```ts
import { Subject } from 'rxjs';

const subject = new Subject<number>();

subject.subscribe((v) => console.log('A:', v));
subject.subscribe((v) => console.log('B:', v));

subject.next(1); // A: 1  B: 1   — both receive the same value
subject.next(2); // A: 2  B: 2
```

Unlike a cold Observable, the Subject doesn't re-run a producer per subscriber — there **is** no producer, just values you push, delivered to everyone at once. That makes a Subject inherently **hot / multicast**.

### `Subject` — no memory

A plain `Subject` has no initial value and no replay. Subscribers only receive values emitted **after** they subscribe; late subscribers miss everything before.

```ts
const s = new Subject<string>();
s.next('early');                       // no subscribers yet — lost
s.subscribe((v) => console.log(v));    // subscribes now
s.next('late');                        // logs 'late'  (but never 'early')
```

### `BehaviorSubject` — the current value

`BehaviorSubject(initial)` **requires** an initial value and always holds the **current** value. New subscribers immediately receive that current value on subscribe, and you can read it synchronously via `.value`.

```ts
import { BehaviorSubject } from 'rxjs';

const count$ = new BehaviorSubject(0); // initial value 0
count$.subscribe((v) => console.log('sub1:', v)); // sub1: 0 (immediately)
count$.next(1);                                    // sub1: 1
count$.subscribe((v) => console.log('sub2:', v)); // sub2: 1 (current value)
console.log('now:', count$.value);                 // now: 1
```

This makes `BehaviorSubject` the natural choice for **state**: there's always a "current" value.

### `ReplaySubject` — replay the last N

`ReplaySubject(bufferSize?, windowTime?)` replays the last `bufferSize` values to **every** new subscriber, regardless of when they subscribe — and, unlike `BehaviorSubject`, it needs no initial value and will replay its buffer to subscribers even **after** it has completed.

```ts
import { ReplaySubject } from 'rxjs';

const r = new ReplaySubject<number>(2); // remember last 2
r.next(1); r.next(2); r.next(3);
r.subscribe((v) => console.log(v));      // 2, 3  (the last two, replayed)
```

`ReplaySubject(1)` is similar to `BehaviorSubject` but has **no** initial value (a subscriber sees nothing until the first `next`).

### `AsyncSubject` — only the final value

`AsyncSubject` emits **only the last value**, and **only when it completes**. Subscribers get nothing until `complete()`, then all receive that final value. It behaves much like a Promise.

```ts
import { AsyncSubject } from 'rxjs';

const a = new AsyncSubject<number>();
a.subscribe((v) => console.log(v));
a.next(1); a.next(2); a.next(3);          // nothing logged yet
a.complete();                              // now logs: 3  (only the last value)
```

### Protecting a Subject

Anyone holding a Subject can call `next`/`complete` on it. Expose it to consumers as a **read-only Observable** with `.asObservable()` so only the owner can push:

```ts
class Store {
  private readonly _state$ = new BehaviorSubject({ user: null });
  readonly state$ = this._state$.asObservable(); // consumers can only subscribe
  setUser(user: unknown) { this._state$.next({ user }); }
}
```

# Key Terminology

- **Subject** — a multicast Observable that is also an Observer; you push values with `next`/`error`/`complete`.
- **Multicast** — one execution shared by all subscribers (contrast unicast/cold).
- **`BehaviorSubject`** — holds a current value (with a required initial); replays it to new subscribers; exposes `.value`.
- **`ReplaySubject`** — replays the last N (optionally time-windowed) values to new subscribers.
- **`AsyncSubject`** — emits only the final value, and only on completion.
- **`.asObservable()`** — returns a read-only Observable view of a Subject.

# Syntax Overview

```ts
import { Subject, BehaviorSubject, ReplaySubject, AsyncSubject } from 'rxjs';

const events$ = new Subject<string>();          // multicast, no memory
const state$ = new BehaviorSubject({ ready: false }); // current value + initial
const recent$ = new ReplaySubject<number>(3);   // replay last 3
const result$ = new AsyncSubject<number>();     // only final value, on complete

state$.value;            // read current value synchronously
events$.asObservable();  // read-only view for consumers
events$.next('hello');   // push a value to all subscribers
events$.complete();      // end the stream for all subscribers
```

# Real World Analogy

The Subject family is a set of **radio stations**. A plain `Subject` is live radio: tune in and you hear only what's broadcast *from now on* — miss the morning show and it's gone. A `BehaviorSubject` is a station that, the instant you tune in, tells you the **current** song title before continuing. A `ReplaySubject` is a station that replays the **last few tracks** for anyone who just joined. An `AsyncSubject` is an awards-show broadcast that stays silent all evening and only announces the **single winner** at the very end. And `.asObservable()` is giving listeners a receiver that can't transmit — they can only tune in, never seize the mic.

# Examples

## Example 1 — Basic: multicast with a plain Subject

```ts
import { Subject } from 'rxjs';

const notifications$ = new Subject<string>();

notifications$.subscribe((msg) => console.log('toast:', msg));
notifications$.subscribe((msg) => console.log('log:', msg));

notifications$.next('Saved!'); // both subscribers fire with the same value
// toast: Saved!  /  log: Saved!
```

**Why this is good:** One `next` reaches every subscriber simultaneously — the defining behaviour of a Subject and the basis of an in-app event bus.

## Example 2 — Real-world: a tiny state store with BehaviorSubject

```ts
import { BehaviorSubject } from 'rxjs';

interface CartState { items: string[]; }

class CartStore {
  private readonly _state$ = new BehaviorSubject<CartState>({ items: [] });
  readonly state$ = this._state$.asObservable(); // consumers subscribe only

  add(item: string) {
    const current = this._state$.value;                 // read current synchronously
    this._state$.next({ items: [...current.items, item] }); // push new state
  }
}

const store = new CartStore();
store.state$.subscribe((s) => console.log('cart:', s.items)); // cart: [] immediately
store.add('book');                                             // cart: ['book']
```

**Why this is good:** `BehaviorSubject` gives every subscriber the current cart on subscribe and a clean way to read/update state. Exposing only `state$` via `asObservable()` prevents outside code from pushing arbitrary states.

## Example 3 — Best practice: choose the Subject by replay need

```ts
import { Subject, BehaviorSubject, ReplaySubject } from 'rxjs';

// Late subscriber test for each type:
const plain = new Subject<number>();
const behavior = new BehaviorSubject<number>(0);
const replay = new ReplaySubject<number>(2);

[plain, behavior, replay].forEach((s) => s.next(1));
[plain, behavior, replay].forEach((s) => s.next(2));

plain.subscribe((v) => console.log('plain:', v));       // (nothing — no memory)
behavior.subscribe((v) => console.log('behavior:', v)); // behavior: 2 (current)
replay.subscribe((v) => console.log('replay:', v));     // replay: 1  replay: 2
```

**Why this is good:** Run side by side, the three make the difference obvious: no memory vs. current-value vs. last-N-replay. That's exactly the decision you make when picking a Subject.

# Common Mistakes

- **Expecting a plain `Subject` to replay to late subscribers.** It has no memory. **Fix:** use `BehaviorSubject` (current value) or `ReplaySubject(n)`.
- **Creating a `BehaviorSubject` without an initial value.** It's required. **Fix:** pass a sensible initial (e.g., `null`, `0`, `[]`).
- **Exposing the Subject itself.** Any holder can `next`/`complete` it, corrupting your stream. **Fix:** expose `subject.asObservable()`.
- **Using `AsyncSubject` for intermediate values.** It only ever emits the final value on completion. **Fix:** use `ReplaySubject`/`BehaviorSubject` if you need in-progress values.
- **Overusing Subjects.** Bridging every stream through a Subject is an anti-pattern. **Fix:** prefer plain Observables/operators; reach for Subjects only when you need multicasting or an imperative entry point.

# Best Practices

- Default to plain Observables and operators; introduce a Subject when you genuinely need **multicasting** or to **push** values imperatively.
- Use **`BehaviorSubject`** for state that always has a current value; read it with `.value` sparingly (prefer subscribing).
- Use **`ReplaySubject(1)`** when you want "latest value" semantics without an initial value.
- Encapsulate Subjects behind **`.asObservable()`** (or getters) so only the owner can emit.
- **Complete** Subjects when done (or manage with `takeUntil`) so subscribers tear down and memory is released.

# Summary

- A **Subject** is both an Observable and an Observer; it **multicasts** pushed values to all subscribers and is inherently hot.
- A plain **`Subject`** has no memory — late subscribers miss earlier values.
- **`BehaviorSubject`** requires an initial value, holds the **current** value, and exposes `.value`.
- **`ReplaySubject(n)`** replays the last `n` values to new subscribers, even after completion.
- **`AsyncSubject`** emits only the **final** value, and only on completion.
- Expose Subjects as **read-only Observables** with `.asObservable()`, and don't overuse them.

# Flash Cards

Q: Why is a Subject described as both an Observable and an Observer?
A: You can subscribe to it (Observable) and also call next/error/complete on it (Observer). Values you push are multicast to all current subscribers.

Q: What does a new subscriber to a `BehaviorSubject` receive immediately?
A: The current value — BehaviorSubject always holds a value (starting from its required initial) and replays it on subscribe. You can also read it via `.value`.

Q: How does a plain `Subject` treat a subscriber that joins after some values were emitted?
A: It misses them entirely — a plain Subject has no memory and only delivers values emitted after subscription.

Q: What does `ReplaySubject(2)` give a brand-new subscriber?
A: The last two values it buffered, replayed on subscribe — regardless of when the subscriber joined (and even after completion).

Q: When does an `AsyncSubject` emit, and what?
A: Only when it completes, and only its final (last) value — behaving much like a Promise.

Q: How do you stop external code from calling `next` on a Subject you expose?
A: Expose `subject.asObservable()` instead of the Subject itself, giving consumers a read-only Observable.

# Exercises

### Easy

Create a `Subject<string>`, subscribe once, push `'a'` and `'b'`, then subscribe a second time and push `'c'`. Confirm the second subscriber only sees `'c'` — demonstrating that a plain Subject has no memory.

### Medium

Build a `BehaviorSubject<number>` initialized to 10. Subscribe and log values, then push 20 and 30. Add a second subscriber after pushing 20 and confirm it immediately receives 20. Read and log `.value` at the end.

### Challenging

Implement a `SettingsStore` class holding a `BehaviorSubject` of `{ theme: 'light' }`. Expose only `settings$` via `asObservable()` and a `setTheme(theme)` method. Subscribe from two places, toggle the theme, and confirm both update. Then swap the internal Subject to a `ReplaySubject(1)` and describe in a comment how the observable behaviour changes for a subscriber that joins before any value is set.

# Further Reading

- [RxJS — Subject](https://rxjs.dev/guide/subject)
- [RxJS — BehaviorSubject](https://rxjs.dev/api/index/class/BehaviorSubject)
- [RxJS — ReplaySubject](https://rxjs.dev/api/index/class/ReplaySubject)
- [RxJS — AsyncSubject](https://rxjs.dev/api/index/class/AsyncSubject)
