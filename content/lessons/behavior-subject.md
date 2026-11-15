---
slug: behavior-subject
title: "Stateful streams with BehaviorSubject"
topic: subjects-multicasting
kind: api
summary: "BehaviorSubject(initialValue) requires a seed, caches the latest value, immediately emits it to every new subscriber, and exposes .getValue()."
tags: [rxjs, behaviorsubject, state, subject, multicasting]
related: [what-is-a-subject, replay-subject, choosing-a-subject-variant, combine-latest-waits-for-all-and-start-with]
---
> **In one line:** `new BehaviorSubject(initialValue)` always holds a **current value** and **synchronously emits that latest value to every subscriber the instant they subscribe**.

## The idea

While a plain `Subject` represents **events** ("a button was clicked"), a **`BehaviorSubject<T>`** represents **state over time** ("the current theme is `'dark'`", "the current user is `Ada`", "isLoading is `false`").

A `BehaviorSubject` differs from a plain `Subject` in three ways:

1. **Mandatory initial value:** you must pass a seed value to `new BehaviorSubject<T>(initialValue)` so it has a valid current value from birth.
2. **Immediate emission on subscribe:** the moment any subscriber calls `.subscribe()`, `BehaviorSubject` synchronously pushes its **most recent value** (or the initial value if `next()` hasn't been called yet), and then continues pushing future updates.
3. **Synchronous getter:** `subject.getValue()` (or `subject.value`) returns the current value synchronously when imperative code needs a snapshot.

```sequence
actor bs: new BehaviorSubject('light') | holds current state @primary
actor s1: Subscriber 1 (early) | @blue
actor s2: Subscriber 2 (late) | still gets latest value! @green
s1 -> bs: subscribe()
bs --> s1: 'light' (initial value emitted immediately!)
bs -> s1: next('dark') (state updates to 'dark')
s2 -> bs: subscribe() (joins late)
bs --> s2: 'dark' (latest cached value emitted immediately!)
```

## See it

```typescript
import { BehaviorSubject } from 'rxjs';

const theme$ = new BehaviorSubject<'light' | 'dark'>('light');

// Subscriber 1 immediately receives the initial seed ('light'):
theme$.subscribe((t) => console.log('Sub 1:', t)); // Logs: "Sub 1: light"

// Update state:
theme$.next('dark'); // Logs: "Sub 1: dark"

// Subscriber 2 joins late, and immediately receives the latest state ('dark'):
theme$.subscribe((t) => console.log('Sub 2:', t)); // Logs: "Sub 2: dark"

// Synchronous snapshot when needed at an imperative boundary:
console.log('Current snapshot:', theme$.getValue()); // "dark"
```

## Go deeper

What happens if a `BehaviorSubject` **completes** (`theme$.complete()`) and a new subscriber subscribes *after* completion? Unlike `ReplaySubject(1)` (which replays its last value even after completion), a **completed `BehaviorSubject` does NOT emit its last value** to post-completion subscribers — it only sends `complete()`! State stores built on `BehaviorSubject` typically stay open for the lifetime of the service or component that owns them.

Also remember: because `BehaviorSubject` emits synchronously on subscribe, it **never blocks `combineLatest`**!

When updating state held in a `BehaviorSubject<State>` based on the previous state (for example incrementing a counter or appending an item to a list), you can read `subject.getValue()` inside your store's action method and pass a brand-new immutable object `subject.next({ ...subject.getValue(), count: subject.getValue().count + 1 })` so all subscribers receive the new state reference synchronously.

## Watch out

- Avoid overusing `theme$.getValue()` / `theme$.value` instead of reactive pipelines — pulling snapshots imperatively bypasses automatic updates when the value changes later.
- If a subscriber wants only *future* changes from a `BehaviorSubject` and wants to ignore the initial synchronous emission on subscribe, pipe it through `skip(1)`.

## Remember

- `new BehaviorSubject(seed)` requires an initial value and always caches the latest value.
- Every new subscriber immediately and synchronously receives the current value on `subscribe()`.
- Once a `BehaviorSubject` completes, late subscribers receive only `complete()`, not the last cached value.
