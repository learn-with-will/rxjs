---
slug: scheduled-creation-function
title: "Creating scheduled streams with scheduled() (replacing deprecated scheduler args)"
topic: schedulers-testing
kind: api
summary: "In RxJS 7+, passing a scheduler to of(), from(), or range() is deprecated; use scheduled(input, scheduler) to create a stream that emits on a scheduler."
tags: [rxjs, scheduled, scheduler, of, from, rxjs-7]
related: [what-is-a-scheduler, built-in-schedulers, observe-on-vs-subscribe-on, of-and-from]
since: "6.5"
---
> **In one line:** replace deprecated `of(1, 2, asyncScheduler)` and `from([1, 2], asyncScheduler)` with **`scheduled([1, 2], asyncScheduler)`**.

## The idea

In RxJS 6 and earlier, creation functions like `of`, `from`, and `range` accepted an optional trailing `SchedulerLike` argument: `of(1, 2, 3, asyncScheduler)`. That overloaded signature had two problems:

1. TypeScript had to guess whether the last argument to `of(...args)` was an item to emit or a `SchedulerLike`.
2. Every creation function had to bundle scheduler-dispatch code even when 99% of calls were synchronous.

In **RxJS 7+**, passing a scheduler to `of`, `from`, or `range` is **deprecated** (and removed in RxJS 8). Instead, RxJS provides the dedicated **`scheduled(input, scheduler)`** creation function, which accepts any `ObservableInput` (an Array, Iterable, Promise, or Observable) and emits its values on the given scheduler.

| Deprecated RxJS 6 syntax | Modern RxJS 7+ replacement |
|---|---|
| `of(1, 2, 3, asyncScheduler)` | `scheduled([1, 2, 3], asyncScheduler)` |
| `from([1, 2, 3], asapScheduler)` | `scheduled([1, 2, 3], asapScheduler)` |
| `range(1, 5, asyncScheduler)` | `range(1, 5).pipe(observeOn(asyncScheduler))` |
| `merge(a$, b$, asyncScheduler)` | `scheduled([a$, b$], asyncScheduler).pipe(mergeAll())` |

## See it

```typescript
import { scheduled, asyncScheduler, asapScheduler, mergeAll } from 'rxjs';

console.log('1. Sync start');

// Emit array elements on the microtask queue (asapScheduler):
scheduled(['A', 'B'], asapScheduler).subscribe((v) =>
  console.log('3. asap:', v),
);

// Emit array elements on the macrotask queue (asyncScheduler):
scheduled([10, 20], asyncScheduler).subscribe((n) =>
  console.log('4. async:', n),
);

// Combining multiple streams on a scheduler with scheduled() + mergeAll():
scheduled([ ['X'], ['Y'] ], asyncScheduler)
  .pipe(mergeAll())
  .subscribe((v) => console.log('5. scheduled mergeAll:', v));

console.log('2. Sync end');
```

## Go deeper

How does `scheduled([1, 2, 3], asyncScheduler)` differ from `from([1, 2, 3]).pipe(observeOn(asyncScheduler))`?

When iterating a large generator or iterable, `from(hugeIterable).pipe(observeOn(asyncScheduler))` still pulls **all** items from `hugeIterable` synchronously in one tight loop and queues thousands of macrotasks at once. By contrast, `scheduled(hugeIterable, asyncScheduler)` schedules **each step of the iterator (`iterator.next()`)** on separate ticks of `asyncScheduler`, yielding back to the browser event loop between items so the UI thread never freezes!

## Watch out

- Remember that `scheduled` takes a **single `ObservableInput`** as its first argument — write `scheduled([1, 2, 3], asyncScheduler)` with an array, not `scheduled(1, 2, 3, asyncScheduler)`.
- Time-based creation functions whose entire purpose is timing (`interval(1000, scheduler)` and `timer(1000, scheduler)`) still accept a scheduler argument in RxJS 7+; only synchronous creators like `of`, `from`, and `range` deprecated their scheduler argument.

## Remember

- Never pass a trailing scheduler argument to `of`, `from`, or `range` in RxJS 7+.
- Use `scheduled([a, b, c], scheduler)` to emit an array, iterable, Promise, or Observable on a scheduler.
- `scheduled(iterable, asyncScheduler)` yields to the event loop between each `iterator.next()` step.
