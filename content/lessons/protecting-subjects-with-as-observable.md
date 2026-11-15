---
slug: protecting-subjects-with-as-observable
title: "Encapsulating Subjects with asObservable()"
topic: subjects-multicasting
kind: pattern
summary: "Keep Subject fields private inside your service and expose subject.asObservable() publicly so external callers can subscribe without calling next()."
tags: [rxjs, subject, asobservable, encapsulation, architecture]
related: [what-is-a-subject, behavior-subject, choosing-a-subject-variant]
---
> **In one line:** keep your `Subject` or `BehaviorSubject` **`private`** inside the class or module that owns it, and expose **`subject.asObservable()`** publicly so consumers can only **read** the stream, not push into it.

## The idea

Because every `Subject` is both an `Observable` and an `Observer`, handing a raw `Subject` or `BehaviorSubject` reference to external components gives every caller the power to call `.next(badData)` or even `.complete()` — which would permanently kill the shared stream for the entire application!

The standard RxJS encapsulation pattern separates the **write side** from the **read side**:

- **Write side (private):** `private readonly stateSubject$ = new BehaviorSubject<State>(initial);` — only methods on the owning store/service can call `this.stateSubject$.next(...)`.
- **Read side (public):** `readonly state$: Observable<State> = this.stateSubject$.asObservable();` — wraps the Subject in a plain read-only `Observable` that hides `.next`, `.error`, `.complete`, and `.getValue`.

```flow
svc: CartStore (Private Owner) | calls #items$.next(...) @primary
sub: private #items$ | BehaviorSubject<Item[]> @amber
pub: public items$ | #items$.asObservable() @blue
ui: UI Components | can only call items$.subscribe(...) @green
svc -> sub: next(updatedItems)
sub -> pub: forwards emissions
pub -> ui: read-only Observable<Item[]>
```

## See it

```typescript
import { BehaviorSubject, Observable } from 'rxjs';

export class CounterStore {
  // 1. Private Subject: only CounterStore methods can push new values
  readonly #count$ = new BehaviorSubject<number>(0);

  // 2. Public read-only Observable: consumers can subscribe and pipe, but not .next()
  readonly count$: Observable<number> = this.#count$.asObservable();

  increment(): void {
    this.#count$.next(this.#count$.getValue() + 1);
  }

  reset(): void {
    this.#count$.next(0);
  }
}

const store = new CounterStore();
store.count$.subscribe((n) => console.log('count:', n)); // 0
store.increment(); // 1
// store.count$.next(999); // <-- Compile error! .next() does not exist on Observable<number>
```

## Go deeper

Why call `.asObservable()` instead of just typing the public field as `readonly count$: Observable<number> = this.#count$`?

1. **Type-level protection:** both approaches remove `.next()` from the TypeScript type so IDE autocomplete and the compiler forbid `store.count$.next(...)`.
2. **Runtime protection:** `.asObservable()` creates a lightweight wrapper `Observable` whose source is the Subject, so even if someone inspects the object at runtime in JavaScript or casts it, `.next` is not a method on the returned instance.

## Watch out

- Never expose a public `Subject` or `BehaviorSubject` property on a shared service — one accidental `service.data$.complete()` call in a single component will silently break every other subscriber in the app.
- Expose explicit domain methods (`increment()`, `addItem(item)`, `logout()`) that validate inputs before calling `#subject$.next(...)`.

## Remember

- Keep `Subject` and `BehaviorSubject` instances `private` (or `#private`) inside their owning store or service.
- Expose `subject.asObservable()` as the public `Observable<T>` property for consumers.
- Provide named methods on the store to perform controlled state updates via `.next()`.
