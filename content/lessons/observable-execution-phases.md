---
slug: observable-execution-phases
title: "The four phases of an Observable execution"
topic: observables-subscriptions
kind: concept
summary: "Every Observable execution moves through four phases: creation, subscription, emission, and teardown."
tags: [rxjs, lifecycle, observable, execution, teardown]
related: [observable-contract-grammar, cold-and-unicast-by-default, producer-teardown-function]
---
> **In one line:** every Observable lifecycle follows four phases in order: **Creation**, **Subscription**, **Emission**, and **Teardown**.

## The idea

Whether you build a stream with `new Observable`, `fromEvent`, or a 10-operator `.pipe()` chain, every Observable execution goes through the exact same four phases:

| Phase | What happens | Work performed? |
|---|---|---|
| **1. Creation** | You define the Observable (`new Observable`, `of`, `fromEvent`, `.pipe()`) | **None** — just stores the recipe |
| **2. Subscription** | A consumer calls `.subscribe(observer)`, starting a fresh execution | **Yes** — invokes the producer function |
| **3. Emission** | The producer pushes `next(v)` notifications, and optionally one `error` or `complete` | **Yes** — values flow to the Observer |
| **4. Teardown** | Cleanup runs **once** when the stream completes, errors, or is unsubscribed | **Yes** — releases timers, sockets, listeners |

Understanding which phase a piece of code runs in answers most "when does this happen?" questions in RxJS. Code outside the producer runs once at **creation** time; code inside the producer runs on every **subscription**; callbacks in `subscribe` run on every **emission**; and the function returned from the producer runs at **teardown**.

## See it

Here you can watch all four phases log in sequence during a single Observable's life:

```typescript
import { Observable } from 'rxjs';

console.log('1. Creation: defining the Observable');
const lifecycle$ = new Observable<number>((subscriber) => {
  console.log('2. Subscription: producer function started');

  subscriber.next(10);
  subscriber.next(20);
  subscriber.complete();

  return () => {
    console.log('4. Teardown: resources cleaned up');
  };
});

console.log('--- Nothing has run yet; calling subscribe() now ---');
lifecycle$.subscribe({
  next: (v) => console.log('3. Emission (next):', v),
  complete: () => console.log('3. Emission (complete)'),
});
```

Notice that between step 1 ("defining the Observable") and the `subscribe()` call, the producer hasn't run at all. Once `subscribe()` is called, Subscription, Emission, and Teardown happen in strict order.

## Go deeper

An Observable is like a **recipe**, and subscribing is **cooking it**:

- Writing the recipe down (**Creation**) puts no food on the table.
- Starting to cook (**Subscription**) heats the pan and uses ingredients.
- Serving courses (**Emission**) delivers values to the diner, ending when the meal is finished (`complete`) or burns (`error`).
- Cleaning the station (**Teardown**) happens once at the very end, whether the meal finished normally or was cancelled mid-course.

## Watch out

- Side effects placed outside the producer function (like `const t = Date.now(); of(t)`) happen in the **Creation** phase, not the **Subscription** phase.
- If a stream neither completes, errors, nor gets unsubscribed, it stays in the **Emission** phase forever and **Teardown** never runs.

## Remember

- Phase 1 (Creation) defines the stream lazily without running the producer.
- Phase 2 (Subscription) starts the producer; Phase 3 (Emission) pushes `next` and terminal signals.
- Phase 4 (Teardown) runs cleanup once on completion, error, or unsubscription.
