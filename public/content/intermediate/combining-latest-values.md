---
id: lesson-10
slug: combining-latest-values
title: "Combining Latest Values: combineLatest and withLatestFrom"
level: intermediate
order: 10
duration: 26
tags:
  - rxjs
  - combinelatest
  - withlatestfrom
  - combination
  - derived-state
---

# Learning Objectives

By the end of this lesson you will be able to:

- Derive a value from several streams with `combineLatest`.
- Understand that `combineLatest` waits for **every** source to emit once.
- Sample other streams on a trigger with `withLatestFrom`.
- Distinguish "emit on any change" from "emit only when the primary changes".
- Use the array and object forms of `combineLatest` (RxJS 7).
- Seed streams with `startWith` so combinations emit promptly.

# Why This Concept Exists

Real UIs are made of several independent streams that together describe one thing: a search box, a category dropdown, and a sort toggle together define "the current query". You don't want to react to each in isolation — you want the **latest combination** of all of them. And sometimes you want the opposite: when *one* event happens (a submit click), grab the **current** value of some other streams without reacting to *their* changes. `combineLatest` and `withLatestFrom` are those two tools.

# Concept Explanation

### `combineLatest` — the latest of each, on any change

`combineLatest([a$, b$, …])` emits an array containing the latest value from **each** source, and re-emits **whenever any source emits** — but only after **every** source has emitted at least once.

```text
a$:                --1------2-----3--|
b$:                ----A-------B------|
combineLatest([a,b]): ---[1,A]-[2,A][2,B][3,B]--|
```

It completes when **all** sources complete. RxJS 7 also offers an **object form**, which emits a labelled object:

```ts
import { combineLatest } from 'rxjs';

combineLatest({ width: width$, height: height$ }).subscribe(({ width, height }) => {
  console.log(width * height);
});
```

> The old spread form `combineLatest(a$, b$)` is **deprecated** — use the array `combineLatest([a$, b$])` or object form.

### The "waits for all" gotcha

Because `combineLatest` needs one value from every source before its first emission, a single silent source **blocks all output**. Seed such sources with `startWith` so they have an immediate value:

```ts
import { combineLatest, startWith } from 'rxjs';

combineLatest([
  search$.pipe(startWith('')),      // start empty so combineLatest can emit
  category$.pipe(startWith('all')),
]);
```

### `withLatestFrom` — sample others on the primary's beat

`withLatestFrom(b$, …)` is a **pipeable** operator. The source it's piped onto is the **primary**; it emits **only when the primary emits**, attaching the **latest** values from the other Observables. The other streams are *sampled*, not reacted to.

```text
primary$:                --1------2-----3--|
b$:                      ----A-------B------
primary$.withLatestFrom(b$): --------[2,A]--[3,B]--|
```

The first primary value (`1`) is **skipped** because `b$` hadn't emitted yet — `withLatestFrom` drops primary values until every secondary has a value.

### combineLatest vs. withLatestFrom

- **`combineLatest`** — emits on **any** source change; use for *derived state* that should update when anything updates.
- **`withLatestFrom`** — emits only on the **primary** change; use for *"when this happens, read the current that"* (event + state snapshot).

### `combineLatestWith`

There's also a pipeable `combineLatestWith(...others)` that does the same as `combineLatest` but reads naturally inside a `.pipe()` on an existing source.

# Key Terminology

- **`combineLatest`** — combines the latest value of each source, re-emitting on any change once all have emitted.
- **`withLatestFrom`** — emits when the source (primary) emits, attaching the latest of the other streams.
- **Primary / secondary** — in `withLatestFrom`, the piped source is primary; the arguments are sampled secondaries.
- **Derived state** — a value computed from several streams (form validity, a query object).
- **`startWith(seed)`** — prepends an initial value so a stream emits immediately.
- **Array / object form** — the two RxJS 7 shapes for `combineLatest` output.

# Syntax Overview

```ts
import { combineLatest, withLatestFrom, startWith } from 'rxjs';

// combineLatest: re-emits on ANY change (once all have emitted).
combineLatest([a$, b$, c$]).subscribe(([a, b, c]) => { /* latest of each */ });
combineLatest({ a: a$, b: b$ }).subscribe(({ a, b }) => { /* labelled */ });

// Seed silent sources so output isn't blocked.
combineLatest([a$.pipe(startWith(0)), b$.pipe(startWith(''))]);

// withLatestFrom: emit only when submit$ fires, sampling the form.
submit$.pipe(withLatestFrom(formValue$)).subscribe(([_, form]) => save(form));
```

# Real World Analogy

`combineLatest` is a **dashboard of live gauges**. Speed, fuel, and temperature each update on their own schedule, and the dashboard's "status" recomputes every time *any* gauge moves — but it can't show a status until all three gauges have reported at least once. `withLatestFrom` is a **camera with a shutter button**: pressing the shutter (the primary event) captures whatever the light meter and focus (the secondaries) currently read — those instruments don't take the photo themselves, they're just *sampled* the instant you press the button.

# Examples

## Example 1 — Basic: derive area from two inputs

```ts
import { combineLatest, fromEvent, map, startWith } from 'rxjs';

const w = document.querySelector<HTMLInputElement>('#width')!;
const h = document.querySelector<HTMLInputElement>('#height')!;

const width$ = fromEvent(w, 'input').pipe(map(() => Number(w.value)), startWith(0));
const height$ = fromEvent(h, 'input').pipe(map(() => Number(h.value)), startWith(0));

combineLatest([width$, height$])
  .pipe(map(([width, height]) => width * height))
  .subscribe((area) => console.log('area:', area));
```

**Why this is good:** Area depends on both inputs and should update whenever *either* changes — the essence of `combineLatest`. The `startWith(0)` seeds each field so the area shows immediately rather than waiting for both inputs to be touched.

## Example 2 — Real-world: a derived query from filter controls

```ts
import { combineLatest, map, startWith, debounceTime, distinctUntilChanged } from 'rxjs';

const query$ = combineLatest({
  term: term$.pipe(startWith(''), debounceTime(300), distinctUntilChanged()),
  category: category$.pipe(startWith('all')),
  sort: sort$.pipe(startWith('relevance')),
}).pipe(map(({ term, category, sort }) => ({ q: term, category, sort })));

query$.subscribe((query) => console.log('fetch with', query));
```

**Why this is good:** Three independent controls combine into one query object that recomputes whenever any control changes. The object form keeps the code readable, and each source is seeded so the query is valid from the start.

## Example 3 — Best practice: snapshot state on submit with withLatestFrom

```ts
import { fromEvent, withLatestFrom, map } from 'rxjs';

const submit$ = fromEvent(document.querySelector('#save')!, 'click');
const formValue$ = /* Observable<FormData> that updates as the user types */ getForm$();

submit$
  .pipe(
    withLatestFrom(formValue$),
    map(([, form]) => form), // discard the click event, keep the form snapshot
  )
  .subscribe((form) => console.log('submitting', form));

function getForm$() {
  return formValue$;
}
```

**Why this is good:** You only want to act when the user clicks **submit**, using whatever the form currently holds. `withLatestFrom` samples the form on the click without re-firing every time the form changes — precisely the "event + current state" pattern.

# Common Mistakes

- **Expecting `combineLatest` to emit before all sources have.** One silent source blocks the entire output. **Fix:** `startWith` a seed for sources that may not emit immediately.
- **Using the deprecated spread form** `combineLatest(a$, b$)`. **Fix:** use the array `combineLatest([a$, b$])` or object form.
- **Reaching for `combineLatest` when you meant to sample.** If you only want to react to one trigger, `combineLatest` will over-emit. **Fix:** use `withLatestFrom`.
- **Forgetting `withLatestFrom` drops early primary values.** Until every secondary has emitted, primary emissions are skipped. **Fix:** seed secondaries (e.g., a `BehaviorSubject` or `startWith`).
- **Assuming pairwise (index) matching.** `combineLatest` combines *latest* values, not values by position — that's `zip` (next lesson). **Fix:** use `zip` when you need index pairing.

# Best Practices

- Use `combineLatest` for **derived state** that must update on any input change; use `withLatestFrom` for **event + snapshot**.
- Seed sources with `startWith` (or use `BehaviorSubject`) so combinations emit without waiting.
- Prefer the **object form** of `combineLatest` when combining more than two sources — labelled output is far clearer.
- Keep the combined projection **pure**; do async work with a following flattening operator.
- Remember completion semantics: `combineLatest` completes only when **all** sources complete.

# Summary

- **`combineLatest`** emits the latest value of each source and re-emits on **any** change — after every source has emitted once.
- Use the RxJS 7 **array or object** form; the spread form is deprecated.
- A single silent source **blocks** `combineLatest`; seed with **`startWith`**.
- **`withLatestFrom`** emits only when the **primary** source emits, sampling the latest of the others.
- Choose `combineLatest` for derived state, `withLatestFrom` for "on this event, read that state".
- `combineLatest` is **latest-value** combination, not index pairing (that's `zip`).

# Flash Cards

Q: When does `combineLatest([a$, b$])` produce its first emission?
A: Only after BOTH a$ and b$ have each emitted at least one value; then it emits the latest pair and re-emits whenever either changes.

Q: How does `withLatestFrom` decide when to emit?
A: It emits only when the primary (piped) source emits, attaching the latest values from the other Observables — the secondaries are sampled, not reacted to.

Q: A source in your `combineLatest` never emits. What happens to the output?
A: Nothing is emitted at all — combineLatest waits for every source. Seed the silent source with startWith so it has an initial value.

Q: Which combination operator gives you "when event X fires, use the current value of Y"?
A: `withLatestFrom` — it samples Y on X's emission without re-emitting when Y alone changes.

Q: Which `combineLatest` call form is deprecated in RxJS 7?
A: The spread form `combineLatest(a$, b$)`. Use the array `combineLatest([a$, b$])` or the object form instead.

Q: Does `combineLatest` pair values by index like `zip`?
A: No. combineLatest combines the LATEST value from each source on any change; zip pairs values by their index/position.

# Exercises

### Easy

Create two `BehaviorSubject`s, `a$` (start 1) and `b$` (start 10). Use `combineLatest([a$, b$])` to log their sum. Push a new value to `a$` and then to `b$`, and confirm the sum re-emits after each change.

### Medium

Model three filter controls with `startWith`-seeded streams (`term`, `category`, `sort`) and combine them with the object form of `combineLatest` into a single query object. Log the query and confirm it emits immediately (thanks to the seeds) and updates when any control changes.

### Challenging

Build a `submit$` click stream and a `form$` stream that changes as the user types. Use `withLatestFrom` so submitting logs the current form value. Then demonstrate the difference by swapping to `combineLatest([submit$, form$])` and explaining, in a comment, why that version incorrectly "submits" every time the form changes.

# Further Reading

- [RxJS — combineLatest](https://rxjs.dev/api/index/function/combineLatest)
- [RxJS — withLatestFrom](https://rxjs.dev/api/index/function/withLatestFrom)
- [RxJS — combineLatestWith](https://rxjs.dev/api/index/function/combineLatestWith)
- [RxJS — startWith](https://rxjs.dev/api/index/function/startWith)
