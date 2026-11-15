---
slug: combine-latest-waits-for-all-and-start-with
title: "The combineLatest silent-hang pitfall and startWith()"
topic: combining-joining
kind: pitfall
summary: "If even one input in combineLatest hasn't emitted yet, the whole pipeline emits nothing; seed lazy DOM or Subject inputs with startWith(initialValue)."
tags: [rxjs, combinelatest, startwith, pitfall]
related: [combine-latest, combine-latest-object-form-and-with, behavior-subject]
---
> **In one line:** `combineLatest` stays **completely silent** until every input emits at least once — use **`startWith(defaultValue)`** (or a `BehaviorSubject`) on lazy inputs so the combined stream can emit immediately.

## The idea

Here is one of the most common bugs in RxJS UI code: you build a filtered product list with `combineLatest([products$, searchInput$, categorySelect$])`. When the page loads, `products$` arrives from the API, the user hasn't typed into the search box or changed the category dropdown yet, and **the product list on screen is completely blank**!

Why? Because `fromEvent(searchInput, 'input')` and `fromEvent(categorySelect, 'change')` only emit *when the user interacts with them*. Since two of the three inputs in `combineLatest` have emitted zero times on initial load, `combineLatest` patiently waits and emits **nothing**.

The fix is **`startWith(...values)`**, which synchronously prepends an initial default value to the front of any Observable upon subscription.

| Input stream on page load | Emits on subscribe? | Effect inside `combineLatest` |
|---|---|---|
| `fromEvent(input, 'input')` | **No** (waits for user typing) | **Blocks `combineLatest`** until user types! |
| `fromEvent(input, 'input').pipe(map(...), startWith(''))` | **Yes** (emits `''` immediately) | Unblocks `combineLatest` right away |
| `new BehaviorSubject('all')` | **Yes** (emits seed `'all'` immediately) | Unblocks `combineLatest` right away |

## See it

Compare a broken `combineLatest` where a lazy filter stream hasn't emitted yet with one seeded using `startWith`:

```typescript
import { of, Subject, combineLatest, startWith } from 'rxjs';

const items$ = of(['RxJS Guide', 'React Patterns', 'TypeScript Handbook']);
const searchChange$ = new Subject<string>(); // Lazy: hasn't emitted yet on load!

// Before (bug): prints nothing on initial load because searchChange$ hasn't emitted!
combineLatest([items$, searchChange$]).subscribe(([items, q]) => {
  console.log('Before (blocked):', items.filter((i) => i.includes(q)));
});

// After: seed searchChange$ with startWith('') so combineLatest emits immediately!
const seededSearch$ = searchChange$.pipe(startWith(''));

combineLatest([items$, seededSearch$]).subscribe(([items, q]) => {
  console.log('After (immediate):', items.filter((i) => i.includes(q)));
});
// Immediately logs all 3 items with q = '', then re-evaluates when searchChange$ emits!
```

## Go deeper

Where should `startWith` sit inside a `.pipe()` chain on a DOM event stream? Place `startWith('')` **after** `map(e => e.target.value)` so the seed value has the already-mapped type (`string`) rather than needing a fake `Event` object:

```typescript
import { fromEvent, map, startWith } from 'rxjs';

const input = document.querySelector<HTMLInputElement>('#search')!;
const query$ = fromEvent(input, 'input').pipe(
  map((e) => (e.target as HTMLInputElement).value.trim()),
  startWith(''), // emits '' of type string immediately on subscribe!
);
```

## Watch out

- Whenever `combineLatest` produces zero output in your app, check each input stream to find the one that hasn't emitted its first value yet (or completed empty).
- Put `startWith(defaultVal)` *after* `map` (and *after* `debounceTime` if you want the initial emission to be synchronous without waiting for the debounce timer!).

## Remember

- `combineLatest` cannot emit until **every** input has emitted at least one value.
- DOM `fromEvent` streams and plain `Subject`s do not emit on subscribe and will block `combineLatest` until triggered.
- Pipe lazy inputs through `startWith(initialValue)` (or use `BehaviorSubject`) so `combineLatest` emits immediately on load.
