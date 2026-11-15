---
slug: with-latest-from
title: "Sampling passive state with withLatestFrom()"
topic: combining-joining
kind: api
summary: "source$.pipe(withLatestFrom(other$)) emits only when primary source$ emits, attaching the latest cached value from other$ without other$ triggering output."
tags: [rxjs, withlatestfrom, combining, state]
related: [combine-latest, combine-latest-vs-with-latest-from, combine-latest-object-form-and-with]
---
> **In one line:** `trigger$.pipe(withLatestFrom(state$))` emits **only when `trigger$` emits**, pairing that trigger event with the most recent value from `state$`.

## The idea

Suppose you have a `saveClicks$` stream (when the user clicks "Save") and a `formData$` stream (which updates on every keystroke). If you combined them with `combineLatest([saveClicks$, formData$])`, then once the user clicked "Save" once, **every subsequent keystroke in `formData$` would trigger another Save**!

In this scenario, the two streams do not have equal roles:

- **`saveClicks$` is the active trigger (primary stream):** you want an output **only** when `saveClicks$` emits.
- **`formData$` is passive context (secondary stream):** you just want to read its latest value at the moment `saveClicks$` fires, without `formData$` ever triggering an output on its own.

That primary/secondary relationship is exactly what the pipeable operator **`withLatestFrom(other1$, other2$, ...)`** models.

```sequence
actor draft: draftText$ (passive) | emits "v1", "v2", "v3" @amber
actor btn: saveClicks$ (trigger) | controls WHEN output fires @blue
actor wlf: saveClicks$.pipe(withLatestFrom(draftText$)) | @primary
draft -> wlf: "v1" (cached silently — no output)
draft -> wlf: "v2" (cached silently — no output)
btn -> wlf: Click! -> emits [clickEvent, "v2"]
draft -> wlf: "v3" (cached silently — no output)
```

## See it

```typescript
import { Subject, withLatestFrom } from 'rxjs';

const saveClick$ = new Subject<void>();
const draftText$ = new Subject<string>();

saveClick$
  .pipe(withLatestFrom(draftText$))
  .subscribe(([, latestDraft]) => {
    console.log('Saving draft to server:', latestDraft);
  });

draftText$.next('Hello');       // Cached silently
draftText$.next('Hello RxJS');  // Cached silently
saveClick$.next();              // Logs: "Saving draft to server: Hello RxJS"

draftText$.next('Edited again'); // Cached silently — does NOT trigger a save!
```

## Go deeper

What happens if `saveClick$` fires **before** `draftText$` has emitted its very first value? Until every secondary Observable passed to `withLatestFrom` has emitted at least once, any trigger emission from the primary stream is **silently ignored** (dropped)! That is why passive state streams passed into `withLatestFrom` are almost always `BehaviorSubject`s or streams seeded with `startWith(initialValue)`, guaranteeing a value is already present before the user can click.

Also note that the lifecycle of `source$.pipe(withLatestFrom(other$))` is tied to the **primary `source$`**: as soon as `source$` completes, the output stream completes and unsubscribes from `other$`.

You can pass multiple secondary streams into a single call — for example, `saveClick$.pipe(withLatestFrom(draftText$, currentUser$, authToken$))` — which emits a 4-element tuple `[click, draft, user, token]` whenever `saveClick$` fires, provided all three secondary streams have emitted at least once.

## Watch out

- If the primary stream emits before a secondary stream in `withLatestFrom(other$)` has emitted its first value, that primary trigger emission is **dropped**, not queued!
- Make sure the trigger stream is the one you call `.pipe(withLatestFrom(...))` on, and the passive state stream is the argument inside `withLatestFrom(state$)`.

## Remember

- `trigger$.pipe(withLatestFrom(state$))` emits **only when `trigger$` emits**.
- Emissions from `state$` update the cached value silently without triggering downstream output.
- Seed `state$` with `BehaviorSubject` or `startWith` so early triggers before the first state change are never dropped.
