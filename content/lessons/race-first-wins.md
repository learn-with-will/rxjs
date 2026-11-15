---
slug: race-first-wins
title: "First-to-emit wins with race()"
topic: combining-joining
kind: api
summary: "race(a$, b$) subscribes to all candidate Observables, mirrors whichever one emits its first value earliest, and immediately unsubscribes from all the others."
tags: [rxjs, race, racewith, joining, combining]
related: [merge-and-concat, fork-join, timeout-operator, pipeable-with-operators]
---
> **In one line:** `race(a$, b$)` subscribes to all competing streams, **adopts the very first one to emit**, and **immediately unsubscribes from every other competitor**.

## The idea

Think of `race(a$, b$, ...)` as the Observable equivalent of `Promise.race`, extended to full streams:

1. When you subscribe to `race(a$, b$)`, it subscribes to `a$` and `b$` simultaneously.
2. The instant one of them (say `b$`) emits its **first `next` value** (or errors/completes first), `b$` wins the race.
3. `race` **immediately unsubscribes from all losing streams** (`a$` is cancelled and its teardown runs) and mirrors every subsequent emission from the winner `b$` until `b$` completes.

```sequence
actor m1: Primary Mirror (250ms) | slower -> cancelled! @red
actor m2: Backup Mirror (80ms) | emits first -> WINS! @green
actor rc: race(m1$, m2$) | mirrors winner & aborts losers @primary
rc -> m1: subscribe()
rc -> m2: subscribe()
m2 --> rc: first chunk at 80ms (m2 wins!)
rc -> m1: unsubscribe() at 80ms (aborts m1!)
m2 --> rc: remaining chunks + complete()
```

## See it

Here two competing endpoints race, and the slower one's teardown runs the instant the faster one emits at 80ms:

```typescript
import { timer, map, finalize, race } from 'rxjs';

const slowServer$ = timer(250).pipe(
  map(() => 'Response from slow server'),
  finalize(() => console.log('slowServer$ torn down')),
);

const fastServer$ = timer(80).pipe(
  map(() => 'Response from fast server'),
  finalize(() => console.log('fastServer$ torn down')),
);

race(slowServer$, fastServer$).subscribe((winner) => {
  console.log('Winner:', winner);
});
// At 80ms:
// slowServer$ torn down  (cancelled immediately when fastServer$ emitted!)
// Winner: Response from fast server
// fastServer$ torn down
```

## Go deeper

Note that after the winner emits its first value, `race` continues forwarding **all subsequent values** from that winning stream (not just its first value!). For example, if you race two multi-value streams `interval(100)` and `interval(250)`, `interval(100)` wins at 100ms, `interval(250)` is unsubscribed, and `interval(100)` keeps emitting `0, 1, 2, 3, …` forever.

You can also use `race` to model "whichever user action happens first" — for example, `race(confirmClick$, cancelClick$, escapeKey$)` combined with `take(1)` to react to the user's decision and tear down all three listeners at once.

## Watch out

- Like `merge`, `concat`, and `zip`, `race` accepts either rest arguments `race(a$, b$)` or an array `race([a$, b$])` (passing an empty array `race([])` returns `EMPTY`).
- If one of the competing Observables **completes empty** or **errors** before any competitor emits a value, that terminal signal ends the race immediately!

## Remember

- `race(a$, b$)` mirrors the first Observable to emit and immediately unsubscribes from all the others.
- Losing streams have their teardown functions called the instant the winner emits.
- The winner continues emitting all of its subsequent values until it completes.
