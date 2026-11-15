---
slug: marble-diagram-syntax
title: "Marble diagram syntax: frames, groups, and time progression"
topic: schedulers-testing
kind: api
summary: "Read and write ASCII marble strings using - frames, a-z values, | completion, # errors, (ab) sync groups, and 100ms time jumps."
tags: [rxjs, marbles, testscheduler, syntax, testing]
related: [virtual-time-and-test-scheduler, cold-hot-and-expect-observable, testing-time-based-operators]
---
> **In one line:** inside `testScheduler.run()`, every single character in a marble string advances virtual time by **1ms** — including parentheses `(` and `)` in synchronous groups — unless you use explicit time progression like `300ms` or `2s`.

## The idea

ASCII marble strings let you draw timelines directly in your unit tests. Every token has a strict virtual-time cost inside `testScheduler.run()`:

| Token | Meaning | Virtual time cost inside `run()` | Example |
|---|---|---|---|
| `-` | 1 frame of silence | **1ms** | `---a` emits `'a'` at frame 3 (3ms) |
| ` ` (space) | Visual alignment whitespace | **0ms** (ignored!) | `'   ---a'` is identical to `'---a'` |
| `a`–`z`, `0`–`9` | Emit value (mapped via `values` object or literal char) | **1ms** | `-a-b` emits `'a'` at 1ms, `'b'` at 3ms |
| `\|` | `complete()` notification | **1ms** | `--a\|` completes at 3ms |
| `#` | `error()` notification | **1ms** | `--a#` errors at 3ms |
| `(ab)` | Synchronous group: emit `a` and `b` in the **same frame** | **4ms total** (1ms per character!) | `(ab)c` emits `a` & `b` at 0ms, then `c` at 4ms |
| `^ ` / `!` | Subscription start (`^`) / unsubscription (`!`) | **1ms** | `--^--!` subscribes at 2ms, unsubscribes at 5ms |
| `100ms`, `2s`, `1m` | Time progression shorthand | **Exact duration** (`100ms`, `2000ms`, `60000ms`) | `300ms a` waits 300ms, then emits `a` at 300ms |

> **Tip:** Because spaces ` ` cost **0ms**, you can freely pad marble strings with spaces to vertically align input streams, subscription markers, and expected output timelines!

## See it

Combine single-frame tokens, synchronous groups `(ab|)`, and millisecond time progression `300ms` in a single test:

```typescript
import { TestScheduler } from 'rxjs/testing';
import { of, delay, concatWith } from 'rxjs';

const testScheduler = new TestScheduler((a, e) => expect(a).toEqual(e));

testScheduler.run(({ cold, expectObservable }) => {
  // of('x', 'y') emits both values and completes synchronously at frame 0: '(xy|)'
  // Note: '(xy|)' is 5 characters wide, so it advances virtual time by 5ms!
  const sync$ = of('x', 'y');
  expectObservable(sync$).toBe('(xy|)');

  // Use time progression syntax ('500ms') instead of typing 500 dashes:
  const delayed$ = cold('a|').pipe(delay(500));
  expectObservable(delayed$).toBe('500ms a|');

  // Spaces are ignored, so you can align columns cleanly:
  const part1$ = cold('--a--b|');
  const part2$ = cold('      --c--|');
  expectObservable(part1$.pipe(concatWith(part2$))).toBe('--a--b--c--|');
});
```

## Go deeper

Why does `(ab)` advance virtual time by **4ms** if `a` and `b` happen synchronously at the same instant? Because ASCII marble diagrams were designed to line up visually in a monospaced font! If `(ab)` occupies 4 character columns on screen, any marble string below it needs 4 dashes `----` to reach the next column after `)`. Therefore, `(ab)` emits both `a` and `b` at the frame where `(` starts, and then advances the virtual clock by 1ms per character inside the group (including `(` and `)`).

## Watch out

- Always put a **space** before and after time progression tokens like `'300ms a'` or `'a 200ms b'` so the parser knows where the number token ends and the emission token begins.
- Remember that `(a|)` takes 4ms of virtual time before any subsequent character in the same marble string.

## Remember

- Inside `testScheduler.run()`, every non-space character (`-`, `a`, `|`, `#`, `(`, `)`) advances virtual time by 1ms.
- Spaces ` ` take 0ms and are purely for visual alignment across lines.
- Use `100ms`, `2s`, or `1m` (surrounded by spaces) for real-world millisecond and second durations.
