# RxJS Study Pathway
### A Comprehensive, Phase-by-Phase Curriculum

> **Based on:** [rxjs.dev](https://rxjs.dev) — the official RxJS documentation
> **RxJS version covered:** RxJS 7+ (pipeable operators, `firstValueFrom`/`lastValueFrom`, `throwError(() => …)`)
> **Approach:** Each lesson builds on the one before it. The progression is intentional — operators in later phases assume the mental models from earlier ones.

---

## Phase 1 — Beginner: Thinking in Streams

**Goal:** Understand what an Observable is, how to create and consume one, and the rules every stream obeys.
**Exit criteria:** You can create Observables from any source, subscribe and unsubscribe safely, and compose a small pipeline with `map`/`filter`/`take`.

1. **What Is RxJS? Observables and Reactive Thinking** — Observables as lazy, push, multi-value streams; the pull/push × single/multiple grid; Observable vs Promise.
2. **Creating Observables** — `of`, `from`, `fromEvent`, `interval`, `timer`, `range`, `EMPTY`, `throwError`, `defer`; which complete and which run forever.
3. **Subscribing: Observers and Subscriptions** — `next`/`error`/`complete`, what `subscribe()` returns, `unsubscribe()`, grouping with `add()`.
4. **The Observable Lifecycle and Contract** — lazy, cold/unicast execution; `next* (error | complete)?`; teardown timing.
5. **The pipe() Function and Pipeable Operators** — operators as functions; `.pipe()`; purity; the standalone `pipe`.
6. **Transforming Values: map, tap, and scan** — reshape, observe, accumulate; `scan` vs `reduce`.
7. **Filtering Values: filter, take, first, and friends** — selection and early completion; the `EmptyError` gotcha.

---

## Phase 2 — Intermediate: Operators and Composition

**Goal:** Compose real reactive data flows — rate-limiting, flattening, combining, multicasting, and error handling.
**Exit criteria:** You can build a typeahead, choose the right flattening operator, share an execution, and recover from errors without killing a stream.

8. **Rate-Limiting Time: debounceTime, throttleTime, distinctUntilChanged** — taming noisy streams; leading vs trailing edges.
9. **Higher-Order Observables: mergeMap, concatMap, switchMap, exhaustMap** — streams of streams and the four flattening strategies.
10. **Combining Latest Values: combineLatest and withLatestFrom** — derived state vs event-and-snapshot.
11. **Joining Streams: merge, concat, zip, forkJoin, race** — interleave, sequence, pair, gather, and first-wins.
12. **Subjects: Subject, BehaviorSubject, ReplaySubject, AsyncSubject** — the multicast bridge and its four flavours.
13. **Multicasting: Hot vs Cold, share, shareReplay** — sharing one execution; the `shareReplay` refCount leak.
14. **Error Handling: catchError, retry, throwError** — errors are terminal; recover, retry with backoff, contain inside `switchMap`.
15. **Completion and Cleanup: finalize, takeUntil, timeout** — ending streams and guaranteed teardown.

---

## Phase 3 — Advanced: Under the Hood and In Production

**Goal:** Understand schedulers and flattening internals, test deterministically, build your own operators, and ship leak-free production patterns.
**Exit criteria:** You can reason about concurrency, write marble tests, author custom operators, and assemble typeahead/polling/caching pipelines that bridge cleanly to Promises.

16. **Flattening Strategies in Depth: mergeAll, concatAll, switchAll, exhaustAll** — the `*All` operators, the `xMap = map + xAll` identity, and concurrency limits.
17. **Schedulers and Concurrency** — `async`/`asap`/`queue`/`animationFrame`; `observeOn` vs `subscribeOn`; `scheduled()`.
18. **Marble Testing with the TestScheduler** — virtual time, marble syntax, `expectObservable`/`expectSubscriptions`.
19. **Building Custom Operators** — operators as functions; composition with `pipe`; the low-level `Observable` constructor.
20. **Buffering, Windowing, and Grouping** — `bufferTime`/`bufferCount`, windowing into sub-streams, `groupBy` (and its memory pitfall).
21. **Memory Management and Anti-Patterns** — leaks, `takeUntil(destroy$)`, nested-subscribe, and disciplined cleanup.
22. **Real-World Patterns and Async Interop** — typeahead, polling, retry-with-backoff, caching; `firstValueFrom`/`lastValueFrom` and `fromFetch`.

---

## How to use this pathway

- **Don't skip.** Phase 2's flattening operators assume Phase 1's contract and subscription model; Phase 3 assumes all of it.
- **Do the exercises.** Each lesson ends with Easy/Medium/Challenging tasks — reactive intuition comes from building, not reading.
- **Take the quizzes.** Every lesson has a quiz; a passing score means the mental model landed.
- **Verify against the docs.** When in doubt about an operator's exact behaviour, check [rxjs.dev](https://rxjs.dev) — accuracy over assumption.
