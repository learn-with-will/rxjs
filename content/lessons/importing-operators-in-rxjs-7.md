---
slug: importing-operators-in-rxjs-7
title: "Importing operators in RxJS 7+"
topic: pipes-transforming
kind: concept
summary: "Since RxJS 7.2, both creation functions and pipeable operators are imported directly from 'rxjs'; 'rxjs/operators' is kept only for backwards compatibility."
tags: [rxjs, imports, operators, rxjs-7]
related: [what-is-a-pipeable-operator, chaining-with-pipe, of-and-from]
since: "7.2"
---
> **In one line:** in RxJS 7.2+, import **everything** — creation functions, pipeable operators, Subjects, and types — directly from `'rxjs'` instead of splitting imports across `'rxjs/operators'`.

## The idea

If you read older tutorials or RxJS 6 codebases, you will see imports split across two entry points: creation functions imported from `'rxjs'` and pipeable operators imported from `'rxjs/operators'`. That split existed in RxJS 6 only because a few deprecated creation functions shared names with pipeable operators (such as `merge`, `concat`, `zip`, and `race`).

In **RxJS 7.2+**, the deprecated prototype/creation collisions were cleaned up and all pipeable operators were re-exported directly from the main **`'rxjs'`** entry point.

| Entry point | Status in RxJS 7.2+ | What lives there |
|---|---|---|
| **`'rxjs'`** | **Preferred** | Creation functions (`of`, `from`), pipeable operators (`map`, `filter`, `switchMap`, ` mergeWith`), `Observable`, `Subject`, schedulers |
| **`'rxjs/operators'`** | Legacy compatibility | Re-exports pipeable operators for older RxJS 6 codebases |
| **`'rxjs/ajax'`** | Active subpath | `ajax` creation helper and types |
| **`'rxjs/fetch'`** | Active subpath | `fromFetch` creation helper |
| **`'rxjs/testing'`** | Active subpath | `TestScheduler` for marble testing |

## See it

```typescript
// Before (RxJS 6 / legacy style — two separate imports):
// import { of } from 'rxjs';
// import { map, filter, take } from 'rxjs/operators';

// After (RxJS 7.2+ idiomatic style — one single import from 'rxjs'):
import { of, map, filter, take } from 'rxjs';

of(1, 2, 3, 4, 5)
  .pipe(
    filter((n) => n % 2 !== 0),
    map((n) => n * n),
    take(2),
  )
  .subscribe((n) => console.log('odd square:', n)); // 1, 9
```

## Go deeper

What about operators whose names used to collide with creation functions, like `merge`, `concat`, `zip`, and `race`? In RxJS 7+, the standalone creation functions keep the short names (`merge(a$, b$)`, `concat(a$, b$)`), while their pipeable counterparts have clear **`*With`** names (`mergeWith`, `concatWith`, `zipWith`, `raceWith`, `combineLatestWith`) exported straight from `'rxjs'`:

```typescript
import { of, merge, mergeWith } from 'rxjs';

// Creation function:
merge(of(1), of(2)).subscribe(console.log);

// Pipeable *With operator:
of(1).pipe(mergeWith(of(2))).subscribe(console.log);
```

Why was consolidating imports into `'rxjs'` such a big quality-of-life improvement? In RxJS 6, developers constantly had to remember whether a symbol like `concat`, `merge`, `zip`, `race`, or `partition` was imported from `'rxjs'` or `'rxjs/operators'`, and auto-import in IDEs frequently picked the deprecated pipeable `merge` from `'rxjs/operators'` instead of the creation function. With RxJS 7.2+, one `import { ... } from 'rxjs'` statement at the top of your file covers every core creation function, operator, Subject, and scheduler, and modern bundlers (Vite, esbuild, Webpack 5, Rollup) tree-shake the single entry point just as effectively.

## Watch out

- Do not mix `import { map } from 'rxjs/operators'` in new RxJS 7+ code when `import { map } from 'rxjs'` keeps all core imports in one statement.
- Remember that specialized utilities like `ajax` (`'rxjs/ajax'`), `fromFetch` (`'rxjs/fetch'`), and `TestScheduler` (`'rxjs/testing'`) still live in their own dedicated subpath entry points.

## Remember

- In RxJS 7.2+, import both creation functions and pipeable operators from `'rxjs'`.
- Use `mergeWith`, `concatWith`, `zipWith`, and `raceWith` when piping inside `.pipe()`.
- Keep `'rxjs/ajax'`, `'rxjs/fetch'`, and `'rxjs/testing'` for their specialized modules.
