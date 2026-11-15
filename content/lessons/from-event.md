---
slug: from-event
title: "Streaming DOM events with fromEvent()"
topic: creating-observables
kind: api
summary: "fromEvent(target, eventName) turns DOM or EventEmitter events into an open-ended Observable that attaches on subscribe and removes the listener on unsubscribe."
tags: [rxjs, fromevent, dom, events, creation]
related: [of-and-from, interval-and-timer, subscription-and-unsubscribe]
---
> **In one line:** `fromEvent(target, eventName)` wraps `addEventListener` and `removeEventListener` into an Observable that **never completes on its own**.

## The idea

Instead of calling `element.addEventListener('click', fn)` and remembering to call `removeEventListener` with the exact same function reference later, `fromEvent(target, eventName)` turns any event target (a DOM `Element`, `Document`, `Window`, or Node `EventEmitter`) into an Observable stream.

Because Observables are lazy, calling `fromEvent(document, 'click')` does **not** attach a listener yet. Each time a consumer calls `.subscribe()`, `fromEvent` registers a listener on the target; when that consumer calls `.unsubscribe()` (or a bounding operator like `take(1)` completes the stream), `fromEvent` automatically removes the listener in its teardown.

```sequence
actor sub: Subscriber | consumer
actor fe: fromEvent | Observable @primary
actor dom: DOM Element | EventTarget @blue
sub -> fe: subscribe() | starts execution
fe -> dom: addEventListener('click', handler)
dom --> sub: click event | pushed via next()
sub -> fe: unsubscribe() | cancels stream
fe -> dom: removeEventListener('click', handler)
```

## See it

Provide a TypeScript type parameter like `fromEvent<MouseEvent>` or `fromEvent<KeyboardEvent>` so your downstream callbacks get full autocomplete on the event object:

```typescript
import { fromEvent } from 'rxjs';

const clicks$ = fromEvent<MouseEvent>(document, 'click');

const sub = clicks$.subscribe((event) => {
  console.log('Clicked at:', event.clientX, event.clientY);
});

// Later, when the view is destroyed, remove the DOM listener:
sub.unsubscribe();
```

## Go deeper

Crucially, `fromEvent` **never completes on its own**. A button or document can always be clicked again, so RxJS has no way to know when "all clicks are done." That means every `fromEvent` subscription stays alive — keeping its DOM listener and closure in memory — until you explicitly unsubscribe or bound the stream with a completing operator such as `take(1)` or `takeUntil(destroy$)`.

> **Tip:** `fromEvent` accepts an optional third argument for listener options, such as `fromEvent(window, 'scroll', { passive: true })` or `{ capture: true }`.

```typescript
import { fromEvent } from 'rxjs';

// Pass standard AddEventListenerOptions as the third argument:
const scroll$ = fromEvent(window, 'scroll', { passive: true });
const sub = scroll$.subscribe(() => console.log('scrollY:', window.scrollY));

// Clean up when the component or page unmounts:
sub.unsubscribe();
```

In Node.js or custom event buses, `fromEvent` also works with `EventEmitter` objects (calling `on` / `addListener` on subscribe and `off` / `removeListener` on unsubscribe), so the exact same stream pipeline handles browser and server events alike.

## Watch out

- Forgetting to unsubscribe from `fromEvent` leaks the DOM event listener and everything referenced by your subscriber callback.
- Because `fromEvent` is cold, subscribing to `clicks$` three times attaches three separate DOM listeners unless you multicast with `share()`.

## Remember

- `fromEvent(target, name)` attaches a listener on `subscribe()` and removes it on `unsubscribe()`.
- Pass a generic type like `fromEvent<KeyboardEvent>(document, 'keydown')` for typed event properties.
- `fromEvent` never self-completes — always plan how the subscription will be torn down.
