---
slug: grouping-subscriptions-with-add
title: "Grouping subscriptions with subscription.add()"
topic: observables-subscriptions
kind: technique
summary: "A parent Subscription can adopt child Subscriptions via add(), allowing a single unsubscribe() call to tear down every grouped stream at once."
tags: [rxjs, subscription, add, teardown, cleanup]
related: [subscription-and-unsubscribe, unsubscribe-vs-complete-pitfall, take-until-destroy-pattern]
---
> **In one line:** `parentSub.add(childSub)` attaches child subscriptions to a single parent so one `parentSub.unsubscribe()` call cancels the entire group.

## The idea

When a component or service subscribes to three or four open-ended streams, storing each `Subscription` in its own variable (`sub1`, `sub2`, `sub3`) and remembering to call `.unsubscribe()` on every one of them in your cleanup method is tedious and error-prone.

RxJS's `Subscription` class doubles as a **composite container**. You can create an empty parent `new Subscription()`, register any number of child subscriptions with `parent.add(child)`, and then tear the whole group down with a single `parent.unsubscribe()` call.

```flow
parent: Parent Subscription | new Subscription() @primary
s1: Interval Stream | child subscription @blue
s2: Resize Stream | child subscription @blue
s3: Click Stream | child subscription @blue
parent -> s1: unsubscribes
parent -> s2: unsubscribes
parent -> s3: unsubscribes
```

## See it

Here is how one parent `Subscription` collects multiple active streams and cancels them together:

```typescript
import { interval, fromEvent, Subscription } from 'rxjs';

const subscriptions = new Subscription();

subscriptions.add(
  interval(1000).subscribe((n) => console.log('timer tick:', n)),
);

subscriptions.add(
  fromEvent(window, 'resize').subscribe(() => console.log('window resized')),
);

// One call tears down both the interval and the resize listener:
function cleanup() {
  subscriptions.unsubscribe();
  console.log('All child subscriptions closed:', subscriptions.closed); // true
}

setTimeout(cleanup, 2500);
```

## Go deeper

What happens if you call `subscriptions.add(newSub)` *after* `subscriptions.unsubscribe()` has already been called? RxJS handles that edge case safely: adding a child subscription to an already-closed parent **immediately unsubscribes** the child. You can also pass a plain teardown function `() => void` directly to `subscriptions.add(() => console.log('custom cleanup'))`, and it will run alongside the child subscriptions when the parent unsubscribes.

> **Tip:** Grouping with `subscription.add()` is the imperative way to manage multiple subscriptions. In later topics you will also learn the declarative alternative, `takeUntil(destroy$)`.

```typescript
import { Subscription } from 'rxjs';

const group = new Subscription();
// You can also register a plain teardown callback with .add():
group.add(() => console.log('Custom teardown ran alongside child streams'));
group.unsubscribe();
```

If you ever need to remove a specific child subscription from the group before the parent unsubscribes, you can either call `childSub.unsubscribe()` directly (which automatically detaches it from `group`) or call `group.remove(childSub)`.

## Watch out

- If a child subscription completes on its own early, RxJS automatically removes it from the parent container so it isn't retained in memory.
- Don't reuse a `Subscription` container after calling `unsubscribe()` on it — because it is now `closed`, any new subscription you `.add()` to it will be cancelled immediately!

## Remember

- Create a container with `const subs = new Subscription()` and attach children via `subs.add(childSub)`.
- Calling `subs.unsubscribe()` unsubscribes every attached child in one go.
- Adding a subscription to an already-closed parent immediately unsubscribes the child.
