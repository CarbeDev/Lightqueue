# lightqueue

An in-memory async queue for Kotlin coroutines. A thin, opinionated layer on top of
`Channel`: a pool of workers, retry with backoff, a dead-letter callback, and explicit
overflow strategies — configured through a small DSL.

## Installation

```kotlin
dependencies {
    implementation("dev.carbe:lightqueue:0.2.0")
}
```

## Usage

```kotlin
import dev.carbe.lightqueue.Backoff
import dev.carbe.lightqueue.InMemoryQueue
import dev.carbe.lightqueue.OverflowStrategy

val queue = InMemoryQueue.create<Event>(scope) {
    capacity = 100
    workers = 3
    overflowStrategy = OverflowStrategy.BACKPRESSURE

    process { event ->
        handle(event) // suspend-friendly
    }

    retryPolicy {
        maxAttempts = 3 // total executions, initial attempt included
        backoff = Backoff.exponential(100.milliseconds)
    }

    onDeadLetter = { event, cause -> log.error("gave up on $event", cause) }
    onDropped = { event -> metrics.dropped(event) }
}

queue.enqueue(event)      // suspends only with BACKPRESSURE when full
queue.tryEnqueue(event)   // never suspends; returns Enqueued / Rejected / Closed

queue.stop()              // closes the queue and drains remaining events
```

Overflow strategies:

- `BACKPRESSURE` (default) — `enqueue` suspends until there is room.
- `REJECT` — `enqueue`/`tryEnqueue` return `EnqueueResult.Rejected` when full.
- `EVICT_OLDEST` — the oldest buffered event is evicted (reported via `onDropped`) to make room.

## When to use / when not to

**Use it when** you want fire-and-forget background processing inside a single JVM
process — webhook fan-out, notifications, log shipping — and you want retries,
bounded buffering and a dead-letter hook without standing up infrastructure.

**Use a raw `Channel` when** you just need to move values between coroutines and
don't care about retry, dead-lettering or overflow policies; the abstraction here
would be overhead.

**Use a real message queue** (RabbitMQ, Kafka, SQS, …) when you need durability,
delivery guarantees across restarts, multi-process consumers, or observability of
the backlog. lightqueue is in-memory: a crash or restart loses everything buffered.

## Design decisions

- **`maxAttempts` counts total executions**, not retries — `maxAttempts = 3` means
  one initial attempt plus two retries. This follows the Resilience4j convention.
- **Backoff occupies the worker.** Retry delays are served inline by the worker that
  is processing the event, so ordering is preserved per worker — but a long backoff
  blocks that worker from picking up the next event. This is a deliberate trade-off:
  predictable ordering over maximum throughput during retries.
- **`onDropped` means definitive loss, and only that.** It fires when an event that
  was *accepted* into the queue is permanently lost: evicted by `EVICT_OLDEST`, or
  abandoned in the buffer when the owning scope is cancelled. It is never invoked for
  `Rejected` — a rejected event was never accepted, the caller still holds it and
  stays responsible for it.
- **The caller owns the lifecycle (structured concurrency).** Workers run in the
  `CoroutineScope` you pass to `create`. If you cancel that scope, the queue dies
  with it and any buffered events are reported through `onDropped`.
- **`stop()` drains but has no timeout.** It closes the queue to new events, then
  waits for the workers to finish everything already buffered — however long that
  takes. If you need a bounded shutdown, wrap the call in `withTimeout` yourself or
  cancel the scope.

## License

[MIT](LICENSE)
