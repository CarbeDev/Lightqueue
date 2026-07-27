# lightqueue

An in-memory async queue for Kotlin coroutines. A thin, opinionated layer on top of
`Channel`: a pool of workers, retry with backoff, a dead-letter callback, and explicit
overflow strategies — configured through a small DSL.

## Installation

```kotlin
dependencies {
    implementation("dev.carbe:lightqueue:0.3.0")
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

## Priorities

`PriorityInMemoryQueue` is a sibling of `InMemoryQueue` that schedules events by
[`Priority`](src/main/kotlin/dev/carbe/lightqueue/Priority.kt):

```kotlin
enum class Priority { HIGH, NORMAL, LOW }
```

```kotlin
import dev.carbe.lightqueue.PriorityInMemoryQueue
import dev.carbe.lightqueue.Priority

val queue = PriorityInMemoryQueue.create<Event>(scope) {
    capacity = 100 // applied to EACH priority level independently
    workers = 3

    process { event ->
        handle(event)
    }
}

queue.enqueue(event)                    // priority defaults to Priority.NORMAL
queue.enqueue(event, Priority.HIGH)
queue.tryEnqueue(event, Priority.LOW)

queue.stop()
```

Every level (`HIGH`, `NORMAL`, `LOW`) has its own buffer, with its own capacity,
overflow handling and metrics — filling or evicting from one level never affects
another.

**Scheduling is strict under load, best-effort when idle.** Workers always check
`HIGH`, then `NORMAL`, then `LOW`, taking the first non-empty buffer. Whenever any
level has a backlog, the highest-priority non-empty level is always served next —
a steady stream of `HIGH` events can starve `LOW` indefinitely. The only relaxation
is when the queue is completely idle (every level empty) and events for multiple
levels arrive at essentially the same instant while a worker is parked waiting:
in that narrow race the worker may pick up whichever event it was woken for, even
if it isn't the highest-priority one — the very next pick immediately re-applies
the strict order.

This strict ordering is deliberate, and there is **no built-in aging or
anti-starvation**: a `LOW` event will wait behind every `HIGH`/`NORMAL` event for
as long as those keep arriving, however old it gets. That is the right default for
a priority queue, but it means starvation is the caller's concern, not the queue's.
If `LOW` work must eventually run, keep the higher levels from being a truly
unbounded firehose — for example bound their inflow, run a separate queue for the
low-priority stream, or periodically promote aged work to a higher level yourself.

**Metrics**: `metrics(priority)` returns a per-level `QueueMetrics` snapshot (its
`name` is the configured `name` suffixed with the level, e.g. `"webhooks:HIGH"`).
`metrics()` returns the aggregate across all levels, with the plain configured
`name`; the usual invariant `enqueued = processed + deadLettered + dropped +
inFlight + depth` holds for both the aggregate and each level.

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
  abandoned in the buffer, interrupted in-flight, or handed to a worker whose
  continuation is cancelled before processing begins. It is never invoked for
  `Rejected`, `Closed`, or a suspended `BACKPRESSURE` enqueue cancelled before
  acceptance — the caller still owns those events.
- **The caller owns the lifecycle (structured concurrency).** Workers run in the
  `CoroutineScope` you pass to `create`. If you cancel that scope, every priority
  level is closed immediately, future enqueues return `Closed`, and accepted events
  that cannot start or finish are reported through `onDropped`. This is an abort,
  not a drain.
- **`stop()` drains but has no timeout.** It closes the queue to new events, then
  waits for the workers to finish everything already buffered — however long that
  takes. If you need a bounded shutdown, wrap the call in `withTimeout` yourself or
  cancel the scope; cancelling switches to the immediate-abort behaviour above.

## License

[MIT](LICENSE)
