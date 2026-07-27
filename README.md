# lightqueue

An in-memory async queue for Kotlin coroutines. A thin, opinionated layer on top of
`Channel`: a pool of workers, retry with backoff, a dead-letter callback, and explicit
overflow strategies — configured through a small DSL.

## Installation

```kotlin
dependencies {
    implementation("dev.carbe:lightqueue:0.4.0")
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

    attemptTimeout = 5.seconds // budget for ONE attempt; unset = no timeout

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

## Timeouts

`attemptTimeout` bounds a **single** processing attempt. It is unset by default, and
available on both `InMemoryQueue` and `PriorityInMemoryQueue` (where it applies
identically to every priority level).

```kotlin
val queue = InMemoryQueue.create<Event>(scope) {
    attemptTimeout = 5.seconds

    process { event -> callFlakyApi(event) }

    retryPolicy {
        maxAttempts = 3
        backoff = Backoff.exponential(100.milliseconds)
    }

    onDeadLetter = { event, cause ->
        if (cause is AttemptTimeoutException) log.error("$event never finished in ${cause.timeout}")
    }
}
```

- **The budget is per attempt, not per event.** With `maxAttempts = 3` above, the
  handler gets 5 seconds *each time*, and the backoff between attempts is not charged
  against it. The worst case for one event is therefore 3 × 5s plus the backoff, not 5s.
- **A timeout is an ordinary failure.** The attempt is cancelled, retried if attempts
  remain, and otherwise dead-lettered with an `AttemptTimeoutException` cause — a plain
  `Exception`, not a `CancellationException`, so you can handle it like any other. The
  worker survives and picks up the next event; `onDropped` is never involved.
- **Cancellation is cooperative, and a fully blocking handler escapes the timeout
  entirely.** The timeout cancels the coroutine running your handler, which only takes
  effect at a suspension point. A handler that blocks its thread and *never suspends*
  (`Thread.sleep`, a blocking JDBC or HTTP call) cannot be interrupted — and because it
  eventually returns a result, that result wins: the event is counted as **processed**,
  `timedOut` stays at zero, and the only trace is that the attempt took longer than its
  budget. The timeout is not a wall clock over your handler; it is a cancellation
  request your handler has to be able to honour. Move genuinely blocking work to
  `withContext(Dispatchers.IO)` and make it interruptible (or poll `ensureActive()`) if
  you want the budget enforced.
- A handler that *catches* the cancellation and returns normally does not escape:
  the attempt is still counted as timed out and dead-lettered.
- **Metrics**: `QueueMetrics.timedOut` counts timed-out *attempts*, like `retries` —
  one event that times out on all three attempts contributes 3 to `timedOut` and 1 to
  `deadLettered`.

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
- **`attemptTimeout` bounds an attempt, not an event.** A per-event deadline would have
  to be split across retries and backoff, making each individual attempt's budget
  depend on how the previous ones failed. Bounding the attempt keeps the guarantee the
  handler actually cares about — "you get 5 seconds" — and leaves the total cost per
  event as the product of `maxAttempts` and the timeout, which the caller chooses.
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
