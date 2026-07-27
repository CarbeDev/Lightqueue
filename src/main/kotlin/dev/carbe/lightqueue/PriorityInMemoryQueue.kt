package dev.carbe.lightqueue

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration

/**
 * A sibling of [InMemoryQueue] with [Priority]-aware scheduling: each [Priority] level gets
 * its own buffer (and its own set of counters), and a shared pool of workers drains the
 * highest-priority non-empty level first.
 *
 * Everything that is not priority-specific — channel configuration, overflow handling, retry
 * with backoff, dead-lettering, `stop()` semantics — mirrors [InMemoryQueue] exactly, level by
 * level. See [InMemoryQueue] for the rationale behind those pieces; only the
 * priority-specific parts are commented here.
 */
class PriorityInMemoryQueue<T> internal constructor(
    scope: CoroutineScope,
    private val onProcess: suspend (T) -> Unit,
    numberOfWorkers: Int,
    private val onDeadLetter: (suspend (T, Throwable) -> Unit)?,
    private val retryPolicy: RetryPolicy?,
    private val attemptTimeout: Duration?,
    private val overflowStrategy: OverflowStrategy,
    private val onDropped: ((T) -> Unit)?,
    capacity: Int,
    private val name: String? = null,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(PriorityInMemoryQueue::class.java)

        fun <T> create(scope: CoroutineScope, block: PriorityQueueDsl<T>.() -> Unit): PriorityInMemoryQueue<T> =
            PriorityQueueDsl<T>(scope).apply(block).toPriorityInMemoryQueue()
    }

    // Prepended to every log statement so a process running several queues can tell them
    // apart. A plain prefix is used rather than MDC because coroutines hop threads freely.
    private val logPrefix = name?.let { "[$it] " } ?: ""

    /**
     * One [QueueLane] per [Priority] level — each lane owns that level's own channel, counters
     * and [EventProcessor] (see QueueLane.kt), configured exactly like [InMemoryQueue]'s single
     * lane. Keeping a full lane per level means eviction, rejection and processing on one level
     * never touch another's gauges.
     *
     * `logTag` reproduces the level-tagged log lines this queue used to emit directly, e.g.
     * `"...Event dropped before processing (HIGH): ..."`.
     */
    private val lanes: Map<Priority, QueueLane<T>> =
        Priority.entries.associateWith { priority ->
            QueueLane(
                capacity = capacity,
                overflowStrategy = overflowStrategy,
                onDropped = onDropped,
                logPrefix = logPrefix,
                logger = logger,
                logTag = " ($priority)",
                onProcess = onProcess,
                retryPolicy = retryPolicy,
                attemptTimeout = attemptTimeout,
                onDeadLetter = onDeadLetter,
            )
        }

    /** An event paired with the level it was read from, so a worker knows which lane to use. */
    private data class Prioritized<T>(val entry: QueueEntry<T>, val priority: Priority)

    /**
     * Selects the next event to process, applying strict top-down priority order.
     *
     * Each iteration first does a non-suspending pass over [Priority.entries] (HIGH, NORMAL,
     * LOW) with [Channel.tryReceive]: the first level with a buffered element wins, full stop.
     * This is the strict guarantee — under any backlog, the highest non-empty level is always
     * served next, regardless of how long lower levels have been waiting.
     *
     * If that pass finds every level empty, and every level is also closed, the queue is done
     * and this returns `null`. Otherwise we [select] across the still-open channels'
     * [Channel.onReceiveCatching] to suspend until *something* becomes available.
     *
     * Correctness note: [Channel.onReceiveCatching] inside [select] *consumes* the element it
     * wakes up on — there is no way to "peek, then re-run the strict scan" without losing it.
     * So the element received by [select] is returned directly from here instead. The strict
     * top-down order is therefore guaranteed whenever step 1 finds *any* non-empty level (i.e.
     * under any real backlog). The only window where it is not strictly enforced is the idle
     * case: all levels were empty at step 1, and elements for *multiple* levels arrive while
     * we are parked in [select] — in that race, [select] may return a lower-priority element
     * even though a higher-priority one became available at essentially the same instant.
     * This is an accepted, documented best-effort relaxation for the idle case; the very next
     * call to [nextEvent] immediately re-applies the strict scan.
     */
    @OptIn(DelicateCoroutinesApi::class)
    private suspend fun nextEvent(): Prioritized<T>? {
        while (true) {
            // 1. Non-suspending strict fast path: drain the highest non-empty level first.
            for (priority in Priority.entries) {
                val result = lanes.getValue(priority).channel.tryReceive()
                if (result.isSuccess) {
                    return Prioritized(result.getOrThrow(), priority)
                }
                // isFailure-but-not-closed => this level is simply empty right now, keep scanning.
                // isClosed => this level is closed and drained, also keep scanning.
            }

            // 2. Every level was empty above. If every level is also closed, there is nothing
            // left to wait for. isClosedForReceive is delicate because it can be momentarily
            // stale under concurrent close(); that's fine here, it's only used to decide
            // whether to build a select clause for this level — a stale "open" channel that
            // closed a moment ago is handled by onReceiveCatching returning a closed result
            // (picked == null), which loops back to step 1.
            val openChannels = Priority.entries.filter { !lanes.getValue(it).channel.isClosedForReceive }
            if (openChannels.isEmpty()) return null

            // 3. Suspend until any open level has an element. See the correctness note above:
            // the element received here is returned directly rather than discarded.
            val picked: Prioritized<T>? = select {
                for (priority in openChannels) {
                    lanes.getValue(priority).channel.onReceiveCatching { result ->
                        if (result.isSuccess) Prioritized(result.getOrThrow(), priority) else null
                    }
                }
            }
            if (picked != null) return picked
            // `picked` is null when the channel we woke up on turned out to be closed (and
            // empty) by the time we received from it. Loop back to re-evaluate from step 1.
        }
    }

    private val aborted = AtomicBoolean()

    private fun abort() {
        if (aborted.compareAndSet(false, true)) {
            logger.debug("{}Worker scope cancelled: dropping buffered events", logPrefix)
            for (priority in Priority.entries) {
                lanes.getValue(priority).abort()
            }
        }
    }

    private val workers = List(numberOfWorkers) { workerIndex ->
        scope.launch {
            logger.debug("{}Worker {} started (capacity={}, overflow={})", logPrefix, workerIndex, capacity, overflowStrategy)

            while (true) {
                val prioritized = nextEvent() ?: break
                val lane = lanes.getValue(prioritized.priority)
                val event = prioritized.entry.event

                // Bump inFlight before decrementing depth so the invariant never transiently
                // under-counts (the event is always accounted for in exactly one of the two).
                lane.markInFlight()
                try {
                    lane.process(event)
                } catch (e: CancellationException) {
                    if (e !is TerminalEventCancellationException) {
                        lane.markInFlightDropped(event)
                    }
                    throw e
                } finally {
                    lane.markDone()
                }
            }

            logger.debug("{}Worker {} stopped", logPrefix, workerIndex)
        }.also { worker ->
            // Covers cancellation before the coroutine body has had a chance to start.
            worker.invokeOnCompletion { cause ->
                if (cause is CancellationException) abort()
            }
        }
    }

    /**
     * Attempts to enqueue [event] at [priority] without suspending, regardless of
     * [overflowStrategy]. Behaves exactly like [InMemoryQueue.tryEnqueue], but against
     * [priority]'s own channel and counters — capacity, eviction and rejection on one level
     * never affect another.
     *
     * [EnqueueResult.Rejected] means "no room right now on this level". With
     * [OverflowStrategy.EVICT_OLDEST] this never happens: the oldest event *on this level* is
     * evicted (and reported through `onDropped`) to make room. With
     * [OverflowStrategy.BACKPRESSURE], a `Rejected` result is not a policy decision — it just
     * means this non-blocking call didn't want to wait; use [enqueue] to wait for room instead.
     *
     * A `Rejected` result never triggers `onDropped`: the event was never accepted into the
     * queue, so the caller still holds it and is responsible for it.
     */
    fun tryEnqueue(event: T, priority: Priority = Priority.NORMAL): EnqueueResult =
        lanes.getValue(priority).tryEnqueue(event)

    /**
     * Enqueues [event] at [priority], honoring [overflowStrategy]. Only
     * [OverflowStrategy.BACKPRESSURE] suspends, waiting for room on [priority]'s own buffer to
     * become available; for every other strategy this behaves like [tryEnqueue].
     *
     * Defaults to [Priority.NORMAL] so callers migrating from [InMemoryQueue] keep working
     * unchanged.
     */
    suspend fun enqueue(event: T, priority: Priority = Priority.NORMAL): EnqueueResult =
        lanes.getValue(priority).enqueue(event)

    /**
     * Snapshot of a single [priority] level's counters, in the same shape as
     * [InMemoryQueue.metrics]. The invariant
     * `enqueued = processed + deadLettered + dropped + inFlight + depth` holds per level at
     * quiescence, exactly as it does for [InMemoryQueue].
     *
     * [QueueMetrics.name] is the configured [name] suffixed with the level (e.g.
     * `"webhooks:HIGH"`), so per-level snapshots exported side by side remain distinguishable.
     * If no [name] was configured, this is `null` (there is nothing to suffix).
     */
    fun metrics(priority: Priority): QueueMetrics =
        lanes.getValue(priority).metrics(name?.let { "$it:$priority" })

    /**
     * Aggregate snapshot across all [Priority] levels: each field is the sum of that field
     * across [metrics] for every level. [QueueMetrics.name] is the plain configured [name]
     * (no level suffix).
     *
     * Because each level individually satisfies
     * `enqueued = processed + deadLettered + dropped + inFlight + depth`, and this snapshot
     * sums every term across levels, the same invariant holds for the aggregate at quiescence.
     */
    fun metrics(): QueueMetrics {
        val perLevel = Priority.entries.map { metrics(it) }
        return QueueMetrics(
            name = name,
            depth = perLevel.sumOf { it.depth },
            inFlight = perLevel.sumOf { it.inFlight },
            enqueued = perLevel.sumOf { it.enqueued },
            processed = perLevel.sumOf { it.processed },
            deadLettered = perLevel.sumOf { it.deadLettered },
            dropped = perLevel.sumOf { it.dropped },
            rejected = perLevel.sumOf { it.rejected },
            wouldBlock = perLevel.sumOf { it.wouldBlock },
            retries = perLevel.sumOf { it.retries },
        )
    }

    suspend fun stop() {
        logger.debug("{}Stopping queue: draining buffers", logPrefix)
        for (priority in Priority.entries) {
            lanes.getValue(priority).channel.close()
        }
        workers.joinAll()
        logger.debug("{}Queue stopped", logPrefix)
    }
}
