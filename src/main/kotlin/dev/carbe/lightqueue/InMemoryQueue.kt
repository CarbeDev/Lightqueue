package dev.carbe.lightqueue

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

class InMemoryQueue<T> internal constructor(
    scope: CoroutineScope,
    private val onProcess: suspend (T) -> Unit,
    numberOfWorkers: Int,
    private val onDeadLetter: (suspend (T, Throwable) -> Unit)?,
    private val retryPolicy: RetryPolicy?,
    private val overflowStrategy: OverflowStrategy,
    private val onDropped: ((T) -> Unit)?,
    capacity: Int,
    private val name: String? = null,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(InMemoryQueue::class.java)

        fun <T> create(scope: CoroutineScope, block: QueueDsl<T>.() -> Unit): InMemoryQueue<T> =
            QueueDsl<T>(scope).apply(block).toInMemoryQueue()
    }

    // Prepended to every log statement so a process running several queues can tell them
    // apart. A plain prefix is used rather than MDC because coroutines hop threads freely.
    private val logPrefix = name?.let { "[$it] " } ?: ""

    // The channel, counters, and EventProcessor, plus the enqueue/overflow/metrics logic that
    // goes with them. See QueueLane.kt. logTag is empty: InMemoryQueue has only one lane, so
    // its drop/buffer-full log lines carry no level tag (unlike PriorityInMemoryQueue's).
    private val lane = QueueLane<T>(
        capacity = capacity,
        overflowStrategy = overflowStrategy,
        onDropped = onDropped,
        logPrefix = logPrefix,
        logger = logger,
        logTag = "",
        onProcess = onProcess,
        retryPolicy = retryPolicy,
        onDeadLetter = onDeadLetter,
    )

    /**
     * Immutable snapshot of the queue's counters. Cheap to call; intended to be polled and
     * wired into an external metrics system (e.g. Micrometer gauges) by the caller.
     *
     * The fields are read independently, so a snapshot taken while events are in motion is
     * eventually-consistent rather than a single atomic instant; the invariant
     * `enqueued = processed + deadLettered + dropped + inFlight + depth` holds at quiescence.
     */
    fun metrics(): QueueMetrics = lane.metrics(name)

    private val aborted = AtomicBoolean()

    private fun abort() {
        if (aborted.compareAndSet(false, true)) {
            logger.debug("{}Worker scope cancelled: dropping buffered events", logPrefix)
            lane.abort()
        }
    }

    private val workers = List(numberOfWorkers) { workerIndex ->
        scope.launch {
            logger.debug("{}Worker {} started (capacity={}, overflow={})", logPrefix, workerIndex, capacity, overflowStrategy)

            for (event in lane.channel) {
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
            // This also runs when the parent scope was already cancelled and the coroutine body
            // never started, ensuring the channel cannot remain open without a consumer.
            worker.invokeOnCompletion { cause ->
                if (cause is CancellationException) abort()
            }
        }
    }

    /**
     * Attempts to enqueue [event] without suspending, regardless of [overflowStrategy].
     *
     * [EnqueueResult.Rejected] means "no room right now". With [OverflowStrategy.EVICT_OLDEST]
     * this never happens: the oldest event is evicted (and reported through `onDropped`) to
     * make room. With [OverflowStrategy.BACKPRESSURE], a `Rejected` result is not a policy
     * decision — it just means this non-blocking call didn't want to wait; use [enqueue] to
     * wait for room instead.
     *
     * A `Rejected` result never triggers `onDropped`: the event was never accepted into the
     * queue, so the caller still holds it and is responsible for it.
     */
    fun tryEnqueue(event: T): EnqueueResult = lane.tryEnqueue(event)

    /**
     * Enqueues [event], honoring [overflowStrategy]. Only [OverflowStrategy.BACKPRESSURE]
     * suspends, waiting for room to become available; for every other strategy this behaves
     * like [tryEnqueue].
     */
    suspend fun enqueue(event: T): EnqueueResult = lane.enqueue(event)

    suspend fun stop() {
        logger.debug("{}Stopping queue: draining buffer", logPrefix)
        lane.channel.close()
        workers.joinAll()
        logger.debug("{}Queue stopped", logPrefix)
    }
}
