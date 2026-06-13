package dev.carbe.lightqueue

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.times

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

    // Gauges: go up and down as events flow through. The other counters are monotonic.
    private val depth = AtomicLong()
    private val inFlight = AtomicLong()
    private val enqueued = AtomicLong()
    private val processed = AtomicLong()
    private val deadLettered = AtomicLong()
    private val dropped = AtomicLong()
    private val rejected = AtomicLong()
    private val retries = AtomicLong()

    /**
     * Immutable snapshot of the queue's counters. Cheap to call; intended to be polled and
     * wired into an external metrics system (e.g. Micrometer gauges) by the caller.
     *
     * The fields are read independently, so a snapshot taken while events are in motion is
     * eventually-consistent rather than a single atomic instant; the invariant
     * `enqueued = processed + deadLettered + dropped + inFlight + depth` holds at quiescence.
     */
    fun metrics(): QueueMetrics = QueueMetrics(
        depth = depth.get(),
        inFlight = inFlight.get(),
        enqueued = enqueued.get(),
        processed = processed.get(),
        deadLettered = deadLettered.get(),
        dropped = dropped.get(),
        rejected = rejected.get(),
        retries = retries.get(),
    )

    private val channel =
        Channel<T>(
            capacity = capacity,
            onBufferOverflow = when (overflowStrategy) {
                OverflowStrategy.EVICT_OLDEST -> BufferOverflow.DROP_OLDEST
                // REJECT relies on trySend failing on a full SUSPEND channel,
                // so the caller can be told the event was rejected.
                OverflowStrategy.REJECT, OverflowStrategy.BACKPRESSURE -> BufferOverflow.SUSPEND
            },
            onUndeliveredElement = { event ->
                logger.debug("{}Event dropped before processing: {}", logPrefix, event)
                // An EVICT_OLDEST eviction (or scope-cancellation abandonment) reaches the event
                // here after it was already counted into depth. We must decrement depth as well as
                // bumping dropped, otherwise the gauge drifts permanently upward.
                dropped.incrementAndGet()
                depth.decrementAndGet()
                onDropped?.invoke(event)
            },
        )

    private val workers = List(numberOfWorkers) { workerIndex ->
        scope.launch {
            logger.debug("{}Worker {} started (capacity={}, overflow={})", logPrefix, workerIndex, capacity, overflowStrategy)

            for (event in channel) {
                // Bump inFlight before decrementing depth so the invariant never transiently
                // under-counts (the event is always accounted for in exactly one of the two).
                inFlight.incrementAndGet()
                depth.decrementAndGet()
                try {
                    processWithRetry(event)
                } finally {
                    inFlight.decrementAndGet()
                }
            }

            logger.debug("{}Worker {} stopped", logPrefix, workerIndex)
        }
    }

    private suspend fun processWithRetry(event: T) {
        val maxAttempts = retryPolicy?.maxAttempts ?: 1
        lateinit var lastError: Throwable

        for (attempt in 1..maxAttempts) {
            // Every attempt past the first is a retry.
            if (attempt > 1) retries.incrementAndGet()
            try {
                onProcess(event)
                processed.incrementAndGet()
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.warn("{}Attempt {}/{} failed for event: {}", logPrefix, attempt, maxAttempts, event, e)
                lastError = e
            }

            if (attempt < maxAttempts) {
                retryPolicy?.let { delay(it.delayForAttempt(attempt)) }
            }
        }

        logger.error("{}All {} attempt(s) exhausted, dead-lettering event: {}", logPrefix, maxAttempts, event, lastError)
        // The event reaches a terminal state here regardless of whether onDeadLetter is null
        // or throws, so the counter is bumped before invoking the callback.
        deadLettered.incrementAndGet()
        try {
            onDeadLetter?.invoke(event, lastError)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // A failing dead-letter callback must not take down the worker loop:
            // the event would simply be lost with no trace otherwise.
            logger.error("{}onDeadLetter callback failed for event: {}", logPrefix, event, e)
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
    fun tryEnqueue(event: T): EnqueueResult {
        val result = channel.trySend(event)
        return when {
            result.isSuccess -> {
                // Accepted: count it into the queue. An EVICT_OLDEST eviction triggered by this
                // send is handled separately in onUndeliveredElement (dropped++ / depth--).
                enqueued.incrementAndGet()
                depth.incrementAndGet()
                EnqueueResult.Enqueued
            }
            // A closed channel touches no counter: the event never entered the queue.
            result.isClosed -> EnqueueResult.Closed
            else -> {
                logger.debug("{}Buffer full, rejecting event: {}", logPrefix, event)
                // Rejected events never entered the queue, so depth is left untouched.
                rejected.incrementAndGet()
                EnqueueResult.Rejected
            }
        }
    }

    /**
     * Enqueues [event], honoring [overflowStrategy]. Only [OverflowStrategy.BACKPRESSURE]
     * suspends, waiting for room to become available; for every other strategy this behaves
     * like [tryEnqueue].
     */
    suspend fun enqueue(event: T): EnqueueResult {
        if (overflowStrategy != OverflowStrategy.BACKPRESSURE) {
            return tryEnqueue(event)
        }

        return try {
            channel.send(event)
            enqueued.incrementAndGet()
            depth.incrementAndGet()
            EnqueueResult.Enqueued
        } catch (e: ClosedSendChannelException) {
            EnqueueResult.Closed
        }
    }

    suspend fun stop() {
        logger.debug("{}Stopping queue: draining buffer", logPrefix)
        channel.close()
        workers.joinAll()
        logger.debug("{}Queue stopped", logPrefix)
    }
}

enum class OverflowStrategy {
    EVICT_OLDEST,
    REJECT,
    BACKPRESSURE,
}

sealed interface EnqueueResult {
    data object Enqueued : EnqueueResult
    data object Rejected : EnqueueResult
    data object Closed : EnqueueResult
}

/**
 * [maxAttempts] is the total number of times the handler is executed for an
 * event (initial attempt included), not the number of retries.
 */
data class RetryPolicy(val maxAttempts: Int, val backoffType: BackoffType) {
    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1, but was $maxAttempts" }
    }

    fun delayForAttempt(attemptNumber: Int): Duration = when (backoffType) {
        is BackoffType.ExponentialBackoff -> backoffType.initialDelay * (1 shl (attemptNumber - 1))
        is BackoffType.LinearBackoff -> attemptNumber * backoffType.initialDelay
        is BackoffType.NoBackoff -> Duration.ZERO
    }
}
