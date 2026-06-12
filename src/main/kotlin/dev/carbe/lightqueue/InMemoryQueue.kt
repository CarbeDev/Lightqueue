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
) {
    companion object {
        private val logger = LoggerFactory.getLogger(InMemoryQueue::class.java)

        fun <T> create(scope: CoroutineScope, block: QueueDsl<T>.() -> Unit): InMemoryQueue<T> =
            QueueDsl<T>(scope).apply(block).toInMemoryQueue()
    }

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
                logger.debug("Event dropped before processing: {}", event)
                onDropped?.invoke(event)
            },
        )

    private val workers = List(numberOfWorkers) { workerIndex ->
        scope.launch {
            logger.debug("Worker {} started (capacity={}, overflow={})", workerIndex, capacity, overflowStrategy)

            for (event in channel) {
                processWithRetry(event)
            }

            logger.debug("Worker {} stopped", workerIndex)
        }
    }

    private suspend fun processWithRetry(event: T) {
        val maxAttempts = retryPolicy?.maxAttempts ?: 1
        lateinit var lastError: Throwable

        for (attempt in 1..maxAttempts) {
            try {
                onProcess(event)
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.warn("Attempt {}/{} failed for event: {}", attempt, maxAttempts, event, e)
                lastError = e
            }

            if (attempt < maxAttempts) {
                retryPolicy?.let { delay(it.delayForAttempt(attempt)) }
            }
        }

        logger.error("All {} attempt(s) exhausted, dead-lettering event: {}", maxAttempts, event, lastError)
        try {
            onDeadLetter?.invoke(event, lastError)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            // A failing dead-letter callback must not take down the worker loop:
            // the event would simply be lost with no trace otherwise.
            logger.error("onDeadLetter callback failed for event: {}", event, e)
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
            result.isSuccess -> EnqueueResult.Enqueued
            result.isClosed -> EnqueueResult.Closed
            else -> {
                logger.debug("Buffer full, rejecting event: {}", event)
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
            EnqueueResult.Enqueued
        } catch (e: ClosedSendChannelException) {
            EnqueueResult.Closed
        }
    }

    suspend fun stop() {
        logger.debug("Stopping queue: draining buffer")
        channel.close()
        workers.joinAll()
        logger.debug("Queue stopped")
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
