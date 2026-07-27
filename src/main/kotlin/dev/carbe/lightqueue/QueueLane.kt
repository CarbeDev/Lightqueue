package dev.carbe.lightqueue

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import org.slf4j.Logger
import java.util.concurrent.atomic.AtomicLong

/**
 * A single `{ channel + counters + EventProcessor }` triplet, plus the enqueue/overflow/metrics
 * logic that goes with it. [InMemoryQueue] owns exactly one [QueueLane]; [PriorityInMemoryQueue]
 * owns one per [Priority] level, each with its own gauges so eviction/abandonment on one level
 * never affects another's counters.
 *
 * What stays *outside* the lane, by design: the worker loop (so callers control exactly when
 * `inFlight`/`depth` move relative to picking the next event), `stop()` (so callers decide how
 * many channels to close and in which order), and metrics aggregation across levels.
 *
 * [logger] is supplied by the owning queue so extracting this helper does not change the
 * externally-visible SLF4J category. [logTag] adds the priority to messages emitted for a
 * [PriorityInMemoryQueue] lane.
 */
internal class QueueLane<T>(
    capacity: Int,
    private val overflowStrategy: OverflowStrategy,
    private val onDropped: ((T) -> Unit)?,
    private val logPrefix: String,
    private val logger: Logger,
    // "" for InMemoryQueue; " (HIGH)" etc. for PriorityInMemoryQueue, inserted right before the
    // trailing ": {}" so the two queues' log lines stay byte-for-byte identical to before.
    private val logTag: String,
    onProcess: suspend (T) -> Unit,
    retryPolicy: RetryPolicy?,
    onDeadLetter: (suspend (T, Throwable) -> Unit)?,
) {
    // Gauges: go up and down as events flow through. The other counters are monotonic.
    private val depth = AtomicLong()
    private val inFlight = AtomicLong()
    private val enqueued = AtomicLong()
    private val processed = AtomicLong()
    private val deadLettered = AtomicLong()
    private val dropped = AtomicLong()
    private val rejected = AtomicLong()
    private val wouldBlock = AtomicLong()
    private val retries = AtomicLong()

    val channel =
        Channel<T>(
            capacity = capacity,
            onBufferOverflow = when (overflowStrategy) {
                OverflowStrategy.EVICT_OLDEST -> BufferOverflow.DROP_OLDEST
                // REJECT relies on trySend failing on a full SUSPEND channel,
                // so the caller can be told the event was rejected.
                OverflowStrategy.REJECT, OverflowStrategy.BACKPRESSURE -> BufferOverflow.SUSPEND
            },
            // A suspended BACKPRESSURE send can be cancelled before it is accepted. Such an
            // element reaches onUndeliveredElement even though enqueue() has not incremented its
            // counters yet, so treating it as dropped would corrupt depth. EVICT_OLDEST is the
            // only strategy that needs this callback: its sends are non-suspending, and the
            // callback always identifies an element that was previously accepted and counted.
            onUndeliveredElement =
                if (overflowStrategy == OverflowStrategy.EVICT_OLDEST) {
                    { event -> markBufferedDropped(event) }
                } else {
                    null
                },
        )

    // Delegates the retry/dead-letter loop to the shared implementation, wired to this
    // lane's own counters and logger prefix. See Processing.kt.
    private val processor = EventProcessor(
        onProcess = onProcess,
        retryPolicy = retryPolicy,
        onDeadLetter = onDeadLetter,
        logPrefix = logPrefix,
        logger = logger,
        onProcessed = { processed.incrementAndGet() },
        onDeadLettered = { deadLettered.incrementAndGet() },
        onRetry = { retries.incrementAndGet() },
    )

    /** Runs [event] through this lane's retry/dead-letter loop. */
    suspend fun process(event: T) = processor.process(event)

    /** Bumps the gauges to reflect [event] moving from the buffer into a worker. */
    fun markInFlight() {
        // Bump inFlight before decrementing depth so the invariant never transiently
        // under-counts (the event is always accounted for in exactly one of the two).
        inFlight.incrementAndGet()
        depth.decrementAndGet()
    }

    /** Bumps the gauges to reflect a worker being done with the event it was processing. */
    fun markDone() {
        inFlight.decrementAndGet()
    }

    /** Records an accepted in-flight event that was interrupted by worker cancellation. */
    fun markInFlightDropped(event: T) {
        dropped.incrementAndGet()
        notifyDropped(event)
    }

    /**
     * Stops accepting new events and records every still-buffered event as dropped.
     *
     * Closing first also resumes suspended BACKPRESSURE senders with [EnqueueResult.Closed].
     * Those pending sends were never counted and are deliberately not reported through
     * `onDropped`.
     */
    fun abort() {
        channel.close()
        while (true) {
            val result = channel.tryReceive()
            if (!result.isSuccess) break
            markBufferedDropped(result.getOrThrow())
        }
    }

    private fun markBufferedDropped(event: T) {
        logger.debug("{}Event dropped before processing{}: {}", logPrefix, logTag, event)
        dropped.incrementAndGet()
        depth.decrementAndGet()
        notifyDropped(event)
    }

    private fun notifyDropped(event: T) {
        try {
            onDropped?.invoke(event)
        } catch (e: Exception) {
            // Lifecycle cleanup must not be interrupted by an observer callback.
            logger.error("{}onDropped callback failed for event{}: {}", logPrefix, logTag, event, e)
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
            // Buffer full (only reachable for REJECT and BACKPRESSURE; EVICT_OLDEST drops the
            // oldest and succeeds instead). Either way the event never entered the queue, so
            // depth is left untouched — but the two cases mean different things:
            overflowStrategy == OverflowStrategy.BACKPRESSURE -> {
                // Not a policy rejection: a blocking enqueue() would have suspended and waited
                // here. Counted separately so `rejected` stays a clean "refused by policy" signal.
                logger.debug("{}Buffer full, tryEnqueue would block{}: {}", logPrefix, logTag, event)
                wouldBlock.incrementAndGet()
                EnqueueResult.Rejected
            }
            else -> {
                logger.debug("{}Buffer full, rejecting event{}: {}", logPrefix, logTag, event)
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

    /**
     * Immutable snapshot of this lane's counters, in the shape expected by
     * [InMemoryQueue.metrics] / [PriorityInMemoryQueue.metrics]. [name] is passed in by the
     * caller: the plain configured name for [InMemoryQueue], or `"$name:$priority"` for
     * [PriorityInMemoryQueue].
     */
    fun metrics(name: String?): QueueMetrics = QueueMetrics(
        name = name,
        depth = depth.get(),
        inFlight = inFlight.get(),
        enqueued = enqueued.get(),
        processed = processed.get(),
        deadLettered = deadLettered.get(),
        dropped = dropped.get(),
        rejected = rejected.get(),
        wouldBlock = wouldBlock.get(),
        retries = retries.get(),
    )
}
