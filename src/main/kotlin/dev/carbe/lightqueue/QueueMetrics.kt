package dev.carbe.lightqueue

/**
 * Immutable snapshot of a queue's counters, read on demand via [InMemoryQueue.metrics].
 *
 * [depth] and [inFlight] are gauges (they go up and down); the remaining fields are
 * monotonic counters. At any quiescent point the following invariant holds:
 *
 * ```
 * enqueued = processed + deadLettered + dropped + inFlight + depth
 * ```
 *
 * [rejected], [wouldBlock] and [retries] sit outside the invariant: rejected and would-block
 * events never entered the queue, and retries count re-executions rather than events.
 */
data class QueueMetrics(
    val name: String?,
    val depth: Long,
    val inFlight: Long,
    val enqueued: Long,
    val processed: Long,
    val deadLettered: Long,
    val dropped: Long,
    /** Events refused by policy ([OverflowStrategy.REJECT], buffer full). A genuine "we are losing events" signal. */
    val rejected: Long,
    /**
     * Non-blocking [InMemoryQueue.tryEnqueue] calls that found no room on a
     * [OverflowStrategy.BACKPRESSURE] queue. Not a policy rejection — a blocking
     * [InMemoryQueue.enqueue] would simply have waited — so it is kept out of [rejected].
     */
    val wouldBlock: Long,
    val retries: Long,
)
