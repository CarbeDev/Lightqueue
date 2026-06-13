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
 */
data class QueueMetrics(
    val depth: Long,
    val inFlight: Long,
    val enqueued: Long,
    val processed: Long,
    val deadLettered: Long,
    val dropped: Long,
    val rejected: Long,
    val retries: Long,
)
