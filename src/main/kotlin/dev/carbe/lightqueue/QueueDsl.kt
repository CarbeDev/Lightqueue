package dev.carbe.lightqueue

import kotlinx.coroutines.CoroutineScope

class QueueDsl<T> internal constructor(private val scope: CoroutineScope) {
    var capacity: Int = 100
    var workers: Int = 1

    /**
     * Optional queue name, prefixed onto the SLF4J log statements (e.g. `[webhooks] Worker 0
     * started`). Useful as soon as a process runs more than one queue, and as a natural tag
     * when exporting metrics.
     */
    var name: String? = null
    var onDeadLetter: (suspend (T, Throwable) -> Unit)? = null
    var overflowStrategy: OverflowStrategy? = null

    /**
     * Invoked only when an event that was already accepted into the queue is permanently lost
     * with no other signal: evicted by [OverflowStrategy.EVICT_OLDEST], or abandoned in the
     * buffer when the queue's scope is cancelled.
     *
     * Never invoked for [EnqueueResult.Rejected] — the caller still holds that event and is
     * responsible for it.
     */
    var onDropped: ((T) -> Unit)? = null

    private var onProcess: (suspend (T) -> Unit)? = null
    private var retryPolicy: RetryPolicy? = null

    /**
     * Test-only escape hatch: lets InMemoryQueueTest build a queue with no
     * worker to observe a buffer that nothing drains. Not part of the public API.
     */
    internal var allowNoWorkers: Boolean = false

    fun process(handler: suspend (T) -> Unit) {
        check(onProcess == null) { "process has already been configured" }
        onProcess = handler
    }

    fun retryPolicy(block: RetryPolicyDsl.() -> Unit) {
        check(retryPolicy == null) { "retryPolicy has already been configured" }
        retryPolicy = RetryPolicyDsl().apply(block).toRetryPolicy()
    }

    internal fun toInMemoryQueue(): InMemoryQueue<T> {
        if (!allowNoWorkers) {
            require(workers >= 1) { "workers must be >= 1, but was $workers" }
        }

        val onProcess = requireNotNull(onProcess) { "process must be configured" }

        return InMemoryQueue(
            scope,
            onProcess,
            workers,
            onDeadLetter,
            retryPolicy,
            overflowStrategy ?: OverflowStrategy.BACKPRESSURE,
            onDropped,
            capacity,
            name,
        )
    }
}

class RetryPolicyDsl {
    var maxAttempts: Int? = null
    var backoff: BackoffType? = null

    internal fun toRetryPolicy(): RetryPolicy {
        requireNotNull(maxAttempts) { "maxAttempts cannot be null" }
        requireNotNull(backoff) { "backoff cannot be null" }
        return RetryPolicy(maxAttempts!!, backoff!!)
    }
}
