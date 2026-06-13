package dev.carbe.lightqueue

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
