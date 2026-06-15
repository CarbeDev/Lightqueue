package dev.carbe.lightqueue

import kotlin.time.Duration
import kotlin.time.times

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

object Backoff {
    fun exponential(initialDelay: Duration): BackoffType.ExponentialBackoff {
        require(initialDelay > Duration.ZERO) { "initialDelay must be > 0, but was $initialDelay" }
        return BackoffType.ExponentialBackoff(initialDelay)
    }

    fun linear(initialDelay: Duration): BackoffType.LinearBackoff {
        require(initialDelay > Duration.ZERO) { "initialDelay must be > 0, but was $initialDelay" }
        return BackoffType.LinearBackoff(initialDelay)
    }

    fun noBackoff(): BackoffType.NoBackoff = BackoffType.NoBackoff
}

sealed class BackoffType {
    class ExponentialBackoff internal constructor(val initialDelay: Duration) : BackoffType()
    class LinearBackoff internal constructor(val initialDelay: Duration) : BackoffType()
    data object NoBackoff : BackoffType()
}
