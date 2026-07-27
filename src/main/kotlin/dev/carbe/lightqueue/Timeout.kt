package dev.carbe.lightqueue

import kotlin.time.Duration

/**
 * Failure cause reported when a single processing attempt exceeded the configured
 * `attemptTimeout`. It is a plain [Exception], not a `CancellationException`: a timed-out
 * attempt is a normal failure, eligible for retry and for dead-lettering.
 */
class AttemptTimeoutException(val timeout: Duration) :
    Exception("Processing attempt timed out after $timeout")
