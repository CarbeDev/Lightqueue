package dev.carbe.lightqueue

import kotlin.time.Duration

/**
 * Reported to `onDeadLetter` when the last processing attempt exceeded its configured timeout.
 *
 * This is deliberately not a [kotlinx.coroutines.CancellationException]: an attempt timeout is
 * a processing failure that may be retried, whereas cancellation of the queue's owning scope
 * still aborts processing immediately.
 */
class AttemptTimeoutException(val timeout: Duration) :
    Exception("Processing attempt timed out after $timeout")

internal fun validateAttemptTimeout(attemptTimeout: Duration?) {
    if (attemptTimeout == null) return

    require(attemptTimeout.isFinite() && attemptTimeout > Duration.ZERO) {
        "attemptTimeout must be finite and > 0, but was $attemptTimeout"
    }
}
