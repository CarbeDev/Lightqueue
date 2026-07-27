package dev.carbe.lightqueue

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.Logger
import kotlin.time.Duration

/**
 * Propagates cancellation after an event has already reached its dead-letter terminal state.
 * Workers use this marker to avoid counting that same event as dropped as well.
 */
internal class TerminalEventCancellationException(cause: CancellationException) :
    CancellationException(cause.message) {
    init {
        initCause(cause)
    }
}

/**
 * Shared retry/dead-letter logic, extracted so [InMemoryQueue] and [PriorityInMemoryQueue]
 * cannot drift apart on this subtle codepath.
 *
 * One instance is bound to a single set of counters and callbacks. [InMemoryQueue] keeps
 * exactly one; [PriorityInMemoryQueue] keeps one per [Priority] level so each level's
 * `processed`/`deadLettered`/`retries` counters move independently.
 */
internal class EventProcessor<T>(
    private val onProcess: suspend (T) -> Unit,
    private val retryPolicy: RetryPolicy?,
    private val attemptTimeout: Duration?,
    private val onDeadLetter: (suspend (T, Throwable) -> Unit)?,
    private val logPrefix: String,
    private val logger: Logger,
    private val onProcessed: () -> Unit,
    private val onDeadLettered: () -> Unit,
    private val onRetry: () -> Unit,
    private val onTimedOut: () -> Unit,
) {
    suspend fun process(event: T) {
        val maxAttempts = retryPolicy?.maxAttempts ?: 1
        val timeout = attemptTimeout
        lateinit var lastError: Throwable

        for (attempt in 1..maxAttempts) {
            // Every attempt past the first is a retry.
            if (attempt > 1) onRetry()
            try {
                if (timeout == null) {
                    onProcess(event)
                    onProcessed()
                    return
                }

                // withTimeoutOrNull only swallows the cancellation raised by its own timeout:
                // a TimeoutCancellationException thrown by a withTimeout inside the handler
                // still propagates, and so does an outer scope cancellation.
                if (withTimeoutOrNull(timeout) { onProcess(event) } != null) {
                    onProcessed()
                    return
                }

                onTimedOut()
                logger.warn(
                    "{}Attempt {}/{} timed out after {} for event: {}",
                    logPrefix,
                    attempt,
                    maxAttempts,
                    timeout,
                    event,
                )
                // A timeout is an ordinary failure: same retry path, and a cause the
                // onDeadLetter callback can act on.
                lastError = AttemptTimeoutException(timeout)
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
        onDeadLettered()
        try {
            onDeadLetter?.invoke(event, lastError)
        } catch (e: CancellationException) {
            throw TerminalEventCancellationException(e)
        } catch (e: Exception) {
            // A failing dead-letter callback must not take down the worker loop:
            // the event would simply be lost with no trace otherwise.
            logger.error("{}onDeadLetter callback failed for event: {}", logPrefix, event, e)
        }
    }
}
