package dev.carbe.lightqueue

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class AttemptTimeoutTest : FunSpec({

    test("an attempt that outruns the budget is dead-lettered with an AttemptTimeoutException") {
        runTest {
            var cause: Throwable? = null
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                attemptTimeout = 100.milliseconds
                process { delay(1.seconds) }
                onDeadLetter = { _, throwable -> cause = throwable }
            }

            queue.enqueue(1)
            queue.stop()

            val timeout = cause.shouldBeInstanceOf<AttemptTimeoutException>()
            timeout.timeout shouldBe 100.milliseconds

            val metrics = queue.metrics()
            metrics.timedOut shouldBe 1
            metrics.deadLettered shouldBe 1
            metrics.processed shouldBe 0
            // The usual invariant still holds: a timeout is a failure, not a lost event.
            metrics.enqueued shouldBe metrics.processed + metrics.deadLettered +
                metrics.dropped + metrics.inFlight + metrics.depth
        }
    }

    test("a handler that finishes within the budget is untouched") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                attemptTimeout = 100.milliseconds
                process {
                    delay(10.milliseconds)
                    processed.add(it)
                }
            }

            queue.enqueue(1)
            queue.enqueue(2)
            queue.stop()

            processed shouldContainExactly listOf(1, 2)
            val metrics = queue.metrics()
            metrics.processed shouldBe 2
            metrics.timedOut shouldBe 0
            metrics.deadLettered shouldBe 0
        }
    }

    test("no timeout is applied when attemptTimeout is left unset") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { delay(1.seconds) }
            }

            queue.enqueue(1)
            queue.stop()

            val metrics = queue.metrics()
            metrics.processed shouldBe 1
            metrics.timedOut shouldBe 0
        }
    }

    test("a timed-out attempt is retried, and each attempt gets a fresh budget") {
        runTest {
            val start = currentTime
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                attemptTimeout = 100.milliseconds
                process { delay(1.seconds) }
                retryPolicy {
                    maxAttempts = 3
                    backoff = Backoff.linear(50.milliseconds)
                }
            }

            queue.enqueue(1)
            queue.stop()

            val metrics = queue.metrics()
            metrics.timedOut shouldBe 3
            metrics.retries shouldBe 2
            metrics.deadLettered shouldBe 1

            // 3 attempts of 100ms each, plus the linear backoff between them (50ms then 100ms).
            // Backoff is not charged against the per-attempt budget.
            currentTime - start shouldBe 450
        }
    }

    test("an event that times out once still succeeds on a later attempt") {
        runTest {
            var attempts = 0
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                attemptTimeout = 100.milliseconds
                process {
                    attempts++
                    if (attempts == 1) delay(1.seconds)
                }
                retryPolicy {
                    maxAttempts = 3
                    backoff = Backoff.noBackoff()
                }
            }

            queue.enqueue(1)
            queue.stop()

            attempts shouldBe 2
            val metrics = queue.metrics()
            metrics.processed shouldBe 1
            metrics.timedOut shouldBe 1
            metrics.retries shouldBe 1
            metrics.deadLettered shouldBe 0
        }
    }

    test("a timeout does not take down the worker") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                attemptTimeout = 100.milliseconds
                process { event ->
                    if (event == 1) delay(1.seconds)
                    processed.add(event)
                }
            }

            queue.enqueue(1)
            queue.enqueue(2)
            queue.enqueue(3)
            queue.stop()

            processed shouldContainExactly listOf(2, 3)
            queue.metrics().timedOut shouldBe 1
            queue.metrics().processed shouldBe 2
        }
    }

    // Real dispatcher: Thread.sleep does not interact with the test scheduler's virtual time,
    // and the point of this test is precisely a handler that never suspends.
    test("a handler that never suspends escapes the timeout and still counts as processed") {
        runBlocking {
            val queueScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
            try {
                val queue = InMemoryQueue.create<Int>(queueScope) {
                    attemptTimeout = 20.milliseconds
                    process { Thread.sleep(150) }
                }

                queue.enqueue(1)
                queue.stop()

                // Cancellation is cooperative: the blocking attempt runs to completion and its
                // result wins over the fired timeout. Documented in the README, pinned here so
                // the behaviour cannot change silently.
                val metrics = queue.metrics()
                metrics.processed shouldBe 1
                metrics.timedOut shouldBe 0
                metrics.deadLettered shouldBe 0
            } finally {
                queueScope.cancel()
            }
        }
    }

    test("a handler that catches the cancellation still counts as timed out") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                attemptTimeout = 100.milliseconds
                process {
                    try {
                        delay(1.seconds)
                    } catch (e: CancellationException) {
                        // Swallowed on purpose: suppressing the timeout must not turn a
                        // timed-out attempt into a success.
                    }
                }
            }

            queue.enqueue(1)
            queue.stop()

            val metrics = queue.metrics()
            metrics.timedOut shouldBe 1
            metrics.deadLettered shouldBe 1
            metrics.processed shouldBe 0
        }
    }

    test("a non-positive attemptTimeout is rejected") {
        runTest {
            shouldThrow<IllegalArgumentException> {
                InMemoryQueue.create<Int>(backgroundScope) {
                    attemptTimeout = Duration.ZERO
                    process {}
                }
            }

            shouldThrow<IllegalArgumentException> {
                PriorityInMemoryQueue.create<Int>(backgroundScope) {
                    attemptTimeout = (-1).milliseconds
                    process {}
                }
            }
        }
    }

    test("the priority queue applies the budget to every level") {
        runTest {
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                attemptTimeout = 100.milliseconds
                process { delay(1.seconds) }
            }

            queue.enqueue(1, Priority.HIGH)
            queue.enqueue(2, Priority.LOW)
            queue.stop()

            queue.metrics(Priority.HIGH).timedOut shouldBe 1
            queue.metrics(Priority.LOW).timedOut shouldBe 1
            queue.metrics(Priority.NORMAL).timedOut shouldBe 0
            queue.metrics().timedOut shouldBe 2
            queue.metrics().deadLettered shouldBe 2
        }
    }
})
