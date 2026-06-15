package dev.carbe.lightqueue

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class InMemoryQueueTest : FunSpec({

    test("processes messages in order with a single worker") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { processed.add(it) }
            }

            queue.enqueue(1)
            queue.enqueue(2)
            queue.enqueue(3)
            queue.stop()

            processed shouldContainExactly listOf(1, 2, 3)
        }
    }

    test("worker survives a handler exception and keeps processing") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { event ->
                    if (event == 2) throw IllegalStateException("boom")
                    processed.add(event)
                }
            }

            queue.enqueue(1)
            queue.enqueue(2)
            queue.enqueue(3)
            queue.stop()

            processed shouldContainExactly listOf(1, 3)
        }
    }

    test("stop drains the buffer before returning") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { processed.add(it) }
            }

            repeat(10) { queue.enqueue(it) }
            // Nothing processed yet: the worker has not been scheduled
            // (the producer never crossed a suspension point).
            processed shouldBe emptyList()

            queue.stop()

            processed shouldContainExactly (0..9).toList()
        }
    }

    test("enqueue after stop returns Closed") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
            }
            queue.stop()

            queue.enqueue(1) shouldBe EnqueueResult.Closed
        }
    }

    test("enqueue after stop returns Closed with REJECT strategy") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                overflowStrategy = OverflowStrategy.REJECT
            }
            queue.stop()

            queue.enqueue(1) shouldBe EnqueueResult.Closed
        }
    }

    test("BACKPRESSURE suspends the producer when the buffer is full") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 2
                workers = 0 // no consumer: the buffer fills up and stays full
                allowNoWorkers = true
                overflowStrategy = OverflowStrategy.BACKPRESSURE
            }

            queue.enqueue(1)
            queue.enqueue(2)

            val producer = launch { queue.enqueue(3) }
            testScheduler.advanceUntilIdle()

            producer.isActive shouldBe true
            producer.cancel()
        }
    }

    test("EVICT_OLDEST drops the oldest element and notifies onDropped") {
        runTest {
            val droppedEvents = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 2
                workers = 0
                allowNoWorkers = true
                overflowStrategy = OverflowStrategy.EVICT_OLDEST
                onDropped = { droppedEvents.add(it) }
            }

            queue.enqueue(1)
            queue.enqueue(2)
            // The enqueue itself succeeds: the evicted victim is reported via onDropped.
            queue.enqueue(3) shouldBe EnqueueResult.Enqueued

            droppedEvents shouldContainExactly listOf(1)
        }
    }

    test("REJECT returns Rejected when the buffer is full without invoking onDropped") {
        runTest {
            val droppedEvents = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 2
                workers = 0
                allowNoWorkers = true
                overflowStrategy = OverflowStrategy.REJECT
                onDropped = { droppedEvents.add(it) }
            }

            queue.enqueue(1) shouldBe EnqueueResult.Enqueued
            queue.enqueue(2) shouldBe EnqueueResult.Enqueued
            queue.enqueue(3) shouldBe EnqueueResult.Rejected

            droppedEvents shouldBe emptyList()
        }
    }

    test("tryEnqueue returns Rejected when a BACKPRESSURE buffer is full, without invoking onDropped") {
        runTest {
            val droppedEvents = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 2
                workers = 0
                allowNoWorkers = true
                overflowStrategy = OverflowStrategy.BACKPRESSURE
                onDropped = { droppedEvents.add(it) }
            }

            queue.tryEnqueue(1) shouldBe EnqueueResult.Enqueued
            queue.tryEnqueue(2) shouldBe EnqueueResult.Enqueued
            queue.tryEnqueue(3) shouldBe EnqueueResult.Rejected

            droppedEvents shouldBe emptyList()
        }
    }

    test("tryEnqueue rejected event can be retried once a worker frees up room, without ever invoking onDropped") {
        runTest {
            val droppedEvents = mutableListOf<Int>()
            val processed = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { event -> processed.add(event) }
                capacity = 1
                overflowStrategy = OverflowStrategy.REJECT
                onDropped = { droppedEvents.add(it) }
            }

            queue.tryEnqueue(1) shouldBe EnqueueResult.Enqueued
            // The buffer is full: there's no room for a second event yet.
            queue.tryEnqueue(2) shouldBe EnqueueResult.Rejected

            // Yield to let the worker drain event 1, freeing up room in the buffer.
            delay(1)
            queue.tryEnqueue(2) shouldBe EnqueueResult.Enqueued

            queue.stop()

            processed shouldContainExactly listOf(1, 2)
            droppedEvents shouldBe emptyList()
        }
    }

    test("tryEnqueue on EVICT_OLDEST never returns Rejected and evicts the oldest element") {
        runTest {
            val droppedEvents = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 2
                workers = 0
                allowNoWorkers = true
                overflowStrategy = OverflowStrategy.EVICT_OLDEST
                onDropped = { droppedEvents.add(it) }
            }

            queue.tryEnqueue(1) shouldBe EnqueueResult.Enqueued
            queue.tryEnqueue(2) shouldBe EnqueueResult.Enqueued
            queue.tryEnqueue(3) shouldBe EnqueueResult.Enqueued

            droppedEvents shouldContainExactly listOf(1)
        }
    }

    test("tryEnqueue after stop returns Closed") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
            }
            queue.stop()

            queue.tryEnqueue(1) shouldBe EnqueueResult.Closed
        }
    }

    test("multiple workers process all messages") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                workers = 3
                process { processed.add(it) }
            }

            repeat(20) { queue.enqueue(it) }
            queue.stop()

            // Ordering across workers is not guaranteed, only the full set is.
            processed.toSet() shouldBe (0..19).toSet()
        }
    }

    test("handler can suspend without blocking the worker thread") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { event ->
                    delay(100)
                    processed.add(event)
                }
            }

            queue.enqueue(1)
            queue.enqueue(2)
            queue.enqueue(3)
            queue.stop()

            processed shouldContainExactly listOf(1, 2, 3)
            currentTime shouldBe 300 // one worker, 100ms of virtual time per event
        }
    }

    test("NoBackoff retries immediately and the worker survives") {
        runTest {
            val attemptsPerEvent = mutableMapOf<Int, Int>()
            val processed = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { event ->
                    val attempt = attemptsPerEvent.merge(event, 1, Int::plus)!!
                    if (event == 2 && attempt == 1) throw IllegalStateException("first attempt fails")
                    processed.add(event)
                }
                retryPolicy {
                    maxAttempts = 2
                    backoff = Backoff.noBackoff()
                }
            }

            queue.enqueue(1)
            queue.enqueue(2)
            queue.enqueue(3)
            queue.stop()

            processed shouldContainExactly listOf(1, 2, 3)
            currentTime shouldBe 0 // no delay between attempts
        }
    }

    test("exhausted retries deliver the event to onDeadLetter") {
        runTest {
            var invocations = 0
            val deadLettered = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { _ ->
                    invocations++
                    throw IllegalStateException("always fails")
                }
                retryPolicy {
                    maxAttempts = 3
                    backoff = Backoff.noBackoff()
                }
                onDeadLetter = { event, _ -> deadLettered.add(event) }
            }

            queue.enqueue(42)
            queue.stop()

            invocations shouldBe 3 // maxAttempts counts total executions, not retries
            deadLettered shouldContainExactly listOf(42)
        }
    }

    test("failed event without retryPolicy goes to onDeadLetter after a single attempt") {
        runTest {
            var invocations = 0
            val deadLettered = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { _ ->
                    invocations++
                    throw IllegalStateException("always fails")
                }
                onDeadLetter = { event, _ -> deadLettered.add(event) }
            }

            queue.enqueue(42)
            queue.stop()

            invocations shouldBe 1
            deadLettered shouldContainExactly listOf(42)
        }
    }

    test("onDeadLetter receives the cause of the last failed attempt") {
        runTest {
            val deadLetterCauses = mutableListOf<Throwable>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { _ ->
                    throw IllegalStateException("always fails")
                }
                onDeadLetter = { _, cause -> deadLetterCauses.add(cause) }
            }

            queue.enqueue(42)
            queue.stop()

            deadLetterCauses shouldHaveSize 1
            deadLetterCauses[0].shouldBeInstanceOf<IllegalStateException>()
            deadLetterCauses[0].message shouldBe "always fails"
        }
    }

    test("a throwing onDeadLetter does not kill the worker") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { event ->
                    if (event == 1) throw IllegalStateException("always fails")
                    processed.add(event)
                }
                onDeadLetter = { _, _ -> throw IllegalStateException("dead-letter sink unavailable") }
            }

            queue.enqueue(1)
            queue.enqueue(2)
            queue.stop()

            // Event 2 still gets processed even though dead-lettering event 1 blew up.
            processed shouldContainExactly listOf(2)
        }
    }

    test("workers = 0 is rejected at construction") {
        runTest {
            shouldThrow<IllegalArgumentException> {
                InMemoryQueue.create<Int>(backgroundScope) {
                    process {}
                    workers = 0
                }
            }
        }
    }

    test("negative workers is rejected at construction") {
        runTest {
            shouldThrow<IllegalArgumentException> {
                InMemoryQueue.create<Int>(backgroundScope) {
                    process {}
                    workers = -1
                }
            }
        }
    }

    test("create without process is rejected") {
        runTest {
            shouldThrow<IllegalArgumentException> {
                InMemoryQueue.create<Int>(backgroundScope) {}
            }
        }
    }

    test("process cannot be configured twice") {
        runTest {
            shouldThrow<IllegalStateException> {
                InMemoryQueue.create<Int>(backgroundScope) {
                    process {}
                    process {}
                }
            }
        }
    }

    test("retryPolicy cannot be configured twice") {
        runTest {
            shouldThrow<IllegalStateException> {
                InMemoryQueue.create<Int>(backgroundScope) {
                    process {}
                    retryPolicy {
                        maxAttempts = 1
                        backoff = Backoff.noBackoff()
                    }
                    retryPolicy {
                        maxAttempts = 2
                        backoff = Backoff.noBackoff()
                    }
                }
            }
        }
    }

    test("retry delays follow the exponential backoff") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { _ ->
                    throw IllegalStateException("always fails")
                }
                retryPolicy {
                    maxAttempts = 3
                    backoff = Backoff.exponential(100.milliseconds)
                }
            }

            queue.enqueue(1)
            queue.stop()

            // 100ms after the 1st failure, 200ms after the 2nd: 300ms of virtual time.
            currentTime shouldBe 300
        }
    }

    test("delayForAttempt computes the delay for each backoff type") {
        val exponential = RetryPolicy(5, Backoff.exponential(100.milliseconds))
        exponential.delayForAttempt(1) shouldBe 100.milliseconds
        exponential.delayForAttempt(2) shouldBe 200.milliseconds
        exponential.delayForAttempt(3) shouldBe 400.milliseconds

        val linear = RetryPolicy(5, Backoff.linear(100.milliseconds))
        linear.delayForAttempt(1) shouldBe 100.milliseconds
        linear.delayForAttempt(2) shouldBe 200.milliseconds
        linear.delayForAttempt(3) shouldBe 300.milliseconds

        val none = RetryPolicy(5, Backoff.noBackoff())
        none.delayForAttempt(1) shouldBe Duration.ZERO
        none.delayForAttempt(3) shouldBe Duration.ZERO
    }

    test("RetryPolicy rejects maxAttempts < 1 regardless of how it is built") {
        shouldThrow<IllegalArgumentException> {
            RetryPolicy(0, Backoff.noBackoff())
        }
    }

    test("Backoff.exponential rejects non-positive delays") {
        shouldThrow<IllegalArgumentException> {
            Backoff.exponential(Duration.ZERO)
        }
        shouldThrow<IllegalArgumentException> {
            Backoff.exponential((-100).milliseconds)
        }
    }

    test("Backoff.linear rejects non-positive delays") {
        shouldThrow<IllegalArgumentException> {
            Backoff.linear(Duration.ZERO)
        }
        shouldThrow<IllegalArgumentException> {
            Backoff.linear((-100).milliseconds)
        }
    }

    test("retryPolicy without maxAttempts is rejected at construction") {
        runTest {
            shouldThrow<IllegalArgumentException> {
                InMemoryQueue.create<Int>(backgroundScope) {
                    process {}
                    retryPolicy {
                        backoff = Backoff.noBackoff()
                    }
                }
            }
        }
    }

    test("retryPolicy with maxAttempts < 1 is rejected at construction") {
        runTest {
            shouldThrow<IllegalArgumentException> {
                InMemoryQueue.create<Int>(backgroundScope) {
                    process {}
                    retryPolicy {
                        maxAttempts = 0
                        backoff = Backoff.noBackoff()
                    }
                }
            }
        }
    }

    test("metrics reflect enqueued and processed counts after a normal run") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
            }

            repeat(5) { queue.enqueue(it) }
            queue.stop()

            val metrics = queue.metrics()
            metrics.enqueued shouldBe 5
            metrics.processed shouldBe 5
            metrics.depth shouldBe 0
            metrics.inFlight shouldBe 0
            metrics.deadLettered shouldBe 0
            metrics.dropped shouldBe 0
            metrics.rejected shouldBe 0
            metrics.retries shouldBe 0
        }
    }

    test("depth reflects events sitting in the buffer with no worker draining them") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 10
                workers = 0
                allowNoWorkers = true
            }

            repeat(3) { queue.enqueue(it) }

            queue.metrics().depth shouldBe 3
            queue.metrics().enqueued shouldBe 3
            queue.metrics().inFlight shouldBe 0
        }
    }

    test("inFlight is positive while a handler is suspended and back to zero afterwards") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { delay(100) }
            }

            queue.enqueue(1)

            // Let the worker pull the event and suspend inside the 100ms handler delay.
            testScheduler.advanceTimeBy(50)
            queue.metrics().inFlight shouldBe 1
            queue.metrics().depth shouldBe 0

            queue.stop()

            queue.metrics().inFlight shouldBe 0
            queue.metrics().processed shouldBe 1
        }
    }

    test("deadLettered counter increments once retries are exhausted") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { _ -> throw IllegalStateException("always fails") }
                retryPolicy {
                    maxAttempts = 3
                    backoff = Backoff.noBackoff()
                }
            }

            queue.enqueue(42)
            queue.stop()

            queue.metrics().deadLettered shouldBe 1
            queue.metrics().processed shouldBe 0
        }
    }

    test("dropped increments and depth decrements under EVICT_OLDEST, preserving the invariant") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 2
                workers = 0
                allowNoWorkers = true
                overflowStrategy = OverflowStrategy.EVICT_OLDEST
            }

            queue.enqueue(1)
            queue.enqueue(2)
            queue.enqueue(3) // evicts 1

            val metrics = queue.metrics()
            metrics.enqueued shouldBe 3
            metrics.dropped shouldBe 1
            metrics.depth shouldBe 2 // 2 and 3 still buffered
            metrics.rejected shouldBe 0

            // The eviction trap: depth must have been decremented, so the invariant holds.
            metrics.enqueued shouldBe
                metrics.processed + metrics.deadLettered + metrics.dropped + metrics.inFlight + metrics.depth
        }
    }

    test("REJECT counts a full-buffer refusal as rejected, not wouldBlock") {
        runTest {
            val rejectQueue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 2
                workers = 0
                allowNoWorkers = true
                overflowStrategy = OverflowStrategy.REJECT
            }

            rejectQueue.enqueue(1)
            rejectQueue.enqueue(2)
            rejectQueue.enqueue(3) shouldBe EnqueueResult.Rejected

            rejectQueue.metrics().rejected shouldBe 1
            rejectQueue.metrics().wouldBlock shouldBe 0
            rejectQueue.metrics().depth shouldBe 2 // unchanged by the rejection
            rejectQueue.metrics().dropped shouldBe 0
            rejectQueue.metrics().enqueued shouldBe 2
        }
    }

    test("BACKPRESSURE counts a non-blocking tryEnqueue on a full buffer as wouldBlock, not rejected") {
        runTest {
            val backpressureQueue = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 2
                workers = 0
                allowNoWorkers = true
                overflowStrategy = OverflowStrategy.BACKPRESSURE
            }

            backpressureQueue.tryEnqueue(1)
            backpressureQueue.tryEnqueue(2)
            backpressureQueue.tryEnqueue(3) shouldBe EnqueueResult.Rejected

            backpressureQueue.metrics().wouldBlock shouldBe 1
            backpressureQueue.metrics().rejected shouldBe 0 // not a policy rejection
            backpressureQueue.metrics().depth shouldBe 2
            backpressureQueue.metrics().dropped shouldBe 0
        }
    }

    test("retries counter equals attempts beyond the first for a failing-then-succeeding event") {
        runTest {
            var attempt = 0
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { _ ->
                    attempt++
                    if (attempt < 3) throw IllegalStateException("not yet")
                }
                retryPolicy {
                    maxAttempts = 5
                    backoff = Backoff.noBackoff()
                }
            }

            queue.enqueue(1)
            queue.stop()

            // 3 executions total => 2 retries beyond the first.
            queue.metrics().retries shouldBe 2
            queue.metrics().processed shouldBe 1
            queue.metrics().deadLettered shouldBe 0
        }
    }

    test("the global invariant holds across a mixed scenario") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { event ->
                    if (event % 7 == 0) throw IllegalStateException("multiples of 7 fail")
                }
                retryPolicy {
                    maxAttempts = 2
                    backoff = Backoff.noBackoff()
                }
                onDeadLetter = { _, _ -> }
            }

            repeat(20) { queue.enqueue(it) }
            queue.stop()

            val metrics = queue.metrics()
            metrics.enqueued shouldBe 20
            metrics.inFlight shouldBe 0
            metrics.depth shouldBe 0
            metrics.deadLettered shouldBe 3 // 0, 7, 14
            metrics.processed shouldBe 17
            metrics.retries shouldBe 3 // one retry per dead-lettered event

            metrics.enqueued shouldBe
                metrics.processed + metrics.deadLettered + metrics.dropped + metrics.inFlight + metrics.depth
        }
    }

    test("metrics counters stay non-negative gauges and the invariant holds mid-flight") {
        runTest {
            val queue = InMemoryQueue.create<Int>(backgroundScope) {
                process { delay(100) } // each event occupies the single worker for 100ms
                workers = 1
                capacity = 10
            }

            repeat(4) { queue.enqueue(it) }

            // 50ms in: the worker is suspended inside the first event's handler, the rest wait.
            testScheduler.advanceTimeBy(50)
            val midFlight = queue.metrics()
            midFlight.inFlight shouldBe 1
            midFlight.depth shouldBe 3
            midFlight.enqueued shouldBe 4
            midFlight.enqueued shouldBe
                midFlight.processed + midFlight.deadLettered + midFlight.dropped +
                midFlight.inFlight + midFlight.depth

            queue.stop()

            queue.metrics().processed shouldBe 4
            queue.metrics().inFlight shouldBe 0
            queue.metrics().depth shouldBe 0
        }
    }

    test("queue name appears as a prefix in the SLF4J output") {
        // Capture everything the InMemoryQueue logger emits via a logback ListAppender.
        val slf4jLogger = LoggerFactory.getLogger(InMemoryQueue::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        slf4jLogger.addAppender(appender)

        try {
            runTest {
                val queue = InMemoryQueue.create<Int>(backgroundScope) {
                    process {}
                    name = "webhooks"
                }
                queue.enqueue(1)
                queue.stop()
            }

            val messages = appender.list.map { it.formattedMessage }
            messages.any { it.startsWith("[webhooks] ") } shouldBe true
        } finally {
            slf4jLogger.detachAppender(appender)
            appender.stop()
        }
    }

    test("the configured name is carried into the metrics snapshot") {
        runTest {
            val named = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
                name = "webhooks"
            }
            named.metrics().name shouldBe "webhooks"

            val anonymous = InMemoryQueue.create<Int>(backgroundScope) {
                process {}
            }
            anonymous.metrics().name shouldBe null
        }
    }
})
