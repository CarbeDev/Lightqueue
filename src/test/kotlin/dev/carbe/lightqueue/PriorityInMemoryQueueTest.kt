package dev.carbe.lightqueue

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue

class PriorityInMemoryQueueTest : FunSpec({

    test("strict priority: HIGH drains before NORMAL before LOW under load") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                process { processed.add(it) }
            }

            // Enqueue everything before the single worker gets scheduled (the producer never
            // crosses a suspension point), so the buffers are fully populated before any
            // draining starts — this is what makes the ordering deterministic.
            queue.enqueue(10, Priority.LOW)
            queue.enqueue(11, Priority.LOW)
            queue.enqueue(20, Priority.HIGH)
            queue.enqueue(21, Priority.HIGH)
            queue.enqueue(30, Priority.NORMAL)
            queue.enqueue(31, Priority.NORMAL)
            processed shouldBe emptyList()

            queue.stop()

            processed shouldContainExactly listOf(20, 21, 30, 31, 10, 11)
        }
    }

    test("events at the same priority come out in enqueue order (FIFO per level)") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                process { processed.add(it) }
            }

            queue.enqueue(1, Priority.NORMAL)
            queue.enqueue(2, Priority.NORMAL)
            queue.enqueue(3, Priority.NORMAL)
            queue.stop()

            processed shouldContainExactly listOf(1, 2, 3)
        }
    }

    test("default priority is NORMAL") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                process { processed.add(it) }
            }

            queue.enqueue(1)
            queue.stop()

            processed shouldContainExactly listOf(1)
            queue.metrics(Priority.NORMAL).enqueued shouldBe 1
            queue.metrics(Priority.HIGH).enqueued shouldBe 0
            queue.metrics(Priority.LOW).enqueued shouldBe 0
        }
    }

    test("nullable events are processed rather than mistaken for a closed select clause") {
        runTest {
            val processed = mutableListOf<String?>()
            val queue = PriorityInMemoryQueue.create<String?>(backgroundScope) {
                process { processed.add(it) }
            }

            queue.enqueue(null, Priority.HIGH)
            queue.stop()

            processed shouldContainExactly listOf(null)
            queue.metrics(Priority.HIGH).processed shouldBe 1
        }
    }

    test("each priority level has its own capacity") {
        runTest {
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 2
                workers = 0
                allowNoWorkers = true
                overflowStrategy = OverflowStrategy.REJECT
            }

            // Fill LOW to capacity.
            queue.enqueue(1, Priority.LOW) shouldBe EnqueueResult.Enqueued
            queue.enqueue(2, Priority.LOW) shouldBe EnqueueResult.Enqueued
            queue.enqueue(3, Priority.LOW) shouldBe EnqueueResult.Rejected

            // HIGH's buffer is independent and still empty.
            queue.enqueue(4, Priority.HIGH) shouldBe EnqueueResult.Enqueued
            queue.enqueue(5, Priority.HIGH) shouldBe EnqueueResult.Enqueued

            queue.metrics(Priority.LOW).depth shouldBe 2
            queue.metrics(Priority.LOW).rejected shouldBe 1
            queue.metrics(Priority.HIGH).depth shouldBe 2
            queue.metrics(Priority.HIGH).rejected shouldBe 0
        }
    }

    test("EVICT_OLDEST on one level does not affect another level") {
        runTest {
            val droppedEvents = mutableListOf<Int>()
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                process {}
                capacity = 2
                workers = 0
                allowNoWorkers = true
                overflowStrategy = OverflowStrategy.EVICT_OLDEST
                onDropped = { droppedEvents.add(it) }
            }

            queue.enqueue(1, Priority.LOW)
            queue.enqueue(2, Priority.LOW)
            // Evicts 1 from LOW.
            queue.enqueue(3, Priority.LOW) shouldBe EnqueueResult.Enqueued

            // HIGH is untouched.
            queue.enqueue(100, Priority.HIGH) shouldBe EnqueueResult.Enqueued

            droppedEvents shouldContainExactly listOf(1)
            queue.metrics(Priority.LOW).dropped shouldBe 1
            queue.metrics(Priority.LOW).depth shouldBe 2 // 2 and 3 still buffered
            queue.metrics(Priority.HIGH).dropped shouldBe 0
            queue.metrics(Priority.HIGH).depth shouldBe 1
        }
    }

    test("aggregate metrics sum per-level counters and satisfy the global invariant") {
        runTest {
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                process {}
            }

            queue.enqueue(1, Priority.HIGH)
            queue.enqueue(2, Priority.HIGH)
            queue.enqueue(3, Priority.NORMAL)
            queue.enqueue(4, Priority.LOW)
            queue.stop()

            val aggregate = queue.metrics()
            aggregate.enqueued shouldBe 4
            aggregate.processed shouldBe 4
            aggregate.enqueued shouldBe
                aggregate.processed + aggregate.deadLettered + aggregate.dropped +
                aggregate.inFlight + aggregate.depth

            val high = queue.metrics(Priority.HIGH)
            high.enqueued shouldBe 2
            high.processed shouldBe 2

            val normal = queue.metrics(Priority.NORMAL)
            normal.enqueued shouldBe 1
            normal.processed shouldBe 1

            val low = queue.metrics(Priority.LOW)
            low.enqueued shouldBe 1
            low.processed shouldBe 1
        }
    }

    test("stop drains all levels before returning") {
        runTest {
            val processed = mutableListOf<Int>()
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                process { processed.add(it) }
            }

            queue.enqueue(1, Priority.HIGH)
            queue.enqueue(2, Priority.NORMAL)
            queue.enqueue(3, Priority.LOW)
            // Nothing processed yet: the worker has not been scheduled
            // (the producer never crossed a suspension point).
            processed shouldBe emptyList()

            queue.stop()

            processed.toSet() shouldBe setOf(1, 2, 3)
            queue.metrics().depth shouldBe 0
            queue.metrics().inFlight shouldBe 0
        }
    }

    test("cancelling the worker scope closes every level and drops buffered events") {
        runTest {
            val droppedEvents = mutableListOf<Int>()
            val ownerJob = Job()
            val ownerScope = CoroutineScope(backgroundScope.coroutineContext + ownerJob)
            val queue = PriorityInMemoryQueue.create<Int>(ownerScope) {
                process {}
                onDropped = { droppedEvents.add(it) }
            }

            queue.enqueue(1, Priority.HIGH)
            queue.enqueue(2, Priority.NORMAL)
            queue.enqueue(3, Priority.LOW)
            ownerJob.cancel()
            testScheduler.runCurrent()

            Priority.entries.forEach { priority ->
                queue.tryEnqueue(4, priority) shouldBe EnqueueResult.Closed
            }
            droppedEvents.toSet() shouldBe setOf(1, 2, 3)
            val metrics = queue.metrics()
            metrics.enqueued shouldBe 3
            metrics.dropped shouldBe 3
            metrics.depth shouldBe 0
            metrics.inFlight shouldBe 0
        }
    }

    test("an always-failing event is dead-lettered through the shared EventProcessor") {
        runTest {
            var invocations = 0
            val deadLettered = mutableListOf<Int>()
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
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

            queue.enqueue(42, Priority.HIGH)
            queue.stop()

            invocations shouldBe 3 // maxAttempts counts total executions, not retries
            deadLettered shouldContainExactly listOf(42)
            queue.metrics(Priority.HIGH).deadLettered shouldBe 1
            queue.metrics(Priority.HIGH).retries shouldBe 2
            queue.metrics(Priority.NORMAL).deadLettered shouldBe 0
            queue.metrics().deadLettered shouldBe 1
        }
    }

    test("workers = 0 is rejected at construction") {
        runTest {
            shouldThrow<IllegalArgumentException> {
                PriorityInMemoryQueue.create<Int>(backgroundScope) {
                    process {}
                    workers = 0
                }
            }
        }
    }

    test("create without process is rejected") {
        runTest {
            shouldThrow<IllegalArgumentException> {
                PriorityInMemoryQueue.create<Int>(backgroundScope) {}
            }
        }
    }

    test("enqueue after stop returns Closed") {
        runTest {
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                process {}
            }
            queue.stop()

            queue.enqueue(1) shouldBe EnqueueResult.Closed
            queue.enqueue(1, Priority.HIGH) shouldBe EnqueueResult.Closed
        }
    }

    test("the configured name is carried into both aggregate and per-level metrics") {
        runTest {
            val named = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                process {}
                name = "webhooks"
            }

            named.metrics().name shouldBe "webhooks"
            named.metrics(Priority.HIGH).name shouldBe "webhooks:HIGH"
            named.metrics(Priority.NORMAL).name shouldBe "webhooks:NORMAL"
            named.metrics(Priority.LOW).name shouldBe "webhooks:LOW"

            val anonymous = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                process {}
            }
            anonymous.metrics().name shouldBe null
            anonymous.metrics(Priority.HIGH).name shouldBe null
        }
    }

    test("multiple workers drain every level and stop leaves the aggregate invariant at quiescence") {
        runTest {
            // process is invoked from several worker coroutines concurrently, so a bare
            // mutableListOf is not safe here (unlike the single-worker tests above).
            val processed = ConcurrentLinkedQueue<Int>()
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                workers = 3
                process { processed.add(it) }
            }

            // A few hundred events spread across all three levels, enqueued before stop().
            val high = (0 until 100).toList()
            val normal = (100 until 250).toList()
            val low = (250 until 400).toList()
            high.forEach { queue.enqueue(it, Priority.HIGH) }
            normal.forEach { queue.enqueue(it, Priority.NORMAL) }
            low.forEach { queue.enqueue(it, Priority.LOW) }

            queue.stop()

            // Every enqueued event was processed exactly once. No ordering assertion across
            // levels: with several workers the strict top-down order is not guaranteed
            // (documented in PriorityInMemoryQueue.nextEvent).
            val all = (high + normal + low)
            processed.size shouldBe all.size
            processed.toSet() shouldBe all.toSet()

            val aggregate = queue.metrics()
            aggregate.enqueued shouldBe all.size
            aggregate.processed shouldBe all.size
            aggregate.inFlight shouldBe 0
            aggregate.depth shouldBe 0
            aggregate.deadLettered shouldBe 0
            aggregate.dropped shouldBe 0
            aggregate.enqueued shouldBe
                aggregate.processed + aggregate.deadLettered + aggregate.dropped +
                aggregate.inFlight + aggregate.depth
        }
    }

    test("BACKPRESSURE enqueue suspends on a full level and resumes once a worker frees room") {
        runTest {
            val processed = ConcurrentLinkedQueue<Int>()
            // The handler holds the worker for 100ms of virtual time on the very first event,
            // keeping the single-slot buffer occupied while we probe the producer's state.
            val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                capacity = 1
                overflowStrategy = OverflowStrategy.BACKPRESSURE
                process { event ->
                    if (event == 1) delay(100)
                    processed.add(event)
                }
            }

            // Event 1 is pulled by the worker, which suspends inside the 100ms handler delay;
            // event 2 then fills the single buffer slot.
            queue.enqueue(1, Priority.NORMAL) shouldBe EnqueueResult.Enqueued
            testScheduler.advanceTimeBy(50) // worker is now parked mid-handler on event 1
            queue.enqueue(2, Priority.NORMAL) shouldBe EnqueueResult.Enqueued

            // Buffer full + the only worker still busy: this enqueue has nowhere to go and must
            // suspend rather than drop or reject.
            val producer = launch { queue.enqueue(3, Priority.NORMAL) }
            testScheduler.advanceTimeBy(40) // still before the handler finishes (90ms < 100ms)
            producer.isActive shouldBe true
            queue.metrics(Priority.NORMAL).dropped shouldBe 0
            queue.metrics(Priority.NORMAL).rejected shouldBe 0

            // Let the handler finish (cross the 100ms mark): the worker drains event 2, freeing
            // room, and the parked enqueue for event 3 resumes.
            testScheduler.advanceTimeBy(100)
            testScheduler.runCurrent()
            producer.isActive shouldBe false

            queue.stop()

            // FIFO within the single NORMAL level, single worker => deterministic order.
            processed.toList() shouldContainExactly listOf(1, 2, 3)
            val metrics = queue.metrics(Priority.NORMAL)
            metrics.enqueued shouldBe 3
            metrics.processed shouldBe 3
            metrics.dropped shouldBe 0
            metrics.rejected shouldBe 0
            metrics.wouldBlock shouldBe 0
            metrics.depth shouldBe 0
            metrics.inFlight shouldBe 0
        }
    }

    test("helper logs stay on the PriorityInMemoryQueue SLF4J category") {
        val slf4jLogger = LoggerFactory.getLogger(PriorityInMemoryQueue::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        slf4jLogger.addAppender(appender)

        try {
            runTest {
                val queue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                    process { throw IllegalStateException("boom") }
                    name = "priority"
                }
                queue.enqueue(1, Priority.HIGH)
                queue.stop()

                val rejectingQueue = PriorityInMemoryQueue.create<Int>(backgroundScope) {
                    process {}
                    name = "priority"
                    capacity = 1
                    workers = 0
                    allowNoWorkers = true
                    overflowStrategy = OverflowStrategy.REJECT
                }
                rejectingQueue.enqueue(1, Priority.LOW)
                rejectingQueue.enqueue(2, Priority.LOW)
                rejectingQueue.stop()
            }

            val messages = appender.list.map { it.formattedMessage }
            messages.any { "Attempt 1/1 failed" in it } shouldBe true
            messages.any { "Buffer full, rejecting event (LOW)" in it } shouldBe true
        } finally {
            slf4jLogger.detachAppender(appender)
            appender.stop()
        }
    }
})
