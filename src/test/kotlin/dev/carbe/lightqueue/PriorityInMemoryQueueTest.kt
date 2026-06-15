package dev.carbe.lightqueue

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.test.runTest

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
})
