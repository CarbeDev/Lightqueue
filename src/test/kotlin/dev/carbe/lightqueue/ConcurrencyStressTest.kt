package dev.carbe.lightqueue

import io.kotest.core.spec.style.FunSpec
import io.kotest.assertions.withClue
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.yield
import kotlinx.coroutines.withTimeout
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

private const val STRESS_TIMEOUT_MILLIS = 15_000L

class ConcurrencyStressTest : FunSpec({

    test("concurrent producers and workers process every accepted event exactly once") {
        withTimeout(STRESS_TIMEOUT_MILLIS) {
            repeat(3) { round ->
                val ownerJob = SupervisorJob()
                val queueScope = CoroutineScope(ownerJob + Dispatchers.Default)
                val processed = ConcurrentLinkedQueue<Int>()
                val producerCount = 8
                val eventsPerProducer = 250
                val expected = (0 until producerCount * eventsPerProducer).toList()

                try {
                    val queue = InMemoryQueue.create<Int>(queueScope) {
                        capacity = 32
                        workers = 4
                        overflowStrategy = OverflowStrategy.BACKPRESSURE
                        process { processed.add(it) }
                    }

                    coroutineScope {
                        List(producerCount) { producer ->
                            async(Dispatchers.Default) {
                                repeat(eventsPerProducer) { sequence ->
                                    val event = producer * eventsPerProducer + sequence
                                    queue.enqueue(event) shouldBe EnqueueResult.Enqueued
                                }
                            }
                        }.awaitAll()
                    }

                    queue.stop()

                    processed.toList().shouldContainExactlyInAnyOrder(expected)
                    val metrics = queue.metrics()
                    metrics.enqueued shouldBe expected.size.toLong()
                    metrics.processed shouldBe expected.size.toLong()
                    metrics.assertQuiescentInvariant("round $round")
                } finally {
                    ownerJob.cancelAndJoin()
                }
            }
        }
    }

    test("priority queue preserves per-level accounting under concurrent load") {
        withTimeout(STRESS_TIMEOUT_MILLIS) {
            val ownerJob = SupervisorJob()
            val queueScope = CoroutineScope(ownerJob + Dispatchers.Default)
            val processed = ConcurrentLinkedQueue<Int>()
            val producerCount = 6
            val eventsPerProducer = 300
            val totalEvents = producerCount * eventsPerProducer

            try {
                val queue = PriorityInMemoryQueue.create<Int>(queueScope) {
                    capacity = 16
                    workers = 4
                    overflowStrategy = OverflowStrategy.BACKPRESSURE
                    process { processed.add(it) }
                }

                coroutineScope {
                    List(producerCount) { producer ->
                        async(Dispatchers.Default) {
                            repeat(eventsPerProducer) { sequence ->
                                val event = producer * eventsPerProducer + sequence
                                val priority = Priority.entries[event % Priority.entries.size]
                                queue.enqueue(event, priority) shouldBe EnqueueResult.Enqueued
                            }
                        }
                    }.awaitAll()
                }

                queue.stop()

                processed.toList().shouldContainExactlyInAnyOrder((0 until totalEvents).toList())
                Priority.entries.forEach { priority ->
                    val expectedForLevel =
                        (0 until totalEvents).count {
                            Priority.entries[it % Priority.entries.size] == priority
                        }
                    val metrics = queue.metrics(priority)
                    metrics.enqueued shouldBe expectedForLevel.toLong()
                    metrics.processed shouldBe expectedForLevel.toLong()
                    metrics.assertQuiescentInvariant(priority.name)
                }

                val aggregate = queue.metrics()
                aggregate.enqueued shouldBe totalEvents.toLong()
                aggregate.processed shouldBe totalEvents.toLong()
                aggregate.assertQuiescentInvariant("aggregate")
            } finally {
                ownerJob.cancelAndJoin()
            }
        }
    }

    test("concurrent stop and tryEnqueue account for every outcome without stranding events") {
        withTimeout(STRESS_TIMEOUT_MILLIS) {
            val ownerJob = SupervisorJob()
            val queueScope = CoroutineScope(ownerJob + Dispatchers.Default)
            val producerCount = 8
            val attemptsPerProducer = 1_000
            val ready = AtomicInteger()
            val allProducersReady = CompletableDeferred<Unit>()
            val raceGate = CompletableDeferred<Unit>()
            val accepted = AtomicLong()
            val rejected = AtomicLong()
            val closed = AtomicLong()

            try {
                val queue = InMemoryQueue.create<Int>(queueScope) {
                    capacity = 32
                    workers = 4
                    overflowStrategy = OverflowStrategy.REJECT
                    process { delay(1) }
                }

                coroutineScope {
                    val producers = List(producerCount) { producer ->
                        async(Dispatchers.Default) {
                            // Ensure every producer has started and attempted at least one enqueue
                            // before stop() and the remaining enqueue calls race each other.
                            queue.tryEnqueue(producer * attemptsPerProducer)
                                .record(accepted, rejected, closed)
                            if (ready.incrementAndGet() == producerCount) {
                                allProducersReady.complete(Unit)
                            }
                            raceGate.await()

                            repeat(attemptsPerProducer - 1) { sequence ->
                                queue.tryEnqueue(producer * attemptsPerProducer + sequence + 1)
                                    .record(accepted, rejected, closed)
                                if (sequence % 32 == 0) yield()
                            }
                        }
                    }

                    allProducersReady.await()
                    val stoppers = List(4) {
                        async(Dispatchers.Default) {
                            raceGate.await()
                            queue.stop()
                        }
                    }

                    raceGate.complete(Unit)
                    producers.awaitAll()
                    stoppers.awaitAll()
                }

                accepted.get() shouldBeGreaterThan 0L
                closed.get() shouldBeGreaterThan 0L

                val metrics = queue.metrics()
                metrics.enqueued shouldBe accepted.get()
                metrics.rejected shouldBe rejected.get()
                metrics.wouldBlock shouldBe 0
                metrics.assertQuiescentInvariant("stop race")
            } finally {
                ownerJob.cancelAndJoin()
            }
        }
    }

    test("scope cancellation under backpressure drops every accepted unfinished event") {
        withTimeout(STRESS_TIMEOUT_MILLIS) {
            val ownerJob = SupervisorJob()
            val queueScope = CoroutineScope(ownerJob + Dispatchers.Default)
            val accepted = AtomicLong()
            val closed = AtomicLong()
            val droppedCallbacks = AtomicLong()
            val enoughAccepted = CompletableDeferred<Unit>()
            val producerCount = 8

            try {
                val queue = InMemoryQueue.create<Int>(queueScope) {
                    capacity = 32
                    workers = 4
                    overflowStrategy = OverflowStrategy.BACKPRESSURE
                    process { awaitCancellation() }
                    onDropped = { droppedCallbacks.incrementAndGet() }
                }

                coroutineScope {
                    val jobs = List(producerCount) { producer ->
                        async(Dispatchers.Default) {
                            repeat(500) { sequence ->
                                val event = producer * 500 + sequence
                                when (queue.enqueue(event)) {
                                    EnqueueResult.Enqueued -> {
                                        if (accepted.incrementAndGet() >= 32) {
                                            enoughAccepted.complete(Unit)
                                        }
                                    }

                                    EnqueueResult.Closed -> {
                                        closed.incrementAndGet()
                                        return@async
                                    }

                                    EnqueueResult.Rejected ->
                                        error("BACKPRESSURE enqueue must suspend instead of rejecting")
                                }
                            }
                        }
                    }

                    enoughAccepted.await()
                    ownerJob.cancelAndJoin()
                    jobs.awaitAll()
                }

                closed.get() shouldBeGreaterThan 0L

                // Completion callbacks can still be finishing the abort drain immediately
                // after the owner Job joins. Poll the public snapshot until every accepted
                // event reaches a terminal state; a real leak fails via the outer timeout.
                val metrics = withTimeout(5_000) {
                    while (true) {
                        val snapshot = queue.metrics()
                        val accountedFor =
                            snapshot.processed + snapshot.deadLettered + snapshot.dropped +
                                snapshot.inFlight + snapshot.depth
                        if (
                            snapshot.depth == 0L &&
                            snapshot.inFlight == 0L &&
                            snapshot.enqueued == accountedFor &&
                            droppedCallbacks.get() == snapshot.dropped
                        ) {
                            return@withTimeout snapshot
                        }
                        yield()
                    }
                    error("unreachable")
                }
                withClue("accepted=${accepted.get()}, closed=${closed.get()}, metrics=$metrics") {
                    metrics.enqueued shouldBe accepted.get()
                    metrics.processed shouldBe 0
                    metrics.dropped shouldBe accepted.get()
                    droppedCallbacks.get() shouldBe metrics.dropped
                    metrics.assertQuiescentInvariant("scope cancellation")
                }
            } finally {
                ownerJob.cancelAndJoin()
            }
        }
    }
})

private fun EnqueueResult.record(
    accepted: AtomicLong,
    rejected: AtomicLong,
    closed: AtomicLong,
) {
    when (this) {
        EnqueueResult.Enqueued -> accepted.incrementAndGet()
        EnqueueResult.Rejected -> rejected.incrementAndGet()
        EnqueueResult.Closed -> closed.incrementAndGet()
    }
}

private fun QueueMetrics.assertQuiescentInvariant(context: String) {
    withClue(context) {
        depth shouldBe 0L
        inFlight shouldBe 0L
        enqueued shouldBe processed + deadLettered + dropped + inFlight + depth
    }
}
