package dev.carbe.lightqueue

/**
 * Priority levels for [PriorityInMemoryQueue].
 *
 * Declaration order *is* priority order: [HIGH] is drained before [NORMAL], which is
 * drained before [LOW]. [PriorityInMemoryQueue]'s selection logic iterates
 * [Priority.entries] in this declared order, so reordering these constants would change
 * the queue's behaviour.
 */
enum class Priority { HIGH, NORMAL, LOW }
