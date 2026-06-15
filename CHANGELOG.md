# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-06-15

### Added

- Pollable `QueueMetrics` snapshot exposed via `InMemoryQueue.metrics()`, backed by
  `AtomicLong` counters with no new dependencies: `depth`, `inFlight`, `enqueued`,
  `processed`, `deadLettered`, `dropped`, `rejected`, `wouldBlock` and `retries`. The
  invariant `enqueued = processed + deadLettered + dropped + inFlight + depth` holds at
  quiescence.
- `wouldBlock` counter, distinct from `rejected`: a non-blocking `tryEnqueue` on a
  `BACKPRESSURE` queue that finds no room is counted as a would-block rather than a policy
  rejection, keeping `rejected` a clean "events refused by policy" signal.
- Optional queue `name`, prefixed onto SLF4J log statements (e.g. `[webhooks] ...`) and
  carried into the `QueueMetrics` snapshot as an export tag.

### Changed

- Regrouped types by concept: `RetryPolicy`/`Backoff` moved into `Retry.kt`,
  `OverflowStrategy`/`EnqueueResult` into `Overflow.kt`. No behaviour change.

## [0.1.0]

### Added

- Initial release: in-memory async queue for Kotlin coroutines on top of `Channel`, with a
  worker pool, retry with configurable backoff, a dead-letter callback, and explicit
  overflow strategies (`BACKPRESSURE`, `REJECT`, `EVICT_OLDEST`) — all configured through a
  small DSL.

[0.2.0]: https://github.com/CarbeDev/Lightqueue/releases/tag/v0.2.0
[0.1.0]: https://github.com/CarbeDev/Lightqueue/releases/tag/v0.1.0
