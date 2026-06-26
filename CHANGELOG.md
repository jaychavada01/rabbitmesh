# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.4.0] - 2026-06-26

### Added

* Delayed message publishing via `publish({ delay })` option
* Automatic delay queue creation (`<queue>.delay.<ms>`) with TTL and dead-letter routing
* `DelayHandler` — dedicated class for delay queue lifecycle and publish
* `DelayError` — thrown for invalid delay values or delay queue failures
* Delay queue reuse — asserts idempotently, no duplicate queues
* Delay validation — rejects `delay <= 0`, `NaN`, and `Infinity` before touching RabbitMQ
* Delayed messages work with retries and DLQ routing
* Delayed messages survive RabbitMQ and consumer restarts (durable + persistent)
* Integration tests covering all 8 delay scenarios
* Unit tests for `DelayHandler` and updated `Publisher` tests

### Compatibility

* No breaking changes
* Existing v0.3.0 implementations continue to work without modification

---

## [0.4.0] - 2026-06-26

### Added

* Dead Letter Queue (DLQ) support for permanently failed messages
* `DLQOptions` interface with:

  * `enabled`
  * `queueName`
* `DLQHandler` for managing DLQ queue creation and message routing
* Custom DLQ queue names via `dlq.queueName`
* DLQ message metadata:

  * `payload`
  * `error`
  * `retryCount`
  * `failedAt`
  * `originalQueue`
* Automatic routing of exhausted messages to DLQ
* Integration tests covering:

  * DLQ queue creation
  * Message routing
  * Metadata preservation
  * Custom DLQ names
  * Consumer stability
  * Backward compatibility

### Improved

* Consumer stability after retry exhaustion
* Failure visibility through structured DLQ payloads
* Retry and DLQ workflow integration

### Compatibility

* No breaking changes
* Existing v0.2.0 implementations continue to work without modification

---

## [0.2.0] - 2026-06-22

### Added

- RabbitMQ-native retry mechanism — survives consumer/container/server restarts
- Dedicated `<queue>.retry` queue with `x-message-ttl` and dead-letter routing
- `x-retry-count` header tracking — incremented on every retry attempt
- Fixed-delay retry strategy (`backoffStrategy: "fixed"`)
- `retries`, `retryDelay`, and `backoffStrategy` options on `SubscribeOptions`
- `RetryError` — preserves original error, queue name, and retry count
- Unit tests for `RetryHandler` (8 cases)
- Integration tests for retry scenarios (9 cases)

## [0.1.0] - 2026-06-22

### Added

* `RabbitMesh` facade class
* Connection management with auto reconnect
* Message publishing with automatic queue creation
* Message consumption with acknowledgment handling
* Persistent messages
* Durable queues
* TypeScript-first API with generics
* Custom error hierarchy:

  * `ConnectionError`
  * `PublishError`
  * `SubscribeError`
  * `SerializationError`
* ESM and CommonJS builds
* Vitest test suite
* GitHub Actions CI pipeline

---

[0.3.0]: https://github.com/jaychavada01/rabbitmesh/releases/tag/v0.3.0
[0.2.0]: https://github.com/jaychavada01/rabbitmesh/releases/tag/v0.2.0
[0.4.0]: https://github.com/jaychavada01/rabbitmesh/releases/tag/v0.4.0
