# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.3.0] - 2026-06-23

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

* RabbitMQ-native retry mechanism
* Dedicated retry queues (`<queue>.retry`)
* Retry count tracking via `x-retry-count`
* Fixed-delay retry strategy
* `retries` subscription option
* `retryDelay` subscription option
* `backoffStrategy` subscription option
* `RetryError` with queue name, retry count, and original error context
* Unit tests for retry handling
* Integration tests for retry workflows

### Improved

* Consumer resilience during temporary failures
* Reliability of message processing across application restarts

---

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
