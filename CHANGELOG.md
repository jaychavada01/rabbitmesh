# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-06-23

### Added

- Dead Letter Queue (DLQ) support — exhausted messages are routed to a DLQ instead of being discarded
- `DLQOptions` interface (`enabled`, `queueName`) on `SubscribeOptions`
- `DLQHandler` class — asserts DLQ, publishes DLQ messages with metadata, never crashes consumer
- DLQ message schema: `payload`, `error`, `retryCount`, `failedAt`, `originalQueue`
- Custom DLQ queue names via `dlq.queueName` (default: `<queue>.dlq`)
- Integration tests for DLQ (6 cases)
- `DLQHandler` and `DLQOptions` exported from package root

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

- `RabbitMesh` — main facade class with `connect()`, `disconnect()`, `publish()`, `subscribe()`
- `ConnectionManager` — AMQP connection and channel lifecycle management
- `Publisher` — JSON-serialized, persistent message publishing with auto queue assertion
- `Subscriber` — JSON-deserialized message consumption with ack/nack handling
- Auto-reconnect with configurable interval and max attempts
- Custom errors: `ConnectionError`, `PublishError`, `SubscribeError`, `SerializationError`
- Internal `Logger` utility (debug / info / warn / error, writes to stderr)
- Full TypeScript strict-mode support with generics on `publish<T>` and `subscribe<T>`
- ESM + CommonJS dual build via tsup
- Vitest test suite: 23 tests (unit + integration), amqplib fully mocked
- GitHub Actions CI workflow

[0.2.0]: https://github.com/your-org/rabbitmash/releases/tag/v0.2.0
[0.1.0]: https://github.com/your-org/rabbitmash/releases/tag/v0.1.0
