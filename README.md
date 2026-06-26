# rabbitmesh

Production-ready RabbitMQ SDK for Node.js microservices with TypeScript support.

[![CI](https://github.com/your-org/rabbitmash/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/rabbitmash/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/rabbitmash.svg)](https://www.npmjs.com/package/rabbitmash)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

rabbitmesh provides a clean, TypeScript-first API over `amqplib` so you can start publishing and consuming RabbitMQ messages without boilerplate. It handles connection management, auto-reconnect, JSON serialization, message acknowledgment, and RabbitMQ-native retries out of the box.

## Features

- Durable queues with persistent messages
- Auto-reconnect with configurable interval and retry cap
- RabbitMQ-native retry mechanism
- Dead Letter Queue (DLQ) routing for exhausted messages
- **Delayed message publishing** — schedule messages without manual RabbitMQ configuration
- JSON serialization and deserialization
- Typed payloads with generics
- Consumer stability — exceptions never crash the consumer loop
- ESM and CommonJS builds
- Custom error types for precise error handling

---

## Installation

```bash
npm install rabbitmesh
```

Requires Node.js ≥ 20 and a running RabbitMQ instance.

## Quick Start

```ts
import { RabbitMesh } from "rabbitmesh";

const rabbit = new RabbitMesh({ url: "amqp://localhost" });

await rabbit.connect();

// Publish
await rabbit.publish({
  queue: "notifications",
  payload: { userId: 1, message: "Welcome!" },
});

// Subscribe
await rabbit.subscribe({
  queue: "notifications",
  handler: async (payload) => {
    console.log(payload);
  },
});

// Graceful shutdown
process.on("SIGINT", async () => {
  await rabbit.disconnect();
  process.exit(0);
});
```

## Publisher Example

```ts
interface OrderCreated {
  orderId: string;
  total: number;
}

await rabbit.publish<OrderCreated>({
  queue: "order.created",
  payload: { orderId: "ord-001", total: 49.99 },
});
```

## Subscriber Example

```ts
interface OrderCreated {
  orderId: string;
  total: number;
}

await rabbit.subscribe<OrderCreated>({
  queue: "order.created",
  handler: async ({ orderId, total }) => {
    console.log(`Processing order ${orderId} for $${total}`);
  },
});
```

## Retry Mechanism

RabbitMesh supports RabbitMQ-native retries via a dedicated retry queue. Retries survive consumer crashes, application restarts, container restarts, and server restarts — no in-process timers or counters are used.

### Basic usage

```ts
await rabbit.subscribe({
  queue: "emails",
  retries: 3,
  retryDelay: 5000,
  handler: async (payload) => {
    await sendEmail(payload);
  },
});
```

When `retries > 0`, RabbitMesh automatically creates an `emails.retry` queue and wires all retry routing.

### Retry flow

**Success path:**
```
Main Queue → Consumer → ACK
```

**Failure path (retries remaining):**
```
Main Queue → Consumer (throws) → emails.retry (waits retryDelay ms) → Main Queue → Consumer
```

**Failure path (max retries reached):**
```
Main Queue → Consumer (throws) → NACK (no requeue) → rejected
```

### Retry queue naming

For a queue named `emails`, the retry queue is automatically named `emails.retry`.

The retry queue is configured with:

```js
{
  "x-message-ttl": retryDelay,          // wait before returning to main queue
  "x-dead-letter-exchange": "",          // default exchange
  "x-dead-letter-routing-key": "emails"  // return to main queue after TTL
}
```

### Retry headers

The current retry count is stored in the message header `x-retry-count` and incremented on every retry attempt:

```js
headers: { "x-retry-count": 2 }
```

### Subscribe options for retry

| Option            | Type               | Default | Description                                   |
| ----------------- | ------------------ | ------- | --------------------------------------------- |
| `retries`         | `number`           | `0`     | Max retry attempts. `0` disables retries.     |
| `retryDelay`      | `number`           | `5000`  | Milliseconds to wait before each retry.       |
| `backoffStrategy` | `"fixed"`          | —       | Delay strategy. Only `"fixed"` in v0.2.0.     |

### RetryError

When all retries are exhausted, a `RetryError` is thrown with the following shape:

```ts
import { RetryError } from "rabbitmesh";

try {
  // ...
} catch (err) {
  if (err instanceof RetryError) {
    console.log(err.queue);       // "emails"
    console.log(err.retryCount);  // 3
    console.log(err.cause);       // original error
  }
}
```

## Dead Letter Queue (DLQ)

When a message exhausts all retry attempts, it is moved to a Dead Letter Queue instead of being silently discarded. The consumer continues running after every DLQ routing.

### Architecture

```
emails
  ↓
Consumer (throws)
  ↓ retry attempt 1
emails.retry ──TTL──► emails
  ↓
Consumer (throws)
  ↓ retry attempt 2
emails.retry ──TTL──► emails
  ↓
Consumer (throws)
  ↓ retries exhausted
emails.dlq
```

### Basic usage

```ts
await rabbit.subscribe({
  queue: "emails",
  retries: 3,
  retryDelay: 5000,
  dlq: { enabled: true },
  handler: async (payload) => {
    await sendEmail(payload);
  },
});
```

RabbitMesh automatically creates `emails`, `emails.retry`, and `emails.dlq`.

### Custom DLQ name

```ts
await rabbit.subscribe({
  queue: "emails",
  retries: 3,
  retryDelay: 5000,
  dlq: { enabled: true, queueName: "emails.dead" },
  handler,
});
```

### Queue naming rules

| Queue         | Default name    | Custom via              |
| ------------- | --------------- | ----------------------- |
| Main queue    | `emails`        | `queue`                 |
| Retry queue   | `emails.retry`  | —                       |
| Dead letter   | `emails.dlq`    | `dlq.queueName`         |

### DLQ message schema

Every message written to the DLQ includes:

```json
{
  "payload": { "email": "user@example.com" },
  "error": "SMTP timeout",
  "retryCount": 3,
  "failedAt": "2026-06-23T10:30:00.000Z",
  "originalQueue": "emails"
}
```

### DLQ disabled (v0.2.0 behavior)

Omitting `dlq` or setting `dlq.enabled: false` preserves the original behavior — exhausted messages are nacked with no requeue.

### Migration from v0.2.0

No breaking changes. All existing `subscribe()` calls work without modification.

To add DLQ support, add `dlq: { enabled: true }` to any subscription:

```diff
 await rabbit.subscribe({
   queue: "emails",
   retries: 3,
   retryDelay: 5000,
+  dlq: { enabled: true },
   handler,
 });
```

## Configuration

### `new RabbitMesh(config)`

| Option                 | Type      | Default | Description                                        |
| ---------------------- | --------- | ------- | -------------------------------------------------- |
| `url`                  | `string`  | —       | AMQP connection URL *(required)*                   |
| `reconnect`            | `boolean` | `true`  | Automatically reconnect on connection loss         |
| `reconnectInterval`    | `number`  | `5000`  | Milliseconds to wait between reconnect attempts    |
| `reconnectMaxAttempts` | `number`  | `0`     | Max reconnect attempts. `0` = unlimited            |

### `publish(options)`

| Option    | Type     | Default     | Description                                            |
| --------- | -------- | ----------- | ------------------------------------------------------ |
| `queue`   | `string` | —           | Target queue name *(required)*                         |
| `payload` | `T`      | —           | Message payload — JSON-serialized *(required)*         |
| `delay`   | `number` | `undefined` | Milliseconds before delivery. Omit for immediate send  |

### `subscribe(options)`

| Option       | Type       | Default | Description                                    |
| ------------ | ---------- | ------- | ---------------------------------------------- |
| `queue`      | `string`   | —       | Queue name *(required)*                        |
| `handler`    | `function` | —       | Async message handler *(required)*             |
| `retries`    | `number`   | `0`     | Max retry attempts. `0` disables retries       |
| `retryDelay` | `number`   | `5000`  | Milliseconds between retry attempts            |
| `dlq.enabled`    | `boolean`  | `false` | Route exhausted messages to a DLQ          |
| `dlq.queueName`  | `string`   | `<queue>.dlq` | Custom DLQ queue name                |

---

## Delayed Messages

Publish a message now and have it delivered to the consumer after a specified delay. rabbitmesh automatically creates and manages the underlying delay infrastructure — no manual RabbitMQ configuration required.

```ts
await rabbit.publish({
  queue: "emails",
  payload: { userId: 42 },
  delay: 60000, // deliver after 60 seconds
});
```

The `delay` value is in milliseconds and must be a finite positive integer greater than 0. Omitting `delay` (or leaving it undefined) publishes immediately — no change from v0.3.0.

### How it works

When `delay` is specified, rabbitmesh creates a dedicated delay queue named `<queue>.delay.<ms>` with a TTL equal to the delay value and dead-letter routing back to the original queue. Once the TTL expires, RabbitMQ automatically delivers the message to the consumer.

```txt
publish({ queue: "emails", delay: 60000 })
    ↓
emails.delay.60000   (TTL = 60000ms, durable, persistent)
    ↓ TTL expires
emails
    ↓
Consumer
```

Delay queues are:
- Created automatically on first use
- Reused on subsequent publishes with the same queue and delay
- Durable and persistent — messages survive RabbitMQ and consumer restarts

### Multiple delays

Each unique delay value maps to a distinct delay queue:

```ts
// Creates emails.delay.5000
await rabbit.publish({ queue: "emails", payload: p1, delay: 5000 });

// Creates emails.delay.60000
await rabbit.publish({ queue: "emails", payload: p2, delay: 60000 });
```

### Delay with retries and DLQ

Delayed messages pass through the normal consumer path after delivery, so retries and DLQ routing work exactly as they do for immediate messages:

```ts
await rabbit.subscribe({
  queue: "emails",
  retries: 3,
  retryDelay: 5000,
  dlq: { enabled: true },
  handler: async (payload) => {
    await sendEmail(payload);
  },
});

// This message will be delivered after 2 minutes, then retried up to 3 times on failure
await rabbit.publish({
  queue: "emails",
  payload: { to: "user@example.com" },
  delay: 120000,
});
```

---

## Error Handling

rabbitmesh exports typed errors for every failure mode.

```ts
import {
  ConnectionError,
  PublishError,
  SubscribeError,
  SerializationError,
  RetryError,
  DelayError,
} from "rabbitmesh";
```

| Error                | When it is thrown                                      |
| -------------------- | ------------------------------------------------------ |
| `ConnectionError`    | Connection cannot be established or is unexpectedly lost |
| `PublishError`       | A `publish()` call fails                               |
| `SubscribeError`     | A `subscribe()` call fails during setup                |
| `SerializationError` | Payload cannot be serialized or deserialized as JSON   |
| `RetryError`         | A message exhausts all configured retry attempts       |
| `DelayError`         | `delay` is invalid, or delay queue setup/publish fails |

`RetryError` carries `queue`, `retryCount`, and `cause` for programmatic handling.

---

## Use Cases

- **Microservices** — decouple services with reliable async messaging
- **Event-driven systems** — publish domain events and fan out to multiple consumers
- **Background jobs** — process tasks out-of-band with retry and failure handling
- **Notification systems** — deliver emails, push notifications, and webhooks reliably
- **Payment workflows** — ensure critical messages are never silently dropped
- **Queue-based pipelines** — build multi-stage processing with guaranteed delivery

---

## Production Readiness

rabbitmesh is built for production workloads:

- **Durable queues and persistent messages** survive broker restarts
- **Auto-reconnect** keeps consumers alive through transient network failures
- **RabbitMQ-native retries** survive application and container restarts — no in-process state
- **Dead Letter Queues** ensure no message is silently discarded after repeated failures
- **Consumer stability guarantee** — uncaught handler errors are caught and logged; the consumer loop never exits

<!-- ---

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Commit your changes following [Conventional Commits](https://www.conventionalcommits.org)
4. Open a pull request

Please ensure `npm run lint`, `npm test`, and `npm run build` all pass before submitting.

## License

[MIT](LICENSE)
