# rabbitmesh

Production-ready RabbitMQ SDK for Node.js microservices with TypeScript support.

[![CI](https://github.com/your-org/rabbitmash/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/rabbitmash/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/rabbitmash.svg)](https://www.npmjs.com/package/rabbitmash)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

rabbitmesh provides a clean, TypeScript-first API over `amqplib` so you can start publishing and consuming RabbitMQ messages without boilerplate. It handles connection management, auto-reconnect, JSON serialization, message acknowledgment, and RabbitMQ-native retries out of the box.

## Features

- Connect / disconnect lifecycle management
- Publish to any queue — auto-creates durable, persistent queues
- Subscribe with typed handlers — auto-creates queues, handles ack/nack
- **RabbitMQ-native retry mechanism** — survives process and container restarts
- **Dead Letter Queue (DLQ) support** — exhausted messages preserved with full metadata
- Auto-reconnect with configurable interval and max attempts
- Custom error hierarchy (`ConnectionError`, `PublishError`, `SubscribeError`, `SerializationError`, `RetryError`)
- Full TypeScript generics on `publish<T>` and `subscribe<T>`
- ESM + CommonJS dual build — works with any module system
- Zero runtime dependencies beyond `amqplib`

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

| Option                  | Type      | Default  | Description                                    |
| ----------------------- | --------- | -------- | ---------------------------------------------- |
| `url`                   | `string`  | —        | AMQP connection URL (required)                 |
| `reconnect`             | `boolean` | `true`   | Enable auto-reconnect on connection loss       |
| `reconnectInterval`     | `number`  | `5000`   | Milliseconds between reconnect attempts        |
| `reconnectMaxAttempts`  | `number`  | `0`      | Max reconnect attempts; `0` = unlimited        |

```ts
const rabbit = new RabbitMesh({
  url: process.env.RABBITMQ_URL!,
  reconnect: true,
  reconnectInterval: 3_000,
  reconnectMaxAttempts: 10,
});
```

## API Reference

### `new RabbitMesh(config: RabbitMeshConfig)`

Creates a new RabbitMesh instance. Does not connect until `connect()` is called.

### `rabbit.connect(): Promise<void>`

Establishes the AMQP connection and opens a channel. Throws `ConnectionError` on failure.

### `rabbit.disconnect(): Promise<void>`

Gracefully closes the channel and connection. Suppresses reconnect scheduling.

### `rabbit.publish<T>(options: PublishOptions<T>): Promise<void>`

Asserts a durable queue then sends a persistent, JSON-serialized message.  
Throws `SerializationError` if payload cannot be serialized.  
Throws `PublishError` on any other publish failure.

### `rabbit.subscribe<T>(options: SubscribeOptions<T>): Promise<void>`

Asserts a durable queue and begins consuming. Deserializes each message and calls `handler`.  
When `retries > 0`, also asserts a `<queue>.retry` queue for RabbitMQ-native retries.  
Acks on success. On failure, routes to the retry queue (if retries remain) or nacks and throws `RetryError` (if exhausted).  
Throws `SubscribeError` on setup failure.

### Errors

| Class                | Thrown when                                            |
| -------------------- | ------------------------------------------------------ |
| `ConnectionError`    | Connection cannot be established or is lost            |
| `PublishError`       | `publish()` fails for any reason                       |
| `SubscribeError`     | `subscribe()` setup fails                              |
| `SerializationError` | JSON serialize/deserialize fails                       |
| `RetryError`         | Message exhausts all retry attempts                    |

## Roadmap

| Version | Features                        |
| ------- | ------------------------------- |
| v0.1.0  | Connection, Publisher, Subscriber, Auto-reconnect ✅ |
| v0.2.0  | Retry mechanism ✅               |
| v0.3.0  | Dead Letter Queue (DLQ) ✅       |
| v0.4.0  | Delayed messages                |
| v0.5.0  | Middleware system               |
| v0.6.0  | Request/Reply (RPC)             |
| v0.7.0  | Metrics & monitoring            |
| v0.8.0  | OpenTelemetry support           |
| v1.0.0  | Stable production release       |

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Commit your changes following [Conventional Commits](https://www.conventionalcommits.org)
4. Open a pull request

Please ensure `npm run lint`, `npm test`, and `npm run build` all pass before submitting.

## License

[MIT](LICENSE)
