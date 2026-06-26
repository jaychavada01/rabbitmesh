# rabbitmesh

[![CI](https://github.com/jaychavada01/rabbitmesh/actions/workflows/ci.yml/badge.svg)](https://github.com/jaychavada01/rabbitmesh/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/rabbitmesh.svg)](https://www.npmjs.com/package/rabbitmesh)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-ready RabbitMQ client for Node.js and TypeScript. rabbitmesh handles connection management, message publishing and consuming, retries, and dead-letter queues — so you can focus on your application logic.

---

## Why rabbitmesh?

Working directly with `amqplib` means writing the same boilerplate every time: connection handling, channel setup, reconnect logic, JSON serialization, queue assertions, retry queues, dead-letter routing. rabbitmesh packages all of that into a clean, typed API.

- **No boilerplate** — queues are auto-created on first use
- **No lost messages** — persistent delivery and durable queues by default
- **No manual reconnect** — automatic reconnection built in
- **No silent failures** — exhausted messages are routed to a DLQ, not discarded
- **Full TypeScript support** — generics on every publish and subscribe call

---

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

---

## Quick Start

```ts
import { RabbitMesh } from "rabbitmesh";

const rabbit = new RabbitMesh({ url: "amqp://localhost" });

await rabbit.connect();

await rabbit.publish({
  queue: "orders",
  payload: { orderId: "ord-001", total: 49.99 },
});

await rabbit.subscribe({
  queue: "orders",
  handler: async (payload) => {
    console.log("Received:", payload);
  },
});

process.on("SIGINT", async () => {
  await rabbit.disconnect();
  process.exit(0);
});
```

---

## Publishing Messages

```ts
interface OrderCreated {
  orderId: string;
  total: number;
}

await rabbit.publish<OrderCreated>({
  queue: "orders",
  payload: { orderId: "ord-001", total: 49.99 },
});
```

The queue is created automatically if it does not exist. All messages are sent as durable and persistent.

---

## Consuming Messages

```ts
interface OrderCreated {
  orderId: string;
  total: number;
}

await rabbit.subscribe<OrderCreated>({
  queue: "orders",
  handler: async ({ orderId, total }) => {
    await processOrder(orderId, total);
  },
});
```

Messages are acknowledged on success. If the handler throws, the message is nacked.

---

## Retries

Pass `retries` and `retryDelay` to automatically retry failed messages before giving up.

```ts
await rabbit.subscribe({
  queue: "emails",
  retries: 3,
  retryDelay: 5000, // ms between retries
  handler: async (payload) => {
    await sendEmail(payload);
  },
});
```

On failure, the message is re-queued and redelivered after `retryDelay` milliseconds. After all attempts are exhausted the message is nacked. Retry state survives consumer restarts.

---

## Dead Letter Queues

Enable DLQ routing to preserve messages that exhaust all retry attempts.

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

Exhausted messages are moved to `emails.dlq` and include the original payload, error message, retry count, timestamp, and source queue — ready for inspection or replay.

Use a custom DLQ name if needed:

```ts
dlq: { enabled: true, queueName: "emails.failed" }
```

---

## Configuration

### `new RabbitMesh(config)`

| Option                 | Type      | Default | Description                                        |
| ---------------------- | --------- | ------- | -------------------------------------------------- |
| `url`                  | `string`  | —       | AMQP connection URL *(required)*                   |
| `reconnect`            | `boolean` | `true`  | Automatically reconnect on connection loss         |
| `reconnectInterval`    | `number`  | `5000`  | Milliseconds to wait between reconnect attempts    |
| `reconnectMaxAttempts` | `number`  | `0`     | Max reconnect attempts. `0` = unlimited            |

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

## Error Handling

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
3. Commit following [Conventional Commits](https://www.conventionalcommits.org)
4. Open a pull request

Run `npm run lint`, `npm test`, and `npm run build` before submitting.

--- -->

## License

[MIT](LICENSE)
