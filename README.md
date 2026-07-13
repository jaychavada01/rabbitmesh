# rabbitmesh

[![CI](https://github.com/jaychavada01/rabbitmesh/actions/workflows/ci.yml/badge.svg)](https://github.com/jaychavada01/rabbitmesh/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/rabbitmesh.svg)](https://www.npmjs.com/package/rabbitmesh)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-ready RabbitMQ client for Node.js and TypeScript. rabbitmesh handles connection management, message publishing and consuming, exchanges, retries, and dead-letter queues — so you can focus on your application logic.

---

## Why rabbitmesh?

Working directly with `amqplib` means writing the same boilerplate every time: connection handling, channel setup, reconnect logic, JSON serialization, queue assertions, exchange declarations, queue bindings, retry queues, dead-letter routing. rabbitmesh packages all of that into a clean, typed API.

- **No boilerplate** — queues and exchanges are auto-created on first use
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
- **Exchange support** — Direct, Fanout, and Topic exchanges
- **Automatic exchange creation and queue binding**
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

### Queue (direct)

```ts
await rabbit.publish({
  queue: "orders",
  payload: { orderId: "ord-001", total: 49.99 },
});
```

The queue is created automatically if it does not exist.

### Direct Exchange

```ts
await rabbit.publish({
  exchange: "notifications",
  exchangeType: "direct",
  routingKey: "email",
  payload: { to: "user@example.com" },
});
```

### Fanout Exchange

```ts
await rabbit.publish({
  exchange: "events",
  exchangeType: "fanout",
  payload: { event: "order.placed" },
});
```

Routing key is optional for fanout — all bound queues receive the message.

### Topic Exchange

```ts
await rabbit.publish({
  exchange: "events",
  exchangeType: "topic",
  routingKey: "user.created",
  payload: { userId: 42 },
});
```

---

## Consuming Messages

### Queue (direct)

```ts
await rabbit.subscribe({
  queue: "orders",
  handler: async ({ orderId, total }) => {
    await processOrder(orderId, total);
  },
});
```

### Exchange subscription

rabbitmesh automatically asserts the exchange, asserts the queue, and binds them — no manual RabbitMQ configuration required.

```ts
// Direct Exchange
await rabbit.subscribe({
  queue: "email-service",
  exchange: "notifications",
  exchangeType: "direct",
  routingKey: "email",
  handler: async (payload) => {
    await sendEmail(payload);
  },
});

// Topic Exchange — wildcard pattern
await rabbit.subscribe({
  queue: "user-service",
  exchange: "events",
  exchangeType: "topic",
  routingKey: "user.*",       // matches user.created, user.updated, user.deleted
  handler: async (payload) => {
    await handleUserEvent(payload);
  },
});

// Fanout Exchange
await rabbit.subscribe({
  queue: "audit-log",
  exchange: "events",
  exchangeType: "fanout",
  handler: async (payload) => {
    await auditLog(payload);
  },
});
```

Messages are acknowledged on success. If the handler throws, the message is nacked.

---

## Retries

Pass `retries` and `retryDelay` to automatically retry failed messages before giving up. Works identically for both queue and exchange subscriptions.

```ts
await rabbit.subscribe({
  queue: "email-service",
  exchange: "notifications",
  exchangeType: "direct",
  routingKey: "email",
  retries: 3,
  retryDelay: 5000,
  handler: async (payload) => {
    await sendEmail(payload);
  },
});
```

After all attempts are exhausted the message is nacked. Retry state survives consumer restarts.

---

## Dead Letter Queues

Enable DLQ routing to preserve messages that exhaust all retry attempts. Works for both queue and exchange subscriptions.

```ts
await rabbit.subscribe({
  queue: "email-service",
  exchange: "notifications",
  exchangeType: "direct",
  routingKey: "email",
  retries: 3,
  retryDelay: 5000,
  dlq: { enabled: true },
  handler: async (payload) => {
    await sendEmail(payload);
  },
});
```

Exhausted messages are moved to `email-service.dlq` and include the original payload, error message, retry count, timestamp, and source queue.

Use a custom DLQ name if needed:

```ts
dlq: { enabled: true, queueName: "emails.failed" }
```

---

## Delayed Messages

Publish a message now and have it delivered after a specified delay. Works for both queue and exchange publishes.

```ts
// Queue
await rabbit.publish({
  queue: "emails",
  payload: { userId: 42 },
  delay: 60000,
});

// Exchange
await rabbit.publish({
  exchange: "events",
  exchangeType: "topic",
  routingKey: "user.created",
  payload: { userId: 42 },
  delay: 60000,
});
```

The `delay` value is in milliseconds, must be a finite positive integer greater than 0.

### How it works

For queue publishes, rabbitmesh creates a `<queue>.delay.<ms>` queue with TTL equal to the delay, dead-lettered back to the original queue.

For exchange publishes, the delay queue dead-letters directly to the exchange with the original routing key — so the message is correctly routed after the TTL expires.

```txt
publish({ exchange: "events", routingKey: "user.created", delay: 60000 })
    ↓
events.delay.60000.user.created   (TTL = 60000ms, DLR → exchange "events" with key "user.created")
    ↓ TTL expires
Exchange "events"
    ↓ routing key: "user.created"
Consumer queue (user.*)
    ↓
Consumer
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

### `publish(options)`

| Option         | Type                            | Default     | Description                                                      |
| -------------- | ------------------------------- | ----------- | ---------------------------------------------------------------- |
| `queue`        | `string`                        | —           | Target queue name. Mutually exclusive with `exchange`            |
| `exchange`     | `string`                        | —           | Target exchange name. Mutually exclusive with `queue`            |
| `exchangeType` | `"direct" \| "fanout" \| "topic"` | —         | Exchange type. Required when `exchange` is set                   |
| `routingKey`   | `string`                        | —           | Routing key. Required for direct/topic. Optional for fanout      |
| `payload`      | `T`                             | —           | Message payload — JSON-serialized *(required)*                   |
| `delay`        | `number`                        | `undefined` | Milliseconds before delivery. Omit for immediate send            |

### `subscribe(options)`

| Option           | Type                            | Default       | Description                                              |
| ---------------- | ------------------------------- | ------------- | -------------------------------------------------------- |
| `queue`          | `string`                        | —             | Queue name *(required)*                                  |
| `exchange`       | `string`                        | —             | Exchange to bind the queue to                            |
| `exchangeType`   | `"direct" \| "fanout" \| "topic"` | —           | Exchange type. Required when `exchange` is set           |
| `routingKey`     | `string`                        | —             | Binding key. Required for direct/topic                   |
| `handler`        | `function`                      | —             | Async message handler *(required)*                       |
| `retries`        | `number`                        | `0`           | Max retry attempts. `0` disables retries                 |
| `retryDelay`     | `number`                        | `5000`        | Milliseconds between retry attempts                      |
| `dlq.enabled`    | `boolean`                       | `false`       | Route exhausted messages to a DLQ                        |
| `dlq.queueName`  | `string`                        | `<queue>.dlq` | Custom DLQ queue name                                    |

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
  ExchangeError,
  ValidationError,
} from "rabbitmesh";
```

| Error                | When it is thrown                                                      |
| -------------------- | ---------------------------------------------------------------------- |
| `ConnectionError`    | Connection cannot be established or is unexpectedly lost               |
| `PublishError`       | A `publish()` call fails (queue path)                                  |
| `SubscribeError`     | A `subscribe()` call fails during setup                                |
| `SerializationError` | Payload cannot be serialized or deserialized as JSON                   |
| `RetryError`         | A message exhausts all configured retry attempts                       |
| `DelayError`         | `delay` is invalid, or delay queue setup/publish fails                 |
| `ExchangeError`      | Exchange assertion, queue binding, or exchange publish fails           |
| `ValidationError`    | `publish()` or `subscribe()` options contain an invalid combination    |

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

- **Durable queues, exchanges, and persistent messages** survive broker restarts
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
