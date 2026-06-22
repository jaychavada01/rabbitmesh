# rabbitmash

Production-ready RabbitMQ SDK for Node.js microservices with TypeScript support.

[![CI](https://github.com/your-org/rabbitmash/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/rabbitmash/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/rabbitmash.svg)](https://www.npmjs.com/package/rabbitmash)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Overview

rabbitmash provides a clean, TypeScript-first API over `amqplib` so you can start publishing and consuming RabbitMQ messages without boilerplate. It handles connection management, auto-reconnect, JSON serialization, and message acknowledgment out of the box.

## Features

- Connect / disconnect lifecycle management
- Publish to any queue — auto-creates durable, persistent queues
- Subscribe with typed handlers — auto-creates queues, handles ack/nack
- Auto-reconnect with configurable interval and max attempts
- Custom error hierarchy (`ConnectionError`, `PublishError`, `SubscribeError`, `SerializationError`)
- Full TypeScript generics on `publish<T>` and `subscribe<T>`
- ESM + CommonJS dual build — works with any module system
- Zero runtime dependencies beyond `amqplib`

## Installation

```bash
npm install rabbitmash
```

Requires Node.js ≥ 20 and a running RabbitMQ instance.

## Quick Start

```ts
import { RabbitMesh } from "rabbitmash";

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
Acks on success, nacks (no requeue) on handler error or deserialization failure.  
Throws `SubscribeError` on setup failure.

### Errors

| Class                | Thrown when                                     |
| -------------------- | ----------------------------------------------- |
| `ConnectionError`    | Connection cannot be established or is lost     |
| `PublishError`       | `publish()` fails for any reason                |
| `SubscribeError`     | `subscribe()` setup fails                       |
| `SerializationError` | JSON serialize/deserialize fails                |

## Roadmap

| Version | Features                        |
| ------- | ------------------------------- |
| v0.1.0  | Connection, Publisher, Subscriber, Auto-reconnect ✅ |
| v0.2.0  | Retry mechanism                 |
| v0.3.0  | Dead Letter Queue (DLQ)         |
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
