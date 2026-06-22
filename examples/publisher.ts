/**
 * Publisher example — run with: npx tsx examples/publisher.ts
 * Requires a RabbitMQ instance at amqp://localhost (e.g. via Docker:
 *   docker run -d -p 5672:5672 rabbitmq:3-alpine
 */
import { RabbitMesh } from "../src/index.js";

interface OrderCreated {
  orderId: string;
  userId: number;
  total: number;
}

const rabbit = new RabbitMesh({
  url: process.env.RABBITMQ_URL ?? "amqp://localhost",
  reconnect: true,
  reconnectInterval: 5_000,
});

async function main(): Promise<void> {
  await rabbit.connect();

  const payload: OrderCreated = {
    orderId: `ord-${Date.now()}`,
    userId: 42,
    total: 199.99,
  };

  await rabbit.publish<OrderCreated>({
    queue: "order.created",
    payload,
  });

  process.stdout.write(`Published: ${JSON.stringify(payload)}\n`);

  await rabbit.disconnect();
}

main().catch((err) => {
  process.stderr.write(`Error: ${String(err)}\n`);
  process.exit(1);
});
