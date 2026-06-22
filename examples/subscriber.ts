/**
 * Subscriber example — run with: npx tsx examples/subscriber.ts
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

  process.stdout.write("Waiting for messages on order.created…\n");

  await rabbit.subscribe<OrderCreated>({
    queue: "order.created",
    handler: async (payload) => {
      process.stdout.write(`Received order: ${JSON.stringify(payload)}\n`);
    },
  });

  // Keep the process alive
  process.on("SIGINT", async () => {
    await rabbit.disconnect();
    process.exit(0);
  });
}

main().catch((err) => {
  process.stderr.write(`Error: ${String(err)}\n`);
  process.exit(1);
});
