/**
 * Topic Exchange example.
 * Consumers bind with wildcard patterns; only matching messages are delivered.
 *
 * Run subscriber first:  npx tsx examples/exchange-topic.ts subscribe
 * Then publish:          npx tsx examples/exchange-topic.ts publish
 */
import { RabbitMesh } from "../src/index.js";

const rabbit = new RabbitMesh({ url: "amqp://guest:guest@localhost:5672" });
const [, , mode] = process.argv;

async function main(): Promise<void> {
  await rabbit.connect();

  if (mode === "publish") {
    // Publish three events — all match "user.*"
    for (const event of ["user.created", "user.updated", "user.deleted"]) {
      await rabbit.publish({
        exchange: "domain-events",
        exchangeType: "topic",
        routingKey: event,
        payload: { event, userId: 42, ts: Date.now() },
      });
      process.stdout.write(`Published ${event}\n`);
    }
    // This one does NOT match "user.*" — the subscriber will not receive it
    await rabbit.publish({
      exchange: "domain-events",
      exchangeType: "topic",
      routingKey: "order.placed",
      payload: { event: "order.placed" },
    });
    process.stdout.write("Published order.placed (not delivered to user-service)\n");
    await rabbit.disconnect();
    return;
  }

  // subscribe (default) — matches user.created, user.updated, user.deleted
  await rabbit.subscribe({
    queue: "user-service",
    exchange: "domain-events",
    exchangeType: "topic",
    routingKey: "user.*",
    handler: async (payload) => {
      process.stdout.write(`Received: ${JSON.stringify(payload)}\n`);
    },
  });

  process.stdout.write("Waiting for domain-events[user.*] → user-service...\n");

  process.on("SIGINT", async () => {
    await rabbit.disconnect();
    process.exit(0);
  });
}

main().catch((err) => {
  process.stderr.write(`Error: ${String(err)}\n`);
  process.exit(1);
});
