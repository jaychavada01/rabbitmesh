/**
 * Direct Exchange example.
 * Publisher routes to a specific queue by routing key.
 *
 * Run subscriber first:  npx tsx examples/exchange-direct.ts subscribe
 * Then publisher:        npx tsx examples/exchange-direct.ts publish
 */
import { RabbitMesh } from "../src/index.js";

const rabbit = new RabbitMesh({ url: "amqp://guest:guest@localhost:5672" });
const [, , mode] = process.argv;

async function main(): Promise<void> {
  await rabbit.connect();

  if (mode === "publish") {
    await rabbit.publish({
      exchange: "notifications",
      exchangeType: "direct",
      routingKey: "email",
      payload: { to: "user@example.com", subject: "Welcome" },
    });
    process.stdout.write("Published to notifications[email]\n");
    await rabbit.disconnect();
    return;
  }

  // subscribe (default)
  await rabbit.subscribe({
    queue: "email-service",
    exchange: "notifications",
    exchangeType: "direct",
    routingKey: "email",
    handler: async (payload) => {
      process.stdout.write(`Received: ${JSON.stringify(payload)}\n`);
    },
  });

  process.stdout.write("Waiting for messages on notifications[email] → email-service...\n");

  process.on("SIGINT", async () => {
    await rabbit.disconnect();
    process.exit(0);
  });
}

main().catch((err) => {
  process.stderr.write(`Error: ${String(err)}\n`);
  process.exit(1);
});
