/**
 * Fanout Exchange example.
 * Every bound queue receives every message, regardless of routing key.
 *
 * Run both subscribers first:
 *   npx tsx examples/exchange-fanout.ts subscribe-a
 *   npx tsx examples/exchange-fanout.ts subscribe-b
 * Then publish:
 *   npx tsx examples/exchange-fanout.ts publish
 */
import { RabbitMesh } from "../src/index.js";

const rabbit = new RabbitMesh({ url: "amqp://guest:guest@localhost:5672" });
const [, , mode] = process.argv;

async function main(): Promise<void> {
  await rabbit.connect();

  if (mode === "publish") {
    await rabbit.publish({
      exchange: "order-events",
      exchangeType: "fanout",
      payload: { orderId: `ord-${Date.now()}`, status: "placed" },
    });
    process.stdout.write("Published to fanout exchange order-events\n");
    await rabbit.disconnect();
    return;
  }

  const queue = mode === "subscribe-b" ? "order-analytics" : "order-notifications";

  await rabbit.subscribe({
    queue,
    exchange: "order-events",
    exchangeType: "fanout",
    handler: async (payload) => {
      process.stdout.write(`[${queue}] Received: ${JSON.stringify(payload)}\n`);
    },
  });

  process.stdout.write(`[${queue}] Waiting for fanout messages...\n`);

  process.on("SIGINT", async () => {
    await rabbit.disconnect();
    process.exit(0);
  });
}

main().catch((err) => {
  process.stderr.write(`Error: ${String(err)}\n`);
  process.exit(1);
});
