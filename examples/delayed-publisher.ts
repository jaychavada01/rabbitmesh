/**
 * v0.4.0 delayed message example.
 * Run with: npx tsx examples/delayed-publisher.ts
 *
 * Publishes one immediate and one delayed message.
 * The delayed message is delivered to the consumer after 10 seconds.
 */
import { RabbitMesh } from "../src/index.js";

const rabbit = new RabbitMesh({ url: "amqp://guest:guest@localhost:5672" });

async function main(): Promise<void> {
  await rabbit.connect();

  // Immediate publish — delivered right away
  await rabbit.publish({
    queue: "emails",
    payload: { type: "immediate", sentAt: new Date().toISOString() },
  });
  process.stdout.write("Published immediate message\n");

  // Delayed publish — delivered after 10 seconds
  await rabbit.publish({
    queue: "emails",
    payload: { type: "delayed", sentAt: new Date().toISOString() },
    delay: 10_000,
  });
  process.stdout.write("Published delayed message (10s)\n");

  await rabbit.disconnect();
}

main().catch((err) => {
  process.stderr.write(`Error: ${String(err)}\n`);
  process.exit(1);
});
