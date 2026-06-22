/**
 * v0.2.0 validation subscriber.
 * Run with: npx tsx examples/subscriber.ts
 */
import { RabbitMesh } from "../src/index.js";

const rabbit = new RabbitMesh({ url: "amqp://guest:guest@localhost:5672" });

let attempts = 0;

async function main(): Promise<void> {
  await rabbit.connect();

  await rabbit.subscribe({
    queue: "emails",
    retries: 3,
    retryDelay: 3000,
    handler: async (payload) => {
      attempts++;
      process.stdout.write(`\nAttempt ${attempts}\nPayload: ${JSON.stringify(payload)}\n`);

      if (attempts < 3) {
        throw new Error("Temporary Failure");
      }

      process.stdout.write("SUCCESS\n");
    },
  });

  process.stdout.write("Subscriber Running\n");

  process.on("SIGINT", async () => {
    await rabbit.disconnect();
    process.exit(0);
  });
}

main().catch((err) => {
  process.stderr.write(`Error: ${String(err)}\n`);
  process.exit(1);
});
