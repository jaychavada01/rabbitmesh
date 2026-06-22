/**
 * v0.2.0 validation publisher.
 * Run with: npx tsx examples/publisher.ts
 */
import { RabbitMesh } from "../src/index.js";

const rabbit = new RabbitMesh({ url: "amqp://guest:guest@localhost:5672" });

async function main(): Promise<void> {
  await rabbit.connect();

  await rabbit.publish({
    queue: "emails",
    payload: { id: Date.now() },
  });

  process.stdout.write("Published\n");

  await rabbit.disconnect();
}

main().catch((err) => {
  process.stderr.write(`Error: ${String(err)}\n`);
  process.exit(1);
});
