import { PUBLISH_OPTIONS, QUEUE_OPTIONS } from "../utils/constants.js";
import { PublishError, SerializationError } from "../utils/errors.js";
import { Logger } from "../utils/logger.js";
import { DelayHandler } from "./delay-handler.js";
import type { ConnectionManager } from "./connection-manager.js";
import type { PublishOptions } from "../interfaces/publish.interface.js";

/**
 * Publishes messages to RabbitMQ queues.
 * Auto-creates durable queues and persists all messages.
 * When `delay` is specified, delegates to {@link DelayHandler}.
 */
export class Publisher {
  private readonly log = new Logger("Publisher");
  private readonly delayHandler = new DelayHandler();

  constructor(private readonly connectionManager: ConnectionManager) {}

  /**
   * Serialize `payload` to JSON and send it to `queue`.
   * Creates the queue if it does not exist.
   * When `delay > 0`, routes through a delay queue.
   */
  async publish<T>(options: PublishOptions<T>): Promise<void> {
    const { queue, payload, delay } = options;

    let content: Buffer;
    try {
      content = Buffer.from(JSON.stringify(payload));
    } catch (err) {
      throw new SerializationError(`Failed to serialize payload for queue "${queue}"`, err);
    }

    // ── Delayed publish ──────────────────────────────────────────────────────
    if (delay !== undefined) {
      DelayHandler.validate(delay);
      const channel = this.connectionManager.getChannel();
      await this.delayHandler.publish(channel, queue, delay, content);
      this.log.debug(`Delayed message published to "${queue}" (delay: ${delay}ms)`);
      return;
    }

    // ── Immediate publish ────────────────────────────────────────────────────
    try {
      const channel = this.connectionManager.getChannel();
      await channel.assertQueue(queue, QUEUE_OPTIONS);
      const sent = channel.sendToQueue(queue, content, PUBLISH_OPTIONS);
      if (!sent) throw new PublishError(`Queue "${queue}" write buffer is full`);
      this.log.debug(`Published to "${queue}"`);
    } catch (err) {
      if (err instanceof PublishError) throw err;
      throw new PublishError(`Failed to publish to queue "${queue}"`, err);
    }
  }
}
