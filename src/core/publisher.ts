import { PUBLISH_OPTIONS, QUEUE_OPTIONS } from "../utils/constants.js";
import { PublishError, SerializationError } from "../utils/errors.js";
import { Logger } from "../utils/logger.js";
import type { ConnectionManager } from "./connection-manager.js";
import type { PublishOptions } from "../interfaces/publish.interface.js";

/**
 * Publishes messages to RabbitMQ queues.
 * Auto-creates durable queues and persists all messages.
 */
export class Publisher {
  private readonly log = new Logger("Publisher");

  constructor(private readonly connectionManager: ConnectionManager) {}

  /**
   * Serialize `payload` to JSON and send it to `queue`.
   * Creates the queue if it does not exist.
   */
  async publish<T>(options: PublishOptions<T>): Promise<void> {
    const { queue, payload } = options;

    let content: Buffer;
    try {
      content = Buffer.from(JSON.stringify(payload));
    } catch (err) {
      throw new SerializationError(`Failed to serialize payload for queue "${queue}"`, err);
    }

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
