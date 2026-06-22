import { QUEUE_OPTIONS } from "../utils/constants.js";
import { SerializationError, SubscribeError } from "../utils/errors.js";
import { Logger } from "../utils/logger.js";
import type { ConnectionManager } from "./connection-manager.js";
import type { SubscribeOptions } from "../interfaces/subscribe.interface.js";

/**
 * Subscribes to RabbitMQ queues and dispatches messages to handlers.
 * Acks on success, nacks (no requeue) on handler or deserialization error.
 */
export class Subscriber {
  private readonly log = new Logger("Subscriber");

  constructor(private readonly connectionManager: ConnectionManager) {}

  /**
   * Begin consuming `queue`, deserializing each message and calling `handler`.
   * Creates the queue if it does not exist.
   */
  async subscribe<T>(options: SubscribeOptions<T>): Promise<void> {
    const { queue, handler } = options;
    const channel = this.connectionManager.getChannel();

    try {
      await channel.assertQueue(queue, QUEUE_OPTIONS);

      await channel.consume(queue, async (msg) => {
        if (!msg) return; // consumer cancelled by broker

        let payload: T;
        try {
          payload = JSON.parse(msg.content.toString()) as T;
        } catch (err) {
          this.log.error(`Deserialization failed for queue "${queue}"`, err);
          channel.nack(msg, false, false);
          throw new SerializationError(`Failed to deserialize message from queue "${queue}"`, err);
        }

        try {
          await handler(payload);
          channel.ack(msg);
        } catch (err) {
          this.log.error(`Handler error for queue "${queue}"`, err);
          channel.nack(msg, false, false);
        }
      });

      this.log.info(`Subscribed to "${queue}"`);
    } catch (err) {
      if (err instanceof SerializationError) throw err;
      throw new SubscribeError(`Failed to subscribe to queue "${queue}"`, err);
    }
  }
}
