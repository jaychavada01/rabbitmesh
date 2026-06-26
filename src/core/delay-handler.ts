import type { Channel } from "amqplib";
import { PUBLISH_OPTIONS } from "../utils/constants.js";
import { DelayError } from "../utils/errors.js";
import { Logger } from "../utils/logger.js";

const log = new Logger("DelayHandler");

/**
 * Handles delayed message publishing via RabbitMQ TTL + dead-letter routing.
 *
 * Delay queue pattern:
 *   `<queue>.delay.<ms>` → TTL expires → dead-letter routed back to `<queue>`
 *
 * Single responsibility: create delay queues, publish delayed messages.
 */
export class DelayHandler {
  /** Returns the deterministic delay queue name. */
  static queueName(queue: string, delay: number): string {
    return `${queue}.delay.${delay}`;
  }

  /**
   * Validate `delay`. Throws {@link DelayError} for invalid values.
   * Valid: finite positive integers > 0.
   */
  static validate(delay: number): void {
    if (!Number.isFinite(delay) || delay <= 0) {
      throw new DelayError(
        `Invalid delay "${delay}" — must be a finite positive number greater than 0`,
      );
    }
  }

  /**
   * Assert the delay queue (idempotent — safe to call multiple times).
   * The delay queue has a TTL equal to `delay` and dead-letters to `queue`.
   */
  async assertDelayQueue(channel: Channel, queue: string, delay: number): Promise<void> {
    const delayQueue = DelayHandler.queueName(queue, delay);
    try {
      await channel.assertQueue(delayQueue, {
        durable: true,
        arguments: {
          "x-message-ttl": delay,
          "x-dead-letter-exchange": "",
          "x-dead-letter-routing-key": queue,
        },
      });
      log.info(`Delay queue "${delayQueue}" created`);
    } catch (err) {
      throw new DelayError(`Failed to create delay queue "${delayQueue}"`, err);
    }
  }

  /**
   * Publish `content` to the delay queue for `queue` with `delay` ms TTL.
   * The message will be dead-lettered to `queue` after TTL expires.
   */
  async publish(channel: Channel, queue: string, delay: number, content: Buffer): Promise<void> {
    log.info(`Publishing delayed message (${delay}ms)`);

    await this.assertDelayQueue(channel, queue, delay);

    const delayQueue = DelayHandler.queueName(queue, delay);
    try {
      const sent = channel.sendToQueue(delayQueue, content, PUBLISH_OPTIONS);
      if (!sent) throw new DelayError(`Delay queue "${delayQueue}" write buffer is full`);
      log.info("Delayed message published");
    } catch (err) {
      if (err instanceof DelayError) throw err;
      throw new DelayError(`Failed to publish delayed message to "${delayQueue}"`, err);
    }
  }
}
