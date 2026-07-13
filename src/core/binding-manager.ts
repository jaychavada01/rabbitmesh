import type { Channel } from "amqplib";
import { QUEUE_OPTIONS } from "../utils/constants.js";
import { ExchangeError } from "../utils/errors.js";
import { Logger } from "../utils/logger.js";

const log = new Logger("BindingManager");

/**
 * Single responsibility: assert queues and bind them to exchanges.
 */
export class BindingManager {
  /**
   * Assert a durable queue and bind it to `exchange` with `routingKey`.
   * Throws {@link ExchangeError} on failure.
   */
  static async assertQueueAndBind(
    channel: Channel,
    queue: string,
    exchange: string,
    routingKey: string,
  ): Promise<void> {
    try {
      await channel.assertQueue(queue, QUEUE_OPTIONS);
      await channel.bindQueue(queue, exchange, routingKey);
      log.info(`Queue "${queue}" bound to exchange "${exchange}" with key "${routingKey}"`);
    } catch (err) {
      throw new ExchangeError(
        `Failed to bind queue "${queue}" to exchange "${exchange}"`,
        err,
      );
    }
  }
}
