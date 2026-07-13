import type { Channel } from "amqplib";
import { ExchangeError } from "../utils/errors.js";
import { Logger } from "../utils/logger.js";

export type ExchangeType = "direct" | "fanout" | "topic";

const log = new Logger("ExchangeManager");

/**
 * Single responsibility: assert RabbitMQ exchanges.
 */
export class ExchangeManager {
  /**
   * Assert an exchange. Safe to call multiple times (idempotent).
   * Throws {@link ExchangeError} on failure.
   */
  static async assertExchange(
    channel: Channel,
    exchange: string,
    type: ExchangeType,
  ): Promise<void> {
    try {
      await channel.assertExchange(exchange, type, { durable: true });
      log.info(`Exchange "${exchange}" asserted`);
    } catch (err) {
      throw new ExchangeError(`Failed to assert exchange "${exchange}" (${type})`, err);
    }
  }
}
