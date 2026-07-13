import type { Channel } from "amqplib";
import { PUBLISH_OPTIONS, QUEUE_OPTIONS } from "../utils/constants.js";
import { ExchangeError, PublishError, SerializationError } from "../utils/errors.js";
import { validatePublishOptions } from "../utils/validation.js";
import { Logger } from "../utils/logger.js";
import { DelayHandler } from "./delay-handler.js";
import { ExchangeManager } from "./exchange-manager.js";
import type { ConnectionManager } from "./connection-manager.js";
import type { PublishOptions } from "../interfaces/publish.interface.js";

/**
 * Publishes messages to RabbitMQ queues or exchanges.
 * Auto-creates durable queues/exchanges and persists all messages.
 * Delegates delayed publishing to {@link DelayHandler}.
 */
export class Publisher {
  private readonly log = new Logger("Publisher");
  private readonly delayHandler = new DelayHandler();

  constructor(private readonly connectionManager: ConnectionManager) {}

  async publish<T>(options: PublishOptions<T>): Promise<void> {
    validatePublishOptions(options);

    const { queue, exchange, exchangeType, routingKey, payload, delay } = options;

    let content: Buffer;
    try {
      content = Buffer.from(JSON.stringify(payload));
    } catch (err) {
      throw new SerializationError(
        `Failed to serialize payload for ${queue ? `queue "${queue}"` : `exchange "${exchange}"`}`,
        err,
      );
    }

    // ── Exchange publish ─────────────────────────────────────────────────────
    if (exchange) {
      const channel = this.connectionManager.getChannel();
      await ExchangeManager.assertExchange(channel, exchange, exchangeType!);

      if (delay !== undefined) {
        DelayHandler.validate(delay);
        await this.publishDelayedToExchange(channel, exchange, routingKey ?? "", delay, content);
        return;
      }

      try {
        const rk = routingKey ?? "";
        channel.publish(exchange, rk, content, PUBLISH_OPTIONS);
        this.log.info(`Published to exchange "${exchange}"`);
        this.log.info("Exchange publish successful");
      } catch (err) {
        throw new ExchangeError(`Failed to publish to exchange "${exchange}"`, err);
      }
      return;
    }

    // ── Queue publish (existing) ─────────────────────────────────────────────
    if (delay !== undefined) {
      DelayHandler.validate(delay);
      const channel = this.connectionManager.getChannel();
      await this.delayHandler.publish(channel, queue!, delay, content);
      this.log.debug(`Delayed message published to "${queue}" (delay: ${delay}ms)`);
      return;
    }

    try {
      const channel = this.connectionManager.getChannel();
      await channel.assertQueue(queue!, QUEUE_OPTIONS);
      const sent = channel.sendToQueue(queue!, content, PUBLISH_OPTIONS);
      if (!sent) throw new PublishError(`Queue "${queue}" write buffer is full`);
      this.log.debug(`Published to "${queue}"`);
    } catch (err) {
      if (err instanceof PublishError) throw err;
      throw new PublishError(`Failed to publish to queue "${queue}"`, err);
    }
  }

  /**
   * Delayed publish to exchange:
   * Creates a delay queue that dead-letters directly to the target exchange
   * with the specified routing key.
   */
  private async publishDelayedToExchange(
    channel: Channel,
    exchange: string,
    routingKey: string,
    delay: number,
    content: Buffer,
  ): Promise<void> {
    const delayQueue = `${exchange}.delay.${delay}${routingKey ? `.${routingKey.replace(/\*/g, "_").replace(/#/g, "_")}` : ""}`;
    try {
      await channel.assertQueue(delayQueue, {
        durable: true,
        arguments: {
          "x-message-ttl": delay,
          "x-dead-letter-exchange": exchange,
          "x-dead-letter-routing-key": routingKey,
        },
      });
      const sent = channel.sendToQueue(delayQueue, content, PUBLISH_OPTIONS);
      if (!sent) throw new ExchangeError(`Delay queue "${delayQueue}" write buffer is full`);
      this.log.info(`Published delayed message to exchange "${exchange}" via "${delayQueue}" (delay: ${delay}ms)`);
    } catch (err) {
      if (err instanceof ExchangeError) throw err;
      throw new ExchangeError(`Failed to publish delayed message to exchange "${exchange}"`, err);
    }
  }
}
