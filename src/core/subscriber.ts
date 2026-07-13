import { QUEUE_OPTIONS } from "../utils/constants.js";
import { RetryHandler } from "../utils/retry-handler.js";
import { DLQHandler } from "./dlq-handler.js";
import { ExchangeManager } from "./exchange-manager.js";
import { BindingManager } from "./binding-manager.js";
import { RetryError, SubscribeError } from "../utils/errors.js";
import { validateSubscribeOptions } from "../utils/validation.js";
import { Logger } from "../utils/logger.js";
import type { ConnectionManager } from "./connection-manager.js";
import type { SubscribeOptions } from "../interfaces/subscribe.interface.js";

/**
 * Subscribes to RabbitMQ queues (direct or exchange-bound) and dispatches messages to handlers.
 * Supports exchange binding, RabbitMQ-native retry, and DLQ routing.
 *
 * STABILITY GUARANTEE: no exception ever escapes the AMQP consumer callback.
 */
export class Subscriber {
  private readonly log = new Logger("Subscriber");

  constructor(private readonly connectionManager: ConnectionManager) {}

  async subscribe<T>(options: SubscribeOptions<T>): Promise<void> {
    validateSubscribeOptions(options);

    const { queue, exchange, exchangeType, routingKey, handler, retries = 0, retryDelay = 5_000, dlq } = options;
    const channel = this.connectionManager.getChannel();

    try {
      // ── Exchange setup ─────────────────────────────────────────────────────
      if (exchange) {
        await ExchangeManager.assertExchange(channel, exchange, exchangeType!);
        await BindingManager.assertQueueAndBind(channel, queue, exchange, routingKey ?? "");
      } else {
        await channel.assertQueue(queue, QUEUE_OPTIONS);
      }

      // ── Retry queue setup ──────────────────────────────────────────────────
      let retryHandler: RetryHandler | null = null;
      if (retries > 0) {
        retryHandler = new RetryHandler({ queue, retries, retryDelay });
        await retryHandler.assertRetryQueue(channel);
      }

      // ── DLQ setup ─────────────────────────────────────────────────────────
      let dlqHandler: DLQHandler | null = null;
      if (dlq?.enabled) {
        dlqHandler = new DLQHandler(queue, dlq.queueName);
        await dlqHandler.assertDLQ(channel);
      }

      // ── Consumer ──────────────────────────────────────────────────────────
      await channel.consume(queue, async (msg) => {
        if (!msg) return;

        let payload: T;
        try {
          payload = JSON.parse(msg.content.toString()) as T;
        } catch (err) {
          this.log.error(`Deserialization failed for queue "${queue}"`, err);
          channel.nack(msg, false, false);
          return;
        }

        try {
          await handler(payload);
          channel.ack(msg);
        } catch (err) {
          this.log.error(`Message processing failed for queue "${queue}"`, err);
          if (retryHandler) {
            try {
              retryHandler.handleFailure(channel, msg, err);
            } catch (retryErr) {
              if (retryErr instanceof RetryError && dlqHandler) {
                await dlqHandler.handle(channel, msg, queue, retryErr.retryCount, retryErr.cause);
              } else {
                this.log.error(`Message permanently failed on queue "${queue}"`, retryErr);
              }
            }
          } else {
            channel.nack(msg, false, false);
          }
        }
      });

      const bindInfo = exchange ? ` bound to exchange "${exchange}"` : "";
      this.log.info(
        `Subscribed to "${queue}"${bindInfo}${retries > 0 ? ` with ${retries} retries` : ""}${dlq?.enabled ? ` → DLQ "${dlqHandler!.queueName}"` : ""}`,
      );
    } catch (err) {
      if (err instanceof SubscribeError) throw err;
      throw new SubscribeError(`Failed to subscribe to queue "${queue}"`, err);
    }
  }
}
