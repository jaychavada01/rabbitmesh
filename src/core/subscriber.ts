import { QUEUE_OPTIONS } from "../utils/constants.js";
import { RetryHandler } from "../utils/retry-handler.js";
import { DLQHandler } from "./dlq-handler.js";
import { RetryError, SubscribeError } from "../utils/errors.js";
import { Logger } from "../utils/logger.js";
import type { ConnectionManager } from "./connection-manager.js";
import type { SubscribeOptions } from "../interfaces/subscribe.interface.js";

/**
 * Subscribes to RabbitMQ queues and dispatches messages to handlers.
 * Supports RabbitMQ-native retry via a dedicated retry queue when `retries > 0`.
 * Supports DLQ routing when `dlq.enabled` is true.
 *
 * STABILITY GUARANTEE: no exception ever escapes the AMQP consumer callback.
 * All errors are caught, logged, and the consumer continues running.
 */
export class Subscriber {
  private readonly log = new Logger("Subscriber");

  constructor(private readonly connectionManager: ConnectionManager) {}

  async subscribe<T>(options: SubscribeOptions<T>): Promise<void> {
    const { queue, handler, retries = 0, retryDelay = 5_000, dlq } = options;
    const channel = this.connectionManager.getChannel();

    try {
      await channel.assertQueue(queue, QUEUE_OPTIONS);

      let retryHandler: RetryHandler | null = null;
      if (retries > 0) {
        retryHandler = new RetryHandler({ queue, retries, retryDelay });
        await retryHandler.assertRetryQueue(channel);
      }

      let dlqHandler: DLQHandler | null = null;
      if (dlq?.enabled) {
        dlqHandler = new DLQHandler(queue, dlq.queueName);
        await dlqHandler.assertDLQ(channel);
      }

      await channel.consume(queue, async (msg) => {
        if (!msg) return;

        // ── Deserialization ────────────────────────────────────────────────
        let payload: T;
        try {
          payload = JSON.parse(msg.content.toString()) as T;
        } catch (err) {
          this.log.error(`Deserialization failed for queue "${queue}"`, err);
          channel.nack(msg, false, false);
          return;
        }

        // ── Handler + retry ────────────────────────────────────────────────
        try {
          await handler(payload);
          channel.ack(msg);
        } catch (err) {
          this.log.error(`Message processing failed for queue "${queue}"`, err);
          if (retryHandler) {
            try {
              retryHandler.handleFailure(channel, msg, err);
            } catch (retryErr) {
              // RetryError: retries exhausted — route to DLQ or log and move on
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

      this.log.info(
        `Subscribed to "${queue}"${retries > 0 ? ` with ${retries} retries` : ""}${dlq?.enabled ? ` → DLQ "${dlqHandler!.queueName}"` : ""}`,
      );
    } catch (err) {
      throw new SubscribeError(`Failed to subscribe to queue "${queue}"`, err);
    }
  }
}
