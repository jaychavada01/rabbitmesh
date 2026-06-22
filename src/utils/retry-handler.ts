import type { Channel, ConsumeMessage } from "amqplib";
import { RetryError } from "./errors.js";
import { Logger } from "./logger.js";

const RETRY_COUNT_HEADER = "x-retry-count";

export interface RetryOptions {
  queue: string;
  retries: number;
  retryDelay: number;
}

/**
 * Manages RabbitMQ-native message retry logic via a dedicated retry queue.
 *
 * Retry queue (`<queue>.retry`) is configured with:
 *  - x-message-ttl  → retryDelay
 *  - x-dead-letter-exchange      → "" (default exchange)
 *  - x-dead-letter-routing-key   → original queue
 *
 * Messages automatically return to the main queue after TTL expires, with no
 * in-process timers or counters.
 */
export class RetryHandler {
  private readonly retryQueue: string;
  private readonly log = new Logger("RetryHandler");

  constructor(private readonly options: RetryOptions) {
    this.retryQueue = `${options.queue}.retry`;
  }

  /** Assert the retry queue on the channel. Must be called once during setup. */
  async assertRetryQueue(channel: Channel): Promise<void> {
    await channel.assertQueue(this.retryQueue, {
      durable: true,
      arguments: {
        "x-message-ttl": this.options.retryDelay,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": this.options.queue,
      },
    });
  }

  /** Extract the current retry count from message headers (0 if absent). */
  getRetryCount(msg: ConsumeMessage): number {
    const count = msg.properties.headers?.[RETRY_COUNT_HEADER];
    return typeof count === "number" ? count : 0;
  }

  /**
   * Handle a failed message:
   *  - If retries remain: publish to retry queue and ack the original.
   *  - If exhausted: nack (no requeue) and throw RetryError.
   */
  handleFailure(channel: Channel, msg: ConsumeMessage, cause: unknown): void {
    const { queue, retries } = this.options;
    const retryCount = this.getRetryCount(msg);

    if (retryCount < retries) {
      const next = retryCount + 1;
      this.log.warn(`Message processing failed — retry attempt ${next}/${retries} for "${queue}"`);

      channel.sendToQueue(
        this.retryQueue,
        msg.content,
        {
          persistent: true,
          headers: { ...msg.properties.headers, [RETRY_COUNT_HEADER]: next },
        },
      );
      channel.ack(msg);
    } else {
      this.log.error(`Max retries reached for "${queue}" after ${retries} attempt(s)`);
      channel.nack(msg, false, false);
      throw new RetryError(
        `Message rejected after ${retries} retry attempt(s) on queue "${queue}"`,
        queue,
        retryCount,
        cause,
      );
    }
  }
}
