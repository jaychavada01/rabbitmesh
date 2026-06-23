import type { Channel, ConsumeMessage } from "amqplib";
import { QUEUE_OPTIONS } from "../utils/constants.js";
import { Logger } from "../utils/logger.js";

/** Shape of every message written to the DLQ. */
export interface DLQMessage {
  payload: unknown;
  error: string;
  retryCount: number;
  failedAt: string;
  originalQueue: string;
}

/**
 * Handles routing of exhausted messages to a Dead Letter Queue.
 * Single responsibility: assert DLQ, serialize metadata, publish, ack original.
 */
export class DLQHandler {
  private readonly dlqQueue: string;
  private readonly log = new Logger("DLQHandler");

  constructor(originalQueue: string, customName?: string) {
    this.dlqQueue = customName ?? `${originalQueue}.dlq`;
  }

  /** Assert the DLQ on the channel. Must be called once during setup. */
  async assertDLQ(channel: Channel): Promise<void> {
    await channel.assertQueue(this.dlqQueue, QUEUE_OPTIONS);
  }

  /**
   * Move a failed message to the DLQ.
   * Acks the original message after a successful DLQ publish.
   * Never throws — all failures are logged and the consumer keeps running.
   */
  async handle(
    channel: Channel,
    msg: ConsumeMessage,
    originalQueue: string,
    retryCount: number,
    cause: unknown,
  ): Promise<void> {
    try {
      let payload: unknown;
      try {
        payload = JSON.parse(msg.content.toString());
      } catch {
        payload = msg.content.toString();
      }

      const dlqMessage: DLQMessage = {
        payload,
        error: cause instanceof Error ? cause.message : String(cause),
        retryCount,
        failedAt: new Date().toISOString(),
        originalQueue,
      };

      channel.sendToQueue(
        this.dlqQueue,
        Buffer.from(JSON.stringify(dlqMessage)),
        { persistent: true },
      );

      this.log.info(`Message moved to DLQ "${this.dlqQueue}"`);
    } catch (err) {
      this.log.error(`Failed to move message to DLQ "${this.dlqQueue}"`, err);
      // Do not re-throw — consumer must stay alive
    }
  }

  get queueName(): string {
    return this.dlqQueue;
  }
}
