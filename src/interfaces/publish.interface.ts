/**
 * Options for publishing a message to a queue.
 */
export interface PublishOptions<T = unknown> {
  /** Target queue name */
  queue: string;
  /** Message payload — will be JSON-serialized */
  payload: T;
  /**
   * Delay in milliseconds before the message is delivered to the consumer.
   * When omitted or undefined, the message is delivered immediately.
   * Must be a finite positive integer greater than 0.
   */
  delay?: number;
}
