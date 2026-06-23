/**
 * Dead Letter Queue configuration.
 */
export interface DLQOptions {
  /** Enable DLQ routing for exhausted messages. Default: false */
  enabled?: boolean;
  /** Custom DLQ queue name. Default: `<queue>.dlq` */
  queueName?: string;
}

/**
 * Options for subscribing to a queue.
 */
export interface SubscribeOptions<T = unknown> {
  /** Source queue name */
  queue: string;
  /** Async handler invoked for every received message */
  handler: (payload: T) => Promise<void>;
  /** Number of retry attempts before rejecting. Default: 0 (no retries) */
  retries?: number;
  /** Milliseconds to wait before redelivering to main queue. Default: 5000 */
  retryDelay?: number;
  /** Retry delay strategy. Only "fixed" is supported in v0.2.0. */
  backoffStrategy?: "fixed";
  /** Dead Letter Queue options. When enabled, exhausted messages are moved to DLQ. */
  dlq?: DLQOptions;
}
