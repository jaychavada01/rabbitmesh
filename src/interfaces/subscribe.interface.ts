import type { ExchangeType } from "../core/exchange-manager.js";

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
 * Options for subscribing to a queue or exchange.
 *
 * Queue subscribe (existing):
 *   `queue` required, `exchange` omitted.
 *
 * Exchange subscribe (new):
 *   `queue` + `exchange` + `exchangeType` required.
 *   `routingKey` required for direct/topic; optional for fanout.
 */
export interface SubscribeOptions<T = unknown> {
  /** Queue name to consume from */
  queue: string;
  /** Exchange to bind the queue to. When set, enables exchange-based routing. */
  exchange?: string;
  /** Exchange type. Required when `exchange` is set. */
  exchangeType?: ExchangeType;
  /** Routing key for binding. Required for direct/topic exchanges. */
  routingKey?: string;
  /** Async handler invoked for every received message */
  handler: (payload: T) => Promise<void>;
  /** Number of retry attempts before rejecting. Default: 0 (no retries) */
  retries?: number;
  /** Milliseconds to wait before redelivering to main queue. Default: 5000 */
  retryDelay?: number;
  /** Retry delay strategy. Only "fixed" is supported. */
  backoffStrategy?: "fixed";
  /** Dead Letter Queue options. When enabled, exhausted messages are moved to DLQ. */
  dlq?: DLQOptions;
}
