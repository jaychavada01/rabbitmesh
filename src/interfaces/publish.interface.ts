import type { ExchangeType } from "../core/exchange-manager.js";

/**
 * Options for publishing a message.
 *
 * Queue publish (existing):
 *   `queue` required, `exchange` forbidden.
 *
 * Exchange publish (new):
 *   `exchange` + `exchangeType` required.
 *   `routingKey` required for direct/topic; optional for fanout.
 *   `queue` forbidden.
 */
export interface PublishOptions<T = unknown> {
  /** Target queue name. Mutually exclusive with `exchange`. */
  queue?: string;
  /** Target exchange name. Mutually exclusive with `queue`. */
  exchange?: string;
  /** Exchange type. Required when `exchange` is set. */
  exchangeType?: ExchangeType;
  /** Routing key. Required for direct/topic exchanges. Ignored for fanout. */
  routingKey?: string;
  /** Message payload — will be JSON-serialized */
  payload: T;
  /**
   * Delay in milliseconds before the message is delivered.
   * Must be a finite positive integer greater than 0.
   */
  delay?: number;
}
