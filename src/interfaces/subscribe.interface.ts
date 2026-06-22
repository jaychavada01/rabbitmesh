/**
 * Options for subscribing to a queue.
 */
export interface SubscribeOptions<T = unknown> {
  /** Source queue name */
  queue: string;
  /** Async handler invoked for every received message */
  handler: (payload: T) => Promise<void>;
}
