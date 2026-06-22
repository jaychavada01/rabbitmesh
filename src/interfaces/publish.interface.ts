/**
 * Options for publishing a message to a queue.
 */
export interface PublishOptions<T = unknown> {
  /** Target queue name */
  queue: string;
  /** Message payload — will be JSON-serialized */
  payload: T;
}
