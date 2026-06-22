import { ConnectionManager } from "./connection-manager.js";
import { Publisher } from "./publisher.js";
import { Subscriber } from "./subscriber.js";
import { DEFAULTS } from "../utils/constants.js";
import type { RabbitMeshConfig } from "../interfaces/config.interface.js";
import type { PublishOptions } from "../interfaces/publish.interface.js";
import type { SubscribeOptions } from "../interfaces/subscribe.interface.js";
import type { ResolvedConfig } from "../types/index.js";

/**
 * Main entry point for rabbitmash.
 *
 * ```ts
 * const rabbit = new RabbitMesh({ url: "amqp://localhost" });
 * await rabbit.connect();
 * await rabbit.publish({ queue: "orders", payload: { id: 1 } });
 * await rabbit.subscribe({ queue: "orders", handler: async (p) => { ... } });
 * await rabbit.disconnect();
 * ```
 */
export class RabbitMesh {
  private readonly connectionManager: ConnectionManager;
  private readonly publisher: Publisher;
  private readonly subscriber: Subscriber;

  constructor(config: RabbitMeshConfig) {
    const resolved: ResolvedConfig = {
      url: config.url,
      reconnect: config.reconnect ?? DEFAULTS.RECONNECT,
      reconnectInterval: config.reconnectInterval ?? DEFAULTS.RECONNECT_INTERVAL_MS,
      reconnectMaxAttempts: config.reconnectMaxAttempts ?? DEFAULTS.RECONNECT_MAX_ATTEMPTS,
    };

    this.connectionManager = new ConnectionManager(resolved);
    this.publisher = new Publisher(this.connectionManager);
    this.subscriber = new Subscriber(this.connectionManager);
  }

  /** Connect to RabbitMQ. Must be called before publish/subscribe. */
  async connect(): Promise<void> {
    await this.connectionManager.connect();
  }

  /** Gracefully close the connection. */
  async disconnect(): Promise<void> {
    await this.connectionManager.disconnect();
  }

  /**
   * Publish a message to a queue.
   * @example
   * await rabbit.publish({ queue: "notifications", payload: { userId: 1 } });
   */
  async publish<T>(options: PublishOptions<T>): Promise<void> {
    await this.publisher.publish(options);
  }

  /**
   * Subscribe to a queue and process messages.
   * @example
   * await rabbit.subscribe({ queue: "notifications", handler: async (p) => { ... } });
   */
  async subscribe<T>(options: SubscribeOptions<T>): Promise<void> {
    await this.subscriber.subscribe(options);
  }
}
