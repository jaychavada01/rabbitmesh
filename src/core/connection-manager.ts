import amqplib from "amqplib";
import type { Connection, Channel } from "amqplib";
import { ConnectionError } from "../utils/errors.js";
import { Logger } from "../utils/logger.js";
import type { ResolvedConfig } from "../types/index.js";

/**
 * Manages the AMQP connection and channel lifecycle.
 * Handles auto-reconnect with configurable retry logic.
 */
export class ConnectionManager {
  private connection: Connection | null = null;
  private channel: Channel | null = null;
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private isShuttingDown = false;

  private readonly log = new Logger("ConnectionManager");

  constructor(private readonly config: ResolvedConfig) {}

  /** Establish the AMQP connection and open a channel. */
  async connect(): Promise<void> {
    try {
      this.log.info(`Connecting to ${this.config.url}`);
      this.connection = await amqplib.connect(this.config.url);
      this.channel = await this.connection.createChannel();
      this.reconnectAttempts = 0;
      this.log.info("Connected");

      this.connection.on("error", (err: Error) => {
        this.log.error("Connection error", err);
        this.scheduleReconnect();
      });

      this.connection.on("close", () => {
        if (!this.isShuttingDown) {
          this.log.warn("Connection closed unexpectedly");
          this.scheduleReconnect();
        }
      });
    } catch (err) {
      throw new ConnectionError("Failed to connect to RabbitMQ", err);
    }
  }

  /** Close channel and connection gracefully. */
  async disconnect(): Promise<void> {
    this.isShuttingDown = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    try {
      await this.channel?.close();
    } catch { /* already closed */ }
    try {
      await this.connection?.close();
    } catch { /* already closed */ }
    this.channel = null;
    this.connection = null;
    this.log.info("Disconnected");
  }

  /** Return the open channel. Throws if not connected. */
  getChannel(): Channel {
    if (!this.channel) {
      throw new ConnectionError("Not connected — call connect() first");
    }
    return this.channel;
  }

  private scheduleReconnect(): void {
    if (!this.config.reconnect || this.isShuttingDown) return;

    const maxAttempts = this.config.reconnectMaxAttempts;
    if (maxAttempts > 0 && this.reconnectAttempts >= maxAttempts) {
      this.log.error(`Max reconnect attempts (${maxAttempts}) reached`);
      return;
    }

    this.reconnectAttempts++;
    this.log.info(`Reconnecting in ${this.config.reconnectInterval}ms (attempt ${this.reconnectAttempts})`);

    this.reconnectTimer = setTimeout(() => {
      this.connect().catch((err) => {
        this.log.error("Reconnect failed", err);
        this.scheduleReconnect();
      });
    }, this.config.reconnectInterval);
  }
}
