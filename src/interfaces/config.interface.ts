/**
 * Configuration options for RabbitMesh.
 */
export interface RabbitMeshConfig {
  /** AMQP connection URL, e.g. amqp://localhost */
  url: string;
  /** Enable auto-reconnect on connection loss. Default: true */
  reconnect?: boolean;
  /** Milliseconds to wait before attempting reconnect. Default: 5000 */
  reconnectInterval?: number;
  /** Maximum reconnect attempts. 0 = unlimited. Default: 0 */
  reconnectMaxAttempts?: number;
}
