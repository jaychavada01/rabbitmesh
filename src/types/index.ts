/** Log level used by the internal logger. */
export type LogLevel = "debug" | "info" | "warn" | "error";

/** Resolved, required connection config (all defaults applied). */
export type ResolvedConfig = Required<{
  url: string;
  reconnect: boolean;
  reconnectInterval: number;
  reconnectMaxAttempts: number;
}>;
