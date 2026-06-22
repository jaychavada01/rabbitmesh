export const DEFAULTS = {
  RECONNECT: true,
  RECONNECT_INTERVAL_MS: 5_000,
  RECONNECT_MAX_ATTEMPTS: 0, // 0 = unlimited
} as const;

export const QUEUE_OPTIONS = {
  durable: true,
} as const;

export const PUBLISH_OPTIONS = {
  persistent: true,
} as const;
