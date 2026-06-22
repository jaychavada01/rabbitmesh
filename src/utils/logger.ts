import type { LogLevel } from "../types/index.js";

const LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

/** Minimal internal logger. Replace by injecting a custom logger in the future. */
export class Logger {
  private readonly prefix: string;
  private readonly level: number;

  constructor(context: string, level: LogLevel = "info") {
    this.prefix = `[rabbitmash:${context}]`;
    this.level = LEVELS[level];
  }

  debug(msg: string): void {
    if (this.level <= LEVELS.debug) process.stderr.write(`DEBUG ${this.prefix} ${msg}\n`);
  }

  info(msg: string): void {
    if (this.level <= LEVELS.info) process.stderr.write(`INFO  ${this.prefix} ${msg}\n`);
  }

  warn(msg: string): void {
    if (this.level <= LEVELS.warn) process.stderr.write(`WARN  ${this.prefix} ${msg}\n`);
  }

  error(msg: string, err?: unknown): void {
    if (this.level <= LEVELS.error) {
      process.stderr.write(`ERROR ${this.prefix} ${msg}${err instanceof Error ? ` — ${err.message}` : ""}\n`);
    }
  }
}
