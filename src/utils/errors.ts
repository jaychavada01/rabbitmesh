/** Thrown when the AMQP connection cannot be established or is lost. */
export class ConnectionError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "ConnectionError";
  }
}

/** Thrown when publishing a message fails. */
export class PublishError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "PublishError";
  }
}

/** Thrown when setting up a consumer fails. */
export class SubscribeError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "SubscribeError";
  }
}

/** Thrown when JSON serialization or deserialization fails. */
export class SerializationError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "SerializationError";
  }
}

/** Thrown when a message has exhausted all retry attempts. */
export class RetryError extends Error {
  constructor(
    message: string,
    public readonly queue: string,
    public readonly retryCount: number,
    cause?: unknown,
  ) {
    super(message, { cause });
    this.name = "RetryError";
  }
}

/** Thrown when delayed message setup or publishing fails, or an invalid delay is supplied. */
export class DelayError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "DelayError";
  }
}
