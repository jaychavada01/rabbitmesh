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
