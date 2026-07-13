import { ValidationError } from "./errors.js";
import type { PublishOptions } from "../interfaces/publish.interface.js";
import type { SubscribeOptions } from "../interfaces/subscribe.interface.js";

/** Validate PublishOptions before any RabbitMQ operation. */
export function validatePublishOptions<T>(options: PublishOptions<T>): void {
  const { queue, exchange, exchangeType, routingKey } = options;

  if (queue && exchange) {
    throw new ValidationError("Specify either 'queue' or 'exchange', not both");
  }

  if (!queue && !exchange) {
    throw new ValidationError("Either 'queue' or 'exchange' is required");
  }

  if (exchange) {
    if (!exchangeType) {
      throw new ValidationError("'exchangeType' is required when publishing to an exchange");
    }
    if (exchangeType === "topic" && !routingKey) {
      throw new ValidationError("'routingKey' is required for topic exchange");
    }
    if (exchangeType === "direct" && !routingKey) {
      throw new ValidationError("'routingKey' is required for direct exchange");
    }
  }
}

/** Validate SubscribeOptions before any RabbitMQ operation. */
export function validateSubscribeOptions<T>(options: SubscribeOptions<T>): void {
  const { exchange, exchangeType, routingKey } = options;

  if (exchange) {
    if (!exchangeType) {
      throw new ValidationError("'exchangeType' is required when subscribing to an exchange");
    }
    if (exchangeType === "topic" && !routingKey) {
      throw new ValidationError("'routingKey' is required for topic exchange subscription");
    }
    if (exchangeType === "direct" && !routingKey) {
      throw new ValidationError("'routingKey' is required for direct exchange subscription");
    }
  }
}
