import { describe, it, expect, vi, beforeEach } from "vitest";
import { ExchangeManager } from "../../src/core/exchange-manager.js";
import { BindingManager } from "../../src/core/binding-manager.js";
import { ExchangeError, ValidationError } from "../../src/utils/errors.js";
import { validatePublishOptions } from "../../src/utils/validation.js";
import { validateSubscribeOptions } from "../../src/utils/validation.js";

const mockChannel = {
  assertExchange: vi.fn().mockResolvedValue({}),
  assertQueue: vi.fn().mockResolvedValue({}),
  bindQueue: vi.fn().mockResolvedValue({}),
};

describe("ExchangeManager", () => {
  beforeEach(() => vi.clearAllMocks());

  it("asserts a direct exchange with durable option", async () => {
    await ExchangeManager.assertExchange(mockChannel as any, "events", "direct");
    expect(mockChannel.assertExchange).toHaveBeenCalledWith("events", "direct", { durable: true });
  });

  it("asserts a fanout exchange", async () => {
    await ExchangeManager.assertExchange(mockChannel as any, "broadcast", "fanout");
    expect(mockChannel.assertExchange).toHaveBeenCalledWith("broadcast", "fanout", { durable: true });
  });

  it("asserts a topic exchange", async () => {
    await ExchangeManager.assertExchange(mockChannel as any, "logs", "topic");
    expect(mockChannel.assertExchange).toHaveBeenCalledWith("logs", "topic", { durable: true });
  });

  it("throws ExchangeError when assertExchange fails", async () => {
    mockChannel.assertExchange.mockRejectedValueOnce(new Error("broker error"));
    await expect(
      ExchangeManager.assertExchange(mockChannel as any, "events", "direct"),
    ).rejects.toThrow(ExchangeError);
  });

  it("preserves cause in ExchangeError", async () => {
    const cause = new Error("broker error");
    mockChannel.assertExchange.mockRejectedValueOnce(cause);
    const err = await ExchangeManager.assertExchange(mockChannel as any, "events", "direct").catch(e => e);
    expect((err as ExchangeError).cause).toBe(cause);
  });
});

describe("BindingManager", () => {
  beforeEach(() => vi.clearAllMocks());

  it("asserts queue and binds it to exchange", async () => {
    await BindingManager.assertQueueAndBind(mockChannel as any, "email-svc", "notifications", "email");
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("email-svc", { durable: true });
    expect(mockChannel.bindQueue).toHaveBeenCalledWith("email-svc", "notifications", "email");
  });

  it("binds with empty routing key for fanout", async () => {
    await BindingManager.assertQueueAndBind(mockChannel as any, "fan-q", "broadcast", "");
    expect(mockChannel.bindQueue).toHaveBeenCalledWith("fan-q", "broadcast", "");
  });

  it("throws ExchangeError when assertQueue fails", async () => {
    mockChannel.assertQueue.mockRejectedValueOnce(new Error("queue error"));
    await expect(
      BindingManager.assertQueueAndBind(mockChannel as any, "q", "ex", "rk"),
    ).rejects.toThrow(ExchangeError);
  });

  it("throws ExchangeError when bindQueue fails", async () => {
    mockChannel.bindQueue.mockRejectedValueOnce(new Error("bind error"));
    await expect(
      BindingManager.assertQueueAndBind(mockChannel as any, "q", "ex", "rk"),
    ).rejects.toThrow(ExchangeError);
  });
});

describe("validatePublishOptions", () => {
  it("accepts valid queue-only options", () => {
    expect(() => validatePublishOptions({ queue: "q", payload: {} })).not.toThrow();
  });

  it("accepts valid direct exchange options", () => {
    expect(() =>
      validatePublishOptions({ exchange: "ex", exchangeType: "direct", routingKey: "rk", payload: {} }),
    ).not.toThrow();
  });

  it("accepts valid fanout options without routingKey", () => {
    expect(() =>
      validatePublishOptions({ exchange: "ex", exchangeType: "fanout", payload: {} }),
    ).not.toThrow();
  });

  it("accepts valid topic exchange options", () => {
    expect(() =>
      validatePublishOptions({ exchange: "ex", exchangeType: "topic", routingKey: "user.*", payload: {} }),
    ).not.toThrow();
  });

  it("throws ValidationError when both queue and exchange are set", () => {
    expect(() =>
      validatePublishOptions({ queue: "q", exchange: "ex", exchangeType: "direct", routingKey: "rk", payload: {} }),
    ).toThrow(ValidationError);
  });

  it("throws ValidationError when neither queue nor exchange is set", () => {
    expect(() => validatePublishOptions({ payload: {} } as any)).toThrow(ValidationError);
  });

  it("throws ValidationError for exchange without exchangeType", () => {
    expect(() =>
      validatePublishOptions({ exchange: "ex", routingKey: "rk", payload: {} }),
    ).toThrow(ValidationError);
  });

  it("throws ValidationError for direct exchange without routingKey", () => {
    expect(() =>
      validatePublishOptions({ exchange: "ex", exchangeType: "direct", payload: {} }),
    ).toThrow(ValidationError);
  });

  it("throws ValidationError for topic exchange without routingKey", () => {
    expect(() =>
      validatePublishOptions({ exchange: "ex", exchangeType: "topic", payload: {} }),
    ).toThrow(ValidationError);
  });
});

describe("validateSubscribeOptions", () => {
  const handler = async () => {};

  it("accepts queue-only options", () => {
    expect(() => validateSubscribeOptions({ queue: "q", handler })).not.toThrow();
  });

  it("accepts exchange subscribe with direct + routingKey", () => {
    expect(() =>
      validateSubscribeOptions({ queue: "q", exchange: "ex", exchangeType: "direct", routingKey: "rk", handler }),
    ).not.toThrow();
  });

  it("accepts fanout without routingKey", () => {
    expect(() =>
      validateSubscribeOptions({ queue: "q", exchange: "ex", exchangeType: "fanout", handler }),
    ).not.toThrow();
  });

  it("throws ValidationError for exchange without exchangeType", () => {
    expect(() =>
      validateSubscribeOptions({ queue: "q", exchange: "ex", handler }),
    ).toThrow(ValidationError);
  });

  it("throws ValidationError for topic exchange without routingKey", () => {
    expect(() =>
      validateSubscribeOptions({ queue: "q", exchange: "ex", exchangeType: "topic", handler }),
    ).toThrow(ValidationError);
  });

  it("throws ValidationError for direct exchange without routingKey", () => {
    expect(() =>
      validateSubscribeOptions({ queue: "q", exchange: "ex", exchangeType: "direct", handler }),
    ).toThrow(ValidationError);
  });
});

describe("ExchangeError", () => {
  it("has correct name", () => {
    const err = new ExchangeError("test");
    expect(err.name).toBe("ExchangeError");
    expect(err).toBeInstanceOf(Error);
  });

  it("preserves cause", () => {
    const cause = new Error("root");
    const err = new ExchangeError("wrapper", cause);
    expect(err.cause).toBe(cause);
  });
});

describe("ValidationError", () => {
  it("has correct name", () => {
    const err = new ValidationError("test");
    expect(err.name).toBe("ValidationError");
    expect(err).toBeInstanceOf(Error);
  });
});
