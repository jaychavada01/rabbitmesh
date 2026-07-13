import { describe, it, expect, vi, beforeEach } from "vitest";
import { Publisher } from "../../src/core/publisher.js";
import { ConnectionError, DelayError, ExchangeError, PublishError, SerializationError, ValidationError } from "../../src/utils/errors.js";
import type { ConnectionManager } from "../../src/core/connection-manager.js";

const mockChannel = {
  assertQueue: vi.fn().mockResolvedValue({}),
  assertExchange: vi.fn().mockResolvedValue({}),
  sendToQueue: vi.fn().mockReturnValue(true),
  publish: vi.fn().mockReturnValue(true),
};

const mockManager = {
  getChannel: vi.fn().mockReturnValue(mockChannel),
} as unknown as ConnectionManager;

describe("Publisher", () => {
  let publisher: Publisher;

  beforeEach(() => {
    vi.clearAllMocks();
    publisher = new Publisher(mockManager);
  });

  // ── validation ────────────────────────────────────────────────────────────

  it("throws ValidationError when both queue and exchange are set", async () => {
    await expect(
      publisher.publish({ queue: "q", exchange: "ex", exchangeType: "direct", routingKey: "rk", payload: {} }),
    ).rejects.toThrow(ValidationError);
  });

  it("throws ValidationError when neither queue nor exchange is set", async () => {
    await expect(publisher.publish({ payload: {} } as any)).rejects.toThrow(ValidationError);
  });

  it("throws ValidationError for exchange without exchangeType", async () => {
    await expect(publisher.publish({ exchange: "ex", payload: {} })).rejects.toThrow(ValidationError);
  });

  // ── queue publish (existing) ──────────────────────────────────────────────

  it("asserts queue and sends message", async () => {
    await publisher.publish({ queue: "test", payload: { id: 1 } });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("test", { durable: true });
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "test",
      Buffer.from(JSON.stringify({ id: 1 })),
      { persistent: true },
    );
  });

  it("throws SerializationError for circular references", async () => {
    const circular: Record<string, unknown> = {};
    circular["self"] = circular;
    await expect(publisher.publish({ queue: "q", payload: circular })).rejects.toThrow(SerializationError);
  });

  it("throws PublishError when sendToQueue returns false", async () => {
    mockChannel.sendToQueue.mockReturnValueOnce(false);
    await expect(publisher.publish({ queue: "q", payload: {} })).rejects.toThrow(PublishError);
  });

  it("throws PublishError when assertQueue rejects", async () => {
    mockChannel.assertQueue.mockRejectedValueOnce(new Error("channel error"));
    await expect(publisher.publish({ queue: "q", payload: {} })).rejects.toThrow(PublishError);
  });

  it("wraps ConnectionError from getChannel into PublishError", async () => {
    vi.mocked(mockManager.getChannel).mockImplementationOnce(() => {
      throw new ConnectionError("not connected");
    });
    await expect(publisher.publish({ queue: "q", payload: {} })).rejects.toThrow(PublishError);
  });

  // ── exchange publish ──────────────────────────────────────────────────────

  it("asserts exchange and publishes with routing key", async () => {
    await publisher.publish({
      exchange: "notifications",
      exchangeType: "direct",
      routingKey: "email",
      payload: { userId: 1 },
    });
    expect(mockChannel.assertExchange).toHaveBeenCalledWith("notifications", "direct", { durable: true });
    expect(mockChannel.publish).toHaveBeenCalledWith(
      "notifications",
      "email",
      Buffer.from(JSON.stringify({ userId: 1 })),
      { persistent: true },
    );
    expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
  });

  it("publishes to fanout exchange with empty routing key", async () => {
    await publisher.publish({
      exchange: "events",
      exchangeType: "fanout",
      payload: { event: "ping" },
    });
    expect(mockChannel.assertExchange).toHaveBeenCalledWith("events", "fanout", { durable: true });
    expect(mockChannel.publish).toHaveBeenCalledWith(
      "events",
      "",
      Buffer.from(JSON.stringify({ event: "ping" })),
      { persistent: true },
    );
  });

  it("publishes to topic exchange", async () => {
    await publisher.publish({
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.created",
      payload: { id: 1 },
    });
    expect(mockChannel.publish).toHaveBeenCalledWith(
      "events",
      "user.created",
      expect.any(Buffer),
      { persistent: true },
    );
  });

  it("throws ExchangeError when assertExchange fails", async () => {
    mockChannel.assertExchange.mockRejectedValueOnce(new Error("broker error"));
    await expect(
      publisher.publish({ exchange: "ex", exchangeType: "direct", routingKey: "rk", payload: {} }),
    ).rejects.toThrow(ExchangeError);
  });

  // ── delayed queue publish ─────────────────────────────────────────────────

  it("routes to delay queue when delay is provided", async () => {
    await publisher.publish({ queue: "emails", payload: { id: 1 }, delay: 5000 });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails.delay.5000", expect.objectContaining({
      durable: true,
      arguments: expect.objectContaining({ "x-message-ttl": 5000 }),
    }));
    expect(mockChannel.assertQueue).not.toHaveBeenCalledWith("emails", expect.anything());
  });

  it("throws DelayError for delay = 0", async () => {
    await expect(publisher.publish({ queue: "q", payload: {}, delay: 0 })).rejects.toThrow(DelayError);
  });

  it("throws DelayError for delay = -1", async () => {
    await expect(publisher.publish({ queue: "q", payload: {}, delay: -1 })).rejects.toThrow(DelayError);
  });

  it("throws DelayError for delay = NaN", async () => {
    await expect(publisher.publish({ queue: "q", payload: {}, delay: NaN })).rejects.toThrow(DelayError);
  });

  it("throws DelayError for delay = Infinity", async () => {
    await expect(publisher.publish({ queue: "q", payload: {}, delay: Infinity })).rejects.toThrow(DelayError);
  });

  // ── delayed exchange publish ──────────────────────────────────────────────

  it("creates exchange delay queue with correct DLR when delay + exchange", async () => {
    await publisher.publish({
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.created",
      payload: { id: 1 },
      delay: 60000,
    });
    expect(mockChannel.assertExchange).toHaveBeenCalledWith("events", "topic", { durable: true });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith(
      expect.stringContaining("events.delay.60000"),
      expect.objectContaining({
        arguments: expect.objectContaining({
          "x-dead-letter-exchange": "events",
          "x-dead-letter-routing-key": "user.created",
        }),
      }),
    );
    expect(mockChannel.sendToQueue).toHaveBeenCalled();
    expect(mockChannel.publish).not.toHaveBeenCalled();
  });
});
