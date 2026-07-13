import { describe, it, expect, vi, beforeEach } from "vitest";
import { Subscriber } from "../../src/core/subscriber.js";
import { SubscribeError, ValidationError } from "../../src/utils/errors.js";
import type { ConnectionManager } from "../../src/core/connection-manager.js";
import type { ConsumeMessage } from "amqplib";

let consumeCallback: ((msg: ConsumeMessage | null) => Promise<void>) | null = null;

const mockChannel = {
  assertQueue: vi.fn().mockResolvedValue({}),
  assertExchange: vi.fn().mockResolvedValue({}),
  bindQueue: vi.fn().mockResolvedValue({}),
  consume: vi.fn().mockImplementation(
    (_queue: string, cb: (msg: ConsumeMessage | null) => Promise<void>) => {
      consumeCallback = cb;
      return Promise.resolve({ consumerTag: "tag1" });
    },
  ),
  ack: vi.fn(),
  nack: vi.fn(),
};

const mockManager = {
  getChannel: vi.fn().mockReturnValue(mockChannel),
} as unknown as ConnectionManager;

function makeMsg(body: unknown): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify(body)),
    fields: {} as ConsumeMessage["fields"],
    properties: { headers: {} } as ConsumeMessage["properties"],
  };
}

describe("Subscriber", () => {
  let subscriber: Subscriber;

  beforeEach(() => {
    vi.clearAllMocks();
    consumeCallback = null;
    subscriber = new Subscriber(mockManager);
  });

  // ── queue subscribe (existing) ────────────────────────────────────────────

  it("asserts queue and starts consuming", async () => {
    await subscriber.subscribe({ queue: "test", handler: vi.fn() });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("test", { durable: true });
    expect(mockChannel.consume).toHaveBeenCalled();
  });

  it("deserializes payload and calls handler then acks", async () => {
    const handler = vi.fn().mockResolvedValue(undefined);
    await subscriber.subscribe({ queue: "test", handler });
    await consumeCallback!(makeMsg({ userId: 42 }));
    expect(handler).toHaveBeenCalledWith({ userId: 42 });
    expect(mockChannel.ack).toHaveBeenCalled();
    expect(mockChannel.nack).not.toHaveBeenCalled();
  });

  it("nacks and resolves (no throw) on invalid JSON", async () => {
    const handler = vi.fn();
    await subscriber.subscribe({ queue: "test", handler });
    const badMsg: ConsumeMessage = {
      content: Buffer.from("not-json{{"),
      fields: {} as ConsumeMessage["fields"],
      properties: { headers: {} } as ConsumeMessage["properties"],
    };
    await expect(consumeCallback!(badMsg)).resolves.toBeUndefined();
    expect(mockChannel.nack).toHaveBeenCalledWith(badMsg, false, false);
    expect(handler).not.toHaveBeenCalled();
  });

  it("nacks when handler throws", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("processing failed"));
    await subscriber.subscribe({ queue: "test", handler });
    const msg = makeMsg({ x: 1 });
    await consumeCallback!(msg);
    expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(mockChannel.ack).not.toHaveBeenCalled();
  });

  it("does nothing when msg is null (consumer cancelled)", async () => {
    const handler = vi.fn();
    await subscriber.subscribe({ queue: "test", handler });
    await consumeCallback!(null);
    expect(handler).not.toHaveBeenCalled();
  });

  it("throws SubscribeError when assertQueue rejects", async () => {
    mockChannel.assertQueue.mockRejectedValueOnce(new Error("channel error"));
    await expect(
      subscriber.subscribe({ queue: "test", handler: vi.fn() }),
    ).rejects.toThrow(SubscribeError);
  });

  // ── exchange subscribe ────────────────────────────────────────────────────

  it("asserts exchange and binds queue for direct exchange", async () => {
    await subscriber.subscribe({
      queue: "email-svc",
      exchange: "notifications",
      exchangeType: "direct",
      routingKey: "email",
      handler: vi.fn(),
    });
    expect(mockChannel.assertExchange).toHaveBeenCalledWith("notifications", "direct", { durable: true });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("email-svc", { durable: true });
    expect(mockChannel.bindQueue).toHaveBeenCalledWith("email-svc", "notifications", "email");
    expect(mockChannel.consume).toHaveBeenCalled();
  });

  it("asserts exchange and binds with empty key for fanout", async () => {
    await subscriber.subscribe({
      queue: "fan-q",
      exchange: "broadcast",
      exchangeType: "fanout",
      handler: vi.fn(),
    });
    expect(mockChannel.assertExchange).toHaveBeenCalledWith("broadcast", "fanout", { durable: true });
    expect(mockChannel.bindQueue).toHaveBeenCalledWith("fan-q", "broadcast", "");
  });

  it("asserts exchange and binds with topic routing key", async () => {
    await subscriber.subscribe({
      queue: "user-events",
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.*",
      handler: vi.fn(),
    });
    expect(mockChannel.bindQueue).toHaveBeenCalledWith("user-events", "events", "user.*");
  });

  it("consumes from bound queue and delivers to handler", async () => {
    const handler = vi.fn().mockResolvedValue(undefined);
    await subscriber.subscribe({
      queue: "svc",
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.*",
      handler,
    });
    await consumeCallback!(makeMsg({ event: "user.created" }));
    expect(handler).toHaveBeenCalledWith({ event: "user.created" });
    expect(mockChannel.ack).toHaveBeenCalled();
  });

  // ── validation ────────────────────────────────────────────────────────────

  it("throws ValidationError for exchange without exchangeType", async () => {
    await expect(
      subscriber.subscribe({ queue: "q", exchange: "ex", handler: vi.fn() }),
    ).rejects.toThrow(ValidationError);
  });

  it("throws ValidationError for topic exchange without routingKey", async () => {
    await expect(
      subscriber.subscribe({ queue: "q", exchange: "ex", exchangeType: "topic", handler: vi.fn() }),
    ).rejects.toThrow(ValidationError);
  });
});
