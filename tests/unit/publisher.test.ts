import { describe, it, expect, vi, beforeEach } from "vitest";
import { Publisher } from "../../src/core/publisher.js";
import { ConnectionError, DelayError, PublishError, SerializationError } from "../../src/utils/errors.js";
import type { ConnectionManager } from "../../src/core/connection-manager.js";

const mockChannel = {
  assertQueue: vi.fn().mockResolvedValue({}),
  sendToQueue: vi.fn().mockReturnValue(true),
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

  // ── immediate publish ─────────────────────────────────────────────────────

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

  // ── delayed publish ───────────────────────────────────────────────────────

  it("routes to delay queue when delay is provided", async () => {
    await publisher.publish({ queue: "emails", payload: { id: 1 }, delay: 5000 });

    // Must assert the delay queue, NOT the original queue
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails.delay.5000", expect.objectContaining({
      durable: true,
      arguments: expect.objectContaining({ "x-message-ttl": 5000 }),
    }));
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails.delay.5000",
      Buffer.from(JSON.stringify({ id: 1 })),
      { persistent: true },
    );
    // Original queue must NOT be asserted directly
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

  it("does not interact with RabbitMQ when delay is invalid", async () => {
    await expect(publisher.publish({ queue: "q", payload: {}, delay: -1 })).rejects.toThrow(DelayError);
    expect(mockChannel.assertQueue).not.toHaveBeenCalled();
    expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
  });

  it("immediate publish is unaffected when delay is undefined", async () => {
    await publisher.publish({ queue: "emails", payload: { id: 2 } });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails", { durable: true });
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith("emails", expect.any(Buffer), { persistent: true });
  });
});
