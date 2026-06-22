import { describe, it, expect, vi, beforeEach } from "vitest";
import { Publisher } from "../../src/core/publisher.js";
import { ConnectionError, PublishError, SerializationError } from "../../src/utils/errors.js";
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
});
