import { describe, it, expect, vi, beforeEach } from "vitest";
import { RetryHandler } from "../../src/utils/retry-handler.js";
import { RetryError } from "../../src/utils/errors.js";
import type { Channel, ConsumeMessage } from "amqplib";

function makeMsg(retryCount?: number): ConsumeMessage {
  return {
    content: Buffer.from('{"id":1}'),
    fields: {} as ConsumeMessage["fields"],
    properties: {
      headers: retryCount !== undefined ? { "x-retry-count": retryCount } : {},
    } as ConsumeMessage["properties"],
  };
}

const mockChannel = {
  assertQueue: vi.fn().mockResolvedValue({}),
  sendToQueue: vi.fn().mockReturnValue(true),
  ack: vi.fn(),
  nack: vi.fn(),
} as unknown as Channel;

describe("RetryHandler", () => {
  let handler: RetryHandler;

  beforeEach(() => {
    vi.clearAllMocks();
    handler = new RetryHandler({ queue: "emails", retries: 3, retryDelay: 1000 });
  });

  // ── assertRetryQueue ──────────────────────────────────────────────────────

  it("asserts retry queue with correct TTL and DLQ args", async () => {
    await handler.assertRetryQueue(mockChannel);

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails.retry", {
      durable: true,
      arguments: {
        "x-message-ttl": 1000,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "emails",
      },
    });
  });

  // ── getRetryCount ─────────────────────────────────────────────────────────

  it("returns 0 when x-retry-count header is absent", () => {
    expect(handler.getRetryCount(makeMsg())).toBe(0);
  });

  it("returns 0 when headers are missing entirely", () => {
    const msg = makeMsg();
    msg.properties.headers = undefined as unknown as Record<string, unknown>;
    expect(handler.getRetryCount(msg)).toBe(0);
  });

  it("returns the correct retry count from headers", () => {
    expect(handler.getRetryCount(makeMsg(2))).toBe(2);
  });

  // ── handleFailure — retries remaining ────────────────────────────────────

  it("routes to retry queue and acks original when retries remain", () => {
    const msg = makeMsg(0);
    handler.handleFailure(mockChannel, msg, new Error("fail"));

    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails.retry",
      msg.content,
      expect.objectContaining({
        persistent: true,
        headers: expect.objectContaining({ "x-retry-count": 1 }),
      }),
    );
    expect(mockChannel.ack).toHaveBeenCalledWith(msg);
    expect(mockChannel.nack).not.toHaveBeenCalled();
  });

  it("increments x-retry-count on each retry", () => {
    handler.handleFailure(mockChannel, makeMsg(1), new Error("fail"));

    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails.retry",
      expect.anything(),
      expect.objectContaining({
        headers: expect.objectContaining({ "x-retry-count": 2 }),
      }),
    );
  });

  it("preserves existing headers when routing to retry queue", () => {
    const msg = makeMsg(0);
    msg.properties.headers = { "x-retry-count": 0, "x-custom": "value" };

    handler.handleFailure(mockChannel, msg, new Error("fail"));

    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails.retry",
      expect.anything(),
      expect.objectContaining({
        headers: { "x-retry-count": 1, "x-custom": "value" },
      }),
    );
  });

  // ── handleFailure — max retries reached ──────────────────────────────────

  it("nacks and throws RetryError when max retries are exhausted", () => {
    const msg = makeMsg(3); // retryCount === retries (3)
    const cause = new Error("processing failed");

    expect(() => handler.handleFailure(mockChannel, msg, cause)).toThrow(RetryError);
    expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(mockChannel.ack).not.toHaveBeenCalled();
    expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
  });

  it("RetryError carries correct queue, retryCount, and cause", () => {
    const cause = new Error("timeout");
    const msg = makeMsg(3);

    let caught: RetryError | null = null;
    try {
      handler.handleFailure(mockChannel, msg, cause);
    } catch (err) {
      caught = err as RetryError;
    }

    expect(caught).toBeInstanceOf(RetryError);
    expect(caught!.queue).toBe("emails");
    expect(caught!.retryCount).toBe(3);
    expect(caught!.cause).toBe(cause);
  });
});
