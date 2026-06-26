import { describe, it, expect, vi, beforeEach } from "vitest";
import { DelayHandler } from "../../src/core/delay-handler.js";
import { DelayError } from "../../src/utils/errors.js";
import type { Channel } from "amqplib";

const mockChannel = {
  assertQueue: vi.fn().mockResolvedValue({}),
  sendToQueue: vi.fn().mockReturnValue(true),
} as unknown as Channel;

describe("DelayHandler", () => {
  let handler: DelayHandler;

  beforeEach(() => {
    vi.clearAllMocks();
    handler = new DelayHandler();
  });

  // ── static queueName ──────────────────────────────────────────────────────

  describe("queueName()", () => {
    it("returns deterministic delay queue name", () => {
      expect(DelayHandler.queueName("emails", 60000)).toBe("emails.delay.60000");
      expect(DelayHandler.queueName("notifications", 300000)).toBe("notifications.delay.300000");
    });
  });

  // ── static validate ───────────────────────────────────────────────────────

  describe("validate()", () => {
    it.each([1000, 30000, 60000, 3600000])("accepts valid delay %d", (delay) => {
      expect(() => DelayHandler.validate(delay)).not.toThrow();
    });

    it.each([-1, 0, NaN, Infinity, -Infinity])(
      "throws DelayError for invalid delay %s",
      (delay) => {
        expect(() => DelayHandler.validate(delay)).toThrow(DelayError);
      },
    );

    it("DelayError carries the invalid value in message", () => {
      expect(() => DelayHandler.validate(-1)).toThrow(/Invalid delay/);
    });
  });

  // ── assertDelayQueue ──────────────────────────────────────────────────────

  describe("assertDelayQueue()", () => {
    it("asserts delay queue with correct TTL and dead-letter args", async () => {
      await handler.assertDelayQueue(mockChannel, "emails", 60000);

      expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails.delay.60000", {
        durable: true,
        arguments: {
          "x-message-ttl": 60000,
          "x-dead-letter-exchange": "",
          "x-dead-letter-routing-key": "emails",
        },
      });
    });

    it("throws DelayError when assertQueue rejects", async () => {
      vi.mocked(mockChannel.assertQueue).mockRejectedValueOnce(new Error("channel error"));
      await expect(handler.assertDelayQueue(mockChannel, "emails", 5000)).rejects.toThrow(DelayError);
    });
  });

  // ── publish ───────────────────────────────────────────────────────────────

  describe("publish()", () => {
    it("asserts delay queue and sends message", async () => {
      const content = Buffer.from('{"id":1}');
      await handler.publish(mockChannel, "emails", 5000, content);

      expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails.delay.5000", expect.any(Object));
      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        "emails.delay.5000",
        content,
        { persistent: true },
      );
    });

    it("throws DelayError when sendToQueue returns false", async () => {
      vi.mocked(mockChannel.sendToQueue).mockReturnValueOnce(false);
      await expect(
        handler.publish(mockChannel, "emails", 5000, Buffer.from("{}")),
      ).rejects.toThrow(DelayError);
    });

    it("throws DelayError when sendToQueue throws", async () => {
      vi.mocked(mockChannel.sendToQueue).mockImplementationOnce(() => {
        throw new Error("network error");
      });
      await expect(
        handler.publish(mockChannel, "emails", 5000, Buffer.from("{}")),
      ).rejects.toThrow(DelayError);
    });
  });
});
