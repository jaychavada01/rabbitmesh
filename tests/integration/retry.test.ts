/**
 * Integration tests — retry mechanism + consumer stability.
 * Exercises the full stack (Subscriber → RetryHandler) with amqplib mocked.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { RabbitMesh } from "../../src/core/rabbitmesh.js";
import type { ConsumeMessage } from "amqplib";

// ---- hoisted mocks ----------------------------------------------------------

const { mockChannel, mockConnection, getConsumeCallback, setConsumeCallback } = vi.hoisted(() => {
  let _cb: ((msg: ConsumeMessage | null) => Promise<void>) | null = null;

  const mockChannel = {
    assertQueue: vi.fn().mockResolvedValue({}),
    sendToQueue: vi.fn().mockReturnValue(true),
    consume: vi.fn().mockImplementation(
      (_q: string, cb: (msg: ConsumeMessage | null) => Promise<void>) => {
        _cb = cb;
        return Promise.resolve({ consumerTag: "tag1" });
      },
    ),
    ack: vi.fn(),
    nack: vi.fn(),
    close: vi.fn().mockResolvedValue(undefined),
  };

  const mockConnection = {
    createChannel: vi.fn().mockResolvedValue(mockChannel),
    close: vi.fn().mockResolvedValue(undefined),
    on: vi.fn(),
  };

  return {
    mockChannel,
    mockConnection,
    getConsumeCallback: () => _cb,
    setConsumeCallback: (cb: ((msg: ConsumeMessage | null) => Promise<void>) | null) => {
      _cb = cb;
    },
  };
});

vi.mock("amqplib", () => ({
  default: { connect: vi.fn().mockResolvedValue(mockConnection) },
}));

// ---- helpers ----------------------------------------------------------------

function makeMsg(body: unknown, retryCount?: number): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify(body)),
    fields: {} as ConsumeMessage["fields"],
    properties: {
      headers: retryCount !== undefined ? { "x-retry-count": retryCount } : {},
    } as ConsumeMessage["properties"],
  };
}

// ---- tests ------------------------------------------------------------------

describe("RabbitMesh retry integration", () => {
  let rabbit: RabbitMesh;

  beforeEach(async () => {
    vi.clearAllMocks();
    setConsumeCallback(null);
    rabbit = new RabbitMesh({ url: "amqp://localhost" });
    await rabbit.connect();
  });

  afterEach(async () => {
    await rabbit.disconnect();
  });

  // ── Retry queue creation ──────────────────────────────────────────────────

  it("asserts both main queue and retry queue on subscribe", async () => {
    await rabbit.subscribe({ queue: "emails", retries: 3, retryDelay: 1000, handler: vi.fn() });

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails", { durable: true });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails.retry", {
      durable: true,
      arguments: {
        "x-message-ttl": 1000,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "emails",
      },
    });
  });

  it("does NOT assert retry queue when retries=0 (backwards compat)", async () => {
    await rabbit.subscribe({ queue: "emails", handler: vi.fn() });
    expect(mockChannel.assertQueue).toHaveBeenCalledTimes(1);
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails", { durable: true });
  });

  // ── Successful processing ─────────────────────────────────────────────────

  it("acks immediately on successful handler", async () => {
    const handler = vi.fn().mockResolvedValue(undefined);
    await rabbit.subscribe({ queue: "emails", retries: 3, retryDelay: 1000, handler });

    await getConsumeCallback()!(makeMsg({ to: "a@b.com" }));

    expect(handler).toHaveBeenCalledWith({ to: "a@b.com" });
    expect(mockChannel.ack).toHaveBeenCalled();
    expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
  });

  // ── Retry attempt (retries remaining) ────────────────────────────────────

  it("publishes to retry queue and acks original on first failure", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("smtp down"));
    await rabbit.subscribe({ queue: "emails", retries: 3, retryDelay: 1000, handler });

    const msg = makeMsg({ to: "a@b.com" }, 0);
    await getConsumeCallback()!(msg);

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

  it("increments retry count on each attempt", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("fail"));
    await rabbit.subscribe({ queue: "emails", retries: 3, retryDelay: 1000, handler });

    const msg = makeMsg({ to: "a@b.com" }, 2);
    await getConsumeCallback()!(msg);

    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails.retry",
      expect.anything(),
      expect.objectContaining({
        headers: expect.objectContaining({ "x-retry-count": 3 }),
      }),
    );
  });

  // ── Message returns to main queue ─────────────────────────────────────────

  it("processes message normally when it returns from retry queue", async () => {
    const handler = vi.fn().mockResolvedValue(undefined);
    await rabbit.subscribe({ queue: "emails", retries: 3, retryDelay: 1000, handler });

    const returnedMsg = makeMsg({ to: "a@b.com" }, 1);
    await getConsumeCallback()!(returnedMsg);

    expect(handler).toHaveBeenCalledWith({ to: "a@b.com" });
    expect(mockChannel.ack).toHaveBeenCalled();
    expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
  });

  // ── Retry exhaustion — consumer stability ────────────────────────────────

  it("nacks the message and resolves (no throw) when max retries are exhausted", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("persistent failure"));
    await rabbit.subscribe({ queue: "emails", retries: 3, retryDelay: 1000, handler });

    const msg = makeMsg({ to: "a@b.com" }, 3);
    // Must resolve, not reject — the consumer callback must never throw
    await expect(getConsumeCallback()!(msg)).resolves.toBeUndefined();

    expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(mockChannel.ack).not.toHaveBeenCalled();
    expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
  });

  it("consumer continues processing after retry exhaustion", async () => {
    let callCount = 0;
    const handler = vi.fn().mockImplementation(() => {
      callCount++;
      // First message always fails (simulates permanent failure at max retries)
      if (callCount === 1) return Promise.reject(new Error("permanent"));
      return Promise.resolve();
    });

    await rabbit.subscribe({ queue: "emails", retries: 3, retryDelay: 1000, handler });
    const cb = getConsumeCallback()!;

    // Message A: at max retries — exhausted
    await cb(makeMsg({ id: "A" }, 3));
    expect(mockChannel.nack).toHaveBeenCalledTimes(1);

    vi.clearAllMocks();

    // Message B: should be processed successfully
    await cb(makeMsg({ id: "B" }));
    expect(handler).toHaveBeenCalledWith({ id: "B" });
    expect(mockChannel.ack).toHaveBeenCalledTimes(1);
    expect(mockChannel.nack).not.toHaveBeenCalled();
  });

  it("consumer continues after multiple consecutive permanent failures", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("always fails"));
    await rabbit.subscribe({ queue: "emails", retries: 1, retryDelay: 100, handler });
    const cb = getConsumeCallback()!;

    // Three exhausted messages — each must resolve without throwing
    await expect(cb(makeMsg({ id: 1 }, 1))).resolves.toBeUndefined();
    await expect(cb(makeMsg({ id: 2 }, 1))).resolves.toBeUndefined();
    await expect(cb(makeMsg({ id: 3 }, 1))).resolves.toBeUndefined();

    expect(mockChannel.nack).toHaveBeenCalledTimes(3);
  });

  // ── Invalid JSON — consumer stability ────────────────────────────────────

  it("nacks and resolves (no throw) on deserialization failure", async () => {
    await rabbit.subscribe({ queue: "emails", handler: vi.fn() });

    const badMsg: ConsumeMessage = {
      content: Buffer.from("not-json{{"),
      fields: {} as ConsumeMessage["fields"],
      properties: { headers: {} } as ConsumeMessage["properties"],
    };

    await expect(getConsumeCallback()!(badMsg)).resolves.toBeUndefined();
    expect(mockChannel.nack).toHaveBeenCalledWith(badMsg, false, false);
  });

  // ── Default retryDelay ────────────────────────────────────────────────────

  it("uses 5000ms as default retryDelay when not specified", async () => {
    await rabbit.subscribe({ queue: "emails", retries: 1, handler: vi.fn() });

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails.retry", {
      durable: true,
      arguments: expect.objectContaining({ "x-message-ttl": 5000 }),
    });
  });
});
