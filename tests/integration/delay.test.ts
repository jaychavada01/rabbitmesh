/**
 * Integration tests — Delayed Message support.
 * Covers all 8 required scenarios with amqplib fully mocked.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { RabbitMesh } from "../../src/core/rabbitmesh.js";
import { DelayError } from "../../src/utils/errors.js";
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
    setConsumeCallback: (cb: ((msg: ConsumeMessage | null) => Promise<void>) | null) => { _cb = cb; },
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

describe("RabbitMesh Delay integration", () => {
  let rabbit: RabbitMesh;

  beforeEach(async () => {
    vi.clearAllMocks();
    setConsumeCallback(null);
    vi.mocked(mockChannel.assertQueue).mockResolvedValue({} as never);
    vi.mocked(mockChannel.sendToQueue).mockReturnValue(true);
    vi.mocked(mockChannel.close).mockResolvedValue(undefined);
    vi.mocked(mockConnection.createChannel).mockResolvedValue(mockChannel);
    vi.mocked(mockConnection.close).mockResolvedValue(undefined);
    rabbit = new RabbitMesh({ url: "amqp://localhost" });
    await rabbit.connect();
  });

  afterEach(async () => {
    await rabbit.disconnect();
  });

  // ── Test 1: Immediate publish ─────────────────────────────────────────────

  it("Test 1 — immediate publish: routes directly to the target queue", async () => {
    await rabbit.publish({ queue: "emails", payload: { id: 1 } });

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails", { durable: true });
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails",
      Buffer.from(JSON.stringify({ id: 1 })),
      { persistent: true },
    );
    // No delay queue must be asserted
    const assertedQueues = vi.mocked(mockChannel.assertQueue).mock.calls.map((c) => c[0]);
    expect(assertedQueues.some((q) => q.includes(".delay."))).toBe(false);
  });

  // ── Test 2: Delayed publish ───────────────────────────────────────────────

  it("Test 2 — delayed publish: routes message through delay queue with correct TTL", async () => {
    await rabbit.publish({ queue: "emails", payload: { id: 2 }, delay: 5000 });

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails.delay.5000", {
      durable: true,
      arguments: {
        "x-message-ttl": 5000,
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": "emails",
      },
    });
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails.delay.5000",
      Buffer.from(JSON.stringify({ id: 2 })),
      { persistent: true },
    );
  });

  // ── Test 3: Queue reuse ───────────────────────────────────────────────────

  it("Test 3 — queue reuse: assertQueue called twice for same delay (idempotent)", async () => {
    await rabbit.publish({ queue: "emails", payload: { id: 1 }, delay: 10000 });
    await rabbit.publish({ queue: "emails", payload: { id: 2 }, delay: 10000 });

    const delayQueueCalls = vi.mocked(mockChannel.assertQueue).mock.calls.filter(
      ([q]) => q === "emails.delay.10000",
    );
    // assertQueue is called each time (RabbitMQ handles idempotency natively)
    expect(delayQueueCalls.length).toBe(2);
    // But both messages go to the same delay queue
    const sendCalls = vi.mocked(mockChannel.sendToQueue).mock.calls.filter(
      ([q]) => q === "emails.delay.10000",
    );
    expect(sendCalls.length).toBe(2);
  });

  // ── Test 4: Multiple delay queues ────────────────────────────────────────

  it("Test 4 — multiple delay queues: creates distinct queue per delay value", async () => {
    await rabbit.publish({ queue: "emails", payload: { a: 1 }, delay: 5000 });
    await rabbit.publish({ queue: "emails", payload: { b: 2 }, delay: 10000 });
    await rabbit.publish({ queue: "emails", payload: { c: 3 }, delay: 60000 });

    const assertedQueues = vi.mocked(mockChannel.assertQueue).mock.calls.map(([q]) => q);
    expect(assertedQueues).toContain("emails.delay.5000");
    expect(assertedQueues).toContain("emails.delay.10000");
    expect(assertedQueues).toContain("emails.delay.60000");
  });

  // ── Test 5: Delayed + Retry ───────────────────────────────────────────────

  it("Test 5 — delayed + retry: consumer retries delayed message on handler failure", async () => {
    let attempt = 0;
    const handler = vi.fn().mockImplementation(() => {
      attempt++;
      if (attempt < 2) return Promise.reject(new Error("transient"));
      return Promise.resolve();
    });

    await rabbit.subscribe({ queue: "emails", retries: 3, retryDelay: 1000, handler });

    // Simulate TTL expiry → RabbitMQ delivers to "emails" queue
    const cb = getConsumeCallback()!;

    // First delivery (attempt 1): handler fails → retry
    const msg = makeMsg({ id: "delayed-msg" }, 0);
    await cb(msg);
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith("emails.retry", expect.any(Buffer), expect.objectContaining({
      headers: expect.objectContaining({ "x-retry-count": 1 }),
    }));
    expect(mockChannel.ack).toHaveBeenCalledWith(msg);

    vi.clearAllMocks();
    vi.mocked(mockChannel.sendToQueue).mockReturnValue(true);

    // Second delivery (attempt 2, from retry queue): handler succeeds
    const retryMsg = makeMsg({ id: "delayed-msg" }, 1);
    await cb(retryMsg);
    expect(mockChannel.ack).toHaveBeenCalledWith(retryMsg);
    expect(mockChannel.nack).not.toHaveBeenCalled();
  });

  // ── Test 6: Delayed + DLQ ────────────────────────────────────────────────

  it("Test 6 — delayed + DLQ: delayed message exceeding retries is routed to DLQ", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("unrecoverable"));
    await rabbit.subscribe({
      queue: "emails",
      retries: 3,
      retryDelay: 1000,
      dlq: { enabled: true },
      handler,
    });

    // Simulate message after TTL expiry, already at retry limit
    const msg = makeMsg({ id: "delayed-dlq-msg" }, 3);
    await getConsumeCallback()!(msg);

    expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails.dlq",
      expect.any(Buffer),
      { persistent: true },
    );

    const [, dlqBuf] = vi.mocked(mockChannel.sendToQueue).mock.calls.find(([q]) => q === "emails.dlq")!;
    const dlqMsg = JSON.parse((dlqBuf as Buffer).toString());
    expect(dlqMsg.payload).toEqual({ id: "delayed-dlq-msg" });
    expect(dlqMsg.originalQueue).toBe("emails");
  });

  // ── Test 7: Restart safety ────────────────────────────────────────────────

  it("Test 7 — restart safety: message sent to delay queue is persistent", async () => {
    await rabbit.publish({ queue: "emails", payload: { id: 7 }, delay: 30000 });

    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails.delay.30000",
      expect.any(Buffer),
      expect.objectContaining({ persistent: true }),
    );
    // Delay queue itself is durable
    expect(mockChannel.assertQueue).toHaveBeenCalledWith(
      "emails.delay.30000",
      expect.objectContaining({ durable: true }),
    );
  });

  // ── Test 8: Validation ────────────────────────────────────────────────────

  describe("Test 8 — validation: invalid delays throw DelayError without touching RabbitMQ", () => {
    it.each([-1, 0, NaN, Infinity])("delay = %s throws DelayError", async (delay) => {
      await expect(
        rabbit.publish({ queue: "emails", payload: {}, delay }),
      ).rejects.toThrow(DelayError);

      expect(mockChannel.assertQueue).not.toHaveBeenCalled();
      expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
    });
  });

  // ── Backward compatibility ────────────────────────────────────────────────

  it("backward compat: existing publish without delay is unchanged", async () => {
    await rabbit.publish({ queue: "orders", payload: { orderId: "ord-001" } });

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("orders", { durable: true });
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "orders",
      Buffer.from(JSON.stringify({ orderId: "ord-001" })),
      { persistent: true },
    );
  });
});
