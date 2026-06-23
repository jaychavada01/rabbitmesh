/**
 * Integration tests — Dead Letter Queue (DLQ) support.
 * Covers all 6 required scenarios with amqplib fully mocked.
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

describe("RabbitMesh DLQ integration", () => {
  let rabbit: RabbitMesh;

  beforeEach(async () => {
    vi.clearAllMocks();
    setConsumeCallback(null);
    // restore defaults after clearAllMocks
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

  // ── Test 1: DLQ queue creation ────────────────────────────────────────────

  it("asserts emails, emails.retry, and emails.dlq queues", async () => {
    await rabbit.subscribe({
      queue: "emails",
      retries: 3,
      retryDelay: 1000,
      dlq: { enabled: true },
      handler: vi.fn(),
    });

    const assertedQueues = vi.mocked(mockChannel.assertQueue).mock.calls.map((c) => c[0]);
    expect(assertedQueues).toContain("emails");
    expect(assertedQueues).toContain("emails.retry");
    expect(assertedQueues).toContain("emails.dlq");
  });

  // ── Test 2: message moved to DLQ after retries exhausted ─────────────────

  it("moves message to DLQ after max retries — nacks original, sends to DLQ", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("SMTP timeout"));
    await rabbit.subscribe({
      queue: "emails",
      retries: 3,
      retryDelay: 1000,
      dlq: { enabled: true },
      handler,
    });

    // Deliver message at retry count 3 (exhausted)
    const msg = makeMsg({ email: "user@test.com" }, 3);
    await getConsumeCallback()!(msg);

    // RetryHandler nacks the original; DLQHandler publishes a copy
    expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails.dlq",
      expect.any(Buffer),
      { persistent: true },
    );
    // ack must NOT be called — original is already nacked
    expect(mockChannel.ack).not.toHaveBeenCalled();
  });

  // ── Test 3: DLQ message metadata ─────────────────────────────────────────

  it("preserves payload, error, retryCount, failedAt, originalQueue in DLQ message", async () => {
    const error = new Error("SMTP timeout");
    const handler = vi.fn().mockRejectedValue(error);
    await rabbit.subscribe({
      queue: "emails",
      retries: 3,
      retryDelay: 1000,
      dlq: { enabled: true },
      handler,
    });

    const msg = makeMsg({ email: "user@test.com" }, 3);
    await getConsumeCallback()!(msg);

    const [, contentBuf] = vi.mocked(mockChannel.sendToQueue).mock.calls.find(
      ([q]) => q === "emails.dlq",
    )!;
    const dlqMsg = JSON.parse((contentBuf as Buffer).toString());

    expect(dlqMsg).toMatchObject({
      payload: { email: "user@test.com" },
      error: "SMTP timeout",
      retryCount: 3,
      originalQueue: "emails",
    });
    expect(typeof dlqMsg.failedAt).toBe("string");
    expect(() => new Date(dlqMsg.failedAt)).not.toThrow();
  });

  // ── Test 4: consumer stability after DLQ routing ─────────────────────────

  it("continues processing Message B after Message A is routed to DLQ", async () => {
    let call = 0;
    const handler = vi.fn().mockImplementation(() => {
      call++;
      if (call === 1) return Promise.reject(new Error("permanent"));
      return Promise.resolve();
    });

    await rabbit.subscribe({
      queue: "emails",
      retries: 3,
      retryDelay: 1000,
      dlq: { enabled: true },
      handler,
    });

    const cb = getConsumeCallback()!;

    // Message A: exhausted → DLQ (nacked by RetryHandler, copy sent to DLQ)
    await cb(makeMsg({ id: "A" }, 3));
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith("emails.dlq", expect.any(Buffer), expect.anything());
    expect(mockChannel.nack).toHaveBeenCalledTimes(1);
    expect(mockChannel.ack).not.toHaveBeenCalled();

    vi.clearAllMocks();
    vi.mocked(mockChannel.sendToQueue).mockReturnValue(true);

    // Message B: should process normally
    await cb(makeMsg({ id: "B" }));
    expect(handler).toHaveBeenCalledWith({ id: "B" });
    expect(mockChannel.ack).toHaveBeenCalledTimes(1);
    expect(mockChannel.sendToQueue).not.toHaveBeenCalledWith("emails.dlq", expect.anything(), expect.anything());
  });

  // ── Test 5: custom DLQ name ───────────────────────────────────────────────

  it("creates and routes to custom DLQ name when queueName is specified", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("fail"));
    await rabbit.subscribe({
      queue: "emails",
      retries: 3,
      retryDelay: 1000,
      dlq: { enabled: true, queueName: "emails.dead" },
      handler,
    });

    const assertedQueues = vi.mocked(mockChannel.assertQueue).mock.calls.map((c) => c[0]);
    expect(assertedQueues).toContain("emails.dead");
    expect(assertedQueues).not.toContain("emails.dlq");

    const msg = makeMsg({ email: "user@test.com" }, 3);
    await getConsumeCallback()!(msg);

    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails.dead",
      expect.any(Buffer),
      { persistent: true },
    );
  });

  // ── Test 6: DLQ disabled preserves v0.2.0 behavior ───────────────────────

  it("nacks (no DLQ) when dlq.enabled is false", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("fail"));
    await rabbit.subscribe({
      queue: "emails",
      retries: 3,
      retryDelay: 1000,
      dlq: { enabled: false },
      handler,
    });

    const assertedQueues = vi.mocked(mockChannel.assertQueue).mock.calls.map((c) => c[0]);
    expect(assertedQueues).not.toContain("emails.dlq");

    const msg = makeMsg({ email: "user@test.com" }, 3);
    await getConsumeCallback()!(msg);

    expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(mockChannel.sendToQueue).not.toHaveBeenCalledWith("emails.dlq", expect.anything(), expect.anything());
  });
});
