/**
 * Integration tests — Exchange Support (v0.5.0).
 * Tests 1–8 as specified. amqplib fully mocked.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { RabbitMesh } from "../../src/core/rabbitmesh.js";
import { ValidationError } from "../../src/utils/errors.js";
import type { ConsumeMessage } from "amqplib";

// ---- hoisted mocks ----------------------------------------------------------

const {
  mockChannel,
  mockConnection,
  getConsumeCallback,
  setConsumeCallback,
  getConnectionOnHandlers,
  triggerConnectionEvent,
} = vi.hoisted(() => {
  const callbacks: Record<string, (() => void)[]> = {};
  let _cb: ((msg: ConsumeMessage | null) => Promise<void>) | null = null;

  const mockChannel = {
    assertQueue: vi.fn().mockResolvedValue({}),
    assertExchange: vi.fn().mockResolvedValue({}),
    bindQueue: vi.fn().mockResolvedValue({}),
    sendToQueue: vi.fn().mockReturnValue(true),
    publish: vi.fn().mockReturnValue(true),
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
    on: vi.fn().mockImplementation((event: string, handler: () => void) => {
      if (!callbacks[event]) callbacks[event] = [];
      callbacks[event].push(handler);
    }),
  };

  return {
    mockChannel,
    mockConnection,
    getConsumeCallback: () => _cb,
    setConsumeCallback: (cb: ((msg: ConsumeMessage | null) => Promise<void>) | null) => { _cb = cb; },
    getConnectionOnHandlers: (event: string) => callbacks[event] ?? [],
    triggerConnectionEvent: (event: string) => { (callbacks[event] ?? []).forEach(h => h()); },
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

describe("RabbitMesh Exchange integration", () => {
  let rabbit: RabbitMesh;

  beforeEach(async () => {
    vi.clearAllMocks();
    setConsumeCallback(null);
    vi.mocked(mockChannel.assertQueue).mockResolvedValue({} as never);
    vi.mocked(mockChannel.assertExchange).mockResolvedValue({} as never);
    vi.mocked(mockChannel.bindQueue).mockResolvedValue({} as never);
    vi.mocked(mockChannel.sendToQueue).mockReturnValue(true);
    vi.mocked(mockChannel.publish).mockReturnValue(true);
    vi.mocked(mockChannel.close).mockResolvedValue(undefined);
    vi.mocked(mockConnection.createChannel).mockResolvedValue(mockChannel);
    vi.mocked(mockConnection.close).mockResolvedValue(undefined);
    rabbit = new RabbitMesh({ url: "amqp://localhost" });
    await rabbit.connect();
  });

  afterEach(async () => {
    await rabbit.disconnect();
  });

  // ── Test 1: Direct Exchange ───────────────────────────────────────────────

  it("Test 1 — Direct Exchange: publisher asserts exchange and consumer binds queue", async () => {
    // Subscriber setup
    const handler = vi.fn().mockResolvedValue(undefined);
    await rabbit.subscribe({
      queue: "email-queue",
      exchange: "notifications",
      exchangeType: "direct",
      routingKey: "email",
      handler,
    });

    expect(mockChannel.assertExchange).toHaveBeenCalledWith("notifications", "direct", { durable: true });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("email-queue", { durable: true });
    expect(mockChannel.bindQueue).toHaveBeenCalledWith("email-queue", "notifications", "email");
    expect(mockChannel.consume).toHaveBeenCalledWith("email-queue", expect.any(Function));

    // Publisher
    vi.clearAllMocks();
    vi.mocked(mockChannel.assertExchange).mockResolvedValue({} as never);
    vi.mocked(mockChannel.publish).mockReturnValue(true);

    await rabbit.publish({
      exchange: "notifications",
      exchangeType: "direct",
      routingKey: "email",
      payload: { to: "user@example.com" },
    });

    expect(mockChannel.assertExchange).toHaveBeenCalledWith("notifications", "direct", { durable: true });
    expect(mockChannel.publish).toHaveBeenCalledWith(
      "notifications",
      "email",
      Buffer.from(JSON.stringify({ to: "user@example.com" })),
      { persistent: true },
    );

    // Deliver message to consumer
    await getConsumeCallback()!(makeMsg({ to: "user@example.com" }));
    expect(handler).toHaveBeenCalledWith({ to: "user@example.com" });
    expect(mockChannel.ack).toHaveBeenCalled();
  });

  // ── Test 2: Fanout Exchange ───────────────────────────────────────────────

  it("Test 2 — Fanout Exchange: both consumers receive the message", async () => {
    const handlerA = vi.fn().mockResolvedValue(undefined);
    const handlerB = vi.fn().mockResolvedValue(undefined);

    // Consumer A
    let cbA: ((msg: ConsumeMessage | null) => Promise<void>) | null = null;
    vi.mocked(mockChannel.consume).mockImplementationOnce((_q, cb) => {
      cbA = cb as any;
      return Promise.resolve({ consumerTag: "tagA" }) as any;
    });
    await rabbit.subscribe({ queue: "queue-a", exchange: "events", exchangeType: "fanout", handler: handlerA });

    // Consumer B
    let cbB: ((msg: ConsumeMessage | null) => Promise<void>) | null = null;
    vi.mocked(mockChannel.consume).mockImplementationOnce((_q, cb) => {
      cbB = cb as any;
      return Promise.resolve({ consumerTag: "tagB" }) as any;
    });
    await rabbit.subscribe({ queue: "queue-b", exchange: "events", exchangeType: "fanout", handler: handlerB });

    expect(mockChannel.bindQueue).toHaveBeenCalledWith("queue-a", "events", "");
    expect(mockChannel.bindQueue).toHaveBeenCalledWith("queue-b", "events", "");

    // Simulate fanout delivery to both
    const msg = makeMsg({ event: "ping" });
    await cbA!(msg);
    await cbB!(msg);

    expect(handlerA).toHaveBeenCalledWith({ event: "ping" });
    expect(handlerB).toHaveBeenCalledWith({ event: "ping" });
    expect(mockChannel.ack).toHaveBeenCalledTimes(2);
  });

  // ── Test 3: Topic Exchange — pattern match ────────────────────────────────

  it("Test 3 — Topic Exchange: consumer with 'user.*' receives 'user.created' message", async () => {
    const handler = vi.fn().mockResolvedValue(undefined);
    await rabbit.subscribe({
      queue: "user-svc",
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.*",
      handler,
    });

    expect(mockChannel.bindQueue).toHaveBeenCalledWith("user-svc", "events", "user.*");

    // Publish user.created
    await rabbit.publish({
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.created",
      payload: { userId: 1 },
    });

    expect(mockChannel.publish).toHaveBeenCalledWith(
      "events",
      "user.created",
      expect.any(Buffer),
      { persistent: true },
    );

    // Simulate RabbitMQ routing the message to the bound queue
    await getConsumeCallback()!(makeMsg({ userId: 1 }));
    expect(handler).toHaveBeenCalledWith({ userId: 1 });
  });

  // ── Test 4: Multiple Routing Keys ────────────────────────────────────────

  it("Test 4 — Multiple Routing Keys: user.* consumer receives created/updated/deleted", async () => {
    const handler = vi.fn().mockResolvedValue(undefined);
    await rabbit.subscribe({
      queue: "user-svc",
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.*",
      handler,
    });

    const cb = getConsumeCallback()!;

    await cb(makeMsg({ event: "user.created" }));
    await cb(makeMsg({ event: "user.updated" }));
    await cb(makeMsg({ event: "user.deleted" }));

    expect(handler).toHaveBeenCalledTimes(3);
    expect(handler).toHaveBeenNthCalledWith(1, { event: "user.created" });
    expect(handler).toHaveBeenNthCalledWith(2, { event: "user.updated" });
    expect(handler).toHaveBeenNthCalledWith(3, { event: "user.deleted" });
    expect(mockChannel.ack).toHaveBeenCalledTimes(3);
  });

  // ── Test 5: Retry Integration ─────────────────────────────────────────────

  it("Test 5 — Retry: exchange-bound queue retries on handler failure then succeeds", async () => {
    let attempt = 0;
    const handler = vi.fn().mockImplementation(() => {
      attempt++;
      if (attempt < 2) return Promise.reject(new Error("transient"));
      return Promise.resolve();
    });

    await rabbit.subscribe({
      queue: "email-svc",
      exchange: "notifications",
      exchangeType: "direct",
      routingKey: "email",
      retries: 3,
      retryDelay: 1000,
      handler,
    });

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("email-svc.retry", expect.objectContaining({
      arguments: expect.objectContaining({ "x-dead-letter-routing-key": "email-svc" }),
    }));

    const cb = getConsumeCallback()!;
    const msg = makeMsg({ to: "user@example.com" }, 0);

    // First delivery: fails → retried
    await cb(msg);
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "email-svc.retry",
      expect.any(Buffer),
      expect.objectContaining({ headers: expect.objectContaining({ "x-retry-count": 1 }) }),
    );
    expect(mockChannel.ack).toHaveBeenCalledWith(msg);

    vi.clearAllMocks();
    vi.mocked(mockChannel.sendToQueue).mockReturnValue(true);

    // Second delivery: succeeds
    const retryMsg = makeMsg({ to: "user@example.com" }, 1);
    await cb(retryMsg);
    expect(mockChannel.ack).toHaveBeenCalledWith(retryMsg);
    expect(mockChannel.nack).not.toHaveBeenCalled();
  });

  // ── Test 6: DLQ Integration ───────────────────────────────────────────────

  it("Test 6 — DLQ: exchange message exhausting retries routes to DLQ", async () => {
    const handler = vi.fn().mockRejectedValue(new Error("unrecoverable"));
    await rabbit.subscribe({
      queue: "email-svc",
      exchange: "notifications",
      exchangeType: "direct",
      routingKey: "email",
      retries: 3,
      retryDelay: 1000,
      dlq: { enabled: true },
      handler,
    });

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("email-svc.dlq", { durable: true });

    // Simulate message at retry limit
    const msg = makeMsg({ to: "user@example.com" }, 3);
    await getConsumeCallback()!(msg);

    expect(mockChannel.nack).toHaveBeenCalledWith(msg, false, false);
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "email-svc.dlq",
      expect.any(Buffer),
      { persistent: true },
    );

    const [, dlqBuf] = vi.mocked(mockChannel.sendToQueue).mock.calls.find(([q]) => q === "email-svc.dlq")!;
    const dlqMsg = JSON.parse((dlqBuf as Buffer).toString());
    expect(dlqMsg.originalQueue).toBe("email-svc");
    expect(dlqMsg.payload).toEqual({ to: "user@example.com" });
  });

  // ── Test 7: Delay Integration ─────────────────────────────────────────────

  it("Test 7 — Delay: delayed exchange publish routes via delay queue with correct DLR to exchange", async () => {
    const handler = vi.fn().mockResolvedValue(undefined);
    await rabbit.subscribe({
      queue: "user-svc",
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.*",
      handler,
    });

    vi.clearAllMocks();
    vi.mocked(mockChannel.assertExchange).mockResolvedValue({} as never);
    vi.mocked(mockChannel.assertQueue).mockResolvedValue({} as never);
    vi.mocked(mockChannel.sendToQueue).mockReturnValue(true);

    await rabbit.publish({
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.created",
      payload: { userId: 1 },
      delay: 60000,
    });

    // Must assert exchange first
    expect(mockChannel.assertExchange).toHaveBeenCalledWith("events", "topic", { durable: true });

    // Delay queue must DLR to the exchange, not default exchange
    expect(mockChannel.assertQueue).toHaveBeenCalledWith(
      expect.stringContaining("events.delay.60000"),
      expect.objectContaining({
        durable: true,
        arguments: expect.objectContaining({
          "x-message-ttl": 60000,
          "x-dead-letter-exchange": "events",
          "x-dead-letter-routing-key": "user.created",
        }),
      }),
    );
    expect(mockChannel.sendToQueue).toHaveBeenCalled();

    // Simulate TTL expiry — RabbitMQ delivers to bound queue
    await getConsumeCallback()!(makeMsg({ userId: 1 }));
    expect(handler).toHaveBeenCalledWith({ userId: 1 });
  });

  // ── Test 8: Reconnect ─────────────────────────────────────────────────────

  it("Test 8 — Reconnect: consumer reconnects and bindings remain functional", async () => {
    const { default: amqplib } = await import("amqplib");

    const handler = vi.fn().mockResolvedValue(undefined);
    await rabbit.subscribe({
      queue: "user-svc",
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.*",
      handler,
    });

    // Simulate connection close → reconnect
    vi.clearAllMocks();
    vi.mocked(mockChannel.assertQueue).mockResolvedValue({} as never);
    vi.mocked(mockChannel.assertExchange).mockResolvedValue({} as never);
    vi.mocked(mockConnection.createChannel).mockResolvedValue(mockChannel);
    vi.mocked(amqplib.connect).mockResolvedValueOnce(mockConnection as any);

    // Trigger reconnect synchronously (scheduleReconnect fires after interval)
    const reconnectRabbit = new RabbitMesh({ url: "amqp://localhost", reconnectInterval: 1 });
    await reconnectRabbit.connect();

    await reconnectRabbit.subscribe({
      queue: "user-svc",
      exchange: "events",
      exchangeType: "topic",
      routingKey: "user.*",
      handler,
    });

    expect(mockChannel.assertExchange).toHaveBeenCalledWith("events", "topic", { durable: true });
    expect(mockChannel.bindQueue).toHaveBeenCalledWith("user-svc", "events", "user.*");

    await reconnectRabbit.disconnect();
  });

  // ── Backward Compatibility ────────────────────────────────────────────────

  it("backward compat: queue-only publish still works", async () => {
    await rabbit.publish({ queue: "orders", payload: { orderId: "ord-001" } });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("orders", { durable: true });
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "orders",
      Buffer.from(JSON.stringify({ orderId: "ord-001" })),
      { persistent: true },
    );
    expect(mockChannel.assertExchange).not.toHaveBeenCalled();
  });

  it("backward compat: queue-only subscribe still works", async () => {
    const handler = vi.fn();
    await rabbit.subscribe({ queue: "orders", handler });
    expect(mockChannel.assertQueue).toHaveBeenCalledWith("orders", { durable: true });
    expect(mockChannel.assertExchange).not.toHaveBeenCalled();
    expect(mockChannel.bindQueue).not.toHaveBeenCalled();
  });

  // ── Validation ────────────────────────────────────────────────────────────

  it("throws ValidationError for exchange publish without exchangeType", async () => {
    await expect(rabbit.publish({ exchange: "ex", payload: {} })).rejects.toThrow(ValidationError);
  });

  it("throws ValidationError for queue + exchange together", async () => {
    await expect(
      rabbit.publish({ queue: "q", exchange: "ex", exchangeType: "direct", routingKey: "rk", payload: {} }),
    ).rejects.toThrow(ValidationError);
  });
});
