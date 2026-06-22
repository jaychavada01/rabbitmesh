/**
 * Integration tests for RabbitMesh.
 * The full stack (ConnectionManager → Publisher/Subscriber) is exercised
 * with amqplib mocked so no real broker is required.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { RabbitMesh } from "../../src/core/rabbitmesh.js";
import { ConnectionError, PublishError } from "../../src/utils/errors.js";
import type { ConsumeMessage } from "amqplib";

// ---- hoisted mocks ----------------------------------------------------------

const { mockChannel, mockConnection, getConsumeCallback, setConsumeCallback } = vi.hoisted(() => {
  let _consumeCallback: ((msg: ConsumeMessage | null) => Promise<void>) | null = null;

  const mockChannel = {
    assertQueue: vi.fn().mockResolvedValue({}),
    sendToQueue: vi.fn().mockReturnValue(true),
    consume: vi.fn().mockImplementation(
      (_queue: string, cb: (msg: ConsumeMessage | null) => Promise<void>) => {
        _consumeCallback = cb;
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
    getConsumeCallback: () => _consumeCallback,
    setConsumeCallback: (cb: ((msg: ConsumeMessage | null) => Promise<void>) | null) => {
      _consumeCallback = cb;
    },
  };
});

vi.mock("amqplib", () => ({
  default: { connect: vi.fn().mockResolvedValue(mockConnection) },
}));

// ---- helpers ----------------------------------------------------------------

function makeMsg(body: unknown): ConsumeMessage {
  return {
    content: Buffer.from(JSON.stringify(body)),
    fields: {} as ConsumeMessage["fields"],
    properties: {} as ConsumeMessage["properties"],
  };
}

// ---- tests ------------------------------------------------------------------

describe("RabbitMesh (integration)", () => {
  let rabbit: RabbitMesh;

  beforeEach(() => {
    vi.clearAllMocks();
    setConsumeCallback(null);
    rabbit = new RabbitMesh({ url: "amqp://localhost" });
  });

  afterEach(async () => {
    await rabbit.disconnect();
  });

  it("connects, publishes, and disconnects", async () => {
    await rabbit.connect();
    await rabbit.publish({ queue: "orders", payload: { id: 99 } });

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("orders", { durable: true });
    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "orders",
      Buffer.from(JSON.stringify({ id: 99 })),
      { persistent: true },
    );
  });

  it("connects, subscribes, and delivers messages to handler", async () => {
    await rabbit.connect();

    const handler = vi.fn().mockResolvedValue(undefined);
    await rabbit.subscribe({ queue: "orders", handler });

    await getConsumeCallback()!(makeMsg({ id: 99 }));

    expect(handler).toHaveBeenCalledWith({ id: 99 });
    expect(mockChannel.ack).toHaveBeenCalled();
  });

  it("throws PublishError when publishing without connect", async () => {
    await expect(rabbit.publish({ queue: "q", payload: {} })).rejects.toThrow(PublishError);
  });

  it("throws ConnectionError when RabbitMQ is unavailable", async () => {
    const { default: amqplib } = await import("amqplib");
    vi.mocked(amqplib.connect).mockRejectedValueOnce(new Error("ECONNREFUSED"));

    const failRabbit = new RabbitMesh({ url: "amqp://bad-host" });
    await expect(failRabbit.connect()).rejects.toThrow(ConnectionError);
  });

  it("applies default config and instantiates without error", () => {
    expect(new RabbitMesh({ url: "amqp://localhost" })).toBeDefined();
  });
});
