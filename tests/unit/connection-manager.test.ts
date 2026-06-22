import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { ConnectionManager } from "../../src/core/connection-manager.js";
import { ConnectionError } from "../../src/utils/errors.js";
import type { ResolvedConfig } from "../../src/types/index.js";

// ---------- hoisted mocks ----------------------------------------------------

const { mockChannel, mockConnection, listeners } = vi.hoisted(() => {
  const listeners: Record<string, ((...args: unknown[]) => void)[]> = {};

  const mockChannel = {
    close: vi.fn().mockResolvedValue(undefined),
    assertQueue: vi.fn().mockResolvedValue({}),
    sendToQueue: vi.fn().mockReturnValue(true),
    consume: vi.fn().mockResolvedValue({}),
    ack: vi.fn(),
    nack: vi.fn(),
  };

  const mockConnection = {
    createChannel: vi.fn().mockResolvedValue(mockChannel),
    close: vi.fn().mockResolvedValue(undefined),
    on: vi.fn((event: string, cb: (...args: unknown[]) => void) => {
      listeners[event] = listeners[event] ?? [];
      listeners[event].push(cb);
    }),
  };

  return { mockChannel, mockConnection, listeners };
});

vi.mock("amqplib", () => ({
  default: {
    connect: vi.fn().mockResolvedValue(mockConnection),
  },
}));

// ---------- config -----------------------------------------------------------

const config: ResolvedConfig = {
  url: "amqp://localhost",
  reconnect: true,
  reconnectInterval: 100,
  reconnectMaxAttempts: 3,
};

// ---------- tests ------------------------------------------------------------

describe("ConnectionManager", () => {
  let manager: ConnectionManager;

  beforeEach(async () => {
    vi.clearAllMocks();
    Object.keys(listeners).forEach((k) => delete listeners[k]);
    // Restore default after clearAllMocks wipes implementations
    const { default: amqplib } = await import("amqplib");
    vi.mocked(amqplib.connect).mockResolvedValue(mockConnection);
    vi.mocked(mockConnection.createChannel).mockResolvedValue(mockChannel);
    vi.mocked(mockChannel.close).mockResolvedValue(undefined);
    vi.mocked(mockConnection.close).mockResolvedValue(undefined);
    manager = new ConnectionManager(config);
  });

  afterEach(async () => {
    await manager.disconnect().catch(() => undefined);
  });

  it("connects and exposes a channel", async () => {
    await manager.connect();
    expect(manager.getChannel()).toBe(mockChannel);
  });

  it("throws ConnectionError when amqplib.connect rejects", async () => {
    const { default: amqplib } = await import("amqplib");
    vi.mocked(amqplib.connect).mockRejectedValueOnce(new Error("ECONNREFUSED"));

    await expect(manager.connect()).rejects.toThrow(ConnectionError);
  });

  it("throws ConnectionError on getChannel before connect", () => {
    expect(() => manager.getChannel()).toThrow(ConnectionError);
  });

  it("disconnects cleanly", async () => {
    await manager.connect();
    await manager.disconnect();
    expect(mockChannel.close).toHaveBeenCalled();
    expect(mockConnection.close).toHaveBeenCalled();
    expect(() => manager.getChannel()).toThrow(ConnectionError);
  });

  it("schedules reconnect on connection close event", async () => {
    vi.useFakeTimers();
    const { default: amqplib } = await import("amqplib");

    await manager.connect();
    listeners["close"]?.[0]?.();

    await vi.advanceTimersByTimeAsync(150);
    expect(amqplib.connect).toHaveBeenCalledTimes(2);
    vi.useRealTimers();
  });

  it("stops reconnecting after max attempts", async () => {
    vi.useFakeTimers();
    const { default: amqplib } = await import("amqplib");

    vi.mocked(amqplib.connect)
      .mockResolvedValueOnce(mockConnection)
      .mockRejectedValue(new Error("refused"));

    await manager.connect();
    listeners["close"]?.[0]?.();

    for (let i = 0; i < 4; i++) {
      await vi.advanceTimersByTimeAsync(150);
    }

    // 1 initial + 3 retries
    expect(amqplib.connect).toHaveBeenCalledTimes(4);
    vi.useRealTimers();
  });

  it("does not reconnect when reconnect is false", async () => {
    const noReconnectManager = new ConnectionManager({ ...config, reconnect: false });
    const { default: amqplib } = await import("amqplib");
    vi.useFakeTimers();

    // Restore default resolved value in case a previous test left a rejection queued
    vi.mocked(amqplib.connect).mockResolvedValue(mockConnection);

    await noReconnectManager.connect();
    listeners["close"]?.[0]?.();
    await vi.advanceTimersByTimeAsync(200);

    expect(amqplib.connect).toHaveBeenCalledTimes(1);
    vi.useRealTimers();
    await noReconnectManager.disconnect();
  });
});
