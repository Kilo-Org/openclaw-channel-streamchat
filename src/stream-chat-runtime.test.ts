import { describe, expect, it, vi, beforeEach } from "vitest";
import type { Mock } from "vitest";
import type { ResolvedAccount } from "./types.js";

// Hoist mock creation — must be self-contained (no external imports)
const mockClient = vi.hoisted(() => ({
  connectUser: vi.fn().mockResolvedValue({}),
  disconnectUser: vi.fn().mockResolvedValue({}),
  queryChannels: vi.fn().mockResolvedValue([]),
  channel: vi.fn(),
  on: vi.fn(),
  off: vi.fn(),
}));

const StreamChatSpy = vi.hoisted(() => vi.fn());

vi.mock("stream-chat", () => {
  // Use a real class so that `new StreamChat(...)` works
  const Ctor = function (this: Record<string, unknown>, ...args: unknown[]) {
    StreamChatSpy(...args);
    Object.assign(this, mockClient);
  } as unknown as { new (...args: unknown[]): unknown };
  return { StreamChat: Ctor };
});

import { StreamChatClientRuntime } from "./stream-chat-runtime.js";
import { createMockChannel } from "./test-utils/index.js";

function makeAccount(overrides?: Partial<ResolvedAccount>): ResolvedAccount {
  return {
    accountId: "default",
    enabled: true,
    configured: true,
    apiKey: "test-api-key",
    botUserId: "bot-1",
    botUserToken: "test-token",
    botUserName: "TestBot",
    dmPolicy: "open",
    ackReaction: "eyes",
    doneReaction: "white_check_mark",
    streamingThrottle: 15,
    watchdogTimeoutMs: 120_000,
    watchdogMaxRetries: 0,
    ...overrides,
  };
}

describe("StreamChatClientRuntime", () => {
  let account: ResolvedAccount;

  beforeEach(() => {
    vi.clearAllMocks();
    account = makeAccount();

    // Reset mock defaults
    mockClient.connectUser.mockResolvedValue({});
    mockClient.disconnectUser.mockResolvedValue({});
    mockClient.queryChannels.mockResolvedValue([]);
    mockClient.channel.mockReturnValue(createMockChannel());
  });

  it("creates StreamChat client with correct apiKey", () => {
    new StreamChatClientRuntime(account);
    expect(StreamChatSpy).toHaveBeenCalledWith("test-api-key", {
      allowServerSideConnect: true,
    });
  });

  describe("start", () => {
    it("connects user with correct credentials", async () => {
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      expect(mockClient.connectUser).toHaveBeenCalledWith(
        { id: "bot-1", name: "TestBot" },
        "test-token",
      );
    });

    it("uses botUserId as name when botUserName is not set", async () => {
      account.botUserName = undefined;
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      expect(mockClient.connectUser).toHaveBeenCalledWith(
        { id: "bot-1", name: "bot-1" },
        "test-token",
      );
    });

    it("queries channels with correct filters", async () => {
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      expect(mockClient.queryChannels).toHaveBeenCalledWith(
        { members: { $in: ["bot-1"] } },
        [{ last_message_at: -1 }],
        { watch: true, limit: 30 },
      );
    });

    it("caches returned channels", async () => {
      const ch1 = createMockChannel({ type: "messaging", id: "ch-1" });
      const ch2 = createMockChannel({ type: "messaging", id: "ch-2" });
      mockClient.queryChannels.mockResolvedValue([ch1, ch2]);

      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();

      expect(runtime.getChannel("messaging", "ch-1")).toBe(ch1);
      expect(runtime.getChannel("messaging", "ch-2")).toBe(ch2);
    });

    it("registers notification.added_to_channel handler", async () => {
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      expect(mockClient.on).toHaveBeenCalledWith(
        "notification.added_to_channel",
        expect.any(Function),
      );
    });

    it("sets isConnected to true", async () => {
      const runtime = new StreamChatClientRuntime(account);
      expect(runtime.isConnected()).toBe(false);
      await runtime.start();
      expect(runtime.isConnected()).toBe(true);
    });
  });

  describe("stop", () => {
    it("disconnects user", async () => {
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      await runtime.stop();
      expect(mockClient.disconnectUser).toHaveBeenCalled();
    });

    it("removes notification.added_to_channel handler", async () => {
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      await runtime.stop();
      expect(mockClient.off).toHaveBeenCalledWith(
        "notification.added_to_channel",
        expect.any(Function),
      );
    });

    it("clears channels", async () => {
      const ch = createMockChannel({ type: "messaging", id: "ch-1" });
      mockClient.queryChannels.mockResolvedValue([ch]);
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      expect(runtime.getChannel("messaging", "ch-1")).toBeDefined();
      await runtime.stop();
      expect(runtime.getChannel("messaging", "ch-1")).toBeUndefined();
    });

    it("sets isConnected to false", async () => {
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      await runtime.stop();
      expect(runtime.isConnected()).toBe(false);
    });

    it("is idempotent (no-op when not connected)", async () => {
      const runtime = new StreamChatClientRuntime(account);
      await runtime.stop(); // should not throw
      expect(mockClient.disconnectUser).not.toHaveBeenCalled();
    });
  });

  describe("getClient", () => {
    it("returns the StreamChat instance with expected methods", () => {
      const runtime = new StreamChatClientRuntime(account);
      const client = runtime.getClient();
      // The client has all the mock methods assigned
      expect(client).toHaveProperty("connectUser");
      expect(client).toHaveProperty("disconnectUser");
      expect(client).toHaveProperty("queryChannels");
      expect(client).toHaveProperty("on");
      expect(client).toHaveProperty("off");
    });
  });

  describe("getChannel", () => {
    it("returns cached channel", async () => {
      const ch = createMockChannel({ type: "messaging", id: "ch-1" });
      mockClient.queryChannels.mockResolvedValue([ch]);
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      expect(runtime.getChannel("messaging", "ch-1")).toBe(ch);
    });

    it("returns undefined for non-existent key", () => {
      const runtime = new StreamChatClientRuntime(account);
      expect(runtime.getChannel("messaging", "no-such")).toBeUndefined();
    });
  });

  describe("getOrQueryChannel", () => {
    it("returns cached channel on hit without creating new", async () => {
      const ch = createMockChannel({ type: "messaging", id: "ch-1" });
      mockClient.queryChannels.mockResolvedValue([ch]);
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      mockClient.channel.mockClear();

      const result = await runtime.getOrQueryChannel("messaging", "ch-1");
      expect(result).toBe(ch);
      expect(mockClient.channel).not.toHaveBeenCalled();
    });

    it("creates, watches, and caches new channel on miss", async () => {
      const newCh = createMockChannel({ type: "messaging", id: "new-ch" });
      mockClient.channel.mockReturnValue(newCh);

      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();

      const result = await runtime.getOrQueryChannel("messaging", "new-ch");
      expect(mockClient.channel).toHaveBeenCalledWith("messaging", "new-ch");
      expect((newCh as Record<string, Mock>).watch).toHaveBeenCalled();
      expect(result).toBe(newCh);
      // Should be cached now
      expect(runtime.getChannel("messaging", "new-ch")).toBe(newCh);
    });
  });

  describe("reconnect", () => {
    it("disconnects, creates fresh client, and reconnects", async () => {
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      const oldClient = runtime.getClient();

      mockClient.connectUser.mockClear();
      mockClient.queryChannels.mockClear();
      StreamChatSpy.mockClear();

      await runtime.reconnect();

      // Should have created a new StreamChat instance
      expect(StreamChatSpy).toHaveBeenCalledWith("test-api-key", {
        allowServerSideConnect: true,
      });
      // Should have called connectUser again
      expect(mockClient.connectUser).toHaveBeenCalled();
      expect(runtime.isConnected()).toBe(true);
    });

    it("removes notification.added_to_channel before disconnect", async () => {
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      mockClient.off.mockClear();

      await runtime.reconnect();

      expect(mockClient.off).toHaveBeenCalledWith(
        "notification.added_to_channel",
        expect.any(Function),
      );
    });

    it("clears channels on reconnect", async () => {
      const ch = createMockChannel({ type: "messaging", id: "ch-1" });
      mockClient.queryChannels.mockResolvedValue([ch]);
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();
      expect(runtime.getChannel("messaging", "ch-1")).toBeDefined();

      // After reconnect, mock returns empty — old channels should be cleared
      mockClient.queryChannels.mockResolvedValue([]);
      await runtime.reconnect();
      expect(runtime.getChannel("messaging", "ch-1")).toBeUndefined();
    });

    it("swallows disconnect errors gracefully", async () => {
      const log = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        debug: vi.fn(),
      };
      const runtime = new StreamChatClientRuntime(account, log);
      await runtime.start();

      mockClient.disconnectUser.mockRejectedValueOnce(new Error("disconnect fail"));
      // Should not throw
      await runtime.reconnect();
      expect(log.warn).toHaveBeenCalledWith(
        expect.stringContaining("Disconnect during reconnect failed"),
      );
      expect(runtime.isConnected()).toBe(true);
    });
  });

  describe("auto-watch handler", () => {
    it("watches and caches channel when bot is added", async () => {
      const runtime = new StreamChatClientRuntime(account);
      await runtime.start();

      // Get the handler that was registered
      const onCall = mockClient.on.mock.calls.find(
        (c: unknown[]) => c[0] === "notification.added_to_channel",
      );
      expect(onCall).toBeDefined();
      const handler = onCall![1] as (event: unknown) => void;

      const newCh = createMockChannel({ type: "team", id: "new-team" });
      mockClient.channel.mockReturnValue(newCh);

      // Simulate the event
      handler({ channel: { type: "team", id: "new-team" } });

      // Wait for the async watch to resolve
      await new Promise((r) => setTimeout(r, 0));

      expect(mockClient.channel).toHaveBeenCalledWith("team", "new-team");
      expect((newCh as Record<string, Mock>).watch).toHaveBeenCalled();
      expect(runtime.getChannel("team", "new-team")).toBe(newCh);
    });

    it("logs error when watch fails", async () => {
      const log = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        debug: vi.fn(),
      };
      const runtime = new StreamChatClientRuntime(account, log);
      await runtime.start();

      const onCall = mockClient.on.mock.calls.find(
        (c: unknown[]) => c[0] === "notification.added_to_channel",
      );
      const handler = onCall![1] as (event: unknown) => void;

      const failCh = createMockChannel();
      (failCh as Record<string, Mock>).watch.mockRejectedValue(
        new Error("fail"),
      );
      mockClient.channel.mockReturnValue(failCh);

      handler({ channel: { type: "messaging", id: "fail-ch" } });
      await new Promise((r) => setTimeout(r, 0));

      expect(log.error).toHaveBeenCalledWith(
        expect.stringContaining("Failed to watch channel"),
      );
    });
  });
});
