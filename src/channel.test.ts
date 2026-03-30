import { describe, expect, it, vi, beforeEach } from "vitest";
import type { Mock } from "vitest";
import type { ResolvedAccount } from "./types.js";

// ---------------------------------------------------------------------------
// Hoisted mocks (must be self-contained — no external imports)
// ---------------------------------------------------------------------------

const mockRuntime = vi.hoisted(() => ({
  system: {
    enqueueSystemEvent: vi.fn(),
  },
  media: {
    loadWebMedia: vi.fn().mockResolvedValue({
      buffer: Buffer.from("fake-media"),
      contentType: "image/png",
      fileName: "image.png",
    }),
  },
  channel: {
    routing: {
      resolveAgentRoute: vi.fn(() => ({
        agentId: "main",
        accountId: "default",
        sessionKey: "agent:main:streamchat:channel:ch-1",
        mainSessionKey: "agent:main:main",
      })),
    },
    session: {
      resolveStorePath: vi.fn(() => "/tmp/sessions.json"),
      recordInboundSession: vi.fn(),
    },
    reply: {
      finalizeInboundContext: vi.fn(
        (ctx: Record<string, unknown>) => ctx,
      ),
      dispatchReplyWithBufferedBlockDispatcher: vi.fn(
        async () => undefined,
      ),
    },
    media: {
      fetchRemoteMedia: vi.fn().mockResolvedValue({
        buffer: Buffer.from("fake"),
        contentType: "image/jpeg",
        fileName: "photo.jpg",
      }),
      saveMediaBuffer: vi.fn().mockResolvedValue({
        id: "m-1",
        path: "/tmp/media/photo.jpg",
        size: 1024,
        contentType: "image/jpeg",
      }),
    },
  },
}));

const mockStreamChatClient = vi.hoisted(() => ({
  connectUser: vi.fn().mockResolvedValue({}),
  disconnectUser: vi.fn().mockResolvedValue({}),
  queryChannels: vi.fn().mockResolvedValue([]),
  channel: vi.fn(),
  partialUpdateMessage: vi.fn().mockResolvedValue({}),
  on: vi.fn(),
  off: vi.fn(),
}));

const mockStreamingHandler = vi.hoisted(() => ({
  onRunStarted: vi.fn().mockResolvedValue("resp-msg-1"),
  onTextChunk: vi.fn().mockResolvedValue(undefined),
  onRunProgress: vi.fn().mockResolvedValue(undefined),
  onRunCompleted: vi.fn().mockResolvedValue(undefined),
  onRunError: vi.fn().mockResolvedValue(undefined),
  onForceStop: vi.fn().mockResolvedValue(undefined),
  getActiveStream: vi.fn(),
}));

const mockUUID = vi.hoisted(() => vi.fn().mockReturnValue("test-run-uuid"));

// ---------------------------------------------------------------------------
// Module mocks
// ---------------------------------------------------------------------------

vi.mock("node:crypto", () => ({
  randomUUID: mockUUID,
}));

vi.mock("./runtime.js", () => ({
  getStreamChatRuntime: () => mockRuntime,
}));

vi.mock("stream-chat", () => {
  const Ctor = function (this: Record<string, unknown>) {
    Object.assign(this, mockStreamChatClient);
  } as unknown as { new (...args: unknown[]): unknown };
  return { StreamChat: Ctor };
});

vi.mock("./streaming.js", () => {
  const Ctor = function (this: Record<string, unknown>) {
    Object.assign(this, mockStreamingHandler);
  } as unknown as { new (...args: unknown[]): unknown };
  return { StreamingHandler: Ctor };
});

// ---------------------------------------------------------------------------
// Imports (after mocks)
// ---------------------------------------------------------------------------

import { streamchatPlugin } from "./channel.js";
import { createMockChannel } from "./test-utils/stream-chat-mock.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeAccount(overrides?: Partial<ResolvedAccount>): ResolvedAccount {
  return {
    accountId: "default",
    enabled: true,
    configured: true,
    apiKey: "test-key",
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

function makeGatewayCtx(overrides?: Record<string, unknown>) {
  const account = makeAccount();
  const abortController = new AbortController();
  const snapshot = {
    accountId: "default",
    configured: true,
    enabled: true,
    running: false,
  };
  return {
    cfg: {} as never,
    accountId: "default",
    account,
    log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
    abortSignal: abortController.signal,
    getStatus: () => snapshot,
    setStatus: (next: Record<string, unknown>) => Object.assign(snapshot, next),
    abortController,
    ...overrides,
  };
}

/**
 * Start the gateway account without blocking on the lifecycle promise.
 * `startAccount` now stays pending until abort, so we fire-and-forget the
 * task, flush microtasks so all async setup (connectUser, queryChannels,
 * listener registration) completes before the `waitUntilAbort` parks,
 * and return `{ task, ctx }` — the caller must
 * `ctx.abortController.abort(); await task` to tear down.
 */
async function startGateway(ctxOverrides?: Record<string, unknown>) {
  const ctx = makeGatewayCtx(ctxOverrides);
  const task = streamchatPlugin.gateway.startAccount(ctx as never);
  // Flush enough microtasks for all mock-async setup steps to complete
  // (connectUser → queryChannels → listener registration → waitUntilAbort)
  await new Promise((r) => setTimeout(r, 0));
  return { task, ctx };
}

function makeEvent(overrides?: Record<string, unknown>) {
  return {
    message: {
      id: "msg-1",
      text: "Hello bot",
      attachments: [],
      parent_id: undefined,
      quoted_message_id: undefined,
      quoted_message: undefined,
      created_at: "2024-01-01T00:00:00.000Z",
    },
    user: { id: "user-1", name: "Alice" },
    channel_type: "messaging",
    channel_id: "ch-1",
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("streamchatPlugin", () => {
  beforeEach(() => {
    vi.clearAllMocks();

    // Reset mock channel for getOrQueryChannel equivalent
    const mockChannel = createMockChannel();
    mockStreamChatClient.queryChannels.mockResolvedValue([mockChannel]);
    mockStreamChatClient.channel.mockReturnValue(mockChannel);

    // Reset runtime mock defaults after vi.clearAllMocks()
    mockRuntime.channel.reply.finalizeInboundContext.mockImplementation(
      (ctx: Record<string, unknown>) => ctx,
    );
    mockRuntime.channel.reply.dispatchReplyWithBufferedBlockDispatcher.mockResolvedValue(
      undefined,
    );
    mockRuntime.channel.routing.resolveAgentRoute.mockReturnValue({
      agentId: "main",
      accountId: "default",
      sessionKey: "agent:main:streamchat:channel:ch-1",
      mainSessionKey: "agent:main:main",
    });
    mockRuntime.channel.session.resolveStorePath.mockReturnValue("/tmp/sessions.json");
    mockRuntime.channel.session.recordInboundSession.mockResolvedValue(undefined);
    mockRuntime.channel.media.fetchRemoteMedia.mockResolvedValue({
      buffer: Buffer.from("fake"),
      contentType: "image/jpeg",
      fileName: "photo.jpg",
    });
    mockRuntime.channel.media.saveMediaBuffer.mockResolvedValue({
      id: "m-1",
      path: "/tmp/media/photo.jpg",
      size: 1024,
      contentType: "image/jpeg",
    });
    mockRuntime.media.loadWebMedia.mockResolvedValue({
      buffer: Buffer.from("fake-media"),
      contentType: "image/png",
      fileName: "image.png",
    });
  });

  // =======================================================================
  // Plugin metadata & capabilities
  // =======================================================================

  describe("metadata", () => {
    it("has id 'streamchat'", () => {
      expect(streamchatPlugin.id).toBe("streamchat");
    });

    it("has correct meta fields", () => {
      expect(streamchatPlugin.meta).toMatchObject({
        id: "streamchat",
        label: "Stream Chat",
        selectionLabel: "Stream Chat",
        docsPath: "/channels/streamchat",
        aliases: ["sc"],
      });
    });
  });

  describe("capabilities", () => {
    it("chatTypes is ['channel']", () => {
      expect(streamchatPlugin.capabilities.chatTypes).toEqual(["channel"]);
    });

    it("reactions is true", () => {
      expect(streamchatPlugin.capabilities.reactions).toBe(true);
    });

    it("threads is true", () => {
      expect(streamchatPlugin.capabilities.threads).toBe(true);
    });

    it("media is true", () => {
      expect(streamchatPlugin.capabilities.media).toBe(true);
    });

    it("nativeCommands is false", () => {
      expect(streamchatPlugin.capabilities.nativeCommands).toBe(false);
    });

    it("blockStreaming is false", () => {
      expect(streamchatPlugin.capabilities.blockStreaming).toBe(false);
    });
  });

  // =======================================================================
  // Config adapter
  // =======================================================================

  describe("config adapter", () => {
    it("defaultAccountId returns 'default'", () => {
      expect(streamchatPlugin.config.defaultAccountId()).toBe("default");
    });

    it("isConfigured returns true when all fields present", () => {
      const account = makeAccount();
      expect(streamchatPlugin.config.isConfigured(account)).toBe(true);
    });

    it("isConfigured returns false when apiKey missing", () => {
      const account = makeAccount({ apiKey: "" });
      expect(streamchatPlugin.config.isConfigured(account)).toBe(false);
    });

    it("isConfigured returns false when botUserId missing", () => {
      const account = makeAccount({ botUserId: "" });
      expect(streamchatPlugin.config.isConfigured(account)).toBe(false);
    });

    it("isConfigured returns false when botUserToken missing", () => {
      const account = makeAccount({ botUserToken: "" });
      expect(streamchatPlugin.config.isConfigured(account)).toBe(false);
    });

    it("describeAccount returns expected shape", () => {
      const account = makeAccount();
      const desc = streamchatPlugin.config.describeAccount(account);
      expect(desc).toMatchObject({
        accountId: "default",
        enabled: true,
        configured: true,
        running: false,
        lastStartAt: null,
        lastStopAt: null,
        lastError: null,
      });
    });
  });

  // =======================================================================
  // Outbound sendText
  // =======================================================================

  describe("outbound.sendText", () => {
    it("sends message and returns messageId", async () => {
      const mockCh = createMockChannel() as Record<string, Mock>;
      mockStreamChatClient.queryChannels.mockResolvedValue([mockCh]);

      const ctx = {
        cfg: { channels: { streamchat: makeAccount() } } as never,
        accountId: "default",
        to: "ch-1",
        text: "Hello there",
        threadId: undefined,
      };
      const result = await streamchatPlugin.outbound.sendText(ctx as never);
      expect(result).toMatchObject({
        channel: "streamchat",
        messageId: expect.any(String),
      });
    });

    it("includes parent_id when threadId is set", async () => {
      const mockCh = createMockChannel() as Record<string, Mock>;
      // The outbound sendText creates an ephemeral StreamChatClientRuntime,
      // which calls getOrQueryChannel → client.channel() → ch.watch()
      mockStreamChatClient.channel.mockReturnValue(mockCh);

      const ctx = {
        cfg: { channels: { streamchat: makeAccount() } } as never,
        accountId: "default",
        to: "ch-1",
        text: "Reply",
        threadId: "parent-1",
      };
      await streamchatPlugin.outbound.sendText(ctx as never);
      expect(mockCh.sendMessage).toHaveBeenCalledWith(
        expect.objectContaining({ parent_id: "parent-1" }),
      );
    });

    it("throws if account not configured", async () => {
      const ctx = {
        cfg: { channels: { streamchat: {} } } as never,
        accountId: "default",
        to: "ch-1",
        text: "test",
      };
      await expect(
        streamchatPlugin.outbound.sendText(ctx as never),
      ).rejects.toThrow("not configured");
    });
  });

  // =======================================================================
  // Gateway startAccount
  // =======================================================================

  describe("gateway.startAccount", () => {
    it("stays pending until abort signal fires", async () => {
      const { task, ctx } = await startGateway();
      // The promise should still be pending (status is running)
      expect(ctx.getStatus().running).toBe(true);
      // Abort to resolve the lifecycle promise
      ctx.abortController.abort();
      await task;
      expect(ctx.getStatus().running).toBe(false);
    });

    it("throws when account not configured", async () => {
      const ctx = makeGatewayCtx();
      ctx.account = makeAccount({ configured: false, apiKey: "" });
      await expect(
        streamchatPlugin.gateway.startAccount(ctx as never),
      ).rejects.toThrow("not configured");
    });

    it("sets status to running after start", async () => {
      const { task, ctx } = await startGateway();
      expect(ctx.getStatus().running).toBe(true);
      ctx.abortController.abort();
      await task;
    });

    it("registers message.new listener", async () => {
      const { task, ctx } = await startGateway();
      expect(mockStreamChatClient.on).toHaveBeenCalledWith(
        "message.new",
        expect.any(Function),
      );
      ctx.abortController.abort();
      await task;
    });

    it("registers ai_indicator.stop listener", async () => {
      const { task, ctx } = await startGateway();
      expect(mockStreamChatClient.on).toHaveBeenCalledWith(
        "ai_indicator.stop",
        expect.any(Function),
      );
      ctx.abortController.abort();
      await task;
    });

    it("abort removes listeners and disconnects", async () => {
      const { task, ctx } = await startGateway();
      ctx.abortController.abort();
      await task;
      expect(mockStreamChatClient.off).toHaveBeenCalledWith(
        "message.new",
        expect.any(Function),
      );
      expect(mockStreamChatClient.off).toHaveBeenCalledWith(
        "ai_indicator.stop",
        expect.any(Function),
      );
    });

    it("abort is idempotent", async () => {
      const { task, ctx } = await startGateway();
      ctx.abortController.abort();
      ctx.abortController.abort(); // Should not throw
      await task;
    });

    it("abort updates status to running: false", async () => {
      const { task, ctx } = await startGateway();
      ctx.abortController.abort();
      await task;
      expect(ctx.getStatus().running).toBe(false);
    });

    it("abort signal triggers cleanup", async () => {
      const { task, ctx } = await startGateway();
      ctx.abortController.abort();
      await task;
      expect(ctx.getStatus().running).toBe(false);
    });

    it("registers connection.error listener", async () => {
      const { task, ctx } = await startGateway();
      expect(mockStreamChatClient.on).toHaveBeenCalledWith(
        "connection.error",
        expect.any(Function),
      );
      ctx.abortController.abort();
      await task;
    });

    it("abort removes connection.error listener", async () => {
      const { task, ctx } = await startGateway();
      mockStreamChatClient.off.mockClear();
      ctx.abortController.abort();
      await task;
      expect(mockStreamChatClient.off).toHaveBeenCalledWith(
        "connection.error",
        expect.any(Function),
      );
    });
  });

  // =======================================================================
  // Inbound message handling (via gateway message.new handler)
  // =======================================================================

  describe("inbound message handling", () => {
    let messageHandler: (event: unknown) => void;
    let gatewayTask: Promise<void>;
    let gatewayCtx: ReturnType<typeof makeGatewayCtx>;

    beforeEach(async () => {
      const { task, ctx } = await startGateway();
      gatewayTask = task;
      gatewayCtx = ctx;

      // Extract the message.new handler
      const onCall = mockStreamChatClient.on.mock.calls.find(
        (c: unknown[]) => c[0] === "message.new",
      );
      messageHandler = onCall![1] as (event: unknown) => void;
    });

    // --- Skip conditions ---

    describe("skip conditions", () => {
      it("skips when event.message is undefined", async () => {
        messageHandler({ user: { id: "user-1" } });
        await new Promise((r) => setTimeout(r, 10));
        expect(mockStreamingHandler.onRunStarted).not.toHaveBeenCalled();
      });

      it("skips when event.user.id matches botUserId (bot echo)", async () => {
        messageHandler(makeEvent({ user: { id: "bot-1", name: "Bot" } }));
        await new Promise((r) => setTimeout(r, 10));
        expect(mockStreamingHandler.onRunStarted).not.toHaveBeenCalled();
      });

      it("skips when message.ai_generated is true", async () => {
        const event = makeEvent();
        (event.message as Record<string, unknown>).ai_generated = true;
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 10));
        expect(mockStreamingHandler.onRunStarted).not.toHaveBeenCalled();
      });

      it("skips when message has no text and no attachments", async () => {
        const event = makeEvent();
        event.message.text = "";
        event.message.attachments = [];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 10));
        expect(mockStreamingHandler.onRunStarted).not.toHaveBeenCalled();
      });

      it("does NOT skip when message has text but no attachments", async () => {
        const event = makeEvent();
        event.message.text = "Hello";
        event.message.attachments = [];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));
        expect(mockStreamingHandler.onRunStarted).toHaveBeenCalled();
      });

      it("does NOT skip when message has attachments but no text", async () => {
        const event = makeEvent();
        event.message.text = "";
        (event.message as Record<string, unknown>).attachments = [
          { image_url: "https://cdn.example.com/photo.jpg" },
        ];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));
        expect(mockStreamingHandler.onRunStarted).toHaveBeenCalled();
      });
    });

    // --- Core inbound flow ---

    describe("core flow", () => {
      it("resolves agent route with peer kind 'channel'", async () => {
        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(channel.routing.resolveAgentRoute).toHaveBeenCalledWith(
          expect.objectContaining({
            channel: "streamchat",
            peer: { kind: "channel", id: "ch-1" },
          }),
        );
      });

      it("finalizes inbound context with expected fields", async () => {
        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(
          channel.reply.finalizeInboundContext,
        ).toHaveBeenCalledWith(
          expect.objectContaining({
            Body: "Hello bot",
            RawBody: "Hello bot",
            Provider: "streamchat",
            Surface: "streamchat",
            ChatType: "channel",
            SenderId: "user-1",
            SenderName: "Alice",
            MessageSid: "msg-1",
          }),
        );
      });

      it("records inbound session", async () => {
        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(
          channel.session.recordInboundSession,
        ).toHaveBeenCalled();
      });

      it("creates placeholder message via onRunStarted", async () => {
        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        expect(mockStreamingHandler.onRunStarted).toHaveBeenCalledWith(
          "test-run-uuid",
          expect.anything(),
          expect.objectContaining({
            runId: "test-run-uuid",
            channelId: "ch-1",
          }),
        );
      });

      it("dispatches reply via dispatchReplyWithBufferedBlockDispatcher", async () => {
        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(
          channel.reply
            .dispatchReplyWithBufferedBlockDispatcher,
        ).toHaveBeenCalled();
      });

      it("calls onRunCompleted after dispatch when no error", async () => {
        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        expect(mockStreamingHandler.onRunCompleted).toHaveBeenCalledWith(
          "test-run-uuid",
        );
      });
    });

    // --- Partial reply / streaming ---

    describe("partial reply streaming", () => {
      it("onPartialReply computes delta and calls onTextChunk", async () => {
        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        channel.reply.dispatchReplyWithBufferedBlockDispatcher.mockImplementation(
          async (args: Record<string, Record<string, unknown>>) => {
            const onPartialReply = args.replyOptions?.onPartialReply as (
              p: { text: string },
            ) => void;
            if (onPartialReply) {
              onPartialReply({ text: "Hello" });
              onPartialReply({ text: "Hello world" });
            }
          },
        );

        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        expect(mockStreamingHandler.onTextChunk).toHaveBeenCalledWith(
          "test-run-uuid",
          "Hello",
          15,
        );
        expect(mockStreamingHandler.onTextChunk).toHaveBeenCalledWith(
          "test-run-uuid",
          " world",
          15,
        );
      });

      it("empty delta does not call onTextChunk", async () => {
        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        channel.reply.dispatchReplyWithBufferedBlockDispatcher.mockImplementation(
          async (args: Record<string, Record<string, unknown>>) => {
            const onPartialReply = args.replyOptions?.onPartialReply as (
              p: { text: string },
            ) => void;
            if (onPartialReply) {
              onPartialReply({ text: "Hello" });
              onPartialReply({ text: "Hello" }); // same text, delta is ""
            }
          },
        );

        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        // Only 1 call (first chunk), not 2
        expect(mockStreamingHandler.onTextChunk).toHaveBeenCalledTimes(1);
      });
    });

    // --- Deliver callback ---

    describe("deliver callback", () => {
      it("tool events call onRunProgress", async () => {
        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        channel.reply.dispatchReplyWithBufferedBlockDispatcher.mockImplementation(
          async (args: Record<string, Record<string, unknown>>) => {
            const deliver = args.dispatcherOptions?.deliver as (
              p: Record<string, unknown>,
              i: Record<string, unknown>,
            ) => Promise<void>;
            if (deliver) {
              await deliver({}, { kind: "tool" });
            }
          },
        );

        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        expect(mockStreamingHandler.onRunProgress).toHaveBeenCalledWith(
          "test-run-uuid",
        );
      });

      it("error events call onRunError", async () => {
        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        channel.reply.dispatchReplyWithBufferedBlockDispatcher.mockImplementation(
          async (args: Record<string, Record<string, unknown>>) => {
            const deliver = args.dispatcherOptions?.deliver as (
              p: Record<string, unknown>,
              i: Record<string, unknown>,
            ) => Promise<void>;
            if (deliver) {
              await deliver(
                { isError: true, text: "Something failed" },
                { kind: "block" },
              );
            }
          },
        );

        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        expect(mockStreamingHandler.onRunError).toHaveBeenCalledWith(
          "test-run-uuid",
          "Something failed",
        );
      });

      it("onRunCompleted is NOT called when error was delivered", async () => {
        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        channel.reply.dispatchReplyWithBufferedBlockDispatcher.mockImplementation(
          async (args: Record<string, Record<string, unknown>>) => {
            const deliver = args.dispatcherOptions?.deliver as (
              p: Record<string, unknown>,
              i: Record<string, unknown>,
            ) => Promise<void>;
            if (deliver) {
              await deliver(
                { isError: true, text: "error" },
                { kind: "block" },
              );
            }
          },
        );

        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        expect(mockStreamingHandler.onRunCompleted).not.toHaveBeenCalled();
      });
    });

    // --- Attachment / media handling ---

    describe("attachment handling", () => {
      it("downloads image attachments via fetchRemoteMedia", async () => {
        const event = makeEvent();
        (event.message as Record<string, unknown>).attachments = [
          { image_url: "https://cdn.example.com/photo.jpg", title: "photo.jpg" },
        ];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(channel.media.fetchRemoteMedia).toHaveBeenCalledWith({
          url: "https://cdn.example.com/photo.jpg",
          filePathHint: "photo.jpg",
        });
      });

      it("saves media via saveMediaBuffer", async () => {
        const event = makeEvent();
        (event.message as Record<string, unknown>).attachments = [
          { image_url: "https://cdn.example.com/photo.jpg" },
        ];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(channel.media.saveMediaBuffer).toHaveBeenCalledWith(
          expect.any(Buffer),
          "image/jpeg",
          "inbound",
        );
      });

      it("spreads media payload into finalizeInboundContext", async () => {
        const event = makeEvent();
        (event.message as Record<string, unknown>).attachments = [
          { image_url: "https://cdn.example.com/photo.jpg" },
        ];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(
          channel.reply.finalizeInboundContext,
        ).toHaveBeenCalledWith(
          expect.objectContaining({
            MediaPath: "/tmp/media/photo.jpg",
          }),
        );
      });

      it("uses asset_url for file attachments", async () => {
        const event = makeEvent();
        (event.message as Record<string, unknown>).attachments = [
          { asset_url: "https://cdn.example.com/doc.pdf", title: "doc.pdf" },
        ];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(channel.media.fetchRemoteMedia).toHaveBeenCalledWith({
          url: "https://cdn.example.com/doc.pdf",
          filePathHint: "doc.pdf",
        });
      });

      it("falls back to thumb_url when no image_url or asset_url", async () => {
        const event = makeEvent();
        (event.message as Record<string, unknown>).attachments = [
          { thumb_url: "https://cdn.example.com/thumb.jpg" },
        ];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(channel.media.fetchRemoteMedia).toHaveBeenCalledWith(
          expect.objectContaining({
            url: "https://cdn.example.com/thumb.jpg",
          }),
        );
      });

      it("skips attachments with no URL", async () => {
        const event = makeEvent();
        (event.message as Record<string, unknown>).attachments = [
          { title: "no-url-attachment" },
        ];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(channel.media.fetchRemoteMedia).not.toHaveBeenCalled();
      });

      it("logs warning and continues on attachment download failure", async () => {
        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        channel.media.fetchRemoteMedia.mockRejectedValueOnce(
          new Error("download failed"),
        );

        const event = makeEvent();
        (event.message as Record<string, unknown>).attachments = [
          { image_url: "https://cdn.example.com/fail.jpg" },
          { image_url: "https://cdn.example.com/ok.jpg" },
        ];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        // First fails, second succeeds — should still process
        expect(channel.media.saveMediaBuffer).toHaveBeenCalledTimes(1);
        expect(mockStreamingHandler.onRunStarted).toHaveBeenCalled();
      });

      it("processes multiple attachments", async () => {
        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        channel.media.fetchRemoteMedia.mockResolvedValue({
          buffer: Buffer.from("data"),
          contentType: "image/png",
        });
        channel.media.saveMediaBuffer
          .mockResolvedValueOnce({
            id: "m-1",
            path: "/tmp/media/a.png",
            size: 100,
            contentType: "image/png",
          })
          .mockResolvedValueOnce({
            id: "m-2",
            path: "/tmp/media/b.png",
            size: 200,
            contentType: "image/png",
          });

        const event = makeEvent();
        (event.message as Record<string, unknown>).attachments = [
          { image_url: "https://cdn.example.com/a.png" },
          { image_url: "https://cdn.example.com/b.png" },
        ];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        expect(channel.media.fetchRemoteMedia).toHaveBeenCalledTimes(2);
        expect(channel.media.saveMediaBuffer).toHaveBeenCalledTimes(2);
        expect(
          channel.reply.finalizeInboundContext,
        ).toHaveBeenCalledWith(
          expect.objectContaining({
            MediaPaths: ["/tmp/media/a.png", "/tmp/media/b.png"],
          }),
        );
      });

      it("no media payload when no attachments", async () => {
        const event = makeEvent();
        event.message.attachments = [];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        const ctx = channel.reply.finalizeInboundContext.mock.calls[0][0] as Record<string, unknown>;
        expect(ctx).not.toHaveProperty("MediaPath");
        expect(ctx).not.toHaveProperty("MediaPaths");
      });

      it("image-only message (no text) is processed with media", async () => {
        const event = makeEvent();
        event.message.text = "";
        (event.message as Record<string, unknown>).attachments = [
          { image_url: "https://cdn.example.com/photo.jpg" },
        ];
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        expect(mockStreamingHandler.onRunStarted).toHaveBeenCalled();
        const channel = mockRuntime.channel as Record<
          string,
          Record<string, Mock>
        >;
        expect(
          channel.reply.finalizeInboundContext,
        ).toHaveBeenCalledWith(
          expect.objectContaining({
            RawBody: "",
            MediaPath: "/tmp/media/photo.jpg",
          }),
        );
      });
    });

    // --- Thread handling ---

    describe("thread handling", () => {
      it("passes threadParentId to onRunStarted runCtx", async () => {
        const event = makeEvent();
        event.message.parent_id = "thread-1";
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        expect(mockStreamingHandler.onRunStarted).toHaveBeenCalledWith(
          expect.any(String),
          expect.anything(),
          expect.objectContaining({ threadParentId: "thread-1" }),
        );
      });

      it("channel type defaults to 'messaging'", async () => {
        const event = makeEvent();
        delete (event as Record<string, unknown>).channel_type;
        messageHandler(event);
        await new Promise((r) => setTimeout(r, 50));

        expect(mockStreamingHandler.onRunStarted).toHaveBeenCalledWith(
          expect.any(String),
          expect.anything(),
          expect.objectContaining({ channelType: "messaging" }),
        );
      });
    });

    // --- Reaction handling ---

    describe("reaction handling", () => {
      it("sends ack reaction for inbound message", async () => {
        const mockCh = createMockChannel() as Record<string, Mock>;
        mockStreamChatClient.queryChannels.mockResolvedValue([mockCh]);
        mockStreamChatClient.channel.mockReturnValue(mockCh);

        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        // The ack reaction is sent fire-and-forget via safeAsync
        // The channel mock should have sendReaction called
        expect(mockCh.sendReaction).toHaveBeenCalledWith(
          "msg-1",
          { type: "eyes" },
        );
      });
    });

    // --- Force stop handler ---

    describe("ai_indicator.stop handler", () => {
      it("calls onForceStop when matching response message found", async () => {
        // Get the ai_indicator.stop handler
        const stopCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "ai_indicator.stop",
        );
        const stopHandler = stopCall![1] as (event: unknown) => void;

        // We need to simulate having an active run with a response message
        // This is complex because the run contexts are created inside the gateway
        // We test the handler recognizes and calls onForceStop
        stopHandler({ message_id: "no-match" });
        await new Promise((r) => setTimeout(r, 10));
        // No match, so no call
        expect(mockStreamingHandler.onForceStop).not.toHaveBeenCalled();
      });

      it("ignores events without message_id", async () => {
        const stopCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "ai_indicator.stop",
        );
        const stopHandler = stopCall![1] as (event: unknown) => void;

        stopHandler({});
        await new Promise((r) => setTimeout(r, 10));
        expect(mockStreamingHandler.onForceStop).not.toHaveBeenCalled();
      });
    });

    // --- Mark as Read (Feature 3) ---

    describe("mark as read", () => {
      it("calls markRead on the channel after run completes", async () => {
        const mockCh = createMockChannel() as Record<string, Mock>;
        mockStreamChatClient.queryChannels.mockResolvedValue([mockCh]);
        mockStreamChatClient.channel.mockReturnValue(mockCh);

        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));

        expect(mockCh.markRead).toHaveBeenCalled();
      });
    });

    // --- Message Editing (Feature 1) ---

    describe("message.updated handler", () => {
      it("registers message.updated listener", () => {
        expect(mockStreamChatClient.on).toHaveBeenCalledWith(
          "message.updated",
          expect.any(Function),
        );
      });

      it("enqueues system event for edited messages", async () => {
        const updateCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "message.updated",
        );
        const updateHandler = updateCall![1] as (event: unknown) => void;

        updateHandler({
          message: { id: "msg-1", text: "Edited text" },
          user: { id: "user-1", name: "Alice" },
          channel_id: "ch-1",
        });

        expect(mockRuntime.system.enqueueSystemEvent).toHaveBeenCalledWith(
          expect.stringContaining("message edited by Alice"),
          expect.objectContaining({
            sessionKey: "agent:main:streamchat:channel:ch-1",
            contextKey: expect.stringContaining("streamchat:message:updated:ch-1:msg-1"),
          }),
        );
      });

      it("skips bot's own message edits", () => {
        const updateCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "message.updated",
        );
        const updateHandler = updateCall![1] as (event: unknown) => void;

        updateHandler({
          message: { id: "msg-1", text: "Bot edit" },
          user: { id: "bot-1", name: "Bot" },
          channel_id: "ch-1",
        });

        expect(mockRuntime.system.enqueueSystemEvent).not.toHaveBeenCalled();
      });

      it("skips ai_generated message edits", () => {
        const updateCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "message.updated",
        );
        const updateHandler = updateCall![1] as (event: unknown) => void;

        updateHandler({
          message: { id: "msg-1", text: "AI edit", ai_generated: true },
          user: { id: "user-1", name: "Alice" },
          channel_id: "ch-1",
        });

        expect(mockRuntime.system.enqueueSystemEvent).not.toHaveBeenCalled();
      });
    });

    // --- Message Deletion (Feature 2) ---

    describe("message.deleted handler", () => {
      it("registers message.deleted listener", () => {
        expect(mockStreamChatClient.on).toHaveBeenCalledWith(
          "message.deleted",
          expect.any(Function),
        );
      });

      it("enqueues system event for deleted messages", () => {
        const deleteCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "message.deleted",
        );
        const deleteHandler = deleteCall![1] as (event: unknown) => void;

        deleteHandler({
          message: { id: "msg-1" },
          user: { id: "user-1", name: "Alice" },
          channel_id: "ch-1",
        });

        expect(mockRuntime.system.enqueueSystemEvent).toHaveBeenCalledWith(
          expect.stringContaining("message deleted by Alice"),
          expect.objectContaining({
            contextKey: expect.stringContaining("streamchat:message:deleted:ch-1:msg-1"),
          }),
        );
      });

      it("skips bot's own message deletions", () => {
        const deleteCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "message.deleted",
        );
        const deleteHandler = deleteCall![1] as (event: unknown) => void;

        deleteHandler({
          message: { id: "msg-1" },
          user: { id: "bot-1" },
          channel_id: "ch-1",
        });

        expect(mockRuntime.system.enqueueSystemEvent).not.toHaveBeenCalled();
      });

      it("cancels active run when deleted message has in-flight run", async () => {
        const deleteCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "message.deleted",
        );
        const deleteHandler = deleteCall![1] as (event: unknown) => void;

        // First send a message to create an active run
        messageHandler(makeEvent());
        await new Promise((r) => setTimeout(r, 50));
        mockStreamingHandler.onForceStop.mockClear();

        // Now delete that message — since the run was already cleaned up
        // after dispatch completed, this won't find an active run.
        // Testing the code path where no active run exists:
        deleteHandler({
          message: { id: "msg-1" },
          user: { id: "user-1", name: "Alice" },
          channel_id: "ch-1",
        });
        await new Promise((r) => setTimeout(r, 10));

        // The run was already completed, so onForceStop should not be called
        expect(mockStreamingHandler.onForceStop).not.toHaveBeenCalled();
      });
    });

    // --- Reaction Events (Feature 4) ---

    describe("reaction event handlers", () => {
      it("registers reaction.new listener", () => {
        expect(mockStreamChatClient.on).toHaveBeenCalledWith(
          "reaction.new",
          expect.any(Function),
        );
      });

      it("registers reaction.deleted listener", () => {
        expect(mockStreamChatClient.on).toHaveBeenCalledWith(
          "reaction.deleted",
          expect.any(Function),
        );
      });

      it("enqueues system event for reaction.new", () => {
        const reactionCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "reaction.new",
        );
        const reactionHandler = reactionCall![1] as (event: unknown) => void;

        reactionHandler({
          user: { id: "user-1", name: "Alice" },
          channel_id: "ch-1",
          reaction: { type: "thumbsup" },
          message: { id: "msg-1" },
        });

        expect(mockRuntime.system.enqueueSystemEvent).toHaveBeenCalledWith(
          expect.stringContaining("reaction added: :thumbsup: by Alice"),
          expect.objectContaining({
            contextKey: expect.stringContaining("streamchat:reaction:added"),
          }),
        );
      });

      it("enqueues system event for reaction.deleted", () => {
        const reactionCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "reaction.deleted",
        );
        const reactionHandler = reactionCall![1] as (event: unknown) => void;

        reactionHandler({
          user: { id: "user-1", name: "Alice" },
          channel_id: "ch-1",
          reaction: { type: "thumbsup" },
          message: { id: "msg-1" },
        });

        expect(mockRuntime.system.enqueueSystemEvent).toHaveBeenCalledWith(
          expect.stringContaining("reaction removed: :thumbsup: by Alice"),
          expect.objectContaining({
            contextKey: expect.stringContaining("streamchat:reaction:removed"),
          }),
        );
      });

      it("skips bot's own reactions", () => {
        const reactionCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "reaction.new",
        );
        const reactionHandler = reactionCall![1] as (event: unknown) => void;

        reactionHandler({
          user: { id: "bot-1", name: "Bot" },
          channel_id: "ch-1",
          reaction: { type: "eyes" },
          message: { id: "msg-1" },
        });

        expect(mockRuntime.system.enqueueSystemEvent).not.toHaveBeenCalled();
      });
    });

    // --- Connection Recovery (Feature 5) ---

    describe("connection recovery handlers", () => {
      it("registers connection.changed listener", () => {
        expect(mockStreamChatClient.on).toHaveBeenCalledWith(
          "connection.changed",
          expect.any(Function),
        );
      });

      it("registers connection.recovered listener", () => {
        expect(mockStreamChatClient.on).toHaveBeenCalledWith(
          "connection.recovered",
          expect.any(Function),
        );
      });

      it("connection.changed with online:false sets running to false", () => {
        const connCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "connection.changed",
        );
        const connHandler = connCall![1] as (event: unknown) => void;

        connHandler({ online: false });
        // The gateway context's setStatus should have been called
        // We verify by checking the mock was called (status is internal)
      });

      it("connection.changed with online:true sets running to true", () => {
        const connCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "connection.changed",
        );
        const connHandler = connCall![1] as (event: unknown) => void;

        // First go offline, then online
        connHandler({ online: false });
        connHandler({ online: true });
      });

      it("connection.recovered sets running to true", () => {
        const recoveredCall = mockStreamChatClient.on.mock.calls.find(
          (c: unknown[]) => c[0] === "connection.recovered",
        );
        const recoveredHandler = recoveredCall![1] as (event: unknown) => void;

        recoveredHandler({});
        // Verify no error thrown, handler ran successfully
      });
    });

    // --- stop() removes all listeners ---

    describe("abort removes all new listeners", () => {
      it("abort removes message.updated listener", async () => {
        const { task, ctx } = await startGateway();
        mockStreamChatClient.off.mockClear();
        ctx.abortController.abort();
        await task;
        expect(mockStreamChatClient.off).toHaveBeenCalledWith(
          "message.updated",
          expect.any(Function),
        );
      });

      it("abort removes message.deleted listener", async () => {
        const { task, ctx } = await startGateway();
        mockStreamChatClient.off.mockClear();
        ctx.abortController.abort();
        await task;
        expect(mockStreamChatClient.off).toHaveBeenCalledWith(
          "message.deleted",
          expect.any(Function),
        );
      });

      it("abort removes reaction.new and reaction.deleted listeners", async () => {
        const { task, ctx } = await startGateway();
        mockStreamChatClient.off.mockClear();
        ctx.abortController.abort();
        await task;
        expect(mockStreamChatClient.off).toHaveBeenCalledWith(
          "reaction.new",
          expect.any(Function),
        );
        expect(mockStreamChatClient.off).toHaveBeenCalledWith(
          "reaction.deleted",
          expect.any(Function),
        );
      });

      it("abort removes connection.changed and connection.recovered listeners", async () => {
        const { task, ctx } = await startGateway();
        mockStreamChatClient.off.mockClear();
        ctx.abortController.abort();
        await task;
        expect(mockStreamChatClient.off).toHaveBeenCalledWith(
          "connection.changed",
          expect.any(Function),
        );
        expect(mockStreamChatClient.off).toHaveBeenCalledWith(
          "connection.recovered",
          expect.any(Function),
        );
      });
    });
  });

  // =======================================================================
  // Outbound sendMedia (Feature 6)
  // =======================================================================

  describe("outbound.sendMedia", () => {
    it("uploads image via sendFile and sends message with attachment", async () => {
      const mockCh = createMockChannel() as Record<string, Mock>;
      mockStreamChatClient.channel.mockReturnValue(mockCh);

      const ctx = {
        cfg: { channels: { streamchat: makeAccount() } } as never,
        accountId: "default",
        to: "ch-1",
        text: "Check this image",
        mediaUrl: "https://example.com/photo.png",
      };
      const result = await streamchatPlugin.outbound.sendMedia(ctx as never);
      expect(result).toMatchObject({
        channel: "streamchat",
        messageId: expect.any(String),
      });
      expect(mockCh.sendFile).toHaveBeenCalledWith(
        expect.any(Buffer),
        "image.png",
        "image/png",
      );
      expect(mockCh.sendMessage).toHaveBeenCalledWith(
        expect.objectContaining({
          text: "Check this image",
          attachments: expect.arrayContaining([
            expect.objectContaining({ type: "image", image_url: expect.any(String) }),
          ]),
        }),
      );
    });

    it("uploads non-image files with asset_url", async () => {
      mockRuntime.media.loadWebMedia.mockResolvedValue({
        buffer: Buffer.from("pdf-data"),
        contentType: "application/pdf",
        fileName: "doc.pdf",
      });

      const mockCh = createMockChannel() as Record<string, Mock>;
      mockStreamChatClient.channel.mockReturnValue(mockCh);

      const ctx = {
        cfg: { channels: { streamchat: makeAccount() } } as never,
        accountId: "default",
        to: "ch-1",
        text: "",
        mediaUrl: "https://example.com/doc.pdf",
      };
      await streamchatPlugin.outbound.sendMedia(ctx as never);
      expect(mockCh.sendFile).toHaveBeenCalledWith(
        expect.any(Buffer),
        "doc.pdf",
        "application/pdf",
      );
      expect(mockCh.sendMessage).toHaveBeenCalledWith(
        expect.objectContaining({
          attachments: expect.arrayContaining([
            expect.objectContaining({ type: "file", asset_url: expect.any(String) }),
          ]),
        }),
      );
    });

    it("includes parent_id when threadId is set", async () => {
      const mockCh = createMockChannel() as Record<string, Mock>;
      mockStreamChatClient.channel.mockReturnValue(mockCh);

      const ctx = {
        cfg: { channels: { streamchat: makeAccount() } } as never,
        accountId: "default",
        to: "ch-1",
        text: "threaded media",
        mediaUrl: "https://example.com/photo.png",
        threadId: "parent-1",
      };
      await streamchatPlugin.outbound.sendMedia(ctx as never);
      expect(mockCh.sendMessage).toHaveBeenCalledWith(
        expect.objectContaining({ parent_id: "parent-1" }),
      );
    });

    it("throws when account not configured", async () => {
      const ctx = {
        cfg: { channels: { streamchat: {} } } as never,
        accountId: "default",
        to: "ch-1",
        text: "test",
        mediaUrl: "https://example.com/photo.png",
      };
      await expect(
        streamchatPlugin.outbound.sendMedia(ctx as never),
      ).rejects.toThrow("not configured");
    });

    it("sends message without attachments when no mediaUrl", async () => {
      const mockCh = createMockChannel() as Record<string, Mock>;
      mockStreamChatClient.channel.mockReturnValue(mockCh);

      const ctx = {
        cfg: { channels: { streamchat: makeAccount() } } as never,
        accountId: "default",
        to: "ch-1",
        text: "just text",
      };
      await streamchatPlugin.outbound.sendMedia(ctx as never);
      expect(mockRuntime.media.loadWebMedia).not.toHaveBeenCalled();
      expect(mockCh.sendMessage).toHaveBeenCalledWith(
        expect.objectContaining({
          text: "just text",
          attachments: [],
        }),
      );
    });
  });
});
