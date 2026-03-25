import { describe, expect, it, vi, beforeEach } from "vitest";
import type { Mock } from "vitest";
import { StreamingHandler } from "./streaming.js";
import { RunContextMap } from "./run-context.js";
import type { RunContext } from "./types.js";
import { createMockChannel, createMockStreamChatClient } from "./test-utils/index.js";

function makeRunCtx(overrides?: Partial<RunContext>): RunContext {
  return {
    runId: "run-1",
    channelType: "messaging",
    channelId: "ch-1",
    threadParentId: null,
    inboundMessageId: "inbound-1",
    senderId: "user-1",
    responseMessageId: null,
    ...overrides,
  };
}

describe("StreamingHandler", () => {
  let client: Record<string, Mock>;
  let channel: Record<string, Mock>;
  let runContexts: RunContextMap;
  let handler: StreamingHandler;
  let log: { info: Mock; warn: Mock; error: Mock; debug: Mock };

  beforeEach(() => {
    vi.clearAllMocks();
    client = createMockStreamChatClient() as Record<string, Mock>;
    channel = createMockChannel() as Record<string, Mock>;
    runContexts = new RunContextMap();
    log = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    handler = new StreamingHandler({
      client: client as never,
      runContexts,
      log,
    });
  });

  // --- onRunStarted ---

  describe("onRunStarted", () => {
    it("creates empty message with ai_generated: true", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);

      expect(channel.sendMessage).toHaveBeenCalledWith(
        expect.objectContaining({ text: "", ai_generated: true }),
      );
    });

    it("sets parent_id when threadParentId is present", async () => {
      const runCtx = makeRunCtx({ threadParentId: "thread-1" });
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);

      expect(channel.sendMessage).toHaveBeenCalledWith(
        expect.objectContaining({ parent_id: "thread-1" }),
      );
    });

    it("does not set parent_id when threadParentId is null", async () => {
      const runCtx = makeRunCtx({ threadParentId: null });
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);

      const call = channel.sendMessage.mock.calls[0][0];
      expect(call).not.toHaveProperty("parent_id");
    });

    it("sends AI_STATE_THINKING indicator", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);

      expect(channel.sendEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "ai_indicator.update",
          ai_state: "AI_STATE_THINKING",
        }),
      );
    });

    it("stores active stream (verifiable via getActiveStream)", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);

      const active = handler.getActiveStream("run-1");
      expect(active).toBeDefined();
      expect(active!.messageId).toBe("resp-msg-1");
    });

    it("returns the message ID", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      const id = await handler.onRunStarted("run-1", channel as never, runCtx);
      expect(id).toBe("resp-msg-1");
    });

    it("calls runContexts.setResponseMessageId", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      expect(runContexts.get("run-1")!.responseMessageId).toBe("resp-msg-1");
    });
  });

  // --- onTextChunk ---

  describe("onTextChunk", () => {
    async function startStream(runId = "run-1") {
      const runCtx = makeRunCtx({ runId });
      runContexts.set(runId, runCtx);
      await handler.onRunStarted(runId, channel as never, runCtx);
      // Reset mocks after startup to isolate chunk behavior
      channel.sendEvent.mockClear();
      client.partialUpdateMessage.mockClear();
    }

    it("first chunk switches indicator to GENERATING", async () => {
      await startStream();
      await handler.onTextChunk("run-1", "Hi");

      expect(channel.sendEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "ai_indicator.update",
          ai_state: "AI_STATE_GENERATING",
        }),
      );
    });

    it("GENERATING indicator is only sent once", async () => {
      await startStream();
      await handler.onTextChunk("run-1", "Hi");
      await handler.onTextChunk("run-1", " there");

      const generatingCalls = channel.sendEvent.mock.calls.filter(
        (c: unknown[]) =>
          (c[0] as Record<string, unknown>).ai_state === "AI_STATE_GENERATING",
      );
      expect(generatingCalls).toHaveLength(1);
    });

    it("updates on odd chunks < 8 (chunks 1, 3, 5, 7)", async () => {
      await startStream();

      // Send 8 chunks
      for (let i = 0; i < 8; i++) {
        await handler.onTextChunk("run-1", `c${i}`);
      }

      // Wait for chained promises
      await new Promise((r) => setTimeout(r, 10));

      // Chunks 1, 3, 5, 7 should trigger updates (4 updates)
      expect(client.partialUpdateMessage).toHaveBeenCalledTimes(4);
    });

    it("does NOT update on even chunks < 8", async () => {
      await startStream();

      // Send exactly 2 chunks — only chunk 1 (first) should update
      await handler.onTextChunk("run-1", "a");
      await handler.onTextChunk("run-1", "b");

      await new Promise((r) => setTimeout(r, 10));

      // Only chunk 1 (first chunk) triggers update
      expect(client.partialUpdateMessage).toHaveBeenCalledTimes(1);
    });

    it("after chunk 8, updates every Nth chunk (default 15)", async () => {
      await startStream();

      // Send 23 chunks (8 early + 15 more)
      for (let i = 0; i < 23; i++) {
        await handler.onTextChunk("run-1", "x");
      }

      await new Promise((r) => setTimeout(r, 10));

      // Early: 1,3,5,7 = 4 updates
      // Then chunk 15 triggers (15 % 15 === 0) = 1 more update
      // Total = 5
      expect(client.partialUpdateMessage).toHaveBeenCalledTimes(5);
    });

    it("partial updates include generating: true", async () => {
      await startStream();
      await handler.onTextChunk("run-1", "Hi");

      await new Promise((r) => setTimeout(r, 10));

      expect(client.partialUpdateMessage).toHaveBeenCalledWith(
        "resp-msg-1",
        expect.objectContaining({
          set: expect.objectContaining({ generating: true }),
        }),
      );
    });

    it("accumulates text across chunks", async () => {
      await startStream();
      await handler.onTextChunk("run-1", "Hello");
      await handler.onTextChunk("run-1", " world");
      await handler.onTextChunk("run-1", "!");

      await new Promise((r) => setTimeout(r, 10));

      // Chunk 3 (odd < 8) triggers update with accumulated text
      const lastCall = client.partialUpdateMessage.mock.calls[
        client.partialUpdateMessage.mock.calls.length - 1
      ];
      expect(lastCall[1].set.text).toBe("Hello world!");
    });

    it("error in partialUpdateMessage is swallowed", async () => {
      client.partialUpdateMessage.mockRejectedValue(new Error("network"));
      await startStream();
      // Should not throw
      await handler.onTextChunk("run-1", "Hi");
      await new Promise((r) => setTimeout(r, 10));

      expect(log.warn).toHaveBeenCalledWith(
        expect.stringContaining("partialUpdate failed"),
      );
    });

    it("no-op when stream is finalized", async () => {
      await startStream();
      await handler.onRunCompleted("run-1");
      channel.sendEvent.mockClear();
      client.partialUpdateMessage.mockClear();

      await handler.onTextChunk("run-1", "late");
      expect(channel.sendEvent).not.toHaveBeenCalled();
      expect(client.partialUpdateMessage).not.toHaveBeenCalled();
    });

    it("no-op when runId does not exist", async () => {
      await handler.onTextChunk("no-such", "hi");
      expect(client.partialUpdateMessage).not.toHaveBeenCalled();
    });

    it("respects custom streamingThrottle", async () => {
      await startStream();

      // With throttle=5, after the early burst (chunks 1-7),
      // chunk 10 (10 % 5 === 0) should trigger
      for (let i = 0; i < 10; i++) {
        await handler.onTextChunk("run-1", "x", 5);
      }

      await new Promise((r) => setTimeout(r, 10));

      // Early: 1,3,5,7 = 4, plus chunk 10 (10%5===0) = 5
      expect(client.partialUpdateMessage).toHaveBeenCalledTimes(5);
    });
  });

  // --- onRunProgress ---

  describe("onRunProgress", () => {
    it("sends AI_STATE_EXTERNAL_SOURCES indicator", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      channel.sendEvent.mockClear();

      await handler.onRunProgress("run-1");

      expect(channel.sendEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "ai_indicator.update",
          ai_state: "AI_STATE_EXTERNAL_SOURCES",
        }),
      );
    });

    it("is de-duplicated (only sends once)", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      channel.sendEvent.mockClear();

      await handler.onRunProgress("run-1");
      await handler.onRunProgress("run-1");

      const externalSourcesCalls = channel.sendEvent.mock.calls.filter(
        (c: unknown[]) =>
          (c[0] as Record<string, unknown>).ai_state ===
          "AI_STATE_EXTERNAL_SOURCES",
      );
      expect(externalSourcesCalls).toHaveLength(1);
    });

    it("no-op when finalized", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      await handler.onRunCompleted("run-1");
      channel.sendEvent.mockClear();

      await handler.onRunProgress("run-1");
      expect(channel.sendEvent).not.toHaveBeenCalled();
    });

    it("no-op when runId does not exist", async () => {
      await handler.onRunProgress("no-such");
      expect(channel.sendEvent).not.toHaveBeenCalled();
    });
  });

  // --- onRunCompleted ---

  describe("onRunCompleted", () => {
    it("sends final partialUpdateMessage with generating: false", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);

      // Accumulate some text
      await handler.onTextChunk("run-1", "Done");
      await new Promise((r) => setTimeout(r, 10));
      client.partialUpdateMessage.mockClear();

      await handler.onRunCompleted("run-1");

      expect(client.partialUpdateMessage).toHaveBeenCalledWith(
        "resp-msg-1",
        expect.objectContaining({
          set: expect.objectContaining({ text: "Done", generating: false }),
        }),
      );
    });

    it("uses '(No response)' when no text accumulated", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      client.partialUpdateMessage.mockClear();

      await handler.onRunCompleted("run-1");

      expect(client.partialUpdateMessage).toHaveBeenCalledWith(
        "resp-msg-1",
        expect.objectContaining({
          set: expect.objectContaining({ text: "(No response)" }),
        }),
      );
    });

    it("sends ai_indicator.clear event", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      channel.sendEvent.mockClear();

      await handler.onRunCompleted("run-1");

      expect(channel.sendEvent).toHaveBeenCalledWith(
        expect.objectContaining({ type: "ai_indicator.clear" }),
      );
    });

    it("deletes the stream", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      await handler.onRunCompleted("run-1");
      expect(handler.getActiveStream("run-1")).toBeUndefined();
    });

    it("no-op when finalized", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      await handler.onRunCompleted("run-1");
      client.partialUpdateMessage.mockClear();
      channel.sendEvent.mockClear();

      await handler.onRunCompleted("run-1");
      expect(client.partialUpdateMessage).not.toHaveBeenCalled();
    });

    it("no-op when runId does not exist", async () => {
      await handler.onRunCompleted("no-such");
      expect(client.partialUpdateMessage).not.toHaveBeenCalled();
    });
  });

  // --- onRunError ---

  describe("onRunError", () => {
    it("appends error to accumulated text", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      await handler.onTextChunk("run-1", "Partial");
      await new Promise((r) => setTimeout(r, 10));
      client.partialUpdateMessage.mockClear();

      await handler.onRunError("run-1", "Something broke");

      expect(client.partialUpdateMessage).toHaveBeenCalledWith(
        "resp-msg-1",
        expect.objectContaining({
          set: expect.objectContaining({
            text: "Partial\n\n---\nError: Something broke",
            generating: false,
          }),
        }),
      );
    });

    it("uses 'Error: ...' when no text accumulated", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      client.partialUpdateMessage.mockClear();

      await handler.onRunError("run-1", "Something broke");

      expect(client.partialUpdateMessage).toHaveBeenCalledWith(
        "resp-msg-1",
        expect.objectContaining({
          set: expect.objectContaining({
            text: "Error: Something broke",
          }),
        }),
      );
    });

    it("sends AI_STATE_ERROR indicator (not clear)", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      channel.sendEvent.mockClear();

      await handler.onRunError("run-1", "error");

      expect(channel.sendEvent).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "ai_indicator.update",
          ai_state: "AI_STATE_ERROR",
        }),
      );
      // Should NOT send ai_indicator.clear
      const clearCalls = channel.sendEvent.mock.calls.filter(
        (c: unknown[]) => (c[0] as Record<string, unknown>).type === "ai_indicator.clear",
      );
      expect(clearCalls).toHaveLength(0);
    });

    it("deletes the stream", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      await handler.onRunError("run-1", "error");
      expect(handler.getActiveStream("run-1")).toBeUndefined();
    });

    it("no-op when finalized", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      await handler.onRunCompleted("run-1");
      client.partialUpdateMessage.mockClear();

      await handler.onRunError("run-1", "late error");
      expect(client.partialUpdateMessage).not.toHaveBeenCalled();
    });
  });

  // --- onForceStop ---

  describe("onForceStop", () => {
    it("sends partialUpdateMessage with generating: false only", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      await handler.onTextChunk("run-1", "partial text");
      await new Promise((r) => setTimeout(r, 10));
      client.partialUpdateMessage.mockClear();

      await handler.onForceStop("run-1");

      expect(client.partialUpdateMessage).toHaveBeenCalledWith(
        "resp-msg-1",
        expect.objectContaining({
          set: expect.objectContaining({ generating: false }),
        }),
      );
      // Should NOT include text (preserves accumulated text)
      const call = client.partialUpdateMessage.mock.calls[0];
      expect(call[1].set).not.toHaveProperty("text");
    });

    it("sends ai_indicator.clear event", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      channel.sendEvent.mockClear();

      await handler.onForceStop("run-1");

      expect(channel.sendEvent).toHaveBeenCalledWith(
        expect.objectContaining({ type: "ai_indicator.clear" }),
      );
    });

    it("deletes the stream", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      await handler.onForceStop("run-1");
      expect(handler.getActiveStream("run-1")).toBeUndefined();
    });

    it("no-op when finalized", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      await handler.onRunCompleted("run-1");
      client.partialUpdateMessage.mockClear();
      channel.sendEvent.mockClear();

      await handler.onForceStop("run-1");
      expect(client.partialUpdateMessage).not.toHaveBeenCalled();
    });

    it("no-op when runId does not exist", async () => {
      await handler.onForceStop("no-such");
      expect(client.partialUpdateMessage).not.toHaveBeenCalled();
    });
  });

  // --- getActiveStream ---

  describe("getActiveStream", () => {
    it("returns { messageId } for active stream", async () => {
      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);
      expect(handler.getActiveStream("run-1")).toEqual({
        messageId: "resp-msg-1",
      });
    });

    it("returns undefined for non-existent runId", () => {
      expect(handler.getActiveStream("no-such")).toBeUndefined();
    });
  });

  // --- safeSendEvent retry behavior (tested indirectly) ---

  describe("safeSendEvent (via indicator sends)", () => {
    it("retries on 429 and succeeds", async () => {
      channel.sendEvent
        .mockRejectedValueOnce({ status: 429 })
        .mockResolvedValueOnce({});

      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      // onRunStarted sends THINKING indicator via safeSendEvent
      await handler.onRunStarted("run-1", channel as never, runCtx);

      // sendMessage + 2 sendEvent attempts (1 fail + 1 success)
      expect(channel.sendEvent).toHaveBeenCalledTimes(2);
    });

    it("retries on 500 and succeeds", async () => {
      channel.sendEvent
        .mockRejectedValueOnce({ status: 500 })
        .mockResolvedValueOnce({});

      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);

      expect(channel.sendEvent).toHaveBeenCalledTimes(2);
    });

    it("does NOT retry on 400 (non-retryable)", async () => {
      channel.sendEvent.mockRejectedValue({ status: 400 });

      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      await handler.onRunStarted("run-1", channel as never, runCtx);

      // Only 1 attempt (no retry)
      expect(channel.sendEvent).toHaveBeenCalledTimes(1);
    });

    it("swallows error after max retries", async () => {
      channel.sendEvent.mockRejectedValue({ status: 429 });

      const runCtx = makeRunCtx();
      runContexts.set("run-1", runCtx);
      // Should not throw despite all retries failing
      await handler.onRunStarted("run-1", channel as never, runCtx);

      expect(channel.sendEvent).toHaveBeenCalledTimes(5); // max attempts
      expect(log.warn).toHaveBeenCalledWith(
        expect.stringContaining("safeSendEvent"),
      );
    });
  });
});
