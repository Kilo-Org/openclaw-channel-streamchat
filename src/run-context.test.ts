import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { RunContextMap } from "./run-context.js";
import type { RunContext } from "./types.js";

function makeCtx(overrides?: Partial<RunContext>): RunContext {
  return {
    runId: "run-1",
    channelType: "messaging",
    channelId: "ch-1",
    threadParentId: null,
    inboundMessageId: "msg-1",
    senderId: "user-1",
    responseMessageId: null,
    ...overrides,
  };
}

describe("RunContextMap", () => {
  let map: RunContextMap;

  beforeEach(() => {
    vi.useFakeTimers();
    map = new RunContextMap();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // --- set / get ---

  it("set/get round-trip stores and retrieves context", () => {
    const ctx = makeCtx();
    map.set("run-1", ctx);
    expect(map.get("run-1")).toBe(ctx);
  });

  it("get returns undefined for non-existent runId", () => {
    expect(map.get("no-such-run")).toBeUndefined();
  });

  it("set overwrites existing entry for same runId", () => {
    const first = makeCtx({ senderId: "first" });
    const second = makeCtx({ senderId: "second" });
    map.set("run-1", first);
    map.set("run-1", second);
    expect(map.get("run-1")).toBe(second);
  });

  // --- delete ---

  it("delete removes entry", () => {
    map.set("run-1", makeCtx());
    map.delete("run-1");
    expect(map.get("run-1")).toBeUndefined();
  });

  it("delete is no-op for non-existent runId", () => {
    expect(() => map.delete("no-such-run")).not.toThrow();
  });

  // --- TTL ---

  it("auto-deletes entry after 5 minutes", () => {
    map.set("run-1", makeCtx());
    expect(map.get("run-1")).toBeDefined();

    vi.advanceTimersByTime(5 * 60 * 1000);
    expect(map.get("run-1")).toBeUndefined();
  });

  it("does not auto-delete before 5 minutes", () => {
    map.set("run-1", makeCtx());
    vi.advanceTimersByTime(5 * 60 * 1000 - 1);
    expect(map.get("run-1")).toBeDefined();
  });

  it("overwriting entry resets the TTL timer", () => {
    map.set("run-1", makeCtx());
    // Advance 4 minutes
    vi.advanceTimersByTime(4 * 60 * 1000);
    // Overwrite (resets timer)
    map.set("run-1", makeCtx({ senderId: "new" }));
    // Advance another 4 minutes (total 8 from start, 4 from overwrite)
    vi.advanceTimersByTime(4 * 60 * 1000);
    expect(map.get("run-1")).toBeDefined();
    // Advance 1 more minute (total 5 from overwrite)
    vi.advanceTimersByTime(1 * 60 * 1000);
    expect(map.get("run-1")).toBeUndefined();
  });

  // --- setResponseMessageId ---

  it("setResponseMessageId mutates stored context", () => {
    map.set("run-1", makeCtx());
    map.setResponseMessageId("run-1", "resp-1");
    expect(map.get("run-1")!.responseMessageId).toBe("resp-1");
  });

  it("setResponseMessageId is no-op for missing runId", () => {
    expect(() => map.setResponseMessageId("no-such", "resp")).not.toThrow();
  });

  // --- findActiveRunForChannel ---

  it("findActiveRunForChannel returns matching context", () => {
    map.set("run-1", makeCtx({ channelType: "messaging", channelId: "ch-1", threadParentId: null }));
    const found = map.findActiveRunForChannel("messaging", "ch-1", null);
    expect(found).toBeDefined();
    expect(found!.runId).toBe("run-1");
  });

  it("findActiveRunForChannel returns undefined when no match", () => {
    map.set("run-1", makeCtx({ channelId: "ch-1" }));
    expect(map.findActiveRunForChannel("messaging", "ch-2", null)).toBeUndefined();
  });

  it("findActiveRunForChannel matches thread parent ID", () => {
    map.set("run-1", makeCtx({ threadParentId: "thread-1" }));
    expect(
      map.findActiveRunForChannel("messaging", "ch-1", "thread-1"),
    ).toBeDefined();
    expect(
      map.findActiveRunForChannel("messaging", "ch-1", "thread-2"),
    ).toBeUndefined();
    expect(
      map.findActiveRunForChannel("messaging", "ch-1", null),
    ).toBeUndefined();
  });

  it("findActiveRunForChannel returns correct entry among multiple", () => {
    map.set("run-1", makeCtx({ runId: "run-1", channelId: "ch-1" }));
    map.set("run-2", makeCtx({ runId: "run-2", channelId: "ch-2" }));
    const found = map.findActiveRunForChannel("messaging", "ch-2", null);
    expect(found!.runId).toBe("run-2");
  });

  // --- findByResponseMessageId ---

  it("findByResponseMessageId returns matching context", () => {
    const ctx = makeCtx({ responseMessageId: "resp-1" });
    map.set("run-1", ctx);
    expect(map.findByResponseMessageId("resp-1")).toBe(ctx);
  });

  it("findByResponseMessageId returns undefined when no match", () => {
    map.set("run-1", makeCtx());
    expect(map.findByResponseMessageId("no-such")).toBeUndefined();
  });

  it("findByResponseMessageId works after setResponseMessageId", () => {
    map.set("run-1", makeCtx());
    map.setResponseMessageId("run-1", "resp-1");
    expect(map.findByResponseMessageId("resp-1")).toBeDefined();
  });

  // --- findByInboundMessageId ---

  it("findByInboundMessageId returns matching context", () => {
    const ctx = makeCtx({ inboundMessageId: "inbound-42" });
    map.set("run-1", ctx);
    expect(map.findByInboundMessageId("inbound-42")).toBe(ctx);
  });

  it("findByInboundMessageId returns undefined when no match", () => {
    map.set("run-1", makeCtx({ inboundMessageId: "inbound-1" }));
    expect(map.findByInboundMessageId("no-such")).toBeUndefined();
  });
});
