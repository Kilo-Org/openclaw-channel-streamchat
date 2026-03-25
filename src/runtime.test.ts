import { describe, expect, it, vi, beforeEach } from "vitest";

describe("runtime singleton", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  it("getStreamChatRuntime throws before set is called", async () => {
    const { getStreamChatRuntime } = await import("./runtime.js");
    expect(() => getStreamChatRuntime()).toThrow(
      "StreamChat runtime not initialized",
    );
  });

  it("getStreamChatRuntime returns the runtime that was set", async () => {
    const { getStreamChatRuntime, setStreamChatRuntime } = await import(
      "./runtime.js"
    );
    const fakeRuntime = { version: "test" } as never;
    setStreamChatRuntime(fakeRuntime);
    expect(getStreamChatRuntime()).toBe(fakeRuntime);
  });

  it("setStreamChatRuntime overwrites previous runtime", async () => {
    const { getStreamChatRuntime, setStreamChatRuntime } = await import(
      "./runtime.js"
    );
    const first = { version: "1" } as never;
    const second = { version: "2" } as never;
    setStreamChatRuntime(first);
    setStreamChatRuntime(second);
    expect(getStreamChatRuntime()).toBe(second);
  });
});
