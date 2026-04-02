import { describe, expect, it, vi, beforeEach } from "vitest";

// Hoist mocks
const mockSetStreamChatRuntime = vi.hoisted(() => vi.fn());
const mockRegisterChannel = vi.hoisted(() => vi.fn());

vi.mock("./src/runtime.js", () => ({
  setStreamChatRuntime: mockSetStreamChatRuntime,
}));

// We don't need to fully mock channel.js — just ensure it exports something
vi.mock("./src/channel.js", () => ({
  streamchatPlugin: { id: "streamchat-mock" },
}));

import plugin from "./index.js";

describe("plugin entry point", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("exports id 'openclaw-channel-streamchat'", () => {
    expect(plugin.id).toBe("openclaw-channel-streamchat");
  });

  it("exports name 'Stream Chat'", () => {
    expect(plugin.name).toBe("Stream Chat");
  });

  it("exports a description", () => {
    expect(typeof plugin.description).toBe("string");
    expect(plugin.description.length).toBeGreaterThan(0);
  });

  it("exports a configSchema", () => {
    expect(plugin.configSchema).toBeDefined();
  });

  it("register() calls setStreamChatRuntime with api.runtime", () => {
    const fakeRuntime = { version: "test" };
    const api = {
      runtime: fakeRuntime,
      registerChannel: mockRegisterChannel,
    };
    plugin.register(api as never);
    expect(mockSetStreamChatRuntime).toHaveBeenCalledWith(fakeRuntime);
  });

  it("register() calls api.registerChannel with the streamchat plugin", () => {
    const api = {
      runtime: { version: "test" },
      registerChannel: mockRegisterChannel,
    };
    plugin.register(api as never);
    expect(mockRegisterChannel).toHaveBeenCalledWith({
      plugin: { id: "streamchat-mock" },
    });
  });

  it("register() sets runtime before registering channel", () => {
    const callOrder: string[] = [];
    mockSetStreamChatRuntime.mockImplementation(() => {
      callOrder.push("setRuntime");
    });
    mockRegisterChannel.mockImplementation(() => {
      callOrder.push("registerChannel");
    });

    const api = {
      runtime: { version: "test" },
      registerChannel: mockRegisterChannel,
    };
    plugin.register(api as never);

    expect(callOrder).toEqual(["setRuntime", "registerChannel"]);
  });
});
