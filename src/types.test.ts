import { describe, expect, it } from "vitest";
import type { OpenClawConfig } from "openclaw/plugin-sdk";
import {
  getStreamChatConfig,
  listStreamChatAccountIds,
  resolveStreamChatAccount,
} from "./types.js";

function makeCfg(streamchat?: Record<string, unknown>): OpenClawConfig {
  if (streamchat === undefined) return {} as OpenClawConfig;
  return { channels: { streamchat } } as unknown as OpenClawConfig;
}

describe("getStreamChatConfig", () => {
  it("returns config from cfg.channels.streamchat", () => {
    const cfg = makeCfg({ apiKey: "k1" });
    expect(getStreamChatConfig(cfg)).toMatchObject({ apiKey: "k1" });
  });

  it("returns {} when cfg.channels is undefined", () => {
    const cfg = {} as OpenClawConfig;
    expect(getStreamChatConfig(cfg)).toEqual({});
  });

  it("returns {} when cfg.channels.streamchat is undefined", () => {
    const cfg = { channels: {} } as unknown as OpenClawConfig;
    expect(getStreamChatConfig(cfg)).toEqual({});
  });
});

describe("listStreamChatAccountIds", () => {
  it("returns ['default'] when base config has apiKey", () => {
    const cfg = makeCfg({ apiKey: "k1" });
    expect(listStreamChatAccountIds(cfg)).toContain("default");
  });

  it("returns ['default'] when base config has botUserId only", () => {
    const cfg = makeCfg({ botUserId: "bot-1" });
    expect(listStreamChatAccountIds(cfg)).toContain("default");
  });

  it("returns ['default'] when no config at all (fallback)", () => {
    const cfg = makeCfg();
    expect(listStreamChatAccountIds(cfg)).toEqual(["default"]);
  });

  it("returns account keys from accounts map", () => {
    const cfg = makeCfg({
      accounts: { "ws-a": { apiKey: "a" }, "ws-b": { apiKey: "b" } },
    });
    const ids = listStreamChatAccountIds(cfg);
    expect(ids).toContain("ws-a");
    expect(ids).toContain("ws-b");
  });

  it("returns both 'default' and account keys when both exist", () => {
    const cfg = makeCfg({
      apiKey: "main",
      accounts: { sub: { apiKey: "sub-key" } },
    });
    const ids = listStreamChatAccountIds(cfg);
    expect(ids).toContain("default");
    expect(ids).toContain("sub");
  });

  it("returns only account keys when base has no direct keys but has accounts", () => {
    const cfg = makeCfg({
      accounts: { "ws-a": { apiKey: "a" } },
    });
    const ids = listStreamChatAccountIds(cfg);
    expect(ids).not.toContain("default");
    expect(ids).toContain("ws-a");
  });
});

describe("resolveStreamChatAccount", () => {
  const fullCfg = makeCfg({
    apiKey: "key-1",
    botUserId: "bot-1",
    botUserToken: "token-1",
    botUserName: "Bot One",
  });

  it("returns default account with correct defaults", () => {
    const account = resolveStreamChatAccount(fullCfg);
    expect(account.accountId).toBe("default");
    expect(account.enabled).toBe(true);
    expect(account.configured).toBe(true);
    expect(account.apiKey).toBe("key-1");
    expect(account.botUserId).toBe("bot-1");
    expect(account.botUserToken).toBe("token-1");
    expect(account.botUserName).toBe("Bot One");
    expect(account.dmPolicy).toBe("open");
    expect(account.ackReaction).toBe("eyes");
    expect(account.doneReaction).toBe("white_check_mark");
    expect(account.streamingThrottle).toBe(15);
  });

  it("accountId defaults to 'default' when null", () => {
    const account = resolveStreamChatAccount(fullCfg, null);
    expect(account.accountId).toBe("default");
  });

  it("accountId defaults to 'default' when undefined", () => {
    const account = resolveStreamChatAccount(fullCfg, undefined);
    expect(account.accountId).toBe("default");
  });

  it("accountId defaults to 'default' when empty string", () => {
    const account = resolveStreamChatAccount(fullCfg, "");
    expect(account.accountId).toBe("default");
  });

  it("named account merges over base config", () => {
    const cfg = makeCfg({
      apiKey: "base-key",
      botUserId: "base-bot",
      botUserToken: "base-token",
      accounts: {
        custom: {
          apiKey: "custom-key",
          botUserId: "custom-bot",
          botUserToken: "custom-token",
          ackReaction: "wave",
        },
      },
    });
    const account = resolveStreamChatAccount(cfg, "custom");
    expect(account.accountId).toBe("custom");
    expect(account.apiKey).toBe("custom-key");
    expect(account.botUserId).toBe("custom-bot");
    expect(account.ackReaction).toBe("wave");
  });

  it("configured is true only when all three fields are present", () => {
    const account = resolveStreamChatAccount(fullCfg);
    expect(account.configured).toBe(true);
  });

  it("configured is false when apiKey is missing", () => {
    const cfg = makeCfg({ botUserId: "bot", botUserToken: "token" });
    const account = resolveStreamChatAccount(cfg);
    expect(account.configured).toBe(false);
  });

  it("configured is false when botUserId is missing", () => {
    const cfg = makeCfg({ apiKey: "key", botUserToken: "token" });
    const account = resolveStreamChatAccount(cfg);
    expect(account.configured).toBe(false);
  });

  it("configured is false when botUserToken is missing", () => {
    const cfg = makeCfg({ apiKey: "key", botUserId: "bot" });
    const account = resolveStreamChatAccount(cfg);
    expect(account.configured).toBe(false);
  });

  it("returns correct defaults for empty config", () => {
    const cfg = makeCfg({});
    const account = resolveStreamChatAccount(cfg);
    expect(account.enabled).toBe(true);
    expect(account.configured).toBe(false);
    expect(account.apiKey).toBe("");
    expect(account.botUserId).toBe("");
    expect(account.botUserToken).toBe("");
    expect(account.dmPolicy).toBe("open");
    expect(account.ackReaction).toBe("eyes");
    expect(account.doneReaction).toBe("white_check_mark");
    expect(account.streamingThrottle).toBe(15);
  });
});
