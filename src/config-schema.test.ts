import { describe, expect, it } from "vitest";
import { StreamChatConfigSchema } from "./config-schema.js";

// StreamChatConfigSchema is typed as z.ZodTypeAny so .parse() returns unknown.
// We use a typed helper to avoid `any` casts in every test.
function parse(input: unknown): Record<string, unknown> {
  return StreamChatConfigSchema.parse(input) as Record<string, unknown>;
}

describe("StreamChatConfigSchema", () => {
  it("parses empty object with correct defaults", () => {
    const result = parse({});
    expect(result).toMatchObject({
      enabled: true,
      dmPolicy: "open",
      ackReaction: "eyes",
      doneReaction: "white_check_mark",
      streamingThrottle: 15,
      watchdogTimeoutMs: 120000,
      watchdogMaxRetries: 0,
    });
  });

  it("parses complete valid config", () => {
    const input = {
      enabled: false,
      apiKey: "abc123",
      botUserId: "bot-1",
      botUserToken: "token-xyz",
      botUserName: "TestBot",
      dmPolicy: "pairing",
      ackReaction: "thumbsup",
      doneReaction: "check",
      streamingThrottle: 10,
    };
    const result = parse(input);
    expect(result).toMatchObject(input);
  });

  it("rejects streamingThrottle of 0", () => {
    expect(() => parse({ streamingThrottle: 0 })).toThrow();
  });

  it("rejects negative streamingThrottle", () => {
    expect(() => parse({ streamingThrottle: -1 })).toThrow();
  });

  it("rejects non-integer streamingThrottle", () => {
    expect(() => parse({ streamingThrottle: 1.5 })).toThrow();
  });

  it("rejects invalid dmPolicy", () => {
    expect(() => parse({ dmPolicy: "closed" })).toThrow();
  });

  it("accepts 'pairing' as dmPolicy", () => {
    const result = parse({ dmPolicy: "pairing" });
    expect(result.dmPolicy).toBe("pairing");
  });

  it("parses recursive accounts map", () => {
    const input = {
      apiKey: "main-key",
      accounts: {
        "workspace-b": {
          apiKey: "ws-b-key",
          botUserId: "bot-b",
          streamingThrottle: 5,
        },
      },
    };
    const result = parse(input);
    const accounts = result.accounts as Record<string, Record<string, unknown>>;
    expect(accounts["workspace-b"]).toMatchObject({
      apiKey: "ws-b-key",
      botUserId: "bot-b",
      streamingThrottle: 5,
    });
  });

  it("applies defaults to nested account configs", () => {
    const result = parse({
      accounts: { sub: {} },
    });
    const accounts = result.accounts as Record<string, Record<string, unknown>>;
    expect(accounts.sub).toMatchObject({
      enabled: true,
      dmPolicy: "open",
      ackReaction: "eyes",
      doneReaction: "white_check_mark",
      streamingThrottle: 15,
      watchdogTimeoutMs: 120000,
      watchdogMaxRetries: 0,
    });
  });

  it("accepts custom watchdog values", () => {
    const result = parse({
      watchdogTimeoutMs: 60000,
      watchdogMaxRetries: 10,
    });
    expect(result.watchdogTimeoutMs).toBe(60000);
    expect(result.watchdogMaxRetries).toBe(10);
  });

  it("rejects watchdogTimeoutMs below 10000", () => {
    expect(() => parse({ watchdogTimeoutMs: 5000 })).toThrow();
  });

  it("rejects negative watchdogMaxRetries", () => {
    expect(() => parse({ watchdogMaxRetries: -1 })).toThrow();
  });

  it("accepts watchdogMaxRetries of 0 (unlimited)", () => {
    const result = parse({ watchdogMaxRetries: 0 });
    expect(result.watchdogMaxRetries).toBe(0);
  });
});
