import { describe, expect, it, vi } from "vitest";
import { truncate, safeAsync } from "./utils.js";

describe("truncate", () => {
  it("returns text unchanged when shorter than maxLength", () => {
    expect(truncate("hello", 10)).toBe("hello");
  });

  it("returns text unchanged when exactly at maxLength", () => {
    expect(truncate("hello", 5)).toBe("hello");
  });

  it("truncates and appends '...' when longer than maxLength", () => {
    expect(truncate("hello world", 8)).toBe("hello...");
  });

  it("handles empty string", () => {
    expect(truncate("", 10)).toBe("");
  });

  it("handles maxLength of 3 (boundary)", () => {
    expect(truncate("abcdef", 3)).toBe("...");
  });

  it("handles single-character truncation", () => {
    // maxLength = 4 → keeps 1 char + "..."
    expect(truncate("abcdef", 4)).toBe("a...");
  });
});

describe("safeAsync", () => {
  it("calls the provided async function", async () => {
    const fn = vi.fn().mockResolvedValue("ok");
    safeAsync(fn);
    // Give it a tick to resolve
    await new Promise((r) => setTimeout(r, 0));
    expect(fn).toHaveBeenCalled();
  });

  it("does not throw when fn succeeds", () => {
    expect(() => safeAsync(() => Promise.resolve("ok"))).not.toThrow();
  });

  it("does not throw when fn rejects", async () => {
    const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    safeAsync(() => Promise.reject(new Error("boom")), log, "test-label");
    // Give it a tick to settle
    await new Promise((r) => setTimeout(r, 0));
    // Should not throw — error is swallowed
  });

  it("logs error with label when fn rejects and log is provided", async () => {
    const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    safeAsync(() => Promise.reject(new Error("boom")), log, "test-label");
    await new Promise((r) => setTimeout(r, 0));
    expect(log.error).toHaveBeenCalledWith(
      expect.stringContaining("boom"),
    );
    expect(log.error).toHaveBeenCalledWith(
      expect.stringContaining("test-label"),
    );
  });

  it("logs error without label when label is undefined", async () => {
    const log = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    safeAsync(() => Promise.reject(new Error("boom")), log);
    await new Promise((r) => setTimeout(r, 0));
    expect(log.error).toHaveBeenCalledWith(
      expect.stringContaining("boom"),
    );
  });

  it("does not throw when log is undefined", async () => {
    safeAsync(() => Promise.reject(new Error("boom")));
    await new Promise((r) => setTimeout(r, 0));
    // No assertion needed — just verifying no uncaught error
  });
});
