import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import { ConnectionWatchdog } from "./connection-watchdog.js";

describe("ConnectionWatchdog", () => {
  const TIMEOUT = 120_000;
  const BACKOFF = 5_000;

  let onReconnect: ReturnType<typeof vi.fn>;
  let onFatalFailure: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.useFakeTimers();
    onReconnect = vi.fn().mockResolvedValue(undefined);
    onFatalFailure = vi.fn();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function createWatchdog(overrides?: {
    disconnectTimeoutMs?: number;
    maxReconnectAttempts?: number;
  }) {
    return new ConnectionWatchdog({
      disconnectTimeoutMs: overrides?.disconnectTimeoutMs ?? TIMEOUT,
      maxReconnectAttempts: overrides?.maxReconnectAttempts ?? 5,
      onReconnect,
      onFatalFailure,
    });
  }

  // -------------------------------------------------------------------------
  // Basic timer behavior
  // -------------------------------------------------------------------------

  it("calls onReconnect after disconnectTimeoutMs of being offline", async () => {
    const wd = createWatchdog();

    wd.markOffline();
    expect(onReconnect).not.toHaveBeenCalled();

    // Advance just under timeout — should not fire yet
    await vi.advanceTimersByTimeAsync(TIMEOUT - 1);
    expect(onReconnect).not.toHaveBeenCalled();

    // Advance past timeout
    await vi.advanceTimersByTimeAsync(1);
    expect(onReconnect).toHaveBeenCalledOnce();

    wd.dispose();
  });

  it("does not call onReconnect if markOnline is called before timeout", async () => {
    const wd = createWatchdog();

    wd.markOffline();
    await vi.advanceTimersByTimeAsync(TIMEOUT / 2);
    wd.markOnline();
    await vi.advanceTimersByTimeAsync(TIMEOUT);

    expect(onReconnect).not.toHaveBeenCalled();

    wd.dispose();
  });

  // -------------------------------------------------------------------------
  // markOnline resets attempt counter
  // -------------------------------------------------------------------------

  it("resets reconnect attempt counter on markOnline", async () => {
    const wd = createWatchdog({ maxReconnectAttempts: 2 });

    // First offline → reconnect
    wd.markOffline();
    await vi.advanceTimersByTimeAsync(TIMEOUT);
    expect(onReconnect).toHaveBeenCalledTimes(1);

    // Come back online
    wd.markOnline();

    // Go offline again — counter should be reset
    wd.markOffline();
    await vi.advanceTimersByTimeAsync(TIMEOUT);
    expect(onReconnect).toHaveBeenCalledTimes(2);
    expect(onFatalFailure).not.toHaveBeenCalled();

    wd.dispose();
  });

  // -------------------------------------------------------------------------
  // Multiple reconnect attempts
  // -------------------------------------------------------------------------

  it("schedules another attempt if still offline after onReconnect resolves", async () => {
    const wd = createWatchdog();

    wd.markOffline();

    // First attempt
    await vi.advanceTimersByTimeAsync(TIMEOUT);
    expect(onReconnect).toHaveBeenCalledTimes(1);

    // Still offline → second attempt after another timeout
    await vi.advanceTimersByTimeAsync(TIMEOUT);
    expect(onReconnect).toHaveBeenCalledTimes(2);

    wd.dispose();
  });

  // -------------------------------------------------------------------------
  // maxReconnectAttempts → onFatalFailure
  // -------------------------------------------------------------------------

  it("calls onFatalFailure when maxReconnectAttempts is exceeded", async () => {
    const wd = createWatchdog({ maxReconnectAttempts: 2 });

    wd.markOffline();

    // Attempt 1
    await vi.advanceTimersByTimeAsync(TIMEOUT);
    expect(onReconnect).toHaveBeenCalledTimes(1);

    // Attempt 2
    await vi.advanceTimersByTimeAsync(TIMEOUT);
    expect(onReconnect).toHaveBeenCalledTimes(2);

    // Attempt 3 would exceed max → fatal
    await vi.advanceTimersByTimeAsync(TIMEOUT);
    expect(onReconnect).toHaveBeenCalledTimes(2); // not called again
    expect(onFatalFailure).toHaveBeenCalledOnce();

    wd.dispose();
  });

  it("does not call onFatalFailure when maxReconnectAttempts is 0 (unlimited)", async () => {
    const wd = createWatchdog({ maxReconnectAttempts: 0 });

    wd.markOffline();

    for (let i = 0; i < 10; i++) {
      await vi.advanceTimersByTimeAsync(TIMEOUT);
    }

    expect(onReconnect).toHaveBeenCalledTimes(10);
    expect(onFatalFailure).not.toHaveBeenCalled();

    wd.dispose();
  });

  // -------------------------------------------------------------------------
  // dispose() prevents further activity
  // -------------------------------------------------------------------------

  it("does not fire after dispose", async () => {
    const wd = createWatchdog();

    wd.markOffline();
    wd.dispose();

    await vi.advanceTimersByTimeAsync(TIMEOUT * 2);
    expect(onReconnect).not.toHaveBeenCalled();
  });

  it("does not schedule new timers after dispose even if markOffline is called", async () => {
    const wd = createWatchdog();
    wd.dispose();

    wd.markOffline();
    await vi.advanceTimersByTimeAsync(TIMEOUT * 2);
    expect(onReconnect).not.toHaveBeenCalled();
  });

  // -------------------------------------------------------------------------
  // Failed onReconnect (throws) → backoff retry
  // -------------------------------------------------------------------------

  it("retries with backoff when onReconnect throws", async () => {
    onReconnect
      .mockRejectedValueOnce(new Error("network error"))
      .mockResolvedValueOnce(undefined);

    const wd = createWatchdog();

    wd.markOffline();

    // First attempt fires after timeout
    await vi.advanceTimersByTimeAsync(TIMEOUT);
    expect(onReconnect).toHaveBeenCalledTimes(1);

    // After failure: 5s backoff, then scheduleReconnect queues another timeout
    await vi.advanceTimersByTimeAsync(BACKOFF);
    // Now scheduleReconnect fires and sets a new timer for disconnectTimeoutMs
    await vi.advanceTimersByTimeAsync(TIMEOUT);
    expect(onReconnect).toHaveBeenCalledTimes(2);

    wd.dispose();
  });

  // -------------------------------------------------------------------------
  // Rapid toggling
  // -------------------------------------------------------------------------

  it("handles rapid markOffline/markOnline toggling without issues", async () => {
    const wd = createWatchdog();

    for (let i = 0; i < 20; i++) {
      wd.markOffline();
      wd.markOnline();
    }

    await vi.advanceTimersByTimeAsync(TIMEOUT * 2);
    expect(onReconnect).not.toHaveBeenCalled();

    wd.dispose();
  });

  // -------------------------------------------------------------------------
  // markOffline when already offline is a no-op
  // -------------------------------------------------------------------------

  it("ignores markOffline when already offline", async () => {
    const wd = createWatchdog();

    wd.markOffline();
    // Second markOffline should not reset the timer
    await vi.advanceTimersByTimeAsync(TIMEOUT / 2);
    wd.markOffline(); // no-op because already offline
    await vi.advanceTimersByTimeAsync(TIMEOUT / 2);

    // Should fire at the original timeout, not delayed by the second markOffline
    expect(onReconnect).toHaveBeenCalledOnce();

    wd.dispose();
  });

  // -------------------------------------------------------------------------
  // markOnline during reconnect attempt stops further retries
  // -------------------------------------------------------------------------

  it("stops retrying if markOnline is called between attempts", async () => {
    const wd = createWatchdog();

    wd.markOffline();
    await vi.advanceTimersByTimeAsync(TIMEOUT);
    expect(onReconnect).toHaveBeenCalledTimes(1);

    // Simulate connection restored during the reconnect
    wd.markOnline();

    // No further attempts
    await vi.advanceTimersByTimeAsync(TIMEOUT * 3);
    expect(onReconnect).toHaveBeenCalledTimes(1);

    wd.dispose();
  });
});
