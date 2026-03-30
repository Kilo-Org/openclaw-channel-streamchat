import type { ChannelLogSink } from "openclaw/plugin-sdk";

// ---------------------------------------------------------------------------
// ConnectionWatchdog — detects prolonged disconnects and forces reconnect
// ---------------------------------------------------------------------------

export interface ConnectionWatchdogOptions {
  /** How long to wait (ms) after going offline before forcing a reconnect. */
  disconnectTimeoutMs: number;
  /** Maximum reconnect attempts before giving up. 0 = unlimited. */
  maxReconnectAttempts: number;
  /** Async callback that performs the actual reconnect cycle. */
  onReconnect: () => Promise<void>;
  /** Called when all reconnect attempts are exhausted. */
  onFatalFailure?: () => void;
  /** Optional logger. */
  log?: ChannelLogSink;
}

const RECONNECT_BACKOFF_BASE_MS = 5_000;
const RECONNECT_BACKOFF_MAX_MS = 60_000;

export class ConnectionWatchdog {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempts = 0;
  private online = true;
  private disposed = false;
  private readonly options: ConnectionWatchdogOptions;

  constructor(options: ConnectionWatchdogOptions) {
    this.options = options;
  }

  /** Signal that the connection is back online. Resets the watchdog. */
  markOnline(): void {
    this.online = true;
    this.reconnectAttempts = 0;
    this.clearTimer();
  }

  /**
   * Signal that the connection went offline. Starts the watchdog timer.
   *
   * The `!this.online` guard is intentional: if the watchdog already considers
   * itself offline (timer running), duplicate markOffline() calls are no-ops.
   * This is safe because the reconnect timer is already armed — re-arming it
   * would reset the timeout window and delay reconnection unnecessarily.
   */
  markOffline(): void {
    if (this.disposed || !this.online) return;
    this.online = false;
    this.scheduleReconnect();
  }

  /** Permanently shut down the watchdog. */
  dispose(): void {
    this.disposed = true;
    this.clearTimer();
  }

  // -------------------------------------------------------------------------
  // Internal
  // -------------------------------------------------------------------------

  /**
   * Schedules the next reconnect attempt after `disconnectTimeoutMs`.
   * Uses a flat interval (not exponential backoff) because this path
   * represents "still offline but no error" — the connection simply
   * hasn't come back yet.  The exponential backoff in the catch block
   * of `attemptReconnect` handles the "reconnect threw an error" case.
   */
  private scheduleReconnect(): void {
    this.clearTimer();
    if (this.disposed) return;
    this.timer = setTimeout(
      () => void this.attemptReconnect(),
      this.options.disconnectTimeoutMs,
    );
  }

  private async attemptReconnect(): Promise<void> {
    if (this.disposed || this.online) return;

    this.reconnectAttempts++;

    if (
      this.options.maxReconnectAttempts > 0 &&
      this.reconnectAttempts > this.options.maxReconnectAttempts
    ) {
      this.options.log?.error?.(
        `[StreamChat] Watchdog: max reconnect attempts (${this.options.maxReconnectAttempts}) exceeded`,
      );
      this.options.onFatalFailure?.();
      return;
    }

    this.options.log?.warn?.(
      `[StreamChat] Watchdog: offline for >${this.options.disconnectTimeoutMs}ms, ` +
        `forcing reconnect (attempt ${this.reconnectAttempts})`,
    );

    try {
      await this.options.onReconnect();
      // If still not online after reconnect resolved, schedule another attempt.
      // In normal operation the `onReconnect` callback calls `markOnline()`,
      // so `this.online` is already `true` and this branch is a no-op.
      // This is a defensive fallback for edge cases where the caller's
      // reconnect succeeds but `markOnline()` is not called (e.g. a future
      // caller or a race with an SDK event).
      if (!this.online && !this.disposed) {
        this.scheduleReconnect();
      }
    } catch (err) {
      this.options.log?.error?.(
        `[StreamChat] Watchdog: reconnect failed: ${String(err)}`,
      );
      if (!this.disposed) {
        // Exponential backoff before scheduling the next attempt
        // (capped at RECONNECT_BACKOFF_MAX_MS to avoid excessively long waits)
        const backoff = Math.min(
          RECONNECT_BACKOFF_BASE_MS * 2 ** Math.min(this.reconnectAttempts - 1, 5),
          RECONNECT_BACKOFF_MAX_MS,
        );
        this.clearTimer();
        this.timer = setTimeout(
          () => this.scheduleReconnect(),
          backoff,
        );
      }
    }
  }

  private clearTimer(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
}
