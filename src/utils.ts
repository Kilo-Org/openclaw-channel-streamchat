import type { Event } from "stream-chat";
import type { ChannelLogSink } from "openclaw/plugin-sdk";

export function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text;
  return text.slice(0, maxLength - 3) + "...";
}

export function safeAsync(
  fn: () => Promise<unknown>,
  log?: ChannelLogSink,
  label?: string,
): void {
  fn().catch((err) => {
    log?.error?.(`[StreamChat]${label ? ` ${label}` : ""} ${String(err)}`);
  });
}

// ---------------------------------------------------------------------------
// Stream Chat SDK type gap helpers
// ---------------------------------------------------------------------------
// The Stream Chat SDK's `Event` type does not include every field that the
// server actually sends. These helpers centralise the unsafe access so it is
// easy to update if/when the SDK types catch up.

/**
 * Extract `message_id` from a Stream Chat event.
 *
 * Some events (e.g. `ai_indicator.stop`, `reaction.new`) carry `message_id`
 * at the top level but the SDK's `Event` type omits it. Falls back to
 * `event.message?.id` when the top-level field is absent.
 */
export function getEventMessageId(event: Event): string | undefined {
  return (
    event.message?.id ??
    ((event as unknown as Record<string, unknown>).message_id as
      | string
      | undefined)
  );
}

/**
 * Extract the `online` boolean from a `connection.changed` event.
 *
 * The SDK's `Event` type does not declare `online`, but the server includes it.
 */
export function getConnectionOnline(event: Event): boolean | undefined {
  return (event as unknown as { online?: boolean }).online;
}
