import { vi } from "vitest";

/**
 * Creates a mock Stream Chat Channel object.
 * All methods are vi.fn() stubs with sensible defaults.
 */
export function createMockChannel(overrides?: Record<string, unknown>): unknown {
  const base: Record<string, unknown> = {
    sendMessage: vi.fn().mockResolvedValue({ message: { id: "resp-msg-1" } }),
    sendEvent: vi.fn().mockResolvedValue({}),
    sendReaction: vi.fn().mockResolvedValue({}),
    deleteReaction: vi.fn().mockResolvedValue({}),
    getReplies: vi.fn().mockResolvedValue({}),
    watch: vi.fn().mockResolvedValue({}),
    markRead: vi.fn().mockResolvedValue(null),
    sendFile: vi.fn().mockResolvedValue({ file: "https://cdn.stream.io/file.jpg" }),
    sendImage: vi.fn().mockResolvedValue({ file: "https://cdn.stream.io/image.jpg" }),
    state: { messages: [] },
    type: "messaging",
    id: "test-channel",
  };
  return { ...base, ...overrides };
}

/**
 * Creates a mock StreamChat client object.
 * All methods are vi.fn() stubs with sensible defaults.
 */
export function createMockStreamChatClient(overrides?: Record<string, unknown>): unknown {
  const defaultChannel = createMockChannel();
  const base: Record<string, unknown> = {
    connectUser: vi.fn().mockResolvedValue({}),
    disconnectUser: vi.fn().mockResolvedValue({}),
    queryChannels: vi.fn().mockResolvedValue([]),
    channel: vi.fn().mockReturnValue(defaultChannel),
    partialUpdateMessage: vi.fn().mockResolvedValue({}),
    on: vi.fn(),
    off: vi.fn(),
  };
  return { ...base, ...overrides };
}
