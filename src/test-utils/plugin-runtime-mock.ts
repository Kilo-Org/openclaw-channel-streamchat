import { vi } from "vitest";

/**
 * Minimal PluginRuntime mock covering only the surface area used by the
 * streamchat plugin.  Uses `unknown` for the return type so that tests
 * can cast to whatever concrete type they need without pulling in the
 * full PluginRuntime type from the SDK (which would require resolving
 * the openclaw peer-dep at type-check time).
 */
export function createPluginRuntimeMock(overrides?: Record<string, unknown>): unknown {
  const base = {
    version: "1.0.0-test",
    system: {
      enqueueSystemEvent: vi.fn(),
    },
    media: {
      loadWebMedia: vi.fn().mockResolvedValue({
        buffer: Buffer.from("fake-media"),
        contentType: "image/png",
        fileName: "image.png",
      }),
    },
    channel: {
      routing: {
        resolveAgentRoute: vi.fn(() => ({
          agentId: "main",
          accountId: "default",
          sessionKey: "agent:main:streamchat:channel:test-channel",
          mainSessionKey: "agent:main:main",
        })),
      },
      session: {
        resolveStorePath: vi.fn(() => "/tmp/sessions.json"),
        recordInboundSession: vi.fn(),
      },
      reply: {
        finalizeInboundContext: vi.fn(
          (ctx: Record<string, unknown>) => ctx,
        ),
        dispatchReplyWithBufferedBlockDispatcher: vi.fn(
          async () => undefined,
        ),
      },
      media: {
        fetchRemoteMedia: vi.fn().mockResolvedValue({
          buffer: Buffer.from("fake-image-data"),
          contentType: "image/jpeg",
          fileName: "photo.jpg",
        }),
        saveMediaBuffer: vi.fn().mockResolvedValue({
          id: "media-1",
          path: "/tmp/media/photo.jpg",
          size: 1024,
          contentType: "image/jpeg",
        }),
      },
    },
  };

  if (overrides) {
    return deepMerge(base, overrides);
  }
  return base;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function deepMerge(
  base: Record<string, unknown>,
  overrides: Record<string, unknown>,
): Record<string, unknown> {
  const result: Record<string, unknown> = { ...base };
  for (const [key, value] of Object.entries(overrides)) {
    if (value === undefined) continue;
    const baseValue = result[key];
    if (isObject(baseValue) && isObject(value)) {
      result[key] = deepMerge(
        baseValue as Record<string, unknown>,
        value as Record<string, unknown>,
      );
    } else {
      result[key] = value;
    }
  }
  return result;
}
