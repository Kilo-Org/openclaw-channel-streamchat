import { randomUUID } from "node:crypto";
import type { Event } from "stream-chat";
import type {
  AgentMediaPayload,
  ChannelGatewayContext,
  ChannelLogSink,
  ChannelOutboundContext,
  OpenClawConfig,
} from "openclaw/plugin-sdk";
import {
  buildAgentMediaPayload,
  buildChannelConfigSchema,
} from "openclaw/plugin-sdk";
import { StreamChatConfigSchema } from "./config-schema.js";
import { getStreamChatRuntime } from "./runtime.js";
import { StreamChatClientRuntime } from "./stream-chat-runtime.js";
import { StreamingHandler } from "./streaming.js";
import { RunContextMap } from "./run-context.js";
import { buildEnvelope } from "./envelope.js";
import { safeAsync, getEventMessageId, getConnectionOnline } from "./utils.js";
import type {
  ResolvedAccount,
  StreamChatChannelPlugin,
  RunContext,
} from "./types.js";
import {
  listStreamChatAccountIds,
  resolveStreamChatAccount,
} from "./types.js";

// Track which threads we've already seen (for first-in-thread detection)
const seenThreads = new Set<string>();

/**
 * Return a promise that resolves when the abort signal fires.
 * Keeps the startAccount task alive (pending) until framework shutdown.
 * Equivalent to the SDK's `waitUntilAbort` from `openclaw/plugin-sdk`.
 */
function waitUntilAbort(
  signal?: AbortSignal,
  onAbort?: () => void | Promise<void>,
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const complete = () => {
      Promise.resolve(onAbort?.()).then(() => resolve(), reject);
    };
    if (!signal) return; // stays pending forever
    if (signal.aborted) {
      complete();
      return;
    }
    signal.addEventListener("abort", complete, { once: true });
  });
}

// Module-level registry of active gateway cleanup functions keyed by accountId.
// Allows startAccount to force-stop a stale connection if the framework calls
// startAccount again without having called stop() first (e.g. in-process reloads).
const activeGatewayCleanup = new Map<string, () => void>();

// ---------------------------------------------------------------------------
// Reactions helper
// ---------------------------------------------------------------------------

async function addReaction(
  runtime: StreamChatClientRuntime,
  channelType: string,
  channelId: string,
  messageId: string,
  reactionType: string,
  log?: ChannelLogSink,
): Promise<void> {
  try {
    const channel = await runtime.getOrQueryChannel(channelType, channelId);
    await channel.sendReaction(messageId, { type: reactionType });
  } catch (err) {
    log?.warn?.(
      `[StreamChat] Failed to add reaction ${reactionType}: ${String(err)}`,
    );
  }
}

async function removeReaction(
  runtime: StreamChatClientRuntime,
  channelType: string,
  channelId: string,
  messageId: string,
  reactionType: string,
  log?: ChannelLogSink,
): Promise<void> {
  try {
    const channel = await runtime.getOrQueryChannel(channelType, channelId);
    await channel.deleteReaction(messageId, reactionType);
  } catch (err) {
    log?.warn?.(
      `[StreamChat] Failed to remove reaction ${reactionType}: ${String(err)}`,
    );
  }
}

// ---------------------------------------------------------------------------
// Inbound message handler
// ---------------------------------------------------------------------------

interface HandleMessageParams {
  cfg: OpenClawConfig;
  accountId: string;
  account: ResolvedAccount;
  event: Event;
  chatRuntime: StreamChatClientRuntime;
  streamingHandler: StreamingHandler;
  runContexts: RunContextMap;
  log?: ChannelLogSink;
}

async function handleStreamChatMessage(params: HandleMessageParams): Promise<void> {
  const {
    cfg,
    accountId,
    account,
    event,
    chatRuntime,
    streamingHandler,
    runContexts,
    log,
  } = params;
  const rt = getStreamChatRuntime();

  const message = event.message;
  if (!message) return;

  // Bot echo prevention: skip our own messages and AI-generated messages
  if (event.user?.id === account.botUserId) return;
  if (message.ai_generated) return;

  const text = message.text?.trim() ?? "";
  const attachments = message.attachments ?? [];
  if (!text && attachments.length === 0) return;

  const channelType = event.channel_type ?? "messaging";
  const channelId = event.channel_id ?? "";
  const messageId = message.id;
  const senderId = event.user?.id ?? "unknown";
  const senderName = event.user?.name || senderId;

  // Determine thread and reply context
  const threadParentId = message.parent_id ?? null;
  const quotedMessageId = message.quoted_message_id ?? null;
  const quotedMessage = message.quoted_message ?? null;

  // Resolve agent route
  // Use peer kind "channel" so the framework builds per-channel session keys:
  //   agent:<agentId>:streamchat:channel:<channelId>
  // This ensures each Stream Chat channel gets its own session (per action plan).
  const route = rt.channel.routing.resolveAgentRoute({
    cfg,
    channel: "streamchat",
    accountId,
    peer: { kind: "channel", id: channelId },
  });

  const storePath = rt.channel.session.resolveStorePath(
    cfg.session?.store,
    { agentId: route.agentId },
  );

  // Build envelope with thread/reply context
  let threadParentInfo: {
    id: string;
    text?: string;
    userId?: string;
    userName?: string;
  } | null = null;

  if (threadParentId) {
    // Try to get the parent message for context
    try {
      const channel = await chatRuntime.getOrQueryChannel(channelType, channelId);
      await channel.getReplies(threadParentId, { limit: 0 });
      // The parent message is embedded in the channel messages
      const state = channel.state;
      const parentMsg = state.messages.find((m) => m.id === threadParentId);
      threadParentInfo = {
        id: threadParentId,
        text: parentMsg?.text ?? undefined,
        userId: parentMsg?.user?.id ?? undefined,
        userName: parentMsg?.user?.name ?? undefined,
      };
    } catch {
      threadParentInfo = { id: threadParentId };
    }
  }

  let quotedInfo: {
    id: string;
    text?: string;
    userId?: string;
    userName?: string;
  } | null = null;

  if (quotedMessageId || quotedMessage) {
    quotedInfo = {
      id: quotedMessageId ?? quotedMessage?.id ?? "",
      text: quotedMessage?.text ?? undefined,
      userId: quotedMessage?.user?.id ?? undefined,
      userName: quotedMessage?.user?.name ?? undefined,
    };
  }

  const isFirstInThread = threadParentId
    ? !seenThreads.has(threadParentId)
    : false;
  if (threadParentId) seenThreads.add(threadParentId);

  // Download attachments and build media payload
  let mediaPayload: AgentMediaPayload = {};

  if (attachments.length > 0) {
    // Download attachments in parallel for lower latency on multi-attachment messages
    const downloadResults = await Promise.allSettled(
      attachments
        .filter((att) => att.image_url || att.asset_url || att.thumb_url)
        .map(async (att) => {
          // Stream Chat attachments have image_url for images, asset_url for files.
          // Use || (not ??) to skip empty-string URLs that some attachment types set.
          const url = (att.image_url || att.asset_url || att.thumb_url)!;
          const fetched = await rt.channel.media.fetchRemoteMedia({
            url,
            filePathHint: att.title ?? att.fallback ?? url,
          });
          const saved = await rt.channel.media.saveMediaBuffer(
            fetched.buffer,
            fetched.contentType ?? att.mime_type,
            "inbound",
          );
          return {
            path: saved.path,
            contentType: saved.contentType ?? att.mime_type ?? null,
          };
        }),
    );

    const mediaList: Array<{ path: string; contentType?: string | null }> = [];
    for (const result of downloadResults) {
      if (result.status === "fulfilled") {
        mediaList.push(result.value);
      } else {
        log?.warn?.(
          `[StreamChat] Failed to download attachment: ${String(result.reason)}`,
        );
      }
    }

    if (mediaList.length > 0) {
      mediaPayload = buildAgentMediaPayload(mediaList);
    } else if (!text) {
      // All attachment downloads failed and there is no text — nothing useful
      // to forward to the agent. Drop the message to avoid a confusing empty inbound.
      log?.warn?.(
        "[StreamChat] All attachment downloads failed and no text — skipping message",
      );
      return;
    }
  }

  const envelope = buildEnvelope({
    text,
    senderId,
    senderName,
    messageId,
    quotedMessage: quotedInfo,
    threadParent: threadParentInfo,
    isFirstInThread,
  });

  // Finalize inbound context
  const to = channelId;
  const fromLabel = `${senderName} (${senderId})`;

  const ctx = rt.channel.reply.finalizeInboundContext({
    Body: envelope.body,
    RawBody: text,
    CommandBody: envelope.commandBody,
    From: to,
    To: to,
    SessionKey: route.sessionKey,
    AccountId: accountId,
    ChatType: "channel" as const,
    ConversationLabel: fromLabel,
    SenderName: senderName,
    SenderId: senderId,
    Provider: "streamchat",
    Surface: "streamchat",
    MessageSid: messageId,
    Timestamp: message.created_at
      ? new Date(message.created_at).getTime()
      : Date.now(),
    OriginatingChannel: "streamchat",
    OriginatingTo: to,
    ...mediaPayload,
  });

  // Record session
  await rt.channel.session.recordInboundSession({
    storePath,
    sessionKey: ctx.SessionKey || route.sessionKey,
    ctx,
    updateLastRoute: {
      sessionKey: route.mainSessionKey,
      channel: "streamchat",
      to,
      accountId,
    },
    onRecordError: (err: unknown) => {
      log?.error?.(
        `[StreamChat] Failed to record inbound session: ${String(err)}`,
      );
    },
  });

  log?.info?.(
    `[StreamChat] Inbound: from=${senderName} text="${text.slice(0, 50)}"`,
  );

  // Send ack reaction
  if (account.ackReaction) {
    safeAsync(
      () =>
        addReaction(
          chatRuntime,
          channelType,
          channelId,
          messageId,
          account.ackReaction,
          log,
        ),
      log,
      "ack reaction",
    );
  }

  // Create RunContext for delivery routing
  const runId = randomUUID();
  const runCtx: RunContext = {
    runId,
    channelType,
    channelId,
    threadParentId,
    inboundMessageId: messageId,
    senderId,
    responseMessageId: null,
  };
  runContexts.set(runId, runCtx);

  // Pre-create the placeholder message before dispatch so the message ID is
  // available when onPartialReply fires (which is called fire-and-forget by
  // OpenClaw and cannot safely do async work itself).
  const responseChannel = await chatRuntime.getOrQueryChannel(channelType, channelId);
  await streamingHandler.onRunStarted(runId, responseChannel, runCtx);

  let errorDelivered = false;

  // Track cumulative text from onPartialReply to compute per-token deltas.
  // onPartialReply gives full accumulated text so far ("2", "2 +", "2 + 2 = 4"),
  // while onTextChunk expects a delta and appends it. We slice to get the new portion.
  let lastPartialText = "";

  // Dispatch reply via the buffered block dispatcher.
  // onPartialReply fires for every streaming token (preview streaming).
  // deliver is called once per complete block; used here only for tool/error events.
  await rt.channel.reply.dispatchReplyWithBufferedBlockDispatcher({
    ctx,
    cfg,
    replyOptions: {
      onPartialReply: (payload: { text?: string }) => {
        const full = payload.text ?? "";
        const delta = full.slice(lastPartialText.length);
        lastPartialText = full;
        if (delta) {
          void streamingHandler.onTextChunk(runId, delta, account.streamingThrottle);
        }
      },
    },
    dispatcherOptions: {
      responsePrefix: "",
      deliver: async (
        payload: { text?: string; isError?: boolean },
        info: { kind: string },
      ) => {
        try {
          // Tool progress: update indicator to EXTERNAL_SOURCES
          if (info.kind === "tool") {
            await streamingHandler.onRunProgress(runId);
            return;
          }

          // Error: finalize with error state
          if (payload.isError) {
            await streamingHandler.onRunError(
              runId,
              payload.text || "Unknown error",
            );
            errorDelivered = true;
            return;
          }

          // Text blocks are handled token-by-token via onPartialReply above.
        } catch (err) {
          log?.error?.(
            `[StreamChat] Deliver failed: ${String(err)}`,
          );
          throw err;
        }
      },
    },
  });

  // Finalize after all deliveries complete
  if (!errorDelivered) {
    await streamingHandler.onRunCompleted(runId);
  }

  // Swap ack → done reaction
  if (account.ackReaction && account.doneReaction) {
    safeAsync(
      async () => {
        await removeReaction(
          chatRuntime,
          channelType,
          channelId,
          messageId,
          account.ackReaction,
          log,
        );
        await addReaction(
          chatRuntime,
          channelType,
          channelId,
          messageId,
          account.doneReaction,
          log,
        );
      },
      log,
      "reaction swap",
    );
  }

  // Mark channel as read to clear unread badges on the bot's side.
  // This runs per-response; the Stream Chat SDK deduplicates markRead calls
  // server-side so repeated calls for the same channel are effectively no-ops.
  safeAsync(
    () => responseChannel.markRead(),
    log,
    "mark read",
  );

  runContexts.delete(runId);
}

// ---------------------------------------------------------------------------
// Message-updated handler (Feature: Message Editing)
// ---------------------------------------------------------------------------

function handleMessageUpdated(params: {
  event: Event;
  account: ResolvedAccount;
  log?: ChannelLogSink;
  cfg: OpenClawConfig;
  accountId: string;
}): void {
  const { event, account, log, cfg, accountId } = params;
  const message = event.message;
  if (!message) return;
  if (event.user?.id === account.botUserId) return;
  if (message.ai_generated) return;

  const rt = getStreamChatRuntime();
  const channelId = event.channel_id ?? "";
  const senderName = event.user?.name || event.user?.id || "unknown";

  const route = rt.channel.routing.resolveAgentRoute({
    cfg,
    channel: "streamchat",
    accountId,
    peer: { kind: "channel", id: channelId },
  });

  const text = message.text?.trim() ?? "";
  const preview = text.slice(0, 100);
  rt.system.enqueueSystemEvent(
    `Stream Chat message edited by ${senderName}: "${preview}"`,
    {
      sessionKey: route.sessionKey,
      contextKey: `streamchat:message:updated:${channelId}:${message.id}`,
    },
  );
  log?.info?.(`[StreamChat] Message updated: ${message.id} by ${senderName}`);
}

// ---------------------------------------------------------------------------
// Message-deleted handler (Feature: Message Deletion)
// ---------------------------------------------------------------------------

function handleMessageDeleted(params: {
  event: Event;
  account: ResolvedAccount;
  runContexts: RunContextMap;
  streamingHandler: StreamingHandler;
  log?: ChannelLogSink;
  cfg: OpenClawConfig;
  accountId: string;
}): void {
  const { event, account, runContexts, streamingHandler, log, cfg, accountId } = params;
  const message = event.message;
  if (!message) return;
  if (event.user?.id === account.botUserId) return;
  if (message.ai_generated) return;

  const rt = getStreamChatRuntime();
  const channelId = event.channel_id ?? "";

  // Cancel active run if the deleted message triggered one.
  // Race with the normal completion path in handleStreamChatMessage is safe —
  // StreamingHandler's internal guards make onForceStop and onRunCompleted idempotent.
  const activeRun = runContexts.findByInboundMessageId(message.id);
  if (activeRun) {
    streamingHandler.onForceStop(activeRun.runId).catch((err) => {
      log?.warn?.(`[StreamChat] Force stop on delete failed: ${String(err)}`);
    });
  }

  // Enqueue system event
  const senderName = event.user?.name || event.user?.id || "unknown";
  const route = rt.channel.routing.resolveAgentRoute({
    cfg,
    channel: "streamchat",
    accountId,
    peer: { kind: "channel", id: channelId },
  });

  rt.system.enqueueSystemEvent(
    `Stream Chat message deleted by ${senderName} (id: ${message.id})`,
    {
      sessionKey: route.sessionKey,
      contextKey: `streamchat:message:deleted:${channelId}:${message.id}`,
    },
  );
  log?.info?.(`[StreamChat] Message deleted: ${message.id} by ${senderName}`);
}

// ---------------------------------------------------------------------------
// Reaction event handler (Feature: Reaction Events)
// ---------------------------------------------------------------------------

function handleReactionEvent(params: {
  event: Event;
  account: ResolvedAccount;
  log?: ChannelLogSink;
  cfg: OpenClawConfig;
  accountId: string;
  action: "added" | "removed";
}): void {
  const { event, account, cfg, accountId, action } = params;
  // Skip bot's own reactions
  if (event.user?.id === account.botUserId) return;

  const rt = getStreamChatRuntime();
  const channelId = event.channel_id ?? "";
  const senderName = event.user?.name || event.user?.id || "unknown";
  const reactionType = event.reaction?.type ?? "unknown";
  const messageId = getEventMessageId(event) ?? "unknown";

  const route = rt.channel.routing.resolveAgentRoute({
    cfg,
    channel: "streamchat",
    accountId,
    peer: { kind: "channel", id: channelId },
  });

  rt.system.enqueueSystemEvent(
    `Stream Chat reaction ${action}: :${reactionType}: by ${senderName} on message ${messageId}`,
    {
      sessionKey: route.sessionKey,
      contextKey: `streamchat:reaction:${action}:${channelId}:${messageId}:${event.user?.id ?? "anon"}:${reactionType}`,
    },
  );
}

// ---------------------------------------------------------------------------
// Channel plugin definition
// ---------------------------------------------------------------------------

export const streamchatPlugin: StreamChatChannelPlugin = {
  id: "streamchat",

  meta: {
    id: "streamchat",
    label: "Stream Chat",
    selectionLabel: "Stream Chat",
    docsPath: "/channels/streamchat",
    blurb: "Stream Chat messaging channel with AI streaming support.",
    aliases: ["sc"],
  },

  capabilities: {
    chatTypes: ["channel"],
    reactions: true,
    threads: true,
    media: true,
    nativeCommands: false,
    blockStreaming: false,
  },

  reload: { configPrefixes: ["channels.streamchat"] },

  configSchema: buildChannelConfigSchema(StreamChatConfigSchema),

  config: {
    listAccountIds: (cfg: OpenClawConfig): string[] =>
      listStreamChatAccountIds(cfg),

    resolveAccount: (
      cfg: OpenClawConfig,
      accountId?: string | null,
    ): ResolvedAccount => resolveStreamChatAccount(cfg, accountId),

    defaultAccountId: () => "default",

    isConfigured: (account: ResolvedAccount): boolean =>
      Boolean(account.apiKey && account.botUserId && account.botUserToken),

    describeAccount: (account: ResolvedAccount) => ({
      accountId: account.accountId,
      name: account.botUserName || account.botUserId || undefined,
      enabled: account.enabled,
      configured: account.configured,
      running: false,
      lastStartAt: null,
      lastStopAt: null,
      lastError: null,
    }),
  },

  outbound: {
    deliveryMode: "direct",

    sendText: async (ctx: ChannelOutboundContext) => {
      const account = resolveStreamChatAccount(ctx.cfg, ctx.accountId);
      if (!account.configured) {
        throw new Error("StreamChat account not configured");
      }

      // We need to create a temporary client to send the outbound message.
      // In gateway mode, we reuse the running runtime, but for outbound-only
      // we create an ephemeral connection.
      const tempRuntime = new StreamChatClientRuntime(account);
      try {
        await tempRuntime.start();
        const channel = await tempRuntime.getOrQueryChannel(
          "messaging",
          ctx.to,
        );

        const msgPayload: Record<string, unknown> = { text: ctx.text };
        if (ctx.threadId) {
          msgPayload.parent_id = String(ctx.threadId);
        }

        const { message } = await channel.sendMessage(
          msgPayload as Parameters<typeof channel.sendMessage>[0],
        );

        return {
          channel: "streamchat" as const,
          messageId: message.id,
        };
      } finally {
        await tempRuntime.stop();
      }
    },

    sendMedia: async (ctx: ChannelOutboundContext) => {
      const account = resolveStreamChatAccount(ctx.cfg, ctx.accountId);
      if (!account.configured) {
        throw new Error("StreamChat account not configured");
      }

      const tempRuntime = new StreamChatClientRuntime(account);
      try {
        await tempRuntime.start();
        const channel = await tempRuntime.getOrQueryChannel(
          "messaging",
          ctx.to,
        );

        let attachments: Record<string, unknown>[] = [];

        if (ctx.mediaUrl) {
          // Use rt.media.loadWebMedia (top-level runtime media API) for outbound,
          // which resolves both local file paths and remote URLs, unlike the inbound
          // path which uses rt.channel.media.fetchRemoteMedia for raw URL fetching.
          const rt = getStreamChatRuntime();
          const media = await rt.media.loadWebMedia(ctx.mediaUrl, {
            localRoots: ctx.mediaLocalRoots,
          });

          // `forceDocument` is an optional extension field on ChannelOutboundContext
          // that some callers set to force file (non-image) attachment behaviour.
          // It is not part of the base SDK type, so we use duck-typing here.
          const forceDocument = "forceDocument" in ctx && ctx.forceDocument === true;
          const isImage =
            (media.contentType?.startsWith("image/") ?? false) && !forceDocument;

          // Both sendFile and sendImage accept Buffer on the server side.
          // sendImage's TS types omit Buffer but it works at runtime with
          // allowServerSideConnect. We use sendFile for both to stay type-safe.
          const uploaded = await channel.sendFile(
            media.buffer,
            media.fileName ?? (isImage ? "image" : "file"),
            media.contentType ?? (isImage ? "image/jpeg" : "application/octet-stream"),
          );

          attachments = [
            {
              type: isImage ? "image" : "file",
              ...(isImage
                ? { image_url: uploaded.file }
                : { asset_url: uploaded.file }),
              ...(uploaded.thumb_url
                ? { thumb_url: uploaded.thumb_url }
                : {}),
              ...(media.fileName ? { title: media.fileName } : {}),
              ...(media.contentType
                ? { mime_type: media.contentType }
                : {}),
            },
          ];
        }

        const msgPayload: Record<string, unknown> = {
          text: ctx.text || "",
          attachments,
        };
        if (ctx.threadId) {
          msgPayload.parent_id = String(ctx.threadId);
        }

        const { message } = await channel.sendMessage(
          msgPayload as Parameters<typeof channel.sendMessage>[0],
        );
        return {
          channel: "streamchat" as const,
          messageId: message.id,
        };
      } finally {
        await tempRuntime.stop();
      }
    },
  },

  gateway: {
    startAccount: async (
      ctx: ChannelGatewayContext<ResolvedAccount>,
    ): Promise<void> => {
      const { cfg, accountId, account, log, abortSignal } = ctx;

      if (!account.configured) {
        throw new Error(
          "StreamChat not configured: apiKey, botUserId, and botUserToken are required",
        );
      }

      // Force-stop any stale runtime for this accountId that was never cleaned up
      // (can happen when the framework does an in-process reload without calling stop()).
      const staleCleanup = activeGatewayCleanup.get(accountId);
      if (staleCleanup) {
        log?.warn?.(
          `[StreamChat] Stale connection detected for account "${accountId}" — forcing cleanup before restart`,
        );
        staleCleanup();
      }

      const chatRuntime = new StreamChatClientRuntime(account, log);
      const runContexts = new RunContextMap();
      const streamingHandler = new StreamingHandler({
        client: chatRuntime.getClient(),
        runContexts,
        log,
      });

      // Connect and watch channels
      await chatRuntime.start();

      ctx.setStatus({
        ...ctx.getStatus(),
        running: true,
        lastStartAt: Date.now(),
        lastError: null,
      });

      // Listen for new messages
      const client = chatRuntime.getClient();
      const handleMessage = (event: Event) => {
        handleStreamChatMessage({
          cfg,
          accountId,
          account,
          event,
          chatRuntime,
          streamingHandler,
          runContexts,
          log,
        }).catch((err) => {
          log?.error?.(
            `[StreamChat] Message handler error: ${String(err)}`,
          );
        });
      };

      // Listen for force stop from client
      const handleAiStop = (event: Event) => {
        const messageId = getEventMessageId(event);
        if (!messageId) return;
        const activeRun = runContexts.findByResponseMessageId(messageId);
        if (activeRun) {
          streamingHandler.onForceStop(activeRun.runId).catch((err) => {
            log?.warn?.(
              `[StreamChat] Force stop error: ${String(err)}`,
            );
          });
        }
      };

      // Listen for message edits (Feature: Message Editing)
      const handleMessageUpdate = (event: Event) => {
        handleMessageUpdated({ event, account, log, cfg, accountId });
      };

      // Listen for message deletions (Feature: Message Deletion)
      const handleMessageDelete = (event: Event) => {
        handleMessageDeleted({
          event,
          account,
          runContexts,
          streamingHandler,
          log,
          cfg,
          accountId,
        });
      };

      // Listen for reaction events (Feature: Reaction Events)
      const handleReactionNew = (event: Event) => {
        handleReactionEvent({ event, account, log, cfg, accountId, action: "added" });
      };
      const handleReactionDeleted = (event: Event) => {
        handleReactionEvent({ event, account, log, cfg, accountId, action: "removed" });
      };

      // Listen for connection state changes (Feature: Connection Recovery)
      const handleConnectionChanged = (event: Event) => {
        const online = getConnectionOnline(event);
        if (online) {
          log?.info?.("[StreamChat] Connection restored");
        } else {
          log?.warn?.("[StreamChat] Connection lost — SDK will auto-reconnect");
        }
        ctx.setStatus({
          ...ctx.getStatus(),
          ...(typeof online === "boolean" ? { running: online } : {}),
        });
      };

      const handleConnectionRecovered = () => {
        log?.info?.("[StreamChat] Connection recovered — state re-synced");
        ctx.setStatus({
          ...ctx.getStatus(),
          running: true,
          lastError: null,
        });
      };

      client.on("message.new", handleMessage);
      client.on("ai_indicator.stop" as "user.watching.start", handleAiStop);
      client.on("message.updated", handleMessageUpdate);
      client.on("message.deleted", handleMessageDelete);
      client.on("reaction.new", handleReactionNew);
      client.on("reaction.deleted", handleReactionDeleted);
      client.on("connection.changed", handleConnectionChanged);
      client.on("connection.recovered", handleConnectionRecovered);

      // Handle abort signal / explicit stop — idempotent via `stopped` guard
      let stopped = false;
      const handleAbort = () => {
        if (stopped) return;
        stopped = true;
        client.off("message.new", handleMessage);
        client.off("ai_indicator.stop" as "user.watching.start", handleAiStop);
        client.off("message.updated", handleMessageUpdate);
        client.off("message.deleted", handleMessageDelete);
        client.off("reaction.new", handleReactionNew);
        client.off("reaction.deleted", handleReactionDeleted);
        client.off("connection.changed", handleConnectionChanged);
        client.off("connection.recovered", handleConnectionRecovered);
        activeGatewayCleanup.delete(accountId);
        chatRuntime.stop().catch((err) => {
          log?.error?.(
            `[StreamChat] Disconnect error: ${String(err)}`,
          );
        });
        ctx.setStatus({
          ...ctx.getStatus(),
          running: false,
          lastStopAt: Date.now(),
        });
      };

      activeGatewayCleanup.set(accountId, handleAbort);

      log?.info?.(
        `[StreamChat] Gateway started for account "${accountId}"`,
      );

      // Keep the startAccount promise pending until the framework signals
      // shutdown via the abort signal.  Without this the promise resolves
      // immediately and the framework's auto-restart loop treats the
      // gateway as "exited", causing a connect → disconnect cycle.
      await waitUntilAbort(abortSignal, handleAbort);
    },
  },

  status: {
    defaultRuntime: {
      accountId: "default",
      name: undefined,
      enabled: true,
      configured: false,
      running: false,
      lastStartAt: null,
      lastStopAt: null,
      lastError: null,
    },
  },
};
