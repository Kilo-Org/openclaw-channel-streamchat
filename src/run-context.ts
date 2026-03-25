import type { RunContext } from "./types.js";

const RUN_CONTEXT_TTL_MS = 5 * 60 * 1000; // 5 minutes

export class RunContextMap {
  private map = new Map<string, RunContext>();
  private timers = new Map<string, ReturnType<typeof setTimeout>>();

  set(runId: string, ctx: RunContext): void {
    this.delete(runId);
    this.map.set(runId, ctx);
    this.timers.set(
      runId,
      setTimeout(() => this.delete(runId), RUN_CONTEXT_TTL_MS),
    );
  }

  get(runId: string): RunContext | undefined {
    return this.map.get(runId);
  }

  setResponseMessageId(runId: string, messageId: string): void {
    const ctx = this.map.get(runId);
    if (ctx) ctx.responseMessageId = messageId;
  }

  delete(runId: string): void {
    this.map.delete(runId);
    const timer = this.timers.get(runId);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(runId);
    }
  }

  findActiveRunForChannel(
    channelType: string,
    channelId: string,
    threadParentId: string | null,
  ): RunContext | undefined {
    for (const ctx of this.map.values()) {
      if (
        ctx.channelType === channelType &&
        ctx.channelId === channelId &&
        ctx.threadParentId === threadParentId
      ) {
        return ctx;
      }
    }
    return undefined;
  }

  findByResponseMessageId(messageId: string): RunContext | undefined {
    for (const ctx of this.map.values()) {
      if (ctx.responseMessageId === messageId) {
        return ctx;
      }
    }
    return undefined;
  }

  findByInboundMessageId(messageId: string): RunContext | undefined {
    for (const ctx of this.map.values()) {
      if (ctx.inboundMessageId === messageId) {
        return ctx;
      }
    }
    return undefined;
  }
}
