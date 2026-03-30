import { z } from "zod";

export const StreamChatConfigSchema: z.ZodTypeAny = z.object({
  enabled: z.boolean().optional().default(true),
  apiKey: z.string().optional(),
  botUserId: z.string().optional(),
  botUserToken: z.string().optional(),
  botUserName: z.string().optional(),
  dmPolicy: z.enum(["open", "pairing"]).optional().default("open"),
  ackReaction: z.string().optional().default("eyes"),
  doneReaction: z.string().optional().default("white_check_mark"),
  streamingThrottle: z.number().int().min(1).optional().default(15),
  watchdogTimeoutMs: z.number().int().min(10000).optional().default(120000),
  watchdogMaxRetries: z.number().int().min(0).optional().default(0),
  accounts: z
    .record(
      z.string(),
      z.lazy(() => StreamChatConfigSchema),
    )
    .optional(),
});

export type StreamChatConfig = z.infer<typeof StreamChatConfigSchema>;
