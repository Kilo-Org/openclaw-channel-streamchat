import { describe, expect, it } from "vitest";
import { buildEnvelope } from "./envelope.js";
import type { EnvelopeInput } from "./envelope.js";

function makeInput(overrides?: Partial<EnvelopeInput>): EnvelopeInput {
  return {
    text: "Hello world",
    senderId: "user-1",
    senderName: "Alice",
    messageId: "msg-1",
    isFirstInThread: false,
    ...overrides,
  };
}

describe("buildEnvelope", () => {
  it("returns plain text body and commandBody when no context", () => {
    const result = buildEnvelope(makeInput());
    expect(result.body).toBe("Hello world");
    expect(result.commandBody).toBe("Hello world");
  });

  it("commandBody is always the raw text regardless of wrappers", () => {
    const result = buildEnvelope(
      makeInput({
        quotedMessage: { id: "q1", text: "quoted" },
        threadParent: { id: "t1", text: "parent" },
        isFirstInThread: true,
      }),
    );
    expect(result.commandBody).toBe("Hello world");
  });

  // --- Quoted message tests ---

  it("wraps quoted message with text", () => {
    const result = buildEnvelope(
      makeInput({
        quotedMessage: {
          id: "q1",
          text: "Original message",
          userName: "Bob",
        },
      }),
    );
    expect(result.body).toContain("[Replying to Bob id:q1]");
    expect(result.body).toContain("Original message");
    expect(result.body).toContain("[/Replying]");
    expect(result.body).toContain("Hello world");
  });

  it("uses '(no text)' when quoted message has no text", () => {
    const result = buildEnvelope(
      makeInput({
        quotedMessage: { id: "q1", userName: "Bob" },
      }),
    );
    expect(result.body).toContain("(no text)");
  });

  it("uses userName for quoted message sender", () => {
    const result = buildEnvelope(
      makeInput({
        quotedMessage: { id: "q1", text: "hi", userName: "Bob", userId: "u1" },
      }),
    );
    expect(result.body).toContain("[Replying to Bob id:q1]");
  });

  it("falls back to userId when userName is missing", () => {
    const result = buildEnvelope(
      makeInput({
        quotedMessage: { id: "q1", text: "hi", userId: "u1" },
      }),
    );
    expect(result.body).toContain("[Replying to u1 id:q1]");
  });

  it("uses 'unknown' when neither userName nor userId", () => {
    const result = buildEnvelope(
      makeInput({
        quotedMessage: { id: "q1", text: "hi" },
      }),
    );
    expect(result.body).toContain("[Replying to unknown id:q1]");
  });

  it("truncates quoted text at 500 characters", () => {
    const longText = "a".repeat(600);
    const result = buildEnvelope(
      makeInput({
        quotedMessage: { id: "q1", text: longText, userName: "Bob" },
      }),
    );
    // The truncated text should be 497 chars + "..."
    expect(result.body).not.toContain(longText);
    expect(result.body).toContain("...");
  });

  // --- Thread parent tests ---

  it("wraps first message in thread with parent text", () => {
    const result = buildEnvelope(
      makeInput({
        threadParent: { id: "t1", text: "Parent message" },
        isFirstInThread: true,
      }),
    );
    expect(result.body).toContain("[Thread thread:t1 on message id:t1]");
    expect(result.body).toContain("Parent message: Parent message");
    expect(result.body).toContain("[/Thread]");
    expect(result.body).toContain("Hello world");
  });

  it("uses '(no text)' for first-in-thread when parent has no text", () => {
    const result = buildEnvelope(
      makeInput({
        threadParent: { id: "t1" },
        isFirstInThread: true,
      }),
    );
    expect(result.body).toContain("Parent message: (no text)");
  });

  it("wraps subsequent thread messages without parent text", () => {
    const result = buildEnvelope(
      makeInput({
        threadParent: { id: "t1", text: "Parent message" },
        isFirstInThread: false,
      }),
    );
    expect(result.body).toContain("[Thread thread:t1]");
    expect(result.body).not.toContain("Parent message:");
    expect(result.body).toContain("[/Thread]");
  });

  // --- Combined tests ---

  it("nests quoted message inside thread wrapper (reply is inner)", () => {
    const result = buildEnvelope(
      makeInput({
        quotedMessage: { id: "q1", text: "Quoted", userName: "Bob" },
        threadParent: { id: "t1", text: "Parent" },
        isFirstInThread: true,
      }),
    );
    // Thread wrapper should be outermost
    const threadStart = result.body.indexOf("[Thread");
    const replyStart = result.body.indexOf("[Replying");
    const replyEnd = result.body.indexOf("[/Replying]");
    const threadEnd = result.body.indexOf("[/Thread]");

    expect(threadStart).toBeLessThan(replyStart);
    expect(replyEnd).toBeLessThan(threadEnd);
  });

  it("handles empty text body", () => {
    const result = buildEnvelope(makeInput({ text: "" }));
    expect(result.body).toBe("");
    expect(result.commandBody).toBe("");
  });

  it("handles empty text with thread context", () => {
    const result = buildEnvelope(
      makeInput({
        text: "",
        threadParent: { id: "t1" },
        isFirstInThread: false,
      }),
    );
    expect(result.body).toContain("[Thread thread:t1]");
    expect(result.body).toContain("[/Thread]");
  });
});
