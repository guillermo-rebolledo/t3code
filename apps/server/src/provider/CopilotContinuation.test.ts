import type { ModelInfo } from "@github/copilot-sdk";
import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  buildCopilotContinuation,
  resolveCopilotContinuation,
  resolveCopilotModelOptions,
} from "./CopilotContinuation.ts";

const instanceId = ProviderInstanceId.make("copilot_work");

const model = (overrides: Partial<ModelInfo> = {}): ModelInfo =>
  ({
    id: "gpt-5.4",
    name: "GPT-5.4",
    supportedReasoningEfforts: ["low", "high"],
    defaultReasoningEffort: "low",
    capabilities: {
      supports: { vision: true, reasoningEffort: true },
      limits: { max_context_window_tokens: 128_000 },
    },
    ...overrides,
  }) as ModelInfo;

describe("resolveCopilotContinuation", () => {
  it("resumes a cursor written by the same provider instance", () => {
    const cursor = buildCopilotContinuation({ instanceId, sessionId: "copilot-session-1" });

    expect(resolveCopilotContinuation(cursor, instanceId)).toEqual({
      kind: "resume",
      sessionId: "copilot-session-1",
    });
  });

  it("treats an absent cursor as a fresh start rather than a failure", () => {
    expect(resolveCopilotContinuation(undefined, instanceId).kind).toBe("none");
    expect(resolveCopilotContinuation(null, instanceId).kind).toBe("none");
  });

  it("refuses a cursor from an older metadata version", () => {
    const resolved = resolveCopilotContinuation(
      { schemaVersion: 0, provider: "copilot", instanceId, sessionId: "old" },
      instanceId,
    );

    expect(resolved.kind).toBe("invalid");
    expect(resolved.kind === "invalid" && resolved.issue).toContain("version");
  });

  it("refuses a cursor another provider instance owns", () => {
    const cursor = buildCopilotContinuation({
      instanceId: ProviderInstanceId.make("copilot_personal"),
      sessionId: "other-session",
    });

    const resolved = resolveCopilotContinuation(cursor, instanceId);

    expect(resolved.kind).toBe("invalid");
    expect(resolved.kind === "invalid" && resolved.issue).toContain("copilot_personal");
  });

  it("refuses a malformed cursor", () => {
    expect(resolveCopilotContinuation("copilot-session-1", instanceId).kind).toBe("invalid");
    expect(
      resolveCopilotContinuation(
        { ...buildCopilotContinuation({ instanceId, sessionId: "x" }), sessionId: "  " },
        instanceId,
      ).kind,
    ).toBe("invalid");
  });
});

describe("resolveCopilotModelOptions", () => {
  it("maps a supported model and its options onto the runtime settings", () => {
    const resolved = resolveCopilotModelOptions({
      model: "gpt-5.4",
      selections: [{ id: "reasoning_effort", value: "high" }],
      inventory: [model()],
    });

    expect(resolved).toEqual({
      kind: "ok",
      options: { model: "gpt-5.4", reasoningEffort: "high" },
    });
  });

  it("refuses a model the account cannot use instead of falling back", () => {
    const resolved = resolveCopilotModelOptions({
      model: "gpt-9",
      inventory: [model()],
    });

    expect(resolved.kind).toBe("invalid");
    expect(resolved.kind === "invalid" && resolved.issue).toContain("gpt-5.4");
  });

  it("refuses an option the model does not offer", () => {
    const resolved = resolveCopilotModelOptions({
      model: "gpt-5.4",
      selections: [{ id: "context_tier", value: "long_context" }],
      inventory: [model()],
    });

    expect(resolved.kind).toBe("invalid");
    expect(resolved.kind === "invalid" && resolved.issue).toContain("context_tier");
  });

  it("refuses an unsupported value for an option the model does offer", () => {
    const resolved = resolveCopilotModelOptions({
      model: "gpt-5.4",
      selections: [{ id: "reasoning_effort", value: "max" }],
      inventory: [model()],
    });

    expect(resolved.kind).toBe("invalid");
    expect(resolved.kind === "invalid" && resolved.issue).toContain("low, high");
  });
});
