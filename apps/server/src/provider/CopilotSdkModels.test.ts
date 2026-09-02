import type { ModelInfo } from "@github/copilot-sdk";
import { describe, expect, it } from "vite-plus/test";

import { buildCopilotModels, resolveCopilotModelOptions } from "./CopilotSdkModels.ts";

const model = (overrides: Partial<ModelInfo> = {}): ModelInfo =>
  ({
    id: "gpt-5.4",
    name: "GPT-5.4",
    capabilities: {
      supports: { vision: true, reasoningEffort: false },
      limits: { max_context_window_tokens: 128_000 },
    },
    ...overrides,
  }) as ModelInfo;

describe("buildCopilotModels", () => {
  it("maps the account inventory, ignores disabled/duplicate entries, and prefers its first model", () => {
    const models = buildCopilotModels([
      model({ id: "claude-sonnet-5", name: "Claude Sonnet 5" }),
      model({ id: "claude-sonnet-5", name: "Duplicate" }),
      model({ id: "blocked", name: "Blocked", policy: { state: "disabled", terms: "" } }),
    ]);

    expect(models.map(({ slug, isDefault }) => ({ slug, isDefault }))).toEqual([
      { slug: "claude-sonnet-5", isDefault: true },
    ]);
  });

  it("maps model-specific reasoning and long-context choices", () => {
    const [mapped] = buildCopilotModels([
      model({
        supportedReasoningEfforts: ["low", "medium", "high"],
        defaultReasoningEffort: "medium",
        capabilities: {
          supports: { vision: true, reasoningEffort: true },
          limits: { max_context_window_tokens: 1_000_000 },
        },
        billing: { tokenPrices: { longContext: { maxPromptTokens: 936_000 } } },
      }),
    ]);

    expect(mapped?.capabilities?.optionDescriptors?.map((descriptor) => descriptor.id)).toEqual([
      "reasoning_effort",
      "context_tier",
    ]);
    expect(mapped?.capabilities?.optionDescriptors?.[0]).toMatchObject({
      currentValue: "medium",
      options: [
        { id: "low", label: "Low" },
        { id: "medium", label: "Medium", isDefault: true },
        { id: "high", label: "High" },
      ],
    });
  });

  it("never fabricates a model for an empty inventory", () => {
    expect(buildCopilotModels([])).toEqual([]);
  });
});

const reasoningModel = () =>
  model({
    supportedReasoningEfforts: ["low", "high"],
    defaultReasoningEffort: "low",
    capabilities: {
      supports: { vision: true, reasoningEffort: true },
      limits: { max_context_window_tokens: 128_000 },
    },
  });

describe("resolveCopilotModelOptions", () => {
  it("maps a supported model and its options onto the runtime settings", () => {
    const resolved = resolveCopilotModelOptions({
      model: "gpt-5.4",
      selections: [{ id: "reasoning_effort", value: "high" }],
      inventory: [reasoningModel()],
    });

    expect(resolved).toEqual({
      kind: "ok",
      options: { model: "gpt-5.4", reasoningEffort: "high" },
    });
  });

  it("refuses a model the account cannot use instead of falling back", () => {
    const resolved = resolveCopilotModelOptions({
      model: "gpt-9",
      inventory: [reasoningModel()],
    });

    expect(resolved.kind).toBe("invalid");
    expect(resolved.kind === "invalid" && resolved.issue).toContain("gpt-5.4");
  });

  it("refuses an option the model does not offer", () => {
    const resolved = resolveCopilotModelOptions({
      model: "gpt-5.4",
      selections: [{ id: "context_tier", value: "long_context" }],
      inventory: [reasoningModel()],
    });

    expect(resolved.kind).toBe("invalid");
    expect(resolved.kind === "invalid" && resolved.issue).toContain("context_tier");
  });

  it("refuses an unsupported value for an option the model does offer", () => {
    const resolved = resolveCopilotModelOptions({
      model: "gpt-5.4",
      selections: [{ id: "reasoning_effort", value: "max" }],
      inventory: [reasoningModel()],
    });

    expect(resolved.kind).toBe("invalid");
    expect(resolved.kind === "invalid" && resolved.issue).toContain("low, high");
  });
});
