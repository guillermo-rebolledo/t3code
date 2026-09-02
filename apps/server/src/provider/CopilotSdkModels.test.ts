import type { ModelInfo } from "@github/copilot-sdk";
import { describe, expect, it } from "vite-plus/test";

import { buildCopilotModels } from "./CopilotSdkModels.ts";

const model = (overrides: Partial<ModelInfo>): ModelInfo =>
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
