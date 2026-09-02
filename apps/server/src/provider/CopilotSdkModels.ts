import type { ModelInfo } from "@github/copilot-sdk";
import type {
  ModelCapabilities,
  ProviderOptionDescriptor,
  ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";

import { buildSelectOptionDescriptor } from "./providerSnapshot.ts";

const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({ optionDescriptors: [] });

function titleCase(value: string): string {
  return value === "xhigh" ? "Extra high" : value.charAt(0).toUpperCase() + value.slice(1);
}

function modelCapabilities(model: ModelInfo): ModelCapabilities {
  const optionDescriptors: ProviderOptionDescriptor[] = [];
  if (model.capabilities?.supports?.reasoningEffort) {
    const seen = new Set<string>();
    const options = (model.supportedReasoningEfforts ?? []).flatMap((effort) => {
      const value = String(effort).trim();
      if (!value || seen.has(value)) return [];
      seen.add(value);
      return [
        {
          value,
          label: titleCase(value),
          ...(value === model.defaultReasoningEffort ? { isDefault: true } : {}),
        },
      ];
    });
    if (options.length > 0) {
      optionDescriptors.push(
        buildSelectOptionDescriptor({
          id: "reasoning_effort",
          label: "Reasoning Effort",
          options,
        }),
      );
    }
  }
  if (model.billing?.tokenPrices?.longContext !== undefined) {
    optionDescriptors.push(
      buildSelectOptionDescriptor({
        id: "context_tier",
        label: "Context Window",
        options: [
          { value: "default", label: "Default", isDefault: true },
          { value: "long_context", label: "Long context" },
        ],
      }),
    );
  }
  return optionDescriptors.length > 0
    ? createModelCapabilities({ optionDescriptors })
    : EMPTY_CAPABILITIES;
}

/** Map the live, account-scoped SDK inventory without adding static rows. */
export function buildCopilotModels(
  inventory: ReadonlyArray<ModelInfo> | null | undefined,
): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  const models: ServerProviderModel[] = [];
  for (const model of inventory ?? []) {
    const slug = model.id?.trim();
    if (!slug || seen.has(slug) || model.policy?.state === "disabled") continue;
    seen.add(slug);
    models.push({
      slug,
      name: model.name?.trim() || slug,
      isCustom: false,
      capabilities: modelCapabilities(model),
    });
  }
  return models.map((model, index) => (index === 0 ? { ...model, isDefault: true } : model));
}
