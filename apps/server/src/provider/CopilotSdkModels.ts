import type { ModelInfo } from "@github/copilot-sdk";
import type {
  ModelCapabilities,
  ProviderOptionDescriptor,
  ProviderOptionSelection,
  ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";

import type { CopilotSdkModelOptions } from "./CopilotSdkRuntime.ts";
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

/** Option ids the provider snapshot advertises, mapped to their SDK fields. */
const OPTION_FIELDS = {
  reasoning_effort: "reasoningEffort",
  context_tier: "contextTier",
} as const satisfies Record<string, keyof CopilotSdkModelOptions>;

export type CopilotModelResolution =
  | { readonly kind: "ok"; readonly options: CopilotSdkModelOptions }
  | { readonly kind: "invalid"; readonly issue: string };

/**
 * Resolves a T3 model selection against the account's live inventory. The same
 * descriptors the settings UI renders decide what is valid here, so a model or
 * option the account cannot use is refused instead of quietly falling back to
 * the runtime default.
 */
export function resolveCopilotModelOptions(input: {
  readonly model: string;
  readonly selections?: ReadonlyArray<ProviderOptionSelection> | undefined;
  readonly inventory: ReadonlyArray<ModelInfo>;
}): CopilotModelResolution {
  const model = input.model.trim();
  const available = buildCopilotModels(input.inventory);
  const row = available.find((entry) => entry.slug === model);
  if (!row) {
    const slugs = available.map((entry) => entry.slug).join(", ");
    return {
      kind: "invalid",
      issue: `Model '${model}' is not available on this GitHub Copilot account.${
        slugs ? ` Available models: ${slugs}.` : ""
      }`,
    };
  }
  const descriptors = row.capabilities?.optionDescriptors ?? [];
  let reasoningEffort: string | undefined;
  let contextTier: string | undefined;
  for (const selection of input.selections ?? []) {
    const field = Object.hasOwn(OPTION_FIELDS, selection.id)
      ? OPTION_FIELDS[selection.id as keyof typeof OPTION_FIELDS]
      : undefined;
    const descriptor = descriptors.find((entry) => entry.id === selection.id);
    if (!field || !descriptor || descriptor.type !== "select") {
      return {
        kind: "invalid",
        issue: `Model '${model}' does not support the '${selection.id}' option.`,
      };
    }
    const value = typeof selection.value === "string" ? selection.value : "";
    const choice = descriptor.options.find((option) => option.id === value);
    if (!choice) {
      return {
        kind: "invalid",
        issue: `'${String(selection.value)}' is not a supported ${
          descriptor.label
        } value for model '${model}'. Supported values: ${descriptor.options
          .map((option) => option.id)
          .join(", ")}.`,
      };
    }
    if (field === "reasoningEffort") reasoningEffort = value;
    else contextTier = value;
  }
  return {
    kind: "ok",
    options: {
      model,
      // Values are validated against the descriptors the snapshot built from
      // this model's own capabilities, so they are members of the SDK unions.
      ...(reasoningEffort
        ? {
            reasoningEffort: reasoningEffort as NonNullable<
              CopilotSdkModelOptions["reasoningEffort"]
            >,
          }
        : {}),
      ...(contextTier
        ? { contextTier: contextTier as NonNullable<CopilotSdkModelOptions["contextTier"]> }
        : {}),
    },
  };
}

export function sameCopilotModelOptions(
  left: CopilotSdkModelOptions | undefined,
  right: CopilotSdkModelOptions | undefined,
): boolean {
  return (
    left?.model === right?.model &&
    left?.reasoningEffort === right?.reasoningEffort &&
    left?.contextTier === right?.contextTier
  );
}
