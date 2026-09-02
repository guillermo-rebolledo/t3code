import type { ModelInfo } from "@github/copilot-sdk";
import type { ProviderInstanceId, ProviderOptionSelection } from "@t3tools/contracts";

import { buildCopilotModels } from "./CopilotSdkModels.ts";
import type { CopilotSdkModelOptions } from "./CopilotSdkRuntime.ts";

/**
 * Continuation and model resolution for Copilot sessions. Both are pure reads
 * over data T3 persisted or the runtime reported, so the adapter can refuse an
 * unusable continuation or an unavailable model before it touches a session.
 */

/** Bump when the cursor's shape changes; older cursors then fail loudly. */
export const COPILOT_CONTINUATION_VERSION = 1 as const;

export interface CopilotContinuation {
  readonly schemaVersion: typeof COPILOT_CONTINUATION_VERSION;
  readonly provider: "copilot";
  readonly instanceId: ProviderInstanceId;
  readonly sessionId: string;
}

export type CopilotContinuationResolution =
  /** No continuation was asked for - a fresh session is the correct outcome. */
  | { readonly kind: "none" }
  | { readonly kind: "resume"; readonly sessionId: string }
  | { readonly kind: "invalid"; readonly issue: string };

export function buildCopilotContinuation(input: {
  readonly instanceId: ProviderInstanceId;
  readonly sessionId: string;
}): CopilotContinuation {
  return {
    schemaVersion: COPILOT_CONTINUATION_VERSION,
    provider: "copilot",
    instanceId: input.instanceId,
    sessionId: input.sessionId,
  };
}

/**
 * Reads a persisted cursor for one provider instance. Anything present but
 * unusable is an error rather than a silent fresh start, because a thread that
 * quietly loses its history looks identical to one that resumed.
 */
export function resolveCopilotContinuation(
  raw: unknown,
  instanceId: ProviderInstanceId,
): CopilotContinuationResolution {
  if (raw === undefined || raw === null) return { kind: "none" };
  if (typeof raw !== "object" || Array.isArray(raw)) {
    return { kind: "invalid", issue: "The stored GitHub Copilot session cursor is not an object." };
  }
  const record = raw as Record<string, unknown>;
  if (record.schemaVersion !== COPILOT_CONTINUATION_VERSION) {
    return {
      kind: "invalid",
      issue: `The stored GitHub Copilot session cursor uses unsupported version '${String(
        record.schemaVersion,
      )}'; version ${COPILOT_CONTINUATION_VERSION} is required. Start a new thread to continue.`,
    };
  }
  if (record.instanceId !== instanceId) {
    return {
      kind: "invalid",
      issue: `The stored GitHub Copilot session belongs to provider instance '${String(
        record.instanceId,
      )}' and cannot be resumed by instance '${instanceId}'.`,
    };
  }
  const sessionId = typeof record.sessionId === "string" ? record.sessionId.trim() : "";
  if (!sessionId) {
    return {
      kind: "invalid",
      issue: "The stored GitHub Copilot session cursor has no session id.",
    };
  }
  return { kind: "resume", sessionId };
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
