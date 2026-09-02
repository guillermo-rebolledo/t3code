import type { ProviderInstanceId } from "@t3tools/contracts";

/**
 * Continuation metadata for Copilot sessions: a pure read over what T3
 * persisted, so the adapter can refuse an unusable cursor before it touches a
 * session.
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
  if (record.provider !== "copilot") {
    return {
      kind: "invalid",
      issue: `The stored session cursor belongs to provider '${String(
        record.provider,
      )}', not GitHub Copilot.`,
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
