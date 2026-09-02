import type { MessageOptions, SessionEvent } from "@github/copilot-sdk";
import {
  ApprovalRequestId,
  EventId,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderItemId,
  type ModelSelection,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionStartInput,
  RuntimeItemId,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import type {
  CopilotSdkConnection,
  CopilotSdkModelOptions,
  CopilotSdkRuntimeError,
  CopilotSdkSession,
} from "../CopilotSdkRuntime.ts";
import {
  buildCopilotContinuation,
  resolveCopilotContinuation,
  resolveCopilotModelOptions,
  sameCopilotModelOptions,
} from "../CopilotContinuation.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import {
  approvalDecisionResult,
  approvalOptionsForPermission,
  automaticPermissionResult,
  canonicalPermissionRequest,
  parseCopilotPermission,
} from "../CopilotPermissions.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("copilot");

export interface CopilotAdapterOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly nativeEventLogger?: EventNdjsonLogger;
}

interface CopilotTurnState {
  readonly id: TurnId;
  readonly items: Array<unknown>;
  readonly startedItems: Set<string>;
  readonly streamedItems: Set<string>;
  usage: unknown | undefined;
  /** Set by the single settlement path so a turn emits `turn.completed` once. */
  settled: boolean;
}

interface CopilotSessionContext {
  session: ProviderSession;
  readonly sdk: CopilotSdkSession;
  /** Model and options currently applied to the live session, if any. */
  modelOptions: CopilotSdkModelOptions | undefined;
  readonly turns: Array<CopilotTurnState>;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly deniedToolCallIds: Set<string>;
  activeTurn: CopilotTurnState | undefined;
  /**
   * True while the runtime still owes the aborted `session.idle` for a turn T3
   * already settled. It is swallowed whenever it lands, even if the runtime
   * only gets to it after accepting the next turn's message.
   */
  awaitingAbortedIdle: boolean;
  /** Resolved once the session is torn down, to release in-flight requests. */
  readonly terminated: Deferred.Deferred<void>;
  stopped: boolean;
}

interface PendingApproval {
  readonly resolve: (decision: ProviderApprovalDecision) => void;
}

/**
 * Queue payload for the single ordered consumer of native SDK events. Each item
 * carries the turn that was live when the runtime emitted it.
 */
interface CopilotNativeItem {
  readonly threadId: ThreadId;
  readonly event: SessionEvent;
  readonly turn: CopilotTurnState | undefined;
}

/** Copilot error categories the runtime recovers from on the same turn. */
function isRecoverableSessionError(data: {
  readonly errorType: string;
  readonly eligibleForAutoSwitch?: boolean;
}) {
  // A rate limit eligible for an automatic model switch is retried on the same
  // turn, and a context-limit response is retried after compaction.
  return data.eligibleForAutoSwitch === true || data.errorType === "context_limit";
}

/** Maps a Copilot `session.error` category onto the canonical error class. */
function runtimeErrorClass(errorType: string) {
  switch (errorType) {
    case "authentication":
    case "authorization":
      return "permission_error" as const;
    case "query":
      return "validation_error" as const;
    default:
      return "provider_error" as const;
  }
}

function errorDetail(error: unknown): string {
  return error instanceof Error && error.message.trim() ? error.message.trim() : String(error);
}

function mapSdkError(method: string, error: CopilotSdkRuntimeError) {
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: error.detail,
    cause: error,
  });
}

export const makeCopilotAdapter = Effect.fn("makeCopilotAdapter")(function* (
  connection: CopilotSdkConnection,
  options: CopilotAdapterOptions = {},
) {
  const instanceId = options.instanceId ?? ProviderInstanceId.make("copilot");
  const { attachmentsDir } = yield* ServerConfig;
  const fileSystem = yield* FileSystem.FileSystem;
  const crypto = yield* Crypto.Crypto;
  const events = yield* PubSub.unbounded<ProviderRuntimeEvent>();
  const nativeEvents = yield* Queue.unbounded<CopilotNativeItem>();
  const sessions = new Map<ThreadId, CopilotSessionContext>();
  const nextId = crypto.randomUUIDv4.pipe(
    Effect.map((id) => EventId.make(id)),
    Effect.orDie,
  );
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const runtimeContext = yield* Effect.context<never>();
  const runPromise = Effect.runPromiseWith(runtimeContext);

  const emit = (event: ProviderRuntimeEvent) => PubSub.publish(events, event).pipe(Effect.asVoid);
  const stamp = () => Effect.all({ eventId: nextId, createdAt: nowIso });

  const baseEvent = (threadId: ThreadId, turnId?: TurnId) =>
    Effect.map(stamp(), (value) => ({
      ...value,
      provider: PROVIDER,
      providerInstanceId: instanceId,
      threadId,
      ...(turnId ? { turnId } : {}),
    }));

  const handlePermissionRequest = Effect.fn("CopilotAdapter.handlePermissionRequest")(function* (
    threadId: ThreadId,
    payload: unknown,
  ) {
    const context = sessions.get(threadId);
    const permission = parseCopilotPermission(payload);
    if (!context || context.stopped || !permission) {
      const base = yield* baseEvent(threadId, context?.activeTurn?.id);
      yield* emit({
        ...base,
        type: "runtime.error",
        payload: {
          message: "Copilot sent an unknown or malformed permission request.",
          class: "permission_error",
          detail: payload,
        },
        raw: {
          source: "copilot.sdk.permission",
          method: "permission.malformed",
          payload,
        },
      });
      return { kind: "reject" } as const;
    }

    const automatic = automaticPermissionResult(context.session.runtimeMode, permission);
    if (automatic) return automatic;

    const requestId = ApprovalRequestId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const canonical = canonicalPermissionRequest(permission);
    let resolveDecision!: (decision: ProviderApprovalDecision) => void;
    const decisionPromise = new Promise<ProviderApprovalDecision>((resolve) => {
      resolveDecision = resolve;
    });
    context.pendingApprovals.set(requestId, { resolve: resolveDecision });
    const openedBase = yield* baseEvent(threadId, context.activeTurn?.id);
    yield* emit({
      ...openedBase,
      type: "request.opened",
      requestId: RuntimeRequestId.make(requestId),
      payload: {
        requestType: canonical.requestType,
        detail: canonical.detail,
        ...(canonical.appName ? { appName: canonical.appName } : {}),
        options: [...approvalOptionsForPermission(permission)],
        args: permission,
      },
      raw: {
        source: "copilot.sdk.permission",
        method: "permission.requested",
        payload,
      },
    });

    const decision = yield* Effect.promise(() => decisionPromise).pipe(
      Effect.ensuring(Effect.sync(() => context.pendingApprovals.delete(requestId))),
    );
    context.pendingApprovals.delete(requestId);
    const result = approvalDecisionResult(decision, permission);
    const resolvedBase = yield* baseEvent(threadId, context.activeTurn?.id);
    yield* emit({
      ...resolvedBase,
      type: "request.resolved",
      requestId: RuntimeRequestId.make(requestId),
      payload: {
        requestType: canonical.requestType,
        decision,
        resolution: result,
      },
      raw: {
        source: "copilot.sdk.permission",
        method: "permission.completed",
        payload: { request: payload, decision },
      },
    });

    if (result.kind === "reject" && permission.toolCallId) {
      context.deniedToolCallIds.add(permission.toolCallId);
      const turn = context.activeTurn;
      if (turn?.startedItems.has(permission.toolCallId)) {
        const deniedBase = yield* baseEvent(threadId, turn.id);
        yield* emit({
          ...deniedBase,
          type: "item.completed",
          itemId: RuntimeItemId.make(permission.toolCallId),
          providerRefs: { providerItemId: ProviderItemId.make(permission.toolCallId) },
          payload: {
            itemType: "dynamic_tool_call",
            status: "declined",
            detail: "The user denied this tool request.",
          },
        });
      }
    }
    return result;
  });

  const requireSession = (
    threadId: ThreadId,
  ): Effect.Effect<CopilotSessionContext, ProviderAdapterSessionNotFoundError> => {
    const context = sessions.get(threadId);
    return context && !context.stopped
      ? Effect.succeed(context)
      : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
  };

  const logNative = (threadId: ThreadId, event: SessionEvent) => {
    if (!options.nativeEventLogger) return Effect.void;
    return options.nativeEventLogger.write(
      {
        observedAt: event.timestamp,
        event: {
          id: event.id,
          kind: "notification",
          provider: PROVIDER,
          providerInstanceId: instanceId,
          threadId,
          createdAt: event.timestamp,
          method: event.type,
          payload: event,
        },
      },
      threadId,
    );
  };

  const emitItemStarted = Effect.fn("CopilotAdapter.emitItemStarted")(function* (
    context: CopilotSessionContext,
    turn: CopilotTurnState,
    key: string,
    itemId: RuntimeItemId,
    itemType: "assistant_message" | "reasoning" | "dynamic_tool_call",
    event: SessionEvent,
    data?: unknown,
    title?: string,
  ) {
    if (turn.startedItems.has(key)) return;
    turn.startedItems.add(key);
    const base = yield* baseEvent(context.session.threadId, turn.id);
    yield* emit({
      ...base,
      type: "item.started",
      itemId,
      providerRefs: { providerItemId: ProviderItemId.make(key) },
      payload: {
        itemType,
        status: "inProgress",
        ...(title ? { title } : {}),
        ...(data !== undefined ? { data } : {}),
      },
      raw: { source: "copilot.sdk.event", messageType: event.type, payload: event },
    });
  });

  const emitTextDelta = Effect.fn("CopilotAdapter.emitTextDelta")(function* (
    context: CopilotSessionContext,
    turn: CopilotTurnState,
    key: string,
    itemType: "assistant_message" | "reasoning",
    streamKind: "assistant_text" | "reasoning_text",
    delta: string,
    event: SessionEvent,
  ) {
    const itemId = RuntimeItemId.make(key);
    yield* emitItemStarted(context, turn, key, itemId, itemType, event);
    turn.streamedItems.add(key);
    const base = yield* baseEvent(context.session.threadId, turn.id);
    yield* emit({
      ...base,
      type: "content.delta",
      itemId,
      providerRefs: { providerItemId: ProviderItemId.make(key) },
      payload: { streamKind, delta },
      raw: { source: "copilot.sdk.event", messageType: event.type, payload: event },
    });
  });

  const emitTextCompleted = Effect.fn("CopilotAdapter.emitTextCompleted")(function* (
    context: CopilotSessionContext,
    turn: CopilotTurnState,
    key: string,
    itemType: "assistant_message" | "reasoning",
    streamKind: "assistant_text" | "reasoning_text",
    content: string,
    data: unknown,
    event: SessionEvent,
  ) {
    const itemId = RuntimeItemId.make(key);
    yield* emitItemStarted(context, turn, key, itemId, itemType, event);
    if (content && !turn.streamedItems.has(key)) {
      yield* emitTextDelta(context, turn, key, itemType, streamKind, content, event);
    }
    const base = yield* baseEvent(context.session.threadId, turn.id);
    yield* emit({
      ...base,
      type: "item.completed",
      itemId,
      providerRefs: { providerItemId: ProviderItemId.make(key) },
      payload: { itemType, status: "completed", data },
      raw: { source: "copilot.sdk.event", messageType: event.type, payload: event },
    });
  });

  /** Releases every open approval so no request outlives the work that opened it. */
  const releasePendingApprovals = (context: CopilotSessionContext) =>
    Effect.sync(() => {
      for (const pending of context.pendingApprovals.values()) pending.resolve("cancel");
      context.pendingApprovals.clear();
    });

  /**
   * The only place a turn settles. Every caller - idle, abort, error, send
   * failure, interrupt, and session stop - funnels through here, so a turn
   * emits exactly one `turn.completed` no matter how many settle attempts race.
   */
  const settleTurn = Effect.fn("CopilotAdapter.settleTurn")(function* (
    context: CopilotSessionContext,
    turn: CopilotTurnState,
    payload: {
      readonly state: "completed" | "failed" | "interrupted" | "cancelled";
      readonly stopReason?: string;
      readonly usage?: unknown;
      readonly errorMessage?: string;
    },
    event?: SessionEvent,
  ) {
    if (turn.settled) return;
    // Mutate before the first yield so a concurrent settle attempt observes the
    // turn as already claimed.
    turn.settled = true;
    const wasActive = context.activeTurn === turn;
    if (wasActive) context.activeTurn = undefined;
    const updatedAt = yield* nowIso;
    if (wasActive && !context.stopped) {
      context.session = {
        ...context.session,
        status: "ready",
        activeTurnId: undefined,
        updatedAt,
        ...(payload.errorMessage ? { lastError: payload.errorMessage } : {}),
      };
    }
    const base = yield* baseEvent(context.session.threadId, turn.id);
    yield* emit({
      ...base,
      type: "turn.completed",
      payload,
      ...(event
        ? { raw: { source: "copilot.sdk.event", messageType: event.type, payload: event } }
        : {}),
    });
  });

  /**
   * Terminal path for a session the Copilot runtime tore down on its own. It
   * settles the visible turn, releases the SDK session, and reports one exit.
   */
  const shutdownSessionFromRuntime = Effect.fn("CopilotAdapter.shutdownSessionFromRuntime")(
    function* (
      context: CopilotSessionContext,
      event: Extract<SessionEvent, { type: "session.shutdown" }>,
    ) {
      const failed = event.data.shutdownType === "error";
      const reason =
        event.data.errorReason?.trim() ||
        (failed
          ? "The GitHub Copilot session ended unexpectedly."
          : "The GitHub Copilot session ended.");
      yield* releasePendingApprovals(context);
      const activeTurn = context.activeTurn;
      if (activeTurn) {
        yield* settleTurn(
          context,
          activeTurn,
          failed
            ? { state: "failed", errorMessage: reason }
            : { state: "interrupted", stopReason: "session_shutdown" },
          event,
        );
      }
      const threadId = context.session.threadId;
      context.stopped = true;
      sessions.delete(threadId);
      yield* Deferred.succeed(context.terminated, undefined);
      yield* context.sdk.disconnect.pipe(Effect.ignore);
      context.session = {
        ...context.session,
        status: "closed",
        activeTurnId: undefined,
        updatedAt: yield* nowIso,
        ...(failed ? { lastError: reason } : {}),
      };
      const base = yield* baseEvent(threadId);
      yield* emit({
        ...base,
        type: "session.exited",
        payload: { reason, recoverable: false, exitKind: failed ? "error" : "graceful" },
        raw: { source: "copilot.sdk.event", messageType: event.type, payload: event },
      });
    },
  );

  const handleEvent = Effect.fn("CopilotAdapter.handleEvent")(function* (
    threadId: ThreadId,
    event: SessionEvent,
    observedTurn: CopilotTurnState | undefined,
  ) {
    yield* logNative(threadId, event).pipe(Effect.ignore);
    const context = sessions.get(threadId);
    if (!context || context.stopped) return;
    // The aborted idle for a turn T3 already settled is dropped outright: it is
    // the one late event that would otherwise complete a newer turn.
    if (
      event.type === "session.idle" &&
      event.data.aborted === true &&
      context.awaitingAbortedIdle
    ) {
      context.awaitingAbortedIdle = false;
      return;
    }
    if (observedTurn) observedTurn.items.push(event);
    // Turn-scoped work only applies to the turn that was live when the runtime
    // emitted the event. Anything trailing an interrupted or settled turn is
    // dropped instead of leaking into whatever turn is running now.
    const turn =
      observedTurn && observedTurn === context.activeTurn && !observedTurn.settled
        ? observedTurn
        : undefined;

    switch (event.type) {
      case "assistant.message_start": {
        if (!turn) break;
        const itemId = RuntimeItemId.make(event.data.messageId);
        yield* emitItemStarted(
          context,
          turn,
          event.data.messageId,
          itemId,
          "assistant_message",
          event,
        );
        break;
      }
      case "assistant.message_delta": {
        if (!turn) break;
        yield* emitTextDelta(
          context,
          turn,
          event.data.messageId,
          "assistant_message",
          "assistant_text",
          event.data.deltaContent,
          event,
        );
        break;
      }
      case "assistant.message": {
        if (!turn) break;
        yield* emitTextCompleted(
          context,
          turn,
          event.data.messageId,
          "assistant_message",
          "assistant_text",
          event.data.content,
          event.data,
          event,
        );
        break;
      }
      case "assistant.reasoning_delta": {
        if (!turn) break;
        yield* emitTextDelta(
          context,
          turn,
          event.data.reasoningId,
          "reasoning",
          "reasoning_text",
          event.data.deltaContent,
          event,
        );
        break;
      }
      case "assistant.reasoning": {
        if (!turn) break;
        yield* emitTextCompleted(
          context,
          turn,
          event.data.reasoningId,
          "reasoning",
          "reasoning_text",
          event.data.content,
          event.data,
          event,
        );
        break;
      }
      case "tool.execution_start": {
        if (!turn) break;
        const key = event.data.toolCallId;
        if (context.deniedToolCallIds.has(key)) break;
        yield* emitItemStarted(
          context,
          turn,
          key,
          RuntimeItemId.make(key),
          "dynamic_tool_call",
          event,
          event.data,
          event.data.toolName,
        );
        break;
      }
      case "tool.execution_partial_result":
      case "tool.execution_progress": {
        if (!turn) break;
        const key = event.data.toolCallId;
        if (context.deniedToolCallIds.has(key)) break;
        const base = yield* baseEvent(threadId, turn.id);
        yield* emit({
          ...base,
          type: "item.updated",
          itemId: RuntimeItemId.make(key),
          providerRefs: { providerItemId: ProviderItemId.make(key) },
          payload: {
            itemType: "dynamic_tool_call",
            status: "inProgress",
            detail:
              event.type === "tool.execution_progress"
                ? event.data.progressMessage
                : event.data.partialOutput,
            data: event.data,
          },
          raw: { source: "copilot.sdk.event", messageType: event.type, payload: event },
        });
        break;
      }
      case "tool.execution_complete": {
        if (!turn) break;
        const key = event.data.toolCallId;
        if (context.deniedToolCallIds.has(key)) break;
        const base = yield* baseEvent(threadId, turn.id);
        yield* emit({
          ...base,
          type: "item.completed",
          itemId: RuntimeItemId.make(key),
          providerRefs: { providerItemId: ProviderItemId.make(key) },
          payload: {
            itemType: "dynamic_tool_call",
            status: event.data.success ? "completed" : "failed",
            ...(event.data.error?.message ? { detail: event.data.error.message } : {}),
            data: event.data,
          },
          raw: { source: "copilot.sdk.event", messageType: event.type, payload: event },
        });
        break;
      }
      case "assistant.usage": {
        if (!turn) break;
        turn.usage = event.data;
        const inputTokens = event.data.inputTokens ?? 0;
        const outputTokens = event.data.outputTokens ?? 0;
        const reasoningTokens = event.data.reasoningTokens ?? 0;
        const base = yield* baseEvent(threadId, turn.id);
        yield* emit({
          ...base,
          type: "thread.token-usage.updated",
          payload: {
            usage: {
              usedTokens: inputTokens + outputTokens,
              totalProcessedTokens: inputTokens + outputTokens,
              inputTokens,
              cachedInputTokens: event.data.cacheReadTokens ?? 0,
              outputTokens,
              reasoningOutputTokens: reasoningTokens,
              lastUsedTokens: inputTokens + outputTokens,
              lastInputTokens: inputTokens,
              lastCachedInputTokens: event.data.cacheReadTokens ?? 0,
              lastOutputTokens: outputTokens,
              lastReasoningOutputTokens: reasoningTokens,
              ...(event.data.duration === undefined ? {} : { durationMs: event.data.duration }),
            },
          },
          raw: { source: "copilot.sdk.event", messageType: event.type, payload: event },
        });
        break;
      }
      case "session.usage_info": {
        const base = yield* baseEvent(threadId, turn?.id);
        yield* emit({
          ...base,
          type: "thread.token-usage.updated",
          payload: {
            usage: { usedTokens: event.data.currentTokens, maxTokens: event.data.tokenLimit },
          },
          raw: { source: "copilot.sdk.event", messageType: event.type, payload: event },
        });
        break;
      }
      case "session.idle": {
        if (!turn) break;
        yield* settleTurn(
          context,
          turn,
          event.data.aborted === true
            ? { state: "interrupted", stopReason: "user_interrupt" }
            : { state: "completed", ...(turn.usage ? { usage: turn.usage } : {}) },
          event,
        );
        break;
      }
      case "abort": {
        // An abort T3 did not initiate; `interruptTurn` claims its own idle.
        context.awaitingAbortedIdle = true;
        if (!turn) break;
        yield* settleTurn(
          context,
          turn,
          { state: "interrupted", stopReason: event.data.reason },
          event,
        );
        break;
      }
      case "session.error": {
        const message = event.data.message.trim() || "GitHub Copilot reported an error.";
        const recoverable = isRecoverableSessionError(event.data);
        const base = yield* baseEvent(threadId, turn?.id);
        const raw = {
          source: "copilot.sdk.event" as const,
          messageType: event.type,
          payload: event,
        };
        if (recoverable) {
          yield* emit({
            ...base,
            type: "runtime.warning",
            payload: { message, detail: event.data },
            raw,
          });
        } else {
          yield* emit({
            ...base,
            type: "runtime.error",
            payload: {
              message,
              class: runtimeErrorClass(event.data.errorType),
              detail: event.data,
            },
            raw,
          });
        }
        if (!turn || recoverable) break;
        yield* settleTurn(context, turn, { state: "failed", errorMessage: message }, event);
        break;
      }
      case "session.shutdown": {
        yield* shutdownSessionFromRuntime(context, event);
        break;
      }
    }
  });
  yield* Stream.runForEach(Stream.fromQueue(nativeEvents), ({ threadId, event, turn }) =>
    handleEvent(threadId, event, turn).pipe(Effect.catchCause(Effect.logError)),
  ).pipe(Effect.forkScoped);

  const stopSessionInternal = Effect.fn("CopilotAdapter.stopSessionInternal")(function* (
    context: CopilotSessionContext,
    emitExit: boolean,
  ) {
    if (context.stopped) return;
    yield* releasePendingApprovals(context);
    const activeTurn = context.activeTurn;
    context.stopped = true;
    sessions.delete(context.session.threadId);
    if (activeTurn) {
      yield* settleTurn(context, activeTurn, {
        state: "cancelled",
        stopReason: "session_stopped",
      });
    }
    // Released after the turn settles so a send waiting on this session sees
    // the settled turn and does not report a second failure for it.
    yield* Deferred.succeed(context.terminated, undefined);
    yield* context.sdk.disconnect.pipe(
      Effect.mapError((error) => mapSdkError("disconnect", error)),
    );
    context.session = {
      ...context.session,
      status: "closed",
      activeTurnId: undefined,
      updatedAt: yield* nowIso,
    };
    if (emitExit) {
      const base = yield* baseEvent(context.session.threadId);
      yield* emit({
        ...base,
        type: "session.exited",
        payload: { reason: "Session stopped.", recoverable: false, exitKind: "graceful" },
      });
    }
  });

  /**
   * Turns a T3 model selection into the runtime's model settings, refusing a
   * model or option this account cannot use. Selections aimed at another
   * provider instance are not this adapter's business and resolve to nothing.
   */
  const resolveModelOptions = Effect.fn("CopilotAdapter.resolveModelOptions")(function* (
    operation: string,
    selection: ModelSelection | undefined,
  ) {
    if (!selection || selection.instanceId !== instanceId) return undefined;
    const inventory = yield* connection.models.pipe(
      Effect.mapError((error) => mapSdkError("listModels", error)),
    );
    const resolved = resolveCopilotModelOptions({
      model: selection.model,
      selections: selection.options,
      inventory,
    });
    if (resolved.kind === "invalid") {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation,
        issue: resolved.issue,
      });
    }
    return resolved.options;
  });

  const startSession = Effect.fn("CopilotAdapter.startSession")(function* (
    input: ProviderSessionStartInput,
  ) {
    if (input.provider !== undefined && input.provider !== PROVIDER) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "startSession",
        issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
      });
    }
    if (input.providerInstanceId !== undefined && input.providerInstanceId !== instanceId) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "startSession",
        issue: `Expected provider instance '${instanceId}'.`,
      });
    }
    if (!input.cwd?.trim()) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "startSession",
        issue: "cwd is required and must be non-empty.",
      });
    }
    // A cursor that cannot be honored fails before anything is torn down, so a
    // thread with unusable continuation state keeps the session it still has.
    const continuation = resolveCopilotContinuation(input.resumeCursor, instanceId);
    if (continuation.kind === "invalid") {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "startSession",
        issue: continuation.issue,
      });
    }
    const modelOptions = yield* resolveModelOptions("startSession", input.modelSelection);

    const existing = sessions.get(input.threadId);
    if (existing) yield* stopSessionInternal(existing, false);

    const threadId = input.threadId;
    const onEvent = (event: SessionEvent) => {
      Queue.offerUnsafe(nativeEvents, {
        threadId,
        event,
        turn: sessions.get(threadId)?.activeTurn,
      });
    };
    const sdkInput = {
      workingDirectory: input.cwd.trim(),
      ...(modelOptions ? { modelOptions } : {}),
      onEvent,
      onPermissionRequest: (payload: unknown) =>
        runPromise(handlePermissionRequest(threadId, payload)),
    };
    const sdk = yield* continuation.kind === "resume"
      ? connection
          .resumeSession({ ...sdkInput, sessionId: continuation.sessionId })
          .pipe(Effect.mapError((error) => mapSdkError("resumeSession", error)))
      : connection
          .createSession(sdkInput)
          .pipe(Effect.mapError((error) => mapSdkError("createSession", error)));
    const createdAt = yield* nowIso;
    const session: ProviderSession = {
      provider: PROVIDER,
      providerInstanceId: instanceId,
      status: "ready",
      runtimeMode: input.runtimeMode,
      cwd: input.cwd.trim(),
      ...(modelOptions ? { model: modelOptions.model } : {}),
      threadId,
      resumeCursor: buildCopilotContinuation({ instanceId, sessionId: sdk.sessionId }),
      createdAt,
      updatedAt: createdAt,
    };
    sessions.set(threadId, {
      session,
      sdk,
      modelOptions,
      turns: [],
      pendingApprovals: new Map(),
      deniedToolCallIds: new Set(),
      activeTurn: undefined,
      awaitingAbortedIdle: false,
      terminated: yield* Deferred.make<void>(),
      stopped: false,
    });
    const sessionBase = yield* baseEvent(threadId);
    yield* emit({
      ...sessionBase,
      type: "session.started",
      payload: {},
    });
    const threadBase = yield* baseEvent(threadId);
    yield* emit({
      ...threadBase,
      type: "thread.started",
      payload: { providerThreadId: sdk.sessionId },
    });
    return session;
  });

  const sendTurn = Effect.fn("CopilotAdapter.sendTurn")(function* (input: ProviderSendTurnInput) {
    const context = yield* requireSession(input.threadId);
    if (!input.input?.trim() && (!input.attachments || input.attachments.length === 0)) {
      return yield* new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "sendTurn",
        issue: "A prompt or attachment is required.",
      });
    }

    const attachments: NonNullable<MessageOptions["attachments"]> = [];
    for (const attachment of input.attachments ?? []) {
      const resolved = resolveAttachmentPath({ attachmentsDir, attachment });
      const exists = resolved
        ? yield* fileSystem.exists(resolved).pipe(Effect.orElseSucceed(() => false))
        : false;
      if (!resolved || !exists) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue: `Attachment '${attachment.id}' is invalid or unavailable.`,
        });
      }
      attachments.push({ type: "file", path: resolved, displayName: attachment.name });
    }

    // The switch lands before the turn exists, so a rejected model or option
    // fails the request outright instead of stranding a started turn.
    const requestedModel = yield* resolveModelOptions("sendTurn", input.modelSelection);
    if (requestedModel && !sameCopilotModelOptions(requestedModel, context.modelOptions)) {
      yield* context.sdk
        .setModel(requestedModel)
        .pipe(Effect.mapError((error) => mapSdkError("setModel", error)));
      context.modelOptions = requestedModel;
      context.session = {
        ...context.session,
        model: requestedModel.model,
        updatedAt: yield* nowIso,
      };
    }

    const turnId = TurnId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const turn: CopilotTurnState = {
      id: turnId,
      items: [],
      startedItems: new Set(),
      streamedItems: new Set(),
      usage: undefined,
      settled: false,
    };
    context.turns.push(turn);
    context.activeTurn = turn;
    context.session = {
      ...context.session,
      status: "running",
      activeTurnId: turnId,
      updatedAt: yield* nowIso,
    };
    const base = yield* baseEvent(input.threadId, turnId);
    yield* emit({
      ...base,
      type: "turn.started",
      payload: context.session.model ? { model: context.session.model } : {},
    });

    const sendResult = yield* context.sdk
      .send({
        prompt: input.input?.trim() ?? "",
        ...(attachments.length > 0 ? { attachments } : {}),
      })
      .pipe(
        // A send left hanging by a dead runtime must not pin the request
        // forever; session teardown releases it.
        Effect.raceFirst(
          Deferred.await(context.terminated).pipe(
            Effect.andThen(
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "send",
                detail: "The GitHub Copilot session ended before the message was accepted.",
              }),
            ),
          ),
        ),
        Effect.result,
      );
    if (sendResult._tag === "Failure") {
      if (turn.settled) {
        // The turn already settled - an interrupt, a runtime error, or a
        // session stop - and this rejection is a consequence of that, not a new
        // failure to report on top of it.
        return { threadId: input.threadId, turnId };
      }
      yield* settleTurn(context, turn, {
        state: "failed",
        errorMessage: sendResult.failure.detail,
      });
      return yield* sendResult.failure._tag === "ProviderAdapterRequestError"
        ? sendResult.failure
        : mapSdkError("send", sendResult.failure);
    }
    return { threadId: input.threadId, turnId };
  });

  /**
   * Interrupting settles the visible turn locally before the SDK abort is
   * awaited, so events the runtime emits while it winds down are already
   * orphaned and cannot attach to the next turn. A failing abort still
   * propagates: the turn is settled for the user either way, but a runtime that
   * refused to stop is worth surfacing rather than swallowing.
   */
  const interruptTurn = Effect.fn("CopilotAdapter.interruptTurn")(function* (
    threadId: ThreadId,
    turnId?: TurnId,
  ) {
    const context = sessions.get(threadId);
    if (!context || context.stopped) return;
    const turn = context.activeTurn;
    if (!turn || turn.settled) return;
    if (turnId !== undefined && turn.id !== turnId) return;
    context.awaitingAbortedIdle = true;
    // An approval the user will never answer must not outlive the turn that
    // asked for it, or the SDK keeps waiting on a request nobody can see.
    yield* releasePendingApprovals(context);
    yield* settleTurn(context, turn, { state: "interrupted", stopReason: "user_interrupt" });
    yield* context.sdk.abort.pipe(
      Effect.catch((error) =>
        // A runtime that would not abort must not keep working with no way to
        // stop it, so fall back to the hard session boundary.
        Effect.logWarning(`GitHub Copilot abort failed: ${error.detail}`).pipe(
          Effect.andThen(stopSessionInternal(context, true)),
        ),
      ),
    );
  });

  const unsupported = (method: string) =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: PROVIDER,
        method,
        detail: "GitHub Copilot SDK does not expose this operation through this adapter.",
      }),
    );

  const stopAll = Effect.fn("CopilotAdapter.stopAll")(function* () {
    const results = yield* Effect.forEach(Array.from(sessions.values()), (context) =>
      stopSessionInternal(context, false).pipe(Effect.result),
    );
    const failure = results.find((result) => result._tag === "Failure");
    if (failure) return yield* failure.failure;
  });

  const adapter: ProviderAdapterShape<
    | ProviderAdapterRequestError
    | ProviderAdapterSessionNotFoundError
    | ProviderAdapterValidationError
  > = {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session" },
    startSession,
    sendTurn,
    interruptTurn,
    respondToRequest: (threadId, requestId, decision) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        const pending = context.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "respondToRequest",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        context.pendingApprovals.delete(requestId);
        pending.resolve(decision);
      }),
    respondToUserInput: () => unsupported("respondToUserInput"),
    stopSession: (threadId) =>
      requireSession(threadId).pipe(
        Effect.flatMap((context) => stopSessionInternal(context, true)),
      ),
    listSessions: () =>
      Effect.sync(() => Array.from(sessions.values(), ({ session }) => ({ ...session }))),
    hasSession: (threadId) =>
      Effect.sync(() => {
        const context = sessions.get(threadId);
        return context !== undefined && !context.stopped;
      }),
    readThread: (threadId) =>
      requireSession(threadId).pipe(
        Effect.map((context) => ({
          threadId,
          turns: context.turns.map((turn) => ({ id: turn.id, items: [...turn.items] })),
        })),
      ),
    rollbackThread: Effect.fn("CopilotAdapter.rollbackThread")(function* (threadId, numTurns) {
      const context = yield* requireSession(threadId);
      if (!Number.isInteger(numTurns) || numTurns <= 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: `Rollback distance must be a positive whole number of turns, received ${numTurns}.`,
        });
      }
      const rewindPoints = yield* context.sdk.rewindPoints.pipe(
        Effect.mapError((error) => mapSdkError("listRewindPoints", error)),
      );
      if (rewindPoints.unavailableReason) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "listRewindPoints",
          detail: `GitHub Copilot cannot rewind this session right now: ${rewindPoints.unavailableReason}.`,
        });
      }
      const points = rewindPoints.points;
      if (numTurns > points.length) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: `Cannot roll back ${numTurns} turn(s); this GitHub Copilot session has ${points.length}.`,
        });
      }
      const boundary = points[points.length - numTurns];
      const result = yield* context.sdk
        .rewind(boundary!.eventId)
        .pipe(Effect.mapError((error) => mapSdkError("rewind", error)));
      // Only an outcome that actually truncated history may touch local state;
      // anything else leaves the session exactly as the caller found it.
      if (result.eventsRemoved === undefined) {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "rewind",
          detail: `GitHub Copilot did not roll back the conversation (${result.outcome})${
            result.error ? `: ${result.error}` : "."
          }`,
        });
      }
      context.turns.splice(Math.max(0, context.turns.length - numTurns));
      context.session = { ...context.session, updatedAt: yield* nowIso };
      return {
        threadId,
        turns: context.turns.map((turn) => ({ id: turn.id, items: [...turn.items] })),
      };
    }),
    stopAll,
    streamEvents: Stream.fromPubSub(events),
  };

  yield* Effect.addFinalizer(() =>
    adapter.stopAll().pipe(
      Effect.catch((error) => Effect.logWarning(errorDetail(error))),
      Effect.andThen(PubSub.shutdown(events)),
    ),
  );

  return adapter;
});
