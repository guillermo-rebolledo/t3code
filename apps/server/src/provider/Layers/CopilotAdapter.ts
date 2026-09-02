import type { MessageOptions, SessionEvent } from "@github/copilot-sdk";
import {
  EventId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderItemId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionStartInput,
  RuntimeItemId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import type {
  CopilotSdkConnection,
  CopilotSdkRuntimeError,
  CopilotSdkSession,
} from "../CopilotSdkRuntime.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
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
}

interface CopilotSessionContext {
  session: ProviderSession;
  readonly sdk: CopilotSdkSession;
  readonly turns: Array<CopilotTurnState>;
  activeTurn: CopilotTurnState | undefined;
  stopped: boolean;
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
  const nativeEvents = yield* Queue.unbounded<{
    readonly threadId: ThreadId;
    readonly event: SessionEvent;
  }>();
  const sessions = new Map<ThreadId, CopilotSessionContext>();
  const nextId = crypto.randomUUIDv4.pipe(
    Effect.map((id) => EventId.make(id)),
    Effect.orDie,
  );
  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

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
    key: string,
    itemId: RuntimeItemId,
    itemType: "assistant_message" | "reasoning" | "dynamic_tool_call",
    event: SessionEvent,
    data?: unknown,
    title?: string,
  ) {
    const turn = context.activeTurn;
    if (!turn || turn.startedItems.has(key)) return;
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
    key: string,
    itemType: "assistant_message" | "reasoning",
    streamKind: "assistant_text" | "reasoning_text",
    delta: string,
    event: SessionEvent,
  ) {
    const turn = context.activeTurn;
    if (!turn) return;
    const itemId = RuntimeItemId.make(key);
    yield* emitItemStarted(context, key, itemId, itemType, event);
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
    key: string,
    itemType: "assistant_message" | "reasoning",
    streamKind: "assistant_text" | "reasoning_text",
    content: string,
    data: unknown,
    event: SessionEvent,
  ) {
    const turn = context.activeTurn;
    if (!turn) return;
    const itemId = RuntimeItemId.make(key);
    yield* emitItemStarted(context, key, itemId, itemType, event);
    if (content && !turn.streamedItems.has(key)) {
      yield* emitTextDelta(context, key, itemType, streamKind, content, event);
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

  const handleEvent = Effect.fn("CopilotAdapter.handleEvent")(function* (
    threadId: ThreadId,
    event: SessionEvent,
  ) {
    yield* logNative(threadId, event).pipe(Effect.ignore);
    const context = sessions.get(threadId);
    if (!context || context.stopped) return;
    const turn = context.activeTurn;
    if (turn) turn.items.push(event);

    switch (event.type) {
      case "assistant.message_start": {
        const itemId = RuntimeItemId.make(event.data.messageId);
        yield* emitItemStarted(context, event.data.messageId, itemId, "assistant_message", event);
        break;
      }
      case "assistant.message_delta": {
        yield* emitTextDelta(
          context,
          event.data.messageId,
          "assistant_message",
          "assistant_text",
          event.data.deltaContent,
          event,
        );
        break;
      }
      case "assistant.message": {
        yield* emitTextCompleted(
          context,
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
        yield* emitTextDelta(
          context,
          event.data.reasoningId,
          "reasoning",
          "reasoning_text",
          event.data.deltaContent,
          event,
        );
        break;
      }
      case "assistant.reasoning": {
        yield* emitTextCompleted(
          context,
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
        yield* emitItemStarted(
          context,
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
        context.activeTurn = undefined;
        context.session = {
          ...context.session,
          status: "ready",
          activeTurnId: undefined,
          updatedAt: yield* nowIso,
        };
        const base = yield* baseEvent(threadId, turn.id);
        yield* emit({
          ...base,
          type: "turn.completed",
          payload: { state: "completed", ...(turn.usage ? { usage: turn.usage } : {}) },
          raw: { source: "copilot.sdk.event", messageType: event.type, payload: event },
        });
        break;
      }
    }
  });
  yield* Stream.runForEach(Stream.fromQueue(nativeEvents), ({ threadId, event }) =>
    handleEvent(threadId, event).pipe(Effect.catchCause(Effect.logError)),
  ).pipe(Effect.forkScoped);

  const stopSessionInternal = Effect.fn("CopilotAdapter.stopSessionInternal")(function* (
    context: CopilotSessionContext,
    emitExit: boolean,
  ) {
    if (context.stopped) return;
    const activeTurn = context.activeTurn;
    context.activeTurn = undefined;
    context.stopped = true;
    sessions.delete(context.session.threadId);
    if (activeTurn) {
      const turnBase = yield* baseEvent(context.session.threadId, activeTurn.id);
      yield* emit({
        ...turnBase,
        type: "turn.completed",
        payload: { state: "cancelled", stopReason: "session_stopped" },
      });
    }
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
    const existing = sessions.get(input.threadId);
    if (existing) yield* stopSessionInternal(existing, false);

    const threadId = input.threadId;
    const onEvent = (event: SessionEvent) => {
      Queue.offerUnsafe(nativeEvents, { threadId, event });
    };
    const sdkInput = {
      workingDirectory: input.cwd.trim(),
      ...(input.modelSelection?.instanceId === instanceId && input.modelSelection.model
        ? { model: input.modelSelection.model }
        : {}),
      onEvent,
    };
    const sdk = yield* connection
      .createSession(sdkInput)
      .pipe(Effect.mapError((error) => mapSdkError("createSession", error)));
    const createdAt = yield* nowIso;
    const session: ProviderSession = {
      provider: PROVIDER,
      providerInstanceId: instanceId,
      status: "ready",
      runtimeMode: input.runtimeMode,
      cwd: input.cwd.trim(),
      ...(input.modelSelection?.instanceId === instanceId
        ? { model: input.modelSelection.model }
        : {}),
      threadId,
      createdAt,
      updatedAt: createdAt,
    };
    sessions.set(threadId, {
      session,
      sdk,
      turns: [],
      activeTurn: undefined,
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

    const turnId = TurnId.make(yield* crypto.randomUUIDv4.pipe(Effect.orDie));
    const turn: CopilotTurnState = {
      id: turnId,
      items: [],
      startedItems: new Set(),
      streamedItems: new Set(),
      usage: undefined,
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
      .pipe(Effect.result);
    if (sendResult._tag === "Failure") {
      context.activeTurn = undefined;
      context.session = {
        ...context.session,
        status: "ready",
        activeTurnId: undefined,
        updatedAt: yield* nowIso,
        lastError: sendResult.failure.detail,
      };
      const failedBase = yield* baseEvent(input.threadId, turnId);
      yield* emit({
        ...failedBase,
        type: "turn.completed",
        payload: { state: "failed", errorMessage: sendResult.failure.detail },
      });
      return yield* mapSdkError("send", sendResult.failure);
    }
    return { threadId: input.threadId, turnId };
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
    capabilities: { sessionModelSwitch: "unsupported" },
    startSession,
    sendTurn,
    interruptTurn: () => unsupported("interruptTurn"),
    respondToRequest: () => unsupported("respondToRequest"),
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
    rollbackThread: () => unsupported("rollbackThread"),
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
