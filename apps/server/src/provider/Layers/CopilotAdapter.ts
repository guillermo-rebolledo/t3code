import {
  ApprovalRequestId,
  EventId,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderUserInputAnswers,
  RuntimeItemId,
  RuntimeRequestId,
  type ThreadId,
  type ToolLifecycleItemType,
  TurnId,
} from "@t3tools/contracts";
import type { SessionEvent } from "@github/copilot-sdk";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type {
  CopilotSdkConnection,
  CopilotSdkRuntimeError,
  CopilotSdkSession,
} from "../CopilotSdkRuntime.ts";
import {
  approvalDecisionResult,
  approvalOptionsForPermission,
  automaticPermissionResult,
  canonicalPermissionRequest,
  type CopilotPermission,
  parseCopilotPermission,
} from "../CopilotPermissions.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const PROVIDER = ProviderDriverKind.make("copilot");

interface SessionContext {
  readonly sdk: CopilotSdkSession;
  session: ProviderSession;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly turns: Array<{ readonly id: TurnId; readonly items: ReadonlyArray<unknown> }>;
  readonly toolItemTypes: Map<string, ToolLifecycleItemType>;
  readonly deniedToolCallIds: Set<string>;
  activeTurn:
    | {
        readonly id: TurnId;
        readonly resolve: (outcome: TurnOutcome) => void;
      }
    | undefined;
}

interface TurnOutcome {
  readonly state: "completed" | "cancelled" | "failed";
  readonly errorMessage?: string;
}

interface PendingApproval {
  readonly permission: CopilotPermission;
  readonly resolve: (decision: ProviderApprovalDecision) => void;
}

type CopilotAdapterError =
  | ProviderAdapterRequestError
  | ProviderAdapterSessionNotFoundError
  | ProviderAdapterValidationError;

export function makeCopilotAdapter(
  connectionSource:
    | CopilotSdkConnection
    | (() => Effect.Effect<CopilotSdkConnection, CopilotSdkRuntimeError, Scope.Scope>),
  options?: { readonly instanceId?: ProviderInstanceId },
) {
  return Effect.gen(function* () {
    const adapterScope = yield* Effect.scope;
    const instanceId = options?.instanceId ?? ProviderInstanceId.make("copilot");
    const sessions = new Map<ThreadId, SessionContext>();
    const events = yield* Queue.unbounded<ProviderRuntimeEvent>();
    let eventSequence = 0;
    let turnSequence = 0;
    const connectionRef = yield* SynchronizedRef.make<CopilotSdkConnection | undefined>(
      typeof connectionSource === "function" ? undefined : connectionSource,
    );

    const getConnection = SynchronizedRef.modifyEffect(connectionRef, (current) => {
      if (current) return Effect.succeed([current, current] as const);
      if (typeof connectionSource !== "function") {
        return Effect.succeed([connectionSource, connectionSource] as const);
      }
      return connectionSource().pipe(
        Effect.provideService(Scope.Scope, adapterScope),
        Effect.map((connection) => [connection, connection] as const),
      );
    });

    const stamp = () => ({
      eventId: EventId.make(`copilot:${instanceId}:event:${++eventSequence}`),
      createdAt: DateTime.formatIso(DateTime.nowUnsafe()),
    });

    const toolItemType = (event: Extract<SessionEvent, { type: "tool.execution_start" }>) => {
      if (event.data.mcpServerName) return "mcp_tool_call" as const;
      const name = event.data.toolName.toLowerCase();
      if (/(bash|shell|exec|terminal|command)/u.test(name)) return "command_execution" as const;
      if (/(write|edit|patch|delete|move|rename)/u.test(name)) return "file_change" as const;
      return "dynamic_tool_call" as const;
    };

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<SessionContext, ProviderAdapterSessionNotFoundError> => {
      const context = sessions.get(threadId);
      return context
        ? Effect.succeed(context)
        : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
    };

    const cancelPending = (context: SessionContext) => {
      for (const pending of context.pendingApprovals.values()) pending.resolve("cancel");
      context.pendingApprovals.clear();
      context.activeTurn?.resolve({ state: "cancelled" });
    };

    const disconnectContext = (context: SessionContext) =>
      Effect.sync(() => cancelPending(context)).pipe(
        Effect.andThen(context.sdk.disconnect),
        Effect.ignore,
      );

    const startSession: ProviderAdapterShape<CopilotAdapterError>["startSession"] = (input) =>
      Effect.gen(function* () {
        if (!input.cwd?.trim()) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "cwd is required and must be non-empty.",
          });
        }
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          });
        }

        const existing = sessions.get(input.threadId);
        if (existing) {
          yield* disconnectContext(existing);
          sessions.delete(input.threadId);
        }

        const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
        let context: SessionContext | undefined;
        let approvalSequence = 0;
        const onEvent = (event: SessionEvent) => {
          if (!context) return;
          const base = {
            ...stamp(),
            provider: PROVIDER,
            providerInstanceId: instanceId,
            threadId: input.threadId,
            turnId: context.activeTurn?.id,
          };
          switch (event.type) {
            case "assistant.message_start":
              Queue.offerUnsafe(events, {
                type: "item.started",
                ...base,
                itemId: RuntimeItemId.make(event.data.messageId),
                payload: { itemType: "assistant_message", status: "inProgress" },
              });
              return;
            case "assistant.message_delta":
              if (event.data.deltaContent) {
                Queue.offerUnsafe(events, {
                  type: "content.delta",
                  ...base,
                  itemId: RuntimeItemId.make(event.data.messageId),
                  payload: { streamKind: "assistant_text", delta: event.data.deltaContent },
                  raw: {
                    source: "copilot.sdk.event",
                    method: event.type,
                    payload: event.data,
                  },
                });
              }
              return;
            case "assistant.message":
              Queue.offerUnsafe(events, {
                type: "item.completed",
                ...base,
                itemId: RuntimeItemId.make(event.data.messageId),
                payload: { itemType: "assistant_message", status: "completed" },
              });
              return;
            case "assistant.reasoning_delta":
              if (event.data.deltaContent) {
                Queue.offerUnsafe(events, {
                  type: "content.delta",
                  ...base,
                  itemId: RuntimeItemId.make(event.data.reasoningId),
                  payload: { streamKind: "reasoning_text", delta: event.data.deltaContent },
                  raw: {
                    source: "copilot.sdk.event",
                    method: event.type,
                    payload: event.data,
                  },
                });
              }
              return;
            case "tool.execution_start": {
              if (context.deniedToolCallIds.has(event.data.toolCallId)) return;
              const itemType = toolItemType(event);
              context.toolItemTypes.set(event.data.toolCallId, itemType);
              Queue.offerUnsafe(events, {
                type: "item.started",
                ...base,
                itemId: RuntimeItemId.make(event.data.toolCallId),
                payload: {
                  itemType,
                  status: "inProgress",
                  title: event.data.mcpToolName?.trim() || event.data.toolName,
                  data: event.data.arguments,
                },
                raw: {
                  source: "copilot.sdk.event",
                  method: event.type,
                  payload: event.data,
                },
              });
              return;
            }
            case "tool.execution_progress":
              if (!context.deniedToolCallIds.has(event.data.toolCallId)) {
                Queue.offerUnsafe(events, {
                  type: "item.updated",
                  ...base,
                  itemId: RuntimeItemId.make(event.data.toolCallId),
                  payload: {
                    itemType:
                      context.toolItemTypes.get(event.data.toolCallId) ?? "dynamic_tool_call",
                    status: "inProgress",
                    detail: event.data.progressMessage,
                  },
                });
              }
              return;
            case "tool.execution_complete": {
              if (context.deniedToolCallIds.has(event.data.toolCallId)) return;
              const itemType =
                context.toolItemTypes.get(event.data.toolCallId) ?? "dynamic_tool_call";
              context.toolItemTypes.delete(event.data.toolCallId);
              Queue.offerUnsafe(events, {
                type: "item.completed",
                ...base,
                itemId: RuntimeItemId.make(event.data.toolCallId),
                payload: {
                  itemType,
                  status: event.data.success ? "completed" : "failed",
                  ...(event.data.error?.message ? { detail: event.data.error.message } : {}),
                },
                raw: {
                  source: "copilot.sdk.event",
                  method: event.type,
                  payload: event.data,
                },
              });
              return;
            }
            case "session.idle":
              context.activeTurn?.resolve({
                state: event.data?.aborted === true ? "cancelled" : "completed",
              });
              return;
            case "session.error": {
              if (event.agentId !== undefined) return;
              const message = event.data.message?.trim() || "GitHub Copilot session error.";
              Queue.offerUnsafe(events, {
                type: "runtime.error",
                ...base,
                payload: { message, class: "provider_error", detail: event.data },
                raw: { source: "copilot.sdk.event", method: event.type, payload: event.data },
              });
              context.activeTurn?.resolve({ state: "failed", errorMessage: message });
              return;
            }
            default:
              return;
          }
        };
        const onPermissionRequest = async (payload: unknown) => {
          const permission = parseCopilotPermission(payload);
          if (!permission) {
            Queue.offerUnsafe(events, {
              type: "runtime.error",
              ...stamp(),
              provider: PROVIDER,
              providerInstanceId: instanceId,
              threadId: input.threadId,
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

          const automatic = automaticPermissionResult(input.runtimeMode, permission);
          if (automatic) return automatic;

          approvalSequence += 1;
          const requestId = ApprovalRequestId.make(
            `copilot:${instanceId}:${input.threadId}:${approvalSequence}`,
          );
          const canonical = canonicalPermissionRequest(permission);
          Queue.offerUnsafe(events, {
            type: "request.opened",
            ...stamp(),
            provider: PROVIDER,
            providerInstanceId: instanceId,
            threadId: input.threadId,
            turnId: context?.session.activeTurnId,
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

          const decision = await new Promise<ProviderApprovalDecision>((resolve) => {
            pendingApprovals.set(requestId, { permission, resolve });
          });
          pendingApprovals.delete(requestId);
          Queue.offerUnsafe(events, {
            type: "request.resolved",
            ...stamp(),
            provider: PROVIDER,
            providerInstanceId: instanceId,
            threadId: input.threadId,
            turnId: context?.session.activeTurnId,
            requestId: RuntimeRequestId.make(requestId),
            payload: {
              requestType: canonical.requestType,
              decision,
              resolution: approvalDecisionResult(decision, permission),
            },
            raw: {
              source: "copilot.sdk.permission",
              method: "permission.completed",
              payload: { request: payload, decision },
            },
          });
          const result = approvalDecisionResult(decision, permission);
          if (result.kind === "reject" && permission.toolCallId && context) {
            context.deniedToolCallIds.add(permission.toolCallId);
            const itemType = context.toolItemTypes.get(permission.toolCallId);
            if (itemType) {
              context.toolItemTypes.delete(permission.toolCallId);
              Queue.offerUnsafe(events, {
                type: "item.completed",
                ...stamp(),
                provider: PROVIDER,
                providerInstanceId: instanceId,
                threadId: input.threadId,
                turnId: context.activeTurn?.id,
                itemId: RuntimeItemId.make(permission.toolCallId),
                payload: {
                  itemType,
                  status: "declined",
                  detail: "The user denied this tool request.",
                },
              });
            }
          }
          return result;
        };

        const connection = yield* getConnection.pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "runtime/connect",
                detail: "Failed to connect to the Copilot SDK runtime.",
                cause,
              }),
          ),
        );
        const sdk = yield* connection
          .createSession({
            workingDirectory: input.cwd,
            ...(input.modelSelection?.instanceId === instanceId
              ? { model: input.modelSelection.model }
              : {}),
            onEvent,
            onPermissionRequest,
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/create",
                  detail: "Failed to create the Copilot session.",
                  cause,
                }),
            ),
          );
        const now = DateTime.formatIso(yield* DateTime.now);
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: instanceId,
          threadId: input.threadId,
          runtimeMode: input.runtimeMode,
          cwd: input.cwd,
          status: "ready",
          resumeCursor: { sessionId: sdk.sessionId },
          createdAt: now,
          updatedAt: now,
        };
        context = {
          sdk,
          session,
          pendingApprovals,
          turns: [],
          toolItemTypes: new Map(),
          deniedToolCallIds: new Set(),
          activeTurn: undefined,
        };
        sessions.set(input.threadId, context);
        Queue.offerUnsafe(events, {
          type: "session.started",
          ...stamp(),
          provider: PROVIDER,
          providerInstanceId: instanceId,
          threadId: input.threadId,
          payload: { resume: { sessionId: sdk.sessionId } },
        });
        Queue.offerUnsafe(events, {
          type: "session.state.changed",
          ...stamp(),
          provider: PROVIDER,
          providerInstanceId: instanceId,
          threadId: input.threadId,
          payload: { state: "ready", reason: "GitHub Copilot SDK session ready" },
        });
        Queue.offerUnsafe(events, {
          type: "thread.started",
          ...stamp(),
          provider: PROVIDER,
          providerInstanceId: instanceId,
          threadId: input.threadId,
          payload: { providerThreadId: sdk.sessionId },
        });
        return session;
      });

    const unsupported = (method: string) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method,
          detail: "This Copilot operation is not supported yet.",
        }),
      );

    yield* Effect.addFinalizer(() =>
      Effect.forEach(sessions.values(), disconnectContext, { discard: true }).pipe(
        Effect.tap(() => Effect.sync(() => sessions.clear())),
        Effect.tap(() => Queue.shutdown(events)),
      ),
    );

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "unsupported" },
      startSession,
      sendTurn: (input) =>
        Effect.gen(function* () {
          const context = yield* requireSession(input.threadId);
          if (context.activeTurn) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "A turn is already in progress for this thread.",
            });
          }
          const prompt = input.input?.trim();
          if (!prompt) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Copilot turns currently require non-empty text.",
            });
          }
          const turnId = TurnId.make(
            `copilot:${instanceId}:${input.threadId}:turn:${++turnSequence}`,
          );
          let settle!: (outcome: TurnOutcome) => void;
          const completion = new Promise<TurnOutcome>((resolve) => {
            settle = resolve;
          });
          context.activeTurn = { id: turnId, resolve: settle };
          context.session = { ...context.session, status: "running", activeTurnId: turnId };
          Queue.offerUnsafe(events, {
            type: "turn.started",
            ...stamp(),
            provider: PROVIDER,
            providerInstanceId: instanceId,
            threadId: input.threadId,
            turnId,
            payload: { model: context.session.model },
          });
          const outcome = yield* context.sdk
            .send({
              prompt,
              agentMode: input.interactionMode === "plan" ? "plan" : "interactive",
            })
            .pipe(
              Effect.flatMap(() => Effect.promise(() => completion)),
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "session/send",
                    detail: "Failed to send the Copilot turn.",
                    cause,
                  }),
              ),
              Effect.ensuring(
                Effect.sync(() => {
                  context.activeTurn = undefined;
                  context.session = {
                    ...context.session,
                    status: "ready",
                    activeTurnId: undefined,
                    updatedAt: DateTime.formatIso(DateTime.nowUnsafe()),
                  };
                }),
              ),
            );
          context.turns.push({ id: turnId, items: [{ prompt }] });
          Queue.offerUnsafe(events, {
            type: "turn.completed",
            ...stamp(),
            provider: PROVIDER,
            providerInstanceId: instanceId,
            threadId: input.threadId,
            turnId,
            payload: {
              state: outcome.state,
              stopReason:
                outcome.state === "completed"
                  ? null
                  : outcome.state === "cancelled"
                    ? "cancelled"
                    : "error",
              ...(outcome.errorMessage ? { errorMessage: outcome.errorMessage } : {}),
            },
          });
          return { threadId: input.threadId, turnId, resumeCursor: context.session.resumeCursor };
        }),
      interruptTurn: (threadId) =>
        Effect.gen(function* () {
          const context = yield* requireSession(threadId);
          cancelPending(context);
          yield* context.sdk.abort.pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/abort",
                  detail: "Failed to interrupt the Copilot turn.",
                  cause,
                }),
            ),
          );
          context.activeTurn?.resolve({ state: "cancelled" });
        }),
      respondToRequest: (threadId, requestId, decision) =>
        Effect.gen(function* () {
          const context = yield* requireSession(threadId);
          const pending = context.pendingApprovals.get(requestId);
          if (!pending) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session/request_permission",
              detail: `Unknown pending approval request: ${requestId}`,
            });
          }
          context.pendingApprovals.delete(requestId);
          pending.resolve(decision);
        }),
      respondToUserInput: (
        _threadId: ThreadId,
        _requestId: ApprovalRequestId,
        _answers: ProviderUserInputAnswers,
      ) => unsupported("session/user_input"),
      stopSession: (threadId) =>
        Effect.gen(function* () {
          const context = yield* requireSession(threadId);
          yield* disconnectContext(context);
          sessions.delete(threadId);
        }),
      listSessions: () => Effect.sync(() => [...sessions.values()].map(({ session }) => session)),
      hasSession: (threadId) => Effect.sync(() => sessions.has(threadId)),
      readThread: (threadId) =>
        requireSession(threadId).pipe(
          Effect.map((context) => ({ threadId, turns: context.turns })),
        ),
      rollbackThread: () => unsupported("session/rollback"),
      stopAll: () =>
        Effect.forEach(sessions.values(), disconnectContext, { discard: true }).pipe(
          Effect.tap(() => Effect.sync(() => sessions.clear())),
        ),
      streamEvents: Stream.fromQueue(events),
    } satisfies ProviderAdapterShape<CopilotAdapterError>;
  });
}
