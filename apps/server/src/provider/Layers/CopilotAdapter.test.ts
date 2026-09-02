import type { ModelInfo, SessionEvent } from "@github/copilot-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import type {
  CopilotSdkConnection,
  CopilotSdkModelOptions,
  CopilotSdkRuntimeShape,
  CopilotSdkSession,
  CopilotSdkSessionResumeInput,
  CopilotSdkSessionStartInput,
} from "../CopilotSdkRuntime.ts";
import { CopilotSdkRuntimeError as SdkError } from "../CopilotSdkRuntime.ts";
import type { CopilotPermission } from "../CopilotPermissions.ts";
import { buildCopilotContinuation } from "../CopilotContinuation.ts";
import { makeCopilotAdapter } from "./CopilotAdapter.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-copilot-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

function fakeSession(
  sessionId: string,
  options: {
    readonly send?: CopilotSdkSession["send"];
    readonly abort?: CopilotSdkSession["abort"];
    readonly onAbort?: () => void;
    readonly onDisconnect?: () => void;
    readonly setModel?: CopilotSdkSession["setModel"];
    readonly onSetModel?: (modelOptions: CopilotSdkModelOptions) => void;
    readonly rewindPoints?: CopilotSdkSession["rewindPoints"];
    readonly rewind?: CopilotSdkSession["rewind"];
  } = {},
): CopilotSdkSession {
  return {
    sessionId,
    send: options.send ?? (() => Effect.succeed("message-1")),
    abort: options.abort ?? Effect.sync(() => options.onAbort?.()),
    setModel:
      options.setModel ?? ((modelOptions) => Effect.sync(() => options.onSetModel?.(modelOptions))),
    rewindPoints: options.rewindPoints ?? Effect.succeed({ points: [] }),
    rewind: options.rewind ?? (() => Effect.succeed({ outcome: "success", eventsRemoved: 0 })),
    disconnect: Effect.sync(() => options.onDisconnect?.()),
  };
}

/**
 * Drains the adapter's runtime events into a queue so a test can advance to a
 * specific event and then assert on everything observed up to that point,
 * without sleeping or polling.
 */
const observeEvents = Effect.fn("observeEvents")(function* (adapter: {
  readonly streamEvents: Stream.Stream<ProviderRuntimeEvent>;
}) {
  const queue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  yield* Stream.runForEach(adapter.streamEvents, (runtimeEvent) =>
    Queue.offer(queue, runtimeEvent),
  ).pipe(Effect.forkChild);
  yield* Effect.yieldNow;
  const seen: Array<ProviderRuntimeEvent> = [];
  return {
    seen,
    until: (predicate: (runtimeEvent: ProviderRuntimeEvent) => boolean) =>
      Effect.gen(function* () {
        for (;;) {
          const runtimeEvent = yield* Queue.take(queue);
          seen.push(runtimeEvent);
          if (predicate(runtimeEvent)) return runtimeEvent;
        }
      }),
  };
});

let eventSequence = 0;
let sessionSequence = 0;

function event<T extends SessionEvent>(value: Omit<T, "id" | "parentId" | "timestamp">): T {
  eventSequence += 1;
  return {
    id: `test-event-${eventSequence}`,
    parentId: null,
    timestamp: "2026-01-01T00:00:00.000Z",
    ...value,
  } as T;
}

function runtimeWith(
  createSession: CopilotSdkConnection["createSession"],
  options: {
    readonly onRelease?: () => void;
    readonly resumeSession?: CopilotSdkConnection["resumeSession"];
    readonly models?: CopilotSdkConnection["models"];
  } = {},
): CopilotSdkRuntimeShape {
  const connection: CopilotSdkConnection = {
    authStatus: Effect.succeed({ isAuthenticated: true }),
    models: options.models ?? Effect.succeed([]),
    createSession,
    resumeSession:
      options.resumeSession ??
      (() =>
        Effect.fail(
          new SdkError({
            operation: "resumeSession",
            kind: "failure",
            detail: "resume was not expected in this test",
          }),
        )),
  };
  return {
    connect: () =>
      Effect.acquireRelease(Effect.succeed(connection), () =>
        Effect.sync(() => options.onRelease?.()),
      ),
  };
}

const makeTestAdapter = (
  runtime: CopilotSdkRuntimeShape,
  options?: Parameters<typeof makeCopilotAdapter>[1],
) =>
  runtime
    .connect({ binaryPath: "copilot", environment: {}, platform: "darwin" })
    .pipe(Effect.flatMap((connection) => makeCopilotAdapter(connection, options)));

const inventory: ReadonlyArray<ModelInfo> = [
  {
    id: "gpt-5.4",
    name: "GPT-5.4",
    supportedReasoningEfforts: ["low", "high"],
    defaultReasoningEffort: "low",
    capabilities: {
      supports: { vision: true, reasoningEffort: true },
      limits: { max_context_window_tokens: 128_000 },
    },
  } as ModelInfo,
];

const rewindPoint = (eventId: string, userMessage: string) => ({
  eventId,
  userMessage,
  timestamp: "2026-01-01T00:00:00.000Z",
});

const startInput = (threadId: ThreadId, instanceId = ProviderInstanceId.make("copilot_work")) => ({
  threadId,
  provider: ProviderDriverKind.make("copilot"),
  providerInstanceId: instanceId,
  cwd: process.cwd(),
  runtimeMode: "full-access" as const,
});

it.layer(testLayer)("CopilotAdapter", (it) => {
  it.effect("starts one isolated SDK session for a T3 thread", () =>
    Effect.gen(function* () {
      const createInputs: Array<Parameters<CopilotSdkConnection["createSession"]>[0]> = [];
      const runtime = runtimeWith((input) => {
        createInputs.push(input);
        return Effect.succeed(fakeSession("copilot-session-1"));
      });
      const instanceId = ProviderInstanceId.make("copilot_work");
      const adapter = yield* makeTestAdapter(runtime, { instanceId });
      const threadId = ThreadId.make("thread-1");

      const session = yield* adapter.startSession(startInput(threadId, instanceId));

      assert.equal(session.threadId, threadId);
      assert.equal(session.providerInstanceId, instanceId);
      assert.equal(createInputs[0]?.workingDirectory, process.cwd());
      assert.isTrue(yield* adapter.hasSession(threadId));
      assert.lengthOf(yield* adapter.listSessions(), 1);
    }),
  );

  it.effect("surfaces SDK session creation failures without retaining ownership", () =>
    Effect.gen(function* () {
      const runtime = runtimeWith(() =>
        Effect.fail(
          new SdkError({
            operation: "createSession",
            kind: "failure",
            detail: "creation rejected",
          }),
        ),
      );
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("create-failure");

      const result = yield* adapter
        .startSession(startInput(threadId, ProviderInstanceId.make("copilot")))
        .pipe(Effect.result);

      assert.equal(result._tag, "Failure");
      if (result._tag === "Failure")
        assert.equal(result.failure._tag, "ProviderAdapterRequestError");
      assert.isFalse(yield* adapter.hasSession(threadId));
    }),
  );

  it.effect("maps streaming assistant, reasoning, tool, usage, and completion events", () =>
    Effect.gen(function* () {
      let sdkInput: CopilotSdkSessionStartInput | undefined;
      const nativeEvents: Array<unknown> = [];
      const runtime = runtimeWith((input) => {
        sdkInput = input;
        return Effect.succeed(
          fakeSession("stream-session", {
            send: () =>
              Effect.sync(() => {
                input.onEvent(
                  event({
                    type: "assistant.reasoning_delta",
                    ephemeral: true,
                    data: { reasoningId: "reason-1", deltaContent: "Thinking" },
                  }),
                );
                input.onEvent(
                  event({
                    type: "assistant.message_delta",
                    ephemeral: true,
                    data: { messageId: "message-1", deltaContent: "Hello" },
                  }),
                );
                input.onEvent(
                  event({
                    type: "assistant.reasoning",
                    data: { reasoningId: "reason-1", content: "Thinking" },
                  }),
                );
                input.onEvent(
                  event({
                    type: "assistant.message",
                    data: { messageId: "message-1", content: "Hello" },
                  }),
                );
                input.onEvent(
                  event({
                    type: "tool.execution_start",
                    data: { toolCallId: "tool-1", toolName: "shell", arguments: { cmd: "pwd" } },
                  }),
                );
                input.onEvent(
                  event({
                    type: "tool.execution_progress",
                    ephemeral: true,
                    data: { toolCallId: "tool-1", progressMessage: "running" },
                  }),
                );
                input.onEvent(
                  event({
                    type: "tool.execution_complete",
                    data: { toolCallId: "tool-1", success: true },
                  }),
                );
                input.onEvent(
                  event({
                    type: "assistant.usage",
                    ephemeral: true,
                    data: { model: "gpt-5", inputTokens: 10, outputTokens: 4, reasoningTokens: 2 },
                  }),
                );
                input.onEvent(event({ type: "session.idle", data: {} }));
                return "message-1";
              }),
          }),
        );
      });
      const adapter = yield* makeTestAdapter(runtime, {
        nativeEventLogger: {
          filePath: "memory",
          write: (entry) => Effect.sync(() => nativeEvents.push(entry)),
          close: () => Effect.void,
        },
      });
      const threadId = ThreadId.make("stream-thread");
      const completed = yield* Deferred.make<void>();
      const observed = yield* Ref.make<Array<string>>([]);
      yield* Stream.runForEach(adapter.streamEvents, (runtimeEvent) =>
        Ref.update(observed, (types) => [...types, runtimeEvent.type]).pipe(
          Effect.andThen(
            runtimeEvent.type === "turn.completed"
              ? Deferred.succeed(completed, undefined)
              : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);
      yield* Effect.yieldNow;

      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      const turn = yield* adapter.sendTurn({ threadId, input: "Say hello" });
      yield* Deferred.await(completed);

      const types = yield* Ref.get(observed);
      assert.includeMembers(types, [
        "turn.started",
        "item.started",
        "content.delta",
        "item.updated",
        "item.completed",
        "thread.token-usage.updated",
        "turn.completed",
      ]);
      assert.isDefined(sdkInput);
      assert.isAtLeast(nativeEvents.length, 9);
      const snapshot = yield* adapter.readThread(threadId);
      assert.equal(snapshot.turns[0]?.id, turn.turnId);
      assert.lengthOf(snapshot.turns[0]?.items ?? [], 9);
    }),
  );

  it.effect("completes an empty-output turn and reports send failures", () =>
    Effect.gen(function* () {
      let sendCount = 0;
      const runtime = runtimeWith((input) =>
        Effect.succeed(
          fakeSession("empty-session", {
            send: () => {
              sendCount += 1;
              if (sendCount === 1) {
                input.onEvent(event({ type: "session.idle", data: {} }));
                return Effect.succeed("empty-message");
              }
              return Effect.fail(
                new SdkError({ operation: "send", kind: "failure", detail: "send rejected" }),
              );
            },
          }),
        ),
      );
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("empty-thread");
      const completions = yield* Ref.make<Array<unknown>>([]);
      const firstCompletion = yield* Deferred.make<void>();
      const twoCompletions = yield* Deferred.make<void>();
      yield* Stream.runForEach(adapter.streamEvents, (runtimeEvent) =>
        runtimeEvent.type === "turn.completed"
          ? Ref.update(completions, (values) => [...values, runtimeEvent.payload]).pipe(
              Effect.andThen(Deferred.succeed(firstCompletion, undefined)),
              Effect.andThen(Ref.get(completions)),
              Effect.flatMap((values) =>
                values.length >= 2 ? Deferred.succeed(twoCompletions, undefined) : Effect.void,
              ),
            )
          : Effect.void,
      ).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));

      yield* adapter.sendTurn({ threadId, input: "Return nothing" });
      yield* Deferred.await(firstCompletion);
      const failed = yield* adapter.sendTurn({ threadId, input: "Fail" }).pipe(Effect.result);
      yield* Deferred.await(twoCompletions);

      assert.equal(failed._tag, "Failure");
      const payloads = yield* Ref.get(completions);
      assert.deepInclude(payloads, { state: "completed" });
      assert.deepInclude(payloads, { state: "failed", errorMessage: "send rejected" });
    }),
  );

  it.effect("passes resolved attachments to the SDK and rejects unresolved identifiers", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig;
      const fileSystem = yield* FileSystem.FileSystem;
      const threadId = ThreadId.make("attachment-thread");
      const attachment = {
        type: "file" as const,
        id: "attachment-thread-00000000-0000-4000-8000-000000000001",
        name: "notes.txt",
        mimeType: "text/plain",
        sizeBytes: 5,
      };
      const storedPath = resolveAttachmentPath({
        attachmentsDir: config.attachmentsDir,
        attachment,
      });
      assert.isNotNull(storedPath);
      yield* fileSystem.writeFileString(storedPath!, "hello");
      const sends: Array<Parameters<CopilotSdkSession["send"]>[0]> = [];
      const runtime = runtimeWith(() =>
        Effect.succeed(
          fakeSession("attachment-session", {
            send: (input) => {
              sends.push(input);
              return Effect.succeed("attachment-message");
            },
          }),
        ),
      );
      const adapter = yield* makeTestAdapter(runtime);
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));

      yield* adapter.sendTurn({ threadId, input: "Read it", attachments: [attachment] });
      const invalid = yield* adapter
        .sendTurn({
          threadId,
          input: "Read it",
          attachments: [{ ...attachment, id: "unresolved-attachment" }],
        })
        .pipe(Effect.result);

      assert.equal(sends[0]?.attachments?.[0]?.type, "file");
      assert.equal(
        sends[0]?.attachments?.[0]?.type === "file" ? sends[0].attachments[0].path : "",
        storedPath,
      );
      assert.equal(invalid._tag, "Failure");
      if (invalid._tag === "Failure") {
        assert.notInclude(invalid.failure.message, config.attachmentsDir);
      }
    }),
  );

  it.effect("keeps session ownership isolated between provider instances", () =>
    Effect.gen(function* () {
      const runtime = runtimeWith(() => Effect.succeed(fakeSession("owned-session")));
      const firstInstance = ProviderInstanceId.make("copilot_personal");
      const secondInstance = ProviderInstanceId.make("copilot_work");
      const firstAdapter = yield* makeTestAdapter(runtime, { instanceId: firstInstance });
      const secondAdapter = yield* makeTestAdapter(runtime, { instanceId: secondInstance });
      const firstThread = ThreadId.make("personal-thread");
      const secondThread = ThreadId.make("work-thread");

      yield* firstAdapter.startSession(startInput(firstThread, firstInstance));
      yield* secondAdapter.startSession(startInput(secondThread, secondInstance));

      assert.isTrue(yield* firstAdapter.hasSession(firstThread));
      assert.isFalse(yield* firstAdapter.hasSession(secondThread));
      assert.isFalse(yield* secondAdapter.hasSession(firstThread));
      assert.isTrue(yield* secondAdapter.hasSession(secondThread));
      assert.deepEqual(
        (yield* firstAdapter.listSessions()).map((session) => session.threadId),
        [firstThread],
      );
      assert.deepEqual(
        (yield* secondAdapter.listSessions()).map((session) => session.threadId),
        [secondThread],
      );
    }),
  );

  it.effect("honors file, shell, and generic approvals across every runtime mode", () =>
    Effect.gen(function* () {
      const createInputs: Array<CopilotSdkSessionStartInput> = [];
      const runtime = runtimeWith((input) => {
        createInputs.push(input);
        return Effect.succeed(fakeSession(`approval-session-${createInputs.length}`));
      });
      const adapter = yield* makeTestAdapter(runtime);
      const opened =
        yield* Queue.unbounded<Extract<ProviderRuntimeEvent, { type: "request.opened" }>>();
      yield* Stream.runForEach(adapter.streamEvents, (runtimeEvent) =>
        runtimeEvent.type === "request.opened" ? Queue.offer(opened, runtimeEvent) : Effect.void,
      ).pipe(Effect.forkChild);
      yield* Effect.yieldNow;

      const write = {
        kind: "write",
        fileName: "src/index.ts",
        intention: "Update source",
        diff: "+export const ready = true",
        canOfferSessionApproval: true,
      } as const;
      const shell = {
        kind: "shell",
        fullCommandText: "pnpm test",
        intention: "Run tests",
        commands: [{ identifier: "pnpm", readOnly: true }],
        canOfferSessionApproval: true,
      } as const;
      const generic = {
        kind: "custom-tool",
        toolName: "deploy_preview",
        toolDescription: "Deploy a preview build",
      } as const;
      const cases: ReadonlyArray<{
        readonly name: string;
        readonly mode: RuntimeMode;
        readonly permission: CopilotPermission;
        readonly requestType:
          | "file_change_approval"
          | "exec_command_approval"
          | "dynamic_tool_call";
        readonly decision?: ProviderApprovalDecision;
        readonly result: unknown;
      }> = [
        {
          name: "approval-file-allow",
          mode: "approval-required",
          permission: write,
          requestType: "file_change_approval",
          decision: "accept",
          result: { kind: "approve-once", approvedInteractively: true },
        },
        {
          name: "approval-shell-deny",
          mode: "approval-required",
          permission: shell,
          requestType: "exec_command_approval",
          decision: "decline",
          result: { kind: "reject", feedback: "The user denied this request." },
        },
        {
          name: "approval-generic-session",
          mode: "approval-required",
          permission: generic,
          requestType: "dynamic_tool_call",
          decision: "acceptForSession",
          result: {
            kind: "approve-for-session",
            approval: { kind: "custom-tool", toolName: "deploy_preview" },
          },
        },
        {
          name: "edits-file-auto",
          mode: "auto-accept-edits",
          permission: write,
          requestType: "file_change_approval",
          result: { kind: "approve-once" },
        },
        {
          name: "edits-shell-allow",
          mode: "auto-accept-edits",
          permission: shell,
          requestType: "exec_command_approval",
          decision: "accept",
          result: { kind: "approve-once", approvedInteractively: true },
        },
        {
          name: "edits-generic-deny",
          mode: "auto-accept-edits",
          permission: generic,
          requestType: "dynamic_tool_call",
          decision: "decline",
          result: { kind: "reject", feedback: "The user denied this request." },
        },
        {
          name: "auto-file-session",
          mode: "auto",
          permission: write,
          requestType: "file_change_approval",
          decision: "acceptForSession",
          result: { kind: "approve-for-session", approval: { kind: "write" } },
        },
        {
          name: "auto-shell-allow",
          mode: "auto",
          permission: shell,
          requestType: "exec_command_approval",
          decision: "accept",
          result: { kind: "approve-once", approvedInteractively: true },
        },
        {
          name: "auto-generic-deny",
          mode: "auto",
          permission: generic,
          requestType: "dynamic_tool_call",
          decision: "decline",
          result: { kind: "reject", feedback: "The user denied this request." },
        },
        ...([write, shell, generic] as const).map((permission, index) => ({
          name: `full-access-${index}`,
          mode: "full-access" as const,
          permission,
          requestType: ["file_change_approval", "exec_command_approval", "dynamic_tool_call"][
            index
          ] as "file_change_approval" | "exec_command_approval" | "dynamic_tool_call",
          result: { kind: "approve-once" },
        })),
      ];

      for (const testCase of cases) {
        const threadId = ThreadId.make(`thread-${testCase.name}`);
        yield* adapter.startSession({
          ...startInput(threadId, ProviderInstanceId.make("copilot")),
          runtimeMode: testCase.mode,
        });
        const sdkInput = createInputs.at(-1)!;
        if (!testCase.decision) {
          assert.deepEqual(
            yield* Effect.promise(() => sdkInput.onPermissionRequest(testCase.permission)),
            testCase.result,
            testCase.name,
          );
          continue;
        }

        const resultFiber = yield* Effect.promise(() =>
          sdkInput.onPermissionRequest(testCase.permission),
        ).pipe(Effect.forkChild);
        const request = yield* Queue.take(opened);
        assert.equal(request.threadId, threadId);
        assert.equal(request.payload.requestType, testCase.requestType);
        yield* adapter.respondToRequest(
          threadId,
          ApprovalRequestId.make(request.requestId!),
          testCase.decision,
        );
        assert.deepEqual(yield* Fiber.join(resultFiber), testCase.result, testCase.name);
      }
    }),
  );

  it.effect("fails malformed requests visibly and keeps denied tools terminal", () =>
    Effect.gen(function* () {
      let sdkInput: CopilotSdkSessionStartInput | undefined;
      const runtime = runtimeWith((input) => {
        sdkInput = input;
        return Effect.succeed(fakeSession("denied-session"));
      });
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("denied-thread");
      const runtimeError =
        yield* Deferred.make<Extract<ProviderRuntimeEvent, { type: "runtime.error" }>>();
      const opened =
        yield* Queue.unbounded<Extract<ProviderRuntimeEvent, { type: "request.opened" }>>();
      const started =
        yield* Queue.unbounded<Extract<ProviderRuntimeEvent, { type: "item.started" }>>();
      const completed =
        yield* Queue.unbounded<Extract<ProviderRuntimeEvent, { type: "item.completed" }>>();
      yield* Stream.runForEach(adapter.streamEvents, (runtimeEvent) => {
        switch (runtimeEvent.type) {
          case "runtime.error":
            return Deferred.succeed(runtimeError, runtimeEvent);
          case "request.opened":
            return Queue.offer(opened, runtimeEvent);
          case "item.started":
            return Queue.offer(started, runtimeEvent);
          case "item.completed":
            return Queue.offer(completed, runtimeEvent);
          default:
            return Effect.void;
        }
      }).pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      yield* adapter.startSession({
        ...startInput(threadId, ProviderInstanceId.make("copilot")),
        runtimeMode: "approval-required",
      });
      assert.isDefined(sdkInput);

      assert.deepEqual(
        yield* Effect.promise(() =>
          sdkInput!.onPermissionRequest({ kind: "shell", fullCommandText: 42 }),
        ),
        { kind: "reject" },
      );
      assert.equal((yield* Deferred.await(runtimeError)).payload.class, "permission_error");

      yield* adapter.sendTurn({ threadId, input: "Run a tool" });
      sdkInput!.onEvent(
        event({
          type: "tool.execution_start",
          data: { toolCallId: "tool-denied", toolName: "shell", arguments: { cmd: "rm example" } },
        }),
      );
      assert.equal((yield* Queue.take(started)).itemId, "tool-denied");

      const deniedResult = yield* Effect.promise(() =>
        sdkInput!.onPermissionRequest({
          kind: "shell",
          fullCommandText: "rm example",
          intention: "Remove example",
          commands: [{ identifier: "rm", readOnly: false }],
          canOfferSessionApproval: true,
          toolCallId: "tool-denied",
        }),
      ).pipe(Effect.forkChild);
      const approval = yield* Queue.take(opened);
      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(approval.requestId!),
        "decline",
      );
      assert.equal((yield* Fiber.join(deniedResult)).kind, "reject");
      assert.equal((yield* Queue.take(completed)).payload.status, "declined");

      for (let index = 0; index < 2; index += 1) {
        sdkInput!.onEvent(
          event({
            type: "tool.execution_complete",
            data: { toolCallId: "tool-denied", success: true },
          }),
        );
      }
      sdkInput!.onEvent(
        event({
          type: "tool.execution_start",
          data: { toolCallId: "tool-sentinel", toolName: "preview", arguments: {} },
        }),
      );
      sdkInput!.onEvent(
        event({
          type: "tool.execution_complete",
          data: { toolCallId: "tool-sentinel", success: true },
        }),
      );
      const sentinel = yield* Queue.take(completed);
      assert.equal(sentinel.itemId, "tool-sentinel");
      assert.equal(sentinel.payload.status, "completed");
    }),
  );

  it.effect("interrupts a running turn exactly once and stays idempotent", () =>
    Effect.gen(function* () {
      let aborts = 0;
      let emitNative: ((value: SessionEvent) => void) | undefined;
      const runtime = runtimeWith((input) => {
        emitNative = input.onEvent;
        return Effect.succeed(
          fakeSession("interrupt-session", {
            onAbort: () => (aborts += 1),
            send: () =>
              Effect.sync(() => {
                input.onEvent(
                  event({
                    type: "assistant.message_delta",
                    ephemeral: true,
                    data: { messageId: "message-1", deltaContent: "Working" },
                  }),
                );
                return "message-1";
              }),
          }),
        );
      });
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("interrupt-thread");
      const observer = yield* observeEvents(adapter);
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      const turn = yield* adapter.sendTurn({ threadId, input: "Work" });
      yield* observer.until((runtimeEvent) => runtimeEvent.type === "content.delta");

      yield* adapter.interruptTurn(threadId);
      yield* adapter.interruptTurn(threadId);
      yield* adapter.interruptTurn(threadId, turn.turnId);
      const settled = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "turn.completed",
      );
      // The runtime's own abort acknowledgement must not settle the turn twice.
      emitNative?.(event({ type: "abort", data: { reason: "user_initiated" } }));
      emitNative?.(event({ type: "session.idle", ephemeral: true, data: { aborted: true } }));
      emitNative?.(
        event({
          type: "session.usage_info",
          ephemeral: true,
          data: { currentTokens: 12, tokenLimit: 100 },
        }),
      );
      yield* observer.until((runtimeEvent) => runtimeEvent.type === "thread.token-usage.updated");

      assert.equal(aborts, 1);
      assert.equal(settled.turnId, turn.turnId);
      assert.deepEqual(settled.payload, { state: "interrupted", stopReason: "user_interrupt" });
      assert.lengthOf(
        observer.seen.filter((runtimeEvent) => runtimeEvent.type === "turn.completed"),
        1,
      );
      const session = (yield* adapter.listSessions())[0];
      assert.equal(session?.status, "ready");
      assert.isUndefined(session?.activeTurnId);
    }),
  );

  it.effect("keeps late events from an interrupted turn out of the next turn", () =>
    Effect.gen(function* () {
      let emitNative: ((value: SessionEvent) => void) | undefined;
      const runtime = runtimeWith((input) => {
        emitNative = input.onEvent;
        return Effect.succeed(
          fakeSession("late-session", {
            send: () =>
              Effect.sync(() => {
                input.onEvent(
                  event({
                    type: "assistant.message_delta",
                    ephemeral: true,
                    data: { messageId: "message-1", deltaContent: "Working" },
                  }),
                );
                return "message-1";
              }),
          }),
        );
      });
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("late-thread");
      const observer = yield* observeEvents(adapter);
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      const first = yield* adapter.sendTurn({ threadId, input: "Work" });
      yield* observer.until((runtimeEvent) => runtimeEvent.type === "content.delta");
      yield* adapter.interruptTurn(threadId);
      yield* observer.until((runtimeEvent) => runtimeEvent.type === "turn.completed");

      // Everything the runtime still had in flight for the interrupted turn.
      emitNative?.(
        event({ type: "assistant.message", data: { messageId: "message-1", content: "Working" } }),
      );
      emitNative?.(
        event({ type: "tool.execution_complete", data: { toolCallId: "tool-1", success: true } }),
      );
      emitNative?.(
        event({
          type: "assistant.usage",
          ephemeral: true,
          data: { model: "gpt-5", inputTokens: 9, outputTokens: 3 },
        }),
      );

      const second = yield* adapter.sendTurn({ threadId, input: "Try again" });
      // The interrupted turn's aborted idle only reaches us once the next turn
      // is already running, so the turn tag alone cannot reject it.
      emitNative?.(event({ type: "session.idle", ephemeral: true, data: { aborted: true } }));
      emitNative?.(event({ type: "session.idle", ephemeral: true, data: {} }));
      const settled = yield* observer.until(
        (runtimeEvent) =>
          runtimeEvent.type === "turn.completed" && runtimeEvent.turnId === second.turnId,
      );

      assert.notEqual(second.turnId, first.turnId);
      assert.deepEqual(settled.payload, { state: "completed" });
      const afterInterrupt = observer.seen.slice(
        observer.seen.findIndex((runtimeEvent) => runtimeEvent.type === "turn.completed") + 1,
      );
      // Only the second turn's own events follow: no item.completed from the
      // interrupted turn's trailing message and no usage it leaked.
      assert.deepEqual(
        afterInterrupt.map((runtimeEvent) => runtimeEvent.type),
        ["turn.started", "item.started", "content.delta", "turn.completed"],
      );
    }),
  );

  it.effect("settles once when a send fails after the turn was interrupted", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const runtime = runtimeWith(() =>
        Effect.succeed(
          fakeSession("pending-send-session", {
            send: () =>
              Deferred.await(release).pipe(
                Effect.andThen(
                  Effect.fail(
                    new SdkError({
                      operation: "send",
                      kind: "failure",
                      detail: "connection closed",
                    }),
                  ),
                ),
              ),
          }),
        ),
      );
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("pending-send-thread");
      const observer = yield* observeEvents(adapter);
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      const sending = yield* Effect.forkChild(
        adapter.sendTurn({ threadId, input: "Work" }).pipe(Effect.result),
      );
      yield* observer.until((runtimeEvent) => runtimeEvent.type === "turn.started");

      yield* adapter.interruptTurn(threadId);
      const settled = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "turn.completed",
      );
      yield* Deferred.succeed(release, undefined);
      const sent = yield* Fiber.join(sending);

      assert.deepEqual(settled.payload, { state: "interrupted", stopReason: "user_interrupt" });
      // The rejection is a consequence of the interrupt, so it is not reported
      // as a second, separate failure on top of the settled turn.
      assert.equal(sent._tag, "Success");
      assert.lengthOf(
        observer.seen.filter((runtimeEvent) => runtimeEvent.type === "turn.completed"),
        1,
      );
      // The session survives a failed send, so the next turn can start.
      assert.isTrue(yield* adapter.hasSession(threadId));
    }),
  );

  it.effect("reports a runtime error and fails the visible turn", () =>
    Effect.gen(function* () {
      let emitNative: ((value: SessionEvent) => void) | undefined;
      const runtime = runtimeWith((input) => {
        emitNative = input.onEvent;
        return Effect.succeed(fakeSession("error-session"));
      });
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("error-thread");
      const observer = yield* observeEvents(adapter);
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      const turn = yield* adapter.sendTurn({ threadId, input: "Work" });

      emitNative?.(
        event({
          type: "session.error",
          data: { errorType: "authentication", message: "Token expired" },
        }),
      );
      const settled = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "turn.completed",
      );

      const runtimeError = observer.seen.find(
        (runtimeEvent) => runtimeEvent.type === "runtime.error",
      );
      assert.isDefined(runtimeError);
      assert.equal(
        runtimeError?.type === "runtime.error" ? runtimeError.payload.class : undefined,
        "permission_error",
      );
      assert.equal(settled.turnId, turn.turnId);
      assert.deepEqual(settled.payload, { state: "failed", errorMessage: "Token expired" });
      assert.isTrue(yield* adapter.hasSession(threadId));
    }),
  );

  it.effect("settles the turn and closes the session when the runtime shuts down", () =>
    Effect.gen(function* () {
      let disconnects = 0;
      let emitNative: ((value: SessionEvent) => void) | undefined;
      const runtime = runtimeWith((input) => {
        emitNative = input.onEvent;
        return Effect.succeed(
          fakeSession("shutdown-session", { onDisconnect: () => (disconnects += 1) }),
        );
      });
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("shutdown-thread");
      const observer = yield* observeEvents(adapter);
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      const turn = yield* adapter.sendTurn({ threadId, input: "Work" });

      emitNative?.(
        event({
          type: "session.shutdown",
          data: {
            shutdownType: "error",
            errorReason: "Copilot runtime exited",
            codeChanges: { filesModified: [], linesAdded: 0, linesRemoved: 0 },
            modelMetrics: {},
            sessionStartTime: 0,
            totalApiDurationMs: 0,
          },
        }),
      );
      const settled = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "turn.completed",
      );
      const exited = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "session.exited",
      );

      assert.equal(settled.turnId, turn.turnId);
      assert.deepEqual(settled.payload, {
        state: "failed",
        errorMessage: "Copilot runtime exited",
      });
      assert.deepEqual(exited.payload, {
        reason: "Copilot runtime exited",
        recoverable: false,
        exitKind: "error",
      });
      assert.equal(disconnects, 1);
      assert.isFalse(yield* adapter.hasSession(threadId));
      // A dead session no longer accepts work.
      const rejected = yield* adapter.sendTurn({ threadId, input: "Again" }).pipe(Effect.result);
      assert.equal(rejected._tag, "Failure");
    }),
  );

  it.effect("treats a recoverable Copilot error as a warning and lets the turn finish", () =>
    Effect.gen(function* () {
      let emitNative: ((value: SessionEvent) => void) | undefined;
      const runtime = runtimeWith((input) => {
        emitNative = input.onEvent;
        return Effect.succeed(fakeSession("recoverable-session"));
      });
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("recoverable-thread");
      const observer = yield* observeEvents(adapter);
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      const turn = yield* adapter.sendTurn({ threadId, input: "Work" });

      // The runtime compacts and retries this one, so it must not end the turn.
      emitNative?.(
        event({
          type: "session.error",
          data: { errorType: "context_limit", message: "Context limit reached" },
        }),
      );
      yield* observer.until((runtimeEvent) => runtimeEvent.type === "runtime.warning");
      emitNative?.(event({ type: "session.idle", ephemeral: true, data: {} }));
      const settled = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "turn.completed",
      );

      assert.equal(settled.turnId, turn.turnId);
      assert.deepEqual(settled.payload, { state: "completed" });
      assert.isUndefined(
        observer.seen.find((runtimeEvent) => runtimeEvent.type === "runtime.error"),
      );
    }),
  );

  it.effect("stops the session when the runtime refuses to abort", () =>
    Effect.gen(function* () {
      let disconnects = 0;
      const runtime = runtimeWith(() =>
        Effect.succeed(
          fakeSession("stubborn-session", {
            abort: Effect.fail(
              new SdkError({ operation: "abort", kind: "failure", detail: "abort rejected" }),
            ),
            onDisconnect: () => (disconnects += 1),
          }),
        ),
      );
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("stubborn-thread");
      const observer = yield* observeEvents(adapter);
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      yield* adapter.sendTurn({ threadId, input: "Work" });

      yield* adapter.interruptTurn(threadId);
      const exited = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "session.exited",
      );

      assert.deepEqual(
        observer.seen
          .filter((runtimeEvent) => runtimeEvent.type === "turn.completed")
          .map((runtimeEvent) => runtimeEvent.payload),
        [{ state: "interrupted", stopReason: "user_interrupt" }],
      );
      assert.deepEqual(exited.payload, {
        reason: "Session stopped.",
        recoverable: false,
        exitKind: "graceful",
      });
      assert.equal(disconnects, 1);
      assert.isFalse(yield* adapter.hasSession(threadId));
    }),
  );

  it.effect("releases a send that the runtime never answers when the session stops", () =>
    Effect.gen(function* () {
      const never = yield* Deferred.make<string>();
      const runtime = runtimeWith(() =>
        Effect.succeed(fakeSession("hanging-session", { send: () => Deferred.await(never) })),
      );
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("hanging-thread");
      const observer = yield* observeEvents(adapter);
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      const sending = yield* Effect.forkChild(
        adapter.sendTurn({ threadId, input: "Work" }).pipe(Effect.result),
      );
      yield* observer.until((runtimeEvent) => runtimeEvent.type === "turn.started");

      yield* adapter.stopSession(threadId);
      // Without the release this join never returns.
      const sent = yield* Fiber.join(sending);
      const settled = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "turn.completed",
      );

      assert.equal(sent._tag, "Success");
      assert.deepEqual(settled.payload, { state: "cancelled", stopReason: "session_stopped" });
      assert.isFalse(yield* adapter.hasSession(threadId));
    }),
  );

  it.effect("releases a pending approval when the turn is interrupted", () =>
    Effect.gen(function* () {
      let sdkInput: CopilotSdkSessionStartInput | undefined;
      const runtime = runtimeWith((input) => {
        sdkInput = input;
        return Effect.succeed(fakeSession("approval-interrupt-session"));
      });
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("approval-interrupt-thread");
      const observer = yield* observeEvents(adapter);
      yield* adapter.startSession({
        ...startInput(threadId, ProviderInstanceId.make("copilot")),
        runtimeMode: "approval-required",
      });
      yield* adapter.sendTurn({ threadId, input: "Work" });
      const permission = yield* Effect.forkChild(
        Effect.promise(() =>
          sdkInput!.onPermissionRequest({
            kind: "shell",
            fullCommandText: "pnpm test",
            intention: "Run tests",
            commands: [{ identifier: "pnpm", readOnly: true }],
            canOfferSessionApproval: true,
          }),
        ),
      );
      const opened = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "request.opened",
      );

      yield* adapter.interruptTurn(threadId);
      const resolved = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "request.resolved",
      );
      // Without the release the SDK waits on this promise forever.
      const outcome = yield* Fiber.join(permission);

      assert.equal(resolved.requestId, opened.requestId);
      assert.deepEqual(outcome, {
        kind: "reject",
        feedback: "The user cancelled this request.",
      });
    }),
  );

  it.effect("disconnects stopped sessions and all remaining sessions during scope teardown", () =>
    Effect.gen(function* () {
      let disconnects = 0;
      let runtimeReleases = 0;
      const runtime = runtimeWith(
        () =>
          Effect.succeed(
            fakeSession(`scope-session-${(sessionSequence += 1)}`, {
              onDisconnect: () => (disconnects += 1),
            }),
          ),
        { onRelease: () => (runtimeReleases += 1) },
      );

      yield* Effect.scoped(
        Effect.gen(function* () {
          const adapter = yield* makeTestAdapter(runtime);
          const first = ThreadId.make("stop-one");
          const second = ThreadId.make("stop-all");
          yield* adapter.startSession(startInput(first, ProviderInstanceId.make("copilot")));
          yield* adapter.startSession(startInput(second, ProviderInstanceId.make("copilot")));
          const stoppedTurn = yield* Deferred.make<unknown>();
          yield* Stream.runForEach(adapter.streamEvents, (runtimeEvent) =>
            runtimeEvent.type === "turn.completed" && runtimeEvent.payload.state === "cancelled"
              ? Deferred.succeed(stoppedTurn, runtimeEvent.payload)
              : Effect.void,
          ).pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          yield* adapter.sendTurn({ threadId: first, input: "Keep working" });
          yield* adapter.stopSession(first);
          yield* Deferred.await(stoppedTurn);
          assert.isFalse(yield* adapter.hasSession(first));
          assert.isTrue(yield* adapter.hasSession(second));
          assert.equal(disconnects, 1);
        }),
      );

      assert.equal(disconnects, 2);
      assert.equal(runtimeReleases, 1);
    }),
  );
  it.effect("resumes the session recorded on the thread and streams its later events", () =>
    Effect.gen(function* () {
      const resumeInputs: Array<CopilotSdkSessionResumeInput> = [];
      let created = 0;
      const runtime = runtimeWith(
        () =>
          Effect.sync(() => {
            created += 1;
            return fakeSession("copilot-session-7");
          }),
        {
          resumeSession: (input) =>
            Effect.sync(() => {
              resumeInputs.push(input);
              return fakeSession(input.sessionId, {
                send: () =>
                  Effect.sync(() => {
                    input.onEvent(
                      event({
                        type: "assistant.message",
                        data: { messageId: "resumed-1", content: "Back" },
                      }),
                    );
                    input.onEvent(event({ type: "session.idle", ephemeral: true, data: {} }));
                    return "resumed-1";
                  }),
              });
            }),
        },
      );
      const instanceId = ProviderInstanceId.make("copilot_work");
      const threadId = ThreadId.make("resume-thread");

      const before = yield* makeTestAdapter(runtime, { instanceId });
      const started = yield* before.startSession(startInput(threadId, instanceId));

      // A restarted server rebuilds the adapter and only has the cursor it stored.
      const after = yield* makeTestAdapter(runtime, { instanceId });
      const observer = yield* observeEvents(after);
      const resumed = yield* after.startSession({
        ...startInput(threadId, instanceId),
        resumeCursor: started.resumeCursor,
      });
      yield* after.sendTurn({ threadId, input: "Where were we?" });
      const completed = yield* observer.until(
        (runtimeEvent) => runtimeEvent.type === "turn.completed",
      );

      assert.equal(created, 1);
      assert.equal(resumeInputs[0]?.sessionId, "copilot-session-7");
      assert.deepEqual(resumed.resumeCursor, started.resumeCursor);
      assert.deepEqual(completed.payload, { state: "completed" });
      assert.deepEqual(
        observer.seen.map((runtimeEvent) => runtimeEvent.type),
        [
          "session.started",
          "thread.started",
          "turn.started",
          "item.started",
          "content.delta",
          "item.completed",
          "turn.completed",
        ],
      );
    }),
  );

  it.effect(
    "reports a session the runtime can no longer resume instead of starting a fresh one",
    () =>
      Effect.gen(function* () {
        let created = 0;
        const runtime = runtimeWith(
          () =>
            Effect.sync(() => {
              created += 1;
              return fakeSession("unused-session");
            }),
          {
            resumeSession: () =>
              Effect.fail(
                new SdkError({
                  operation: "resumeSession",
                  kind: "failure",
                  detail: "session copilot-session-9 was not found",
                }),
              ),
          },
        );
        const instanceId = ProviderInstanceId.make("copilot_work");
        const adapter = yield* makeTestAdapter(runtime, { instanceId });
        const threadId = ThreadId.make("missing-remote-session");

        const result = yield* adapter
          .startSession({
            ...startInput(threadId, instanceId),
            resumeCursor: buildCopilotContinuation({ instanceId, sessionId: "copilot-session-9" }),
          })
          .pipe(Effect.result);

        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.equal(result.failure._tag, "ProviderAdapterRequestError");
          assert.include(result.failure.message, "was not found");
        }
        assert.equal(created, 0);
        assert.isFalse(yield* adapter.hasSession(threadId));
      }),
  );

  it.effect("refuses continuation metadata it cannot trust", () =>
    Effect.gen(function* () {
      let touched = 0;
      const runtime = runtimeWith(
        () =>
          Effect.sync(() => {
            touched += 1;
            return fakeSession("unused-session");
          }),
        {
          resumeSession: () =>
            Effect.sync(() => {
              touched += 1;
              return fakeSession("unused-session");
            }),
        },
      );
      const instanceId = ProviderInstanceId.make("copilot_work");
      const adapter = yield* makeTestAdapter(runtime, { instanceId });

      const cursors = [
        { schemaVersion: 0, provider: "copilot", instanceId, sessionId: "stale" },
        buildCopilotContinuation({
          instanceId: ProviderInstanceId.make("copilot_personal"),
          sessionId: "someone-elses",
        }),
        "copilot-session-1",
      ];
      const failures = yield* Effect.forEach(cursors, (resumeCursor, index) =>
        adapter
          .startSession({
            ...startInput(ThreadId.make(`untrusted-${index}`), instanceId),
            resumeCursor,
          })
          .pipe(Effect.flip),
      );

      assert.deepEqual(
        failures.map((failure) => failure._tag),
        [
          "ProviderAdapterValidationError",
          "ProviderAdapterValidationError",
          "ProviderAdapterValidationError",
        ],
      );
      assert.equal(touched, 0);
    }),
  );

  it.effect("applies a supported model and option change before the next turn", () =>
    Effect.gen(function* () {
      const switches: Array<CopilotSdkModelOptions> = [];
      const runtime = runtimeWith(
        () =>
          Effect.succeed(
            fakeSession("model-session", { onSetModel: (value) => switches.push(value) }),
          ),
        { models: Effect.succeed(inventory) },
      );
      const instanceId = ProviderInstanceId.make("copilot_work");
      const adapter = yield* makeTestAdapter(runtime, { instanceId });
      const threadId = ThreadId.make("model-thread");
      yield* adapter.startSession(startInput(threadId, instanceId));

      const modelSelection = {
        instanceId,
        model: "gpt-5.4",
        options: [{ id: "reasoning_effort", value: "high" }],
      } as const;
      yield* adapter.sendTurn({ threadId, input: "First", modelSelection });
      yield* adapter.sendTurn({ threadId, input: "Second", modelSelection });

      // The switch is applied once and then remembered, so an unchanged
      // selection does not re-negotiate the model on every turn.
      assert.deepEqual(switches, [{ model: "gpt-5.4", reasoningEffort: "high" }]);
      assert.equal((yield* adapter.listSessions())[0]?.model, "gpt-5.4");
    }),
  );

  it.effect("fails a turn asking for an unavailable model without starting it", () =>
    Effect.gen(function* () {
      const switches: Array<CopilotSdkModelOptions> = [];
      const runtime = runtimeWith(
        () =>
          Effect.succeed(
            fakeSession("model-failure-session", { onSetModel: (value) => switches.push(value) }),
          ),
        { models: Effect.succeed(inventory) },
      );
      const instanceId = ProviderInstanceId.make("copilot_work");
      const adapter = yield* makeTestAdapter(runtime, { instanceId });
      const threadId = ThreadId.make("model-failure-thread");
      const observer = yield* observeEvents(adapter);
      yield* adapter.startSession(startInput(threadId, instanceId));

      const missingModel = yield* adapter
        .sendTurn({
          threadId,
          input: "Use something else",
          modelSelection: { instanceId, model: "gpt-9" },
        })
        .pipe(Effect.flip);
      const missingOption = yield* adapter
        .sendTurn({
          threadId,
          input: "Think harder",
          modelSelection: {
            instanceId,
            model: "gpt-5.4",
            options: [{ id: "reasoning_effort", value: "max" }],
          },
        })
        .pipe(Effect.flip);

      // The first turn the adapter accepts is the valid one, so neither refusal
      // left a started turn behind.
      yield* adapter.sendTurn({
        threadId,
        input: "Carry on",
        modelSelection: { instanceId, model: "gpt-5.4" },
      });
      yield* observer.until((runtimeEvent) => runtimeEvent.type === "turn.started");

      assert.equal(missingModel._tag, "ProviderAdapterValidationError");
      assert.equal(missingOption._tag, "ProviderAdapterValidationError");
      assert.deepEqual(switches, [{ model: "gpt-5.4" }]);
      assert.deepEqual(
        observer.seen.map((runtimeEvent) => runtimeEvent.type),
        ["session.started", "thread.started", "turn.started"],
      );
    }),
  );

  it.effect("rolls the provider conversation back and returns the truncated snapshot", () =>
    Effect.gen(function* () {
      const rewound: Array<string> = [];
      const runtime = runtimeWith(() =>
        Effect.succeed(
          fakeSession("rewind-session", {
            rewindPoints: Effect.succeed({
              points: [rewindPoint("event-1", "First"), rewindPoint("event-2", "Second")],
            }),
            rewind: (eventId) =>
              Effect.sync(() => {
                rewound.push(eventId);
                return { outcome: "success", eventsRemoved: 4 };
              }),
          }),
        ),
      );
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("rewind-thread");
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      const first = yield* adapter.sendTurn({ threadId, input: "First" });
      yield* adapter.sendTurn({ threadId, input: "Second" });

      const snapshot = yield* adapter.rollbackThread(threadId, 1);

      assert.deepEqual(rewound, ["event-2"]);
      assert.deepEqual(
        snapshot.turns.map((turn) => turn.id),
        [first.turnId],
      );
      assert.deepEqual(yield* adapter.readThread(threadId), snapshot);
    }),
  );

  it.effect("reports invalid distances and refused rollbacks without dropping history", () =>
    Effect.gen(function* () {
      const runtime = runtimeWith(() =>
        Effect.succeed(
          fakeSession("rewind-failure-session", {
            rewindPoints: Effect.succeed({
              points: [rewindPoint("event-1", "First"), rewindPoint("event-2", "Second")],
            }),
            rewind: () => Effect.succeed({ outcome: "session-busy" }),
          }),
        ),
      );
      const adapter = yield* makeTestAdapter(runtime);
      const threadId = ThreadId.make("rewind-failure-thread");
      yield* adapter.startSession(startInput(threadId, ProviderInstanceId.make("copilot")));
      yield* adapter.sendTurn({ threadId, input: "First" });
      yield* adapter.sendTurn({ threadId, input: "Second" });

      const notATurn = yield* adapter.rollbackThread(threadId, 0).pipe(Effect.flip);
      const tooFar = yield* adapter.rollbackThread(threadId, 5).pipe(Effect.flip);
      const refused = yield* adapter.rollbackThread(threadId, 1).pipe(Effect.flip);

      assert.equal(notATurn._tag, "ProviderAdapterValidationError");
      assert.equal(tooFar._tag, "ProviderAdapterValidationError");
      assert.equal(refused._tag, "ProviderAdapterRequestError");
      assert.include(refused.message, "session-busy");
      assert.lengthOf((yield* adapter.readThread(threadId)).turns, 2);
    }),
  );
});
