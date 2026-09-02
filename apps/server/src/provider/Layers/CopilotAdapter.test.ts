import type { SessionEvent } from "@github/copilot-sdk";
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
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import type {
  CopilotSdkConnection,
  CopilotSdkRuntimeShape,
  CopilotSdkSession,
  CopilotSdkSessionStartInput,
} from "../CopilotSdkRuntime.ts";
import { CopilotSdkRuntimeError as SdkError } from "../CopilotSdkRuntime.ts";
import type { CopilotPermission } from "../CopilotPermissions.ts";
import { makeCopilotAdapter } from "./CopilotAdapter.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-copilot-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

function fakeSession(
  sessionId: string,
  options: {
    readonly send?: CopilotSdkSession["send"];
    readonly onDisconnect?: () => void;
  } = {},
): CopilotSdkSession {
  return {
    sessionId,
    send: options.send ?? (() => Effect.succeed("message-1")),
    disconnect: Effect.sync(() => options.onDisconnect?.()),
  };
}

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
  options: { readonly onRelease?: () => void } = {},
): CopilotSdkRuntimeShape {
  const connection: CopilotSdkConnection = {
    authStatus: Effect.succeed({ isAuthenticated: true }),
    models: Effect.succeed([]),
    createSession,
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
});
