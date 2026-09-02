import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import type { SessionEvent } from "@github/copilot-sdk";
import {
  ApprovalRequestId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderApprovalDecision,
  type RuntimeMode,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import type { CopilotSdkConnection, CopilotSdkSession } from "../CopilotSdkRuntime.ts";
import type { CopilotPermission } from "../CopilotPermissions.ts";
import { makeCopilotAdapter } from "./CopilotAdapter.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-copilot-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

function fakeSession(sessionId: string): CopilotSdkSession {
  return {
    sessionId,
    send: () => Effect.succeed("message-1"),
    abort: Effect.void,
    disconnect: Effect.void,
  };
}

function fakeConnection() {
  const createInputs: Array<Parameters<CopilotSdkConnection["createSession"]>[0]> = [];
  const connection: CopilotSdkConnection = {
    authStatus: Effect.succeed({ isAuthenticated: true }),
    models: Effect.succeed([]),
    createSession: (input) => {
      createInputs.push(input);
      return Effect.succeed(fakeSession(`copilot-session-${createInputs.length}`));
    },
  };
  return { connection, createInputs };
}

it.layer(testLayer)("CopilotAdapter", (it) => {
  it.effect("starts one isolated SDK session for a T3 thread", () =>
    Effect.gen(function* () {
      const { connection, createInputs } = fakeConnection();
      const instanceId = ProviderInstanceId.make("copilot_work");
      const adapter = yield* makeCopilotAdapter(connection, { instanceId });
      const threadId = ThreadId.make("thread-1");

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("copilot"),
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      assert.equal(session.threadId, threadId);
      assert.equal(session.providerInstanceId, instanceId);
      assert.deepEqual(session.resumeCursor, { sessionId: "copilot-session-1" });
      assert.equal(createInputs[0]?.workingDirectory, process.cwd());
      assert.isTrue(yield* adapter.hasSession(threadId));
      assert.lengthOf(yield* adapter.listSessions(), 1);
    }),
  );

  it.effect("streams a basic Copilot turn through canonical runtime events", () =>
    Effect.gen(function* () {
      const connection: CopilotSdkConnection = {
        authStatus: Effect.succeed({ isAuthenticated: true }),
        models: Effect.succeed([]),
        createSession: (input) =>
          Effect.succeed({
            ...fakeSession("copilot-session-stream"),
            send: () =>
              Effect.sync(() => {
                input.onEvent({
                  id: "assistant-start",
                  parentId: null,
                  timestamp: "2026-09-02T00:00:00.000Z",
                  type: "assistant.message_start",
                  data: { messageId: "message-1" },
                } as unknown as SessionEvent);
                input.onEvent({
                  id: "assistant-delta",
                  parentId: "assistant-start",
                  timestamp: "2026-09-02T00:00:01.000Z",
                  type: "assistant.message_delta",
                  data: { messageId: "message-1", deltaContent: "Hello" },
                } as unknown as SessionEvent);
                input.onEvent({
                  id: "assistant-complete",
                  parentId: "assistant-delta",
                  timestamp: "2026-09-02T00:00:02.000Z",
                  type: "assistant.message",
                  data: { messageId: "message-1", content: "Hello" },
                } as unknown as SessionEvent);
                input.onEvent({
                  id: "session-idle",
                  parentId: "assistant-complete",
                  timestamp: "2026-09-02T00:00:03.000Z",
                  type: "session.idle",
                  data: {},
                } as unknown as SessionEvent);
                return "message-1";
              }),
          }),
      };
      const adapter = yield* makeCopilotAdapter(connection);
      const threadId = ThreadId.make("thread-stream");
      const eventsFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.threadId === threadId),
        Stream.take(8),
        Stream.runCollect,
        Effect.forkChild,
      );
      yield* adapter.startSession({
        threadId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      const turn = yield* adapter.sendTurn({ threadId, input: "Say hello" });
      const events = [...(yield* Fiber.join(eventsFiber))];

      assert.includeMembers(
        events.map(({ type }) => type),
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
      assert.isTrue(events.every((event) => event.providerInstanceId === "copilot"));
      assert.isTrue(
        events
          .filter((event) => event.turnId !== undefined)
          .every((event) => event.turnId === turn.turnId),
      );
    }),
  );

  it.effect("opens a canonical shell approval and returns an interactive allow", () =>
    Effect.gen(function* () {
      const { connection, createInputs } = fakeConnection();
      const adapter = yield* makeCopilotAdapter(connection);
      const threadId = ThreadId.make("thread-approval");
      yield* adapter.startSession({
        threadId,
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });

      const responseFiber = yield* Effect.promise(() =>
        createInputs[0]!.onPermissionRequest({
          kind: "shell",
          fullCommandText: "pnpm test",
          intention: "Run the focused test",
          commands: [{ identifier: "pnpm", readOnly: true }],
          possiblePaths: [],
          possibleUrls: [],
          hasWriteFileRedirection: false,
          canOfferSessionApproval: true,
          toolCallId: "tool-1",
        }),
      ).pipe(Effect.forkChild);

      const opened = Option.getOrThrow(
        yield* adapter.streamEvents.pipe(
          Stream.filter((event) => event.type === "request.opened"),
          Stream.runHead,
        ),
      );
      assert.equal(opened.type, "request.opened");
      if (opened.type !== "request.opened") return;
      assert.equal(opened.payload.requestType, "exec_command_approval");
      assert.equal(opened.payload.detail, "pnpm test");

      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(opened.requestId!),
        "accept",
      );
      assert.deepEqual(yield* Fiber.join(responseFiber), {
        kind: "approve-once",
        approvedInteractively: true,
      });
      const resolved = Option.getOrThrow(
        yield* adapter.streamEvents.pipe(
          Stream.filter((event) => event.type === "request.resolved"),
          Stream.runHead,
        ),
      );
      assert.equal(resolved.type, "request.resolved");
      if (resolved.type === "request.resolved") {
        assert.equal(resolved.payload.decision, "accept");
      }
    }),
  );

  it.effect("honors file, shell, and generic approvals across every runtime mode", () =>
    Effect.gen(function* () {
      const { connection, createInputs } = fakeConnection();
      const adapter = yield* makeCopilotAdapter(connection);
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
        readonly decision?: ProviderApprovalDecision;
        readonly result: unknown;
      }> = [
        {
          name: "approval-file-allow",
          mode: "approval-required",
          permission: write,
          decision: "accept",
          result: { kind: "approve-once", approvedInteractively: true },
        },
        {
          name: "approval-shell-deny",
          mode: "approval-required",
          permission: shell,
          decision: "decline",
          result: { kind: "reject", feedback: "The user denied this request." },
        },
        {
          name: "approval-generic-session",
          mode: "approval-required",
          permission: generic,
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
          result: { kind: "approve-once" },
        },
        {
          name: "edits-shell-allow",
          mode: "auto-accept-edits",
          permission: shell,
          decision: "accept",
          result: { kind: "approve-once", approvedInteractively: true },
        },
        {
          name: "edits-generic-deny",
          mode: "auto-accept-edits",
          permission: generic,
          decision: "decline",
          result: { kind: "reject", feedback: "The user denied this request." },
        },
        {
          name: "auto-file-session",
          mode: "auto",
          permission: write,
          decision: "acceptForSession",
          result: { kind: "approve-for-session", approval: { kind: "write" } },
        },
        {
          name: "auto-shell-allow",
          mode: "auto",
          permission: shell,
          decision: "accept",
          result: { kind: "approve-once", approvedInteractively: true },
        },
        {
          name: "auto-generic-deny",
          mode: "auto",
          permission: generic,
          decision: "decline",
          result: { kind: "reject", feedback: "The user denied this request." },
        },
        ...([write, shell, generic] as const).map((permission, index) => ({
          name: `full-access-${index}`,
          mode: "full-access" as const,
          permission,
          result: { kind: "approve-once" },
        })),
      ];

      for (const testCase of cases) {
        const threadId = ThreadId.make(`thread-${testCase.name}`);
        yield* adapter.startSession({
          threadId,
          cwd: process.cwd(),
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

        const openedFiber = yield* adapter.streamEvents.pipe(
          Stream.filter((event) => event.type === "request.opened" && event.threadId === threadId),
          Stream.runHead,
          Effect.forkChild,
        );
        const resultFiber = yield* Effect.promise(() =>
          sdkInput.onPermissionRequest(testCase.permission),
        ).pipe(Effect.forkChild);
        const opened = Option.getOrThrow(yield* Fiber.join(openedFiber));
        yield* adapter.respondToRequest(
          threadId,
          ApprovalRequestId.make(opened.requestId!),
          testCase.decision,
        );
        assert.deepEqual(yield* Fiber.join(resultFiber), testCase.result, testCase.name);
      }
    }),
  );

  it.effect("returns an exact session scope without approving unrelated operations", () =>
    Effect.gen(function* () {
      const { connection, createInputs } = fakeConnection();
      const adapter = yield* makeCopilotAdapter(connection);
      const threadId = ThreadId.make("thread-scoped-session");
      yield* adapter.startSession({
        threadId,
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      const sdkInput = createInputs[0]!;
      const firstOpenedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "request.opened" && event.threadId === threadId),
        Stream.runHead,
        Effect.forkChild,
      );
      const firstResultFiber = yield* Effect.promise(() =>
        sdkInput.onPermissionRequest({
          kind: "custom-tool",
          toolName: "preview",
          toolDescription: "Run preview",
        }),
      ).pipe(Effect.forkChild);
      const firstOpened = Option.getOrThrow(yield* Fiber.join(firstOpenedFiber));
      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(firstOpened.requestId!),
        "acceptForSession",
      );
      assert.deepEqual(yield* Fiber.join(firstResultFiber), {
        kind: "approve-for-session",
        approval: { kind: "custom-tool", toolName: "preview" },
      });

      const unrelatedOpenedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "request.opened" && event.threadId === threadId),
        Stream.runHead,
        Effect.forkChild,
      );
      const unrelatedResultFiber = yield* Effect.promise(() =>
        sdkInput.onPermissionRequest({
          kind: "custom-tool",
          toolName: "deploy",
          toolDescription: "Run deploy",
        }),
      ).pipe(Effect.forkChild);
      const unrelatedOpened = Option.getOrThrow(yield* Fiber.join(unrelatedOpenedFiber));
      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(unrelatedOpened.requestId!),
        "decline",
      );
      assert.equal((yield* Fiber.join(unrelatedResultFiber)).kind, "reject");
    }),
  );

  it.effect("rejects malformed payloads with a visible canonical failure", () =>
    Effect.gen(function* () {
      const { connection, createInputs } = fakeConnection();
      const adapter = yield* makeCopilotAdapter(connection);
      yield* adapter.startSession({
        threadId: ThreadId.make("thread-malformed"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });

      assert.deepEqual(
        yield* Effect.promise(() =>
          createInputs[0]!.onPermissionRequest({ kind: "shell", fullCommandText: 42 }),
        ),
        { kind: "reject" },
      );
      const failure = Option.getOrThrow(
        yield* adapter.streamEvents.pipe(
          Stream.filter((event) => event.type === "runtime.error"),
          Stream.runHead,
        ),
      );
      assert.equal(failure.type, "runtime.error");
      if (failure.type === "runtime.error") {
        assert.equal(failure.payload.class, "permission_error");
      }
    }),
  );

  it.effect("isolates pending approvals by thread and request identifier", () =>
    Effect.gen(function* () {
      const { connection, createInputs } = fakeConnection();
      const instanceId = ProviderInstanceId.make("copilot_isolated");
      const adapter = yield* makeCopilotAdapter(connection, { instanceId });
      const firstThread = ThreadId.make("thread-first");
      const secondThread = ThreadId.make("thread-second");
      for (const threadId of [firstThread, secondThread]) {
        yield* adapter.startSession({
          threadId,
          cwd: process.cwd(),
          runtimeMode: "approval-required",
        });
      }

      const openedFiber = yield* adapter.streamEvents.pipe(
        Stream.filter((event) => event.type === "request.opened"),
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild,
      );
      const request = {
        kind: "custom-tool",
        toolName: "preview",
        toolDescription: "Create a preview",
      } as const;
      const firstResponse = yield* Effect.promise(() =>
        createInputs[0]!.onPermissionRequest(request),
      ).pipe(Effect.forkChild);
      const secondResponse = yield* Effect.promise(() =>
        createInputs[1]!.onPermissionRequest(request),
      ).pipe(Effect.forkChild);
      const opened = [...(yield* Fiber.join(openedFiber))].filter(
        (event) => event.type === "request.opened",
      );
      assert.lengthOf(opened, 2);
      const firstRequest = opened.find((event) => event.threadId === firstThread)!;
      const secondRequest = opened.find((event) => event.threadId === secondThread)!;
      assert.notEqual(firstRequest.requestId, secondRequest.requestId);

      const crossThread = yield* adapter
        .respondToRequest(secondThread, ApprovalRequestId.make(firstRequest.requestId!), "accept")
        .pipe(Effect.result);
      assert.isTrue(Result.isFailure(crossThread));

      yield* adapter.respondToRequest(
        firstThread,
        ApprovalRequestId.make(firstRequest.requestId!),
        "decline",
      );
      yield* adapter.respondToRequest(
        secondThread,
        ApprovalRequestId.make(secondRequest.requestId!),
        "acceptForSession",
      );
      assert.equal((yield* Fiber.join(firstResponse)).kind, "reject");
      assert.equal((yield* Fiber.join(secondResponse)).kind, "approve-for-session");
    }),
  );

  it.effect("keeps a denied tool declined when Copilot later reports completion", () =>
    Effect.gen(function* () {
      const { connection, createInputs } = fakeConnection();
      const adapter = yield* makeCopilotAdapter(connection);
      const threadId = ThreadId.make("thread-denied");
      yield* adapter.startSession({
        threadId,
        cwd: process.cwd(),
        runtimeMode: "approval-required",
      });
      const sdkInput = createInputs[0]!;
      sdkInput.onEvent({
        id: "event-tool-start",
        parentId: null,
        timestamp: "2026-09-02T00:00:00.000Z",
        type: "tool.execution_start",
        data: {
          toolCallId: "tool-denied",
          toolName: "shell",
          arguments: { command: "rm example" },
        },
      } as SessionEvent);
      const responseFiber = yield* Effect.promise(() =>
        sdkInput.onPermissionRequest({
          kind: "shell",
          fullCommandText: "rm example",
          intention: "Remove example",
          commands: [{ identifier: "rm", readOnly: false }],
          possiblePaths: ["example"],
          possibleUrls: [],
          hasWriteFileRedirection: false,
          canOfferSessionApproval: true,
          toolCallId: "tool-denied",
        }),
      ).pipe(Effect.forkChild);
      const initial = [
        ...(yield* adapter.streamEvents.pipe(
          Stream.filter(
            (event) => event.type === "item.started" || event.type === "request.opened",
          ),
          Stream.take(2),
          Stream.runCollect,
        )),
      ];
      const opened = initial.find((event) => event.type === "request.opened")!;
      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make(opened.requestId!),
        "decline",
      );
      assert.equal((yield* Fiber.join(responseFiber)).kind, "reject");
      const denied = [
        ...(yield* adapter.streamEvents.pipe(
          Stream.filter(
            (event) => event.type === "request.resolved" || event.type === "item.completed",
          ),
          Stream.take(2),
          Stream.runCollect,
        )),
      ];
      assert.equal(denied[0]?.type, "request.resolved");
      assert.equal(denied[1]?.type, "item.completed");
      if (denied[1]?.type === "item.completed") {
        assert.equal(denied[1].payload.itemType, "command_execution");
        assert.equal(denied[1].payload.status, "declined");
      }

      sdkInput.onEvent({
        id: "event-tool-complete",
        parentId: "event-tool-start",
        timestamp: "2026-09-02T00:00:01.000Z",
        type: "tool.execution_complete",
        data: { toolCallId: "tool-denied", success: true },
      } as SessionEvent);
      sdkInput.onEvent({
        id: "event-tool-complete-replayed",
        parentId: "event-tool-complete",
        timestamp: "2026-09-02T00:00:02.000Z",
        type: "tool.execution_complete",
        data: { toolCallId: "tool-denied", success: true },
      } as SessionEvent);
      yield* Effect.promise(() => sdkInput.onPermissionRequest({ kind: "unknown" }));
      const next = Option.getOrThrow(
        yield* adapter.streamEvents.pipe(
          Stream.filter(
            (event) => event.type === "item.completed" || event.type === "runtime.error",
          ),
          Stream.runHead,
        ),
      );
      assert.equal(next.type, "runtime.error");
    }),
  );
});
