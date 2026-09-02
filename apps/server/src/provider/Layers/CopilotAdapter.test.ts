import type { SessionEvent } from "@github/copilot-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
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
