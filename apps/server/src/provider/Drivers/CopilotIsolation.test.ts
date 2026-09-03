import type { ModelInfo, PermissionRequestResult, SessionEvent } from "@github/copilot-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  ProviderDriverKind,
  type ProviderInstanceConfig,
  type ProviderInstanceConfigMap,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import type {
  CopilotSdkConnection,
  CopilotSdkRuntimeShape,
  CopilotSdkSession,
  CopilotSdkSessionStartInput,
} from "../CopilotSdkRuntime.ts";
import { CopilotSdkRuntime, CopilotSdkRuntimeError } from "../CopilotSdkRuntime.ts";
import { makeProviderInstanceRegistry } from "../Layers/ProviderInstanceRegistryLive.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { CopilotDriver } from "./CopilotDriver.ts";

const COPILOT = ProviderDriverKind.make("copilot");
const TEST_EPOCH = DateTime.makeUnsafe("1970-01-01T00:00:00.000Z");
const encoder = new TextEncoder();

const versionSpawner = ChildProcessSpawner.make(() =>
  Effect.succeed(
    ChildProcessSpawner.makeHandle({
      pid: ChildProcessSpawner.ProcessId(1),
      exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
      isRunning: Effect.succeed(false),
      kill: () => Effect.void,
      unref: Effect.succeed(Effect.void),
      stdin: Sink.drain,
      stdout: Stream.make(encoder.encode("GitHub Copilot CLI 1.0.80\n")),
      stderr: Stream.empty,
      all: Stream.empty,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
    }),
  ),
);

const BackgroundPolicyAlwaysRunLayer = Layer.mock(BackgroundPolicy.BackgroundPolicy)({
  reportClientActivity: () => Effect.void,
  removeRpcClient: () => Effect.void,
  reportHostPowerState: () => Effect.void,
  snapshot: Effect.succeed({
    hostPower: {
      source: "unknown",
      idle: "unknown",
      idleSeconds: null,
      locked: "unknown",
      suspended: false,
      onBattery: "unknown",
      lowPowerMode: "unknown",
      thermalState: "unknown",
      stale: true,
      updatedAt: TEST_EPOCH,
    },
    leases: [],
    activeForegroundLeaseCount: 0,
    activeScopeKeys: [],
    shouldRunOpportunisticWork: true,
    updatedAt: TEST_EPOCH,
  }),
  streamChanges: Stream.empty,
  hasDemand: () => Effect.succeed(true),
  shouldRunScopeWork: () => Effect.succeed(true),
  shouldRunOpportunisticWork: Effect.succeed(true),
});

interface TrackedSession {
  readonly id: string;
  readonly input: CopilotSdkSessionStartInput;
  disconnects: number;
}

interface TrackedConnection {
  readonly id: number;
  readonly binaryPath: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly sessions: Array<TrackedSession>;
  releases: number;
}

function makeTrackedRuntime() {
  const connections: Array<TrackedConnection> = [];
  const connectionRecords = new WeakMap<CopilotSdkConnection, TrackedConnection>();
  let nextConnectionId = 0;
  let nextSessionId = 0;

  const runtime: CopilotSdkRuntimeShape = {
    connect: (input) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          const tracked: TrackedConnection = {
            id: (nextConnectionId += 1),
            binaryPath: input.binaryPath,
            environment: input.environment,
            sessions: [],
            releases: 0,
          };
          connections.push(tracked);
          const modelId = input.binaryPath.includes("work") ? "work-model" : "personal-model";
          const models = Effect.succeed([
            {
              id: modelId,
              name: modelId,
              capabilities: { supports: {} },
            } as ModelInfo,
          ]);
          const createSession = (sessionInput: CopilotSdkSessionStartInput) =>
            Effect.sync(() => {
              const session: TrackedSession = {
                id: `${tracked.id}:${(nextSessionId += 1)}`,
                input: sessionInput,
                disconnects: 0,
              };
              tracked.sessions.push(session);
              const sdk: CopilotSdkSession = {
                sessionId: session.id,
                send: () => Effect.succeed(`message-${session.id}`),
                abort: Effect.void,
                setModel: () => Effect.void,
                listCommands: Effect.succeed([]),
                invokeCommand: () =>
                  Effect.fail(
                    new CopilotSdkRuntimeError({
                      operation: "invokeCommand",
                      kind: "failure",
                      detail: "not used by isolation tests",
                    }),
                  ),
                rewindPoints: Effect.succeed({ points: [] }),
                rewind: () => Effect.succeed({ outcome: "success", eventsRemoved: 0 }),
                disconnect: Effect.sync(() => {
                  session.disconnects += 1;
                }),
              };
              return sdk;
            });
          const connection: CopilotSdkConnection = {
            authStatus: Effect.succeed({ isAuthenticated: true }),
            models,
            createSession,
            resumeSession: (sessionInput) => createSession(sessionInput),
            workspaceCommands: () => Effect.succeed([]),
            workspaceSkills: () => Effect.succeed([]),
          };
          connectionRecords.set(connection, tracked);
          return connection;
        }),
        (connection) =>
          Effect.sync(() => {
            const tracked = connectionRecords.get(connection);
            if (tracked) tracked.releases += 1;
          }),
      ),
  };

  return { runtime, connections };
}

const baseLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-copilot-isolation-test-",
}).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(BackgroundPolicyAlwaysRunLayer),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
);

const testLayer = (runtime: CopilotSdkRuntimeShape) =>
  Layer.mergeAll(
    baseLayer,
    Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, versionSpawner),
    Layer.succeed(CopilotSdkRuntime, runtime),
  );

function connectionFor(
  connections: ReadonlyArray<TrackedConnection>,
  binaryPath: string,
): TrackedConnection {
  const connection = connections.find((candidate) => candidate.binaryPath === binaryPath);
  assert.isDefined(connection, `No tracked Copilot connection for '${binaryPath}'.`);
  return connection;
}

const entry = (binaryPath: string, account: string, enabled = true): ProviderInstanceConfig => ({
  driver: COPILOT,
  enabled,
  environment: [{ name: "COPILOT_ACCOUNT", value: account, sensitive: false }],
  config: { enabled, binaryPath },
});

const startInput = (threadId: ThreadId, instanceId: ProviderInstanceId, model: string) => ({
  provider: COPILOT,
  providerInstanceId: instanceId,
  threadId,
  cwd: process.cwd(),
  runtimeMode: "approval-required" as const,
  modelSelection: { instanceId, model },
});

let eventId = 0;
const event = <T extends SessionEvent>(value: Omit<T, "id" | "parentId" | "timestamp">): T =>
  ({
    id: `isolation-event-${(eventId += 1)}`,
    parentId: null,
    timestamp: "2026-01-01T00:00:00.000Z",
    ...value,
  }) as T;

const observeEvents = Effect.fn("observeCopilotIsolationEvents")(function* (
  stream: Stream.Stream<ProviderRuntimeEvent>,
) {
  const queue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  yield* Stream.runForEach(stream, (runtimeEvent) => Queue.offer(queue, runtimeEvent)).pipe(
    Effect.forkChild,
  );
  yield* Effect.yieldNow;
  const seen: Array<ProviderRuntimeEvent> = [];
  return {
    seen,
    until: Effect.fn("observeCopilotIsolationEvents.until")(function* (
      predicate: (runtimeEvent: ProviderRuntimeEvent) => boolean,
    ) {
      for (;;) {
        const runtimeEvent = yield* Queue.take(queue);
        seen.push(runtimeEvent);
        if (predicate(runtimeEvent)) return runtimeEvent;
      }
    }),
  };
});

it.effect(
  "isolates Copilot clients, models, threads, approvals, continuations, and event streams",
  () =>
    Effect.gen(function* () {
      const tracked = makeTrackedRuntime();
      yield* Effect.scoped(
        Effect.gen(function* () {
          const personalId = ProviderInstanceId.make("copilot_personal");
          const workId = ProviderInstanceId.make("copilot_work");
          const { registry } = yield* makeProviderInstanceRegistry({
            drivers: [CopilotDriver],
            configMap: {
              [personalId]: entry("/opt/personal/copilot", "personal"),
              [workId]: entry("/opt/work/copilot", "work"),
            },
          });
          const personal = yield* registry.getInstance(personalId);
          const work = yield* registry.getInstance(workId);
          assert.isDefined(personal);
          assert.isDefined(work);
          assert.notStrictEqual(personal!.adapter, work!.adapter);
          assert.notStrictEqual(personal!.snapshot, work!.snapshot);
          assert.notStrictEqual(personal!.textGeneration, work!.textGeneration);
          assert.equal(tracked.connections.length, 2);
          const personalConnection = connectionFor(tracked.connections, "/opt/personal/copilot");
          const workConnection = connectionFor(tracked.connections, "/opt/work/copilot");
          assert.equal(personalConnection.environment.COPILOT_ACCOUNT, "personal");
          assert.equal(workConnection.environment.COPILOT_ACCOUNT, "work");

          const personalSnapshot = yield* personal!.snapshot.getSnapshot;
          const workSnapshot = yield* work!.snapshot.getSnapshot;
          assert.equal(personalSnapshot.instanceId, personalId);
          assert.equal(workSnapshot.instanceId, workId);

          const personalEvents = yield* observeEvents(personal!.adapter.streamEvents);
          const workEvents = yield* observeEvents(work!.adapter.streamEvents);
          const personalThread = ThreadId.make("personal-thread");
          const personalSecondThread = ThreadId.make("personal-second-thread");
          const workThread = ThreadId.make("work-thread");
          const personalSession = yield* personal!.adapter.startSession(
            startInput(personalThread, personalId, "personal-model"),
          );
          yield* personal!.adapter.startSession(
            startInput(personalSecondThread, personalId, "personal-model"),
          );
          yield* work!.adapter.startSession(startInput(workThread, workId, "work-model"));

          assert.isTrue(yield* personal!.adapter.hasSession(personalThread));
          assert.isTrue(yield* personal!.adapter.hasSession(personalSecondThread));
          assert.isFalse(yield* personal!.adapter.hasSession(workThread));
          assert.isTrue(yield* work!.adapter.hasSession(workThread));
          assert.isFalse(yield* work!.adapter.hasSession(personalThread));

          const crossModel = yield* personal!.adapter
            .startSession(startInput(ThreadId.make("cross-model-thread"), personalId, "work-model"))
            .pipe(Effect.result);
          assert.equal(crossModel._tag, "Failure");
          const crossContinuation = yield* work!.adapter
            .startSession({
              ...startInput(ThreadId.make("cross-continuation-thread"), workId, "work-model"),
              resumeCursor: personalSession.resumeCursor,
            })
            .pipe(Effect.result);
          assert.equal(crossContinuation._tag, "Failure");

          const personalTurn = yield* personal!.adapter.sendTurn({
            threadId: personalThread,
            input: "personal work",
          });
          const workTurn = yield* work!.adapter.sendTurn({
            threadId: workThread,
            input: "work work",
          });
          const personalSdk = personalConnection.sessions[0]!.input;
          const workSdk = workConnection.sessions[0]!.input;
          personalSdk.onEvent(
            event({
              type: "assistant.message",
              data: { messageId: "personal-message", content: "personal output" },
            }),
          );
          personalSdk.onEvent(event({ type: "session.idle", data: {} }));
          workSdk.onEvent(
            event({
              type: "assistant.message",
              data: { messageId: "work-message", content: "work output" },
            }),
          );
          workSdk.onEvent(event({ type: "session.idle", data: {} }));
          yield* personalEvents.until((runtimeEvent) => runtimeEvent.type === "turn.completed");
          yield* workEvents.until((runtimeEvent) => runtimeEvent.type === "turn.completed");
          assert.isTrue(
            personalEvents.seen.every(
              (runtimeEvent) => runtimeEvent.providerInstanceId === personalId,
            ),
          );
          assert.isTrue(
            workEvents.seen.every((runtimeEvent) => runtimeEvent.providerInstanceId === workId),
          );
          assert.notEqual(personalTurn.turnId, workTurn.turnId);
          assert.deepEqual(
            (yield* personal!.adapter.readThread(personalThread)).turns[0]?.items.map(
              (item) => (item as SessionEvent).type,
            ),
            ["assistant.message", "session.idle"],
          );
          assert.deepEqual((yield* personal!.adapter.readThread(personalSecondThread)).turns, []);

          const personalPermission = yield* Effect.promise(() =>
            personalSdk.onPermissionRequest({
              kind: "shell",
              fullCommandText: "echo personal",
              intention: "Personal command",
              commands: [{ identifier: "echo", readOnly: true }],
              canOfferSessionApproval: true,
            }),
          ).pipe(Effect.forkChild);
          const workPermission = yield* Effect.promise(() =>
            workSdk.onPermissionRequest({
              kind: "shell",
              fullCommandText: "echo work",
              intention: "Work command",
              commands: [{ identifier: "echo", readOnly: true }],
              canOfferSessionApproval: true,
            }),
          ).pipe(Effect.forkChild);
          const personalRequest = yield* personalEvents.until(
            (runtimeEvent) => runtimeEvent.type === "request.opened",
          );
          const workRequest = yield* workEvents.until(
            (runtimeEvent) => runtimeEvent.type === "request.opened",
          );
          const crossApproval = yield* work!.adapter
            .respondToRequest(
              workThread,
              ApprovalRequestId.make(personalRequest.requestId!),
              "acceptForSession",
            )
            .pipe(Effect.result);
          assert.equal(crossApproval._tag, "Failure");
          yield* personal!.adapter.respondToRequest(
            personalThread,
            ApprovalRequestId.make(personalRequest.requestId!),
            "acceptForSession",
          );
          assert.deepEqual(yield* Fiber.join(personalPermission), {
            kind: "approve-for-session",
            approval: { kind: "commands", commandIdentifiers: ["echo"] },
          });
          assert.isUndefined(workPermission.pollUnsafe());
          yield* work!.adapter.respondToRequest(
            workThread,
            ApprovalRequestId.make(workRequest.requestId!),
            "decline",
          );
          assert.deepEqual(yield* Fiber.join(workPermission), {
            kind: "reject",
            feedback: "The user denied this request.",
          });
        }).pipe(Effect.provide(testLayer(tracked.runtime))),
      );
    }),
);

it.effect(
  "releases only replaced Copilot scopes and drains every scope across repeated cycles",
  () =>
    Effect.gen(function* () {
      const tracked = makeTrackedRuntime();
      let shutdownPermission: Promise<PermissionRequestResult> | undefined;

      yield* Effect.scoped(
        Effect.gen(function* () {
          const personalId = ProviderInstanceId.make("copilot_personal");
          const workId = ProviderInstanceId.make("copilot_work");
          const initial: ProviderInstanceConfigMap = {
            [personalId]: entry("/opt/personal/copilot", "personal"),
            [workId]: entry("/opt/work/copilot", "work"),
          };
          const { registry, mutator } = yield* makeProviderInstanceRegistry({
            drivers: [CopilotDriver],
            configMap: initial,
          });
          const originalPersonal = (yield* registry.getInstance(personalId))!;
          const originalWork = (yield* registry.getInstance(workId))!;
          yield* originalPersonal.adapter.startSession(
            startInput(ThreadId.make("original-personal"), personalId, "personal-model"),
          );
          yield* originalWork.adapter.startSession(
            startInput(ThreadId.make("original-work"), workId, "work-model"),
          );

          yield* mutator.reconcile({
            ...initial,
            [personalId]: entry("/opt/personal-v2/copilot", "personal-v2"),
          });
          const replacementPersonal = (yield* registry.getInstance(personalId))!;
          assert.notStrictEqual(replacementPersonal.adapter, originalPersonal.adapter);
          assert.strictEqual((yield* registry.getInstance(workId))!.adapter, originalWork.adapter);
          assert.isFalse(
            yield* originalPersonal.adapter.hasSession(ThreadId.make("original-personal")),
          );
          assert.isTrue(yield* originalWork.adapter.hasSession(ThreadId.make("original-work")));
          const originalPersonalConnection = connectionFor(
            tracked.connections,
            "/opt/personal/copilot",
          );
          const originalWorkConnection = connectionFor(tracked.connections, "/opt/work/copilot");
          assert.equal(originalPersonalConnection.releases, 1);
          assert.equal(originalPersonalConnection.sessions[0]?.disconnects, 1);
          assert.equal(originalWorkConnection.releases, 0);

          yield* replacementPersonal.adapter.startSession(
            startInput(ThreadId.make("replacement-personal"), personalId, "personal-model"),
          );
          yield* mutator.reconcile({
            [personalId]: entry("/opt/personal-v2/copilot", "personal-v2", false),
            [workId]: initial[workId]!,
          });
          const replacementConnection = connectionFor(
            tracked.connections,
            "/opt/personal-v2/copilot",
          );
          assert.equal(replacementConnection.releases, 1);
          assert.equal(replacementConnection.sessions[0]?.disconnects, 1);
          assert.isFalse((yield* registry.getInstance(personalId))!.enabled);
          assert.isTrue(yield* originalWork.adapter.hasSession(ThreadId.make("original-work")));

          yield* mutator.reconcile({});
          assert.equal(originalWorkConnection.releases, 1);
          assert.equal(originalWorkConnection.sessions[0]?.disconnects, 1);

          for (let cycle = 0; cycle < 3; cycle += 1) {
            const binaryPath = `/opt/cycle-${cycle}/copilot`;
            yield* mutator.reconcile({ [personalId]: entry(binaryPath, `cycle-${cycle}`) });
            const instance = (yield* registry.getInstance(personalId))!;
            const eventsFiber = yield* Stream.runCollect(instance.adapter.streamEvents).pipe(
              Effect.forkChild,
            );
            yield* Effect.yieldNow;
            const threadId = ThreadId.make(`cycle-thread-${cycle}`);
            yield* instance.adapter.startSession(
              startInput(threadId, personalId, "personal-model"),
            );
            const turn = yield* instance.adapter.sendTurn({ threadId, input: `cycle ${cycle}` });
            const connection = connectionFor(tracked.connections, binaryPath);
            const sdkInput = connection.sessions[0]!.input;
            sdkInput.onEvent(event({ type: "session.idle", data: {} }));
            yield* mutator.reconcile({});
            const events = yield* Fiber.join(eventsFiber);
            assert.equal(
              Array.from(events).filter(
                (runtimeEvent) =>
                  runtimeEvent.type === "turn.completed" && runtimeEvent.turnId === turn.turnId,
              ).length,
              1,
            );
            assert.equal(connection.releases, 1);
            assert.equal(connection.sessions[0]?.disconnects, 1);
          }

          yield* mutator.reconcile({
            [workId]: entry("/opt/work-shutdown/copilot", "work-shutdown"),
          });
          const shutdownInstance = (yield* registry.getInstance(workId))!;
          const shutdownThread = ThreadId.make("shutdown-thread");
          yield* shutdownInstance.adapter.startSession(
            startInput(shutdownThread, workId, "work-model"),
          );
          const requestOpened = yield* Queue.unbounded<void>();
          yield* Stream.runForEach(shutdownInstance.adapter.streamEvents, (runtimeEvent) =>
            runtimeEvent.type === "request.opened"
              ? Queue.offer(requestOpened, undefined)
              : Effect.void,
          ).pipe(Effect.forkChild);
          yield* Effect.yieldNow;
          shutdownPermission = connectionFor(
            tracked.connections,
            "/opt/work-shutdown/copilot",
          ).sessions[0]!.input.onPermissionRequest({
            kind: "shell",
            fullCommandText: "echo shutdown",
            intention: "Wait for shutdown",
            commands: [{ identifier: "echo", readOnly: true }],
            canOfferSessionApproval: true,
          });
          yield* Queue.take(requestOpened);
        }).pipe(Effect.provide(testLayer(tracked.runtime))),
      );

      assert.isDefined(shutdownPermission);
      assert.deepEqual(yield* Effect.promise(() => shutdownPermission!), {
        kind: "reject",
        feedback: "The user cancelled this request.",
      });
      assert.isTrue(tracked.connections.every((connection) => connection.releases === 1));
      assert.isTrue(
        tracked.connections.every((connection) =>
          connection.sessions.every((session) => session.disconnects === 1),
        ),
      );
    }),
);
