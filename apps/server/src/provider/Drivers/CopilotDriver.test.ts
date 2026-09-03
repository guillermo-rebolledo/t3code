import type { ModelInfo, SessionEvent } from "@github/copilot-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import type { CopilotCommandInfo } from "../CopilotCommands.ts";
import {
  CopilotSdkRuntime,
  CopilotSdkRuntimeError,
  type CopilotSdkRuntimeShape,
} from "../CopilotSdkRuntime.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { CopilotDriver } from "./CopilotDriver.ts";

const TEST_EPOCH = DateTime.makeUnsafe("1970-01-01T00:00:00.000Z");
const encoder = new TextEncoder();

const versionSpawner = (version: string) =>
  ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(1),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        unref: Effect.succeed(Effect.void),
        stdin: Sink.drain,
        stdout: Stream.make(encoder.encode(`GitHub Copilot CLI ${version}\n`)),
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

const workspaces: Record<
  string,
  { readonly commands: ReadonlyArray<CopilotCommandInfo>; readonly skills: ReadonlyArray<unknown> }
> = {
  "/repo/app": {
    commands: [{ name: "app-review" }],
    skills: [{ name: "app-deploy", enabled: true, path: "/repo/app/skill.md", source: "project" }],
  },
  "/repo/worktree": {
    commands: [{ name: "worktree-review" }],
    skills: [
      {
        name: "worktree-deploy",
        enabled: true,
        path: "/repo/worktree/skill.md",
        source: "project",
      },
    ],
  },
};

const unused = (operation: string) =>
  Effect.fail(
    new CopilotSdkRuntimeError({
      operation,
      kind: "failure",
      detail: "not used by the workspace snapshot test",
    }),
  );

const runtime: CopilotSdkRuntimeShape = {
  connect: () =>
    Effect.succeed({
      authStatus: unused("authStatus"),
      models: Effect.succeed([
        {
          id: "gpt-5.4",
          name: "GPT-5.4",
          capabilities: { supports: {} },
        } as ModelInfo,
      ]),
      createSession: (input) =>
        Effect.succeed({
          sessionId: "aux-session",
          send: () =>
            Effect.sync(() => {
              input.onEvent({
                id: "message-event",
                parentId: null,
                timestamp: "2026-01-01T00:00:00.000Z",
                type: "assistant.message",
                data: { messageId: "message-1", content: '{"title":"Copilot helper"}' },
              } as SessionEvent);
              input.onEvent({
                id: "idle-event",
                parentId: "message-event",
                timestamp: "2026-01-01T00:00:00.000Z",
                type: "session.idle",
                data: {},
              } as SessionEvent);
              return "message-1";
            }),
          abort: Effect.void,
          setModel: () => Effect.void,
          listCommands: Effect.succeed([]),
          invokeCommand: () => unused("invokeCommand"),
          rewindPoints: Effect.succeed({ points: [] }),
          rewind: () => unused("rewind"),
          disconnect: Effect.void,
        }),
      resumeSession: () => unused("resumeSession"),
      workspaceCommands: (cwd) => Effect.succeed(workspaces[cwd]?.commands ?? []),
      workspaceSkills: (cwd) => Effect.succeed(workspaces[cwd]?.skills ?? []),
    }),
};

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3code-copilot-driver-test-",
}).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(BackgroundPolicyAlwaysRunLayer),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
  Layer.provideMerge(Layer.succeed(CopilotSdkRuntime, runtime)),
);

it.layer(testLayer)("CopilotDriver workspace snapshots", (it) => {
  it.effect("rejects an incompatible CLI before starting the SDK runtime", () =>
    Effect.gen(function* () {
      const starts: number[] = [];
      const incompatibleRuntime: CopilotSdkRuntimeShape = {
        ...runtime,
        connect: (input) =>
          Effect.sync(() => starts.push(1)).pipe(Effect.andThen(runtime.connect(input))),
      };

      const error = yield* CopilotDriver.create({
        instanceId: ProviderInstanceId.make("copilot_old"),
        displayName: "Old Copilot",
        environment: [],
        enabled: true,
        config: { enabled: true, binaryPath: "copilot" },
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, versionSpawner("1.0.78")),
        Effect.provideService(CopilotSdkRuntime, incompatibleRuntime),
        Effect.flip,
      );

      assert.include(error.detail, "v1.0.78 is too old");
      assert.include(error.detail, "v1.0.79 or newer");
      assert.isEmpty(starts);
    }),
  );

  it.effect("scopes commands and skills to their working directory", () =>
    Effect.gen(function* () {
      const instance = yield* CopilotDriver.create({
        instanceId: ProviderInstanceId.make("copilot_work"),
        displayName: "Copilot Work",
        environment: [],
        enabled: true,
        config: { enabled: true, binaryPath: "copilot" },
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, versionSpawner("1.0.80")),
      );

      const machineSnapshot = yield* instance.snapshot.getSnapshot;
      const app = yield* instance.snapshotForCwd!("/repo/app");
      const worktree = yield* instance.snapshotForCwd!("/repo/worktree");

      assert.isEmpty(machineSnapshot.slashCommands);
      assert.isEmpty(machineSnapshot.skills);
      assert.deepEqual(
        app.slashCommands.map((command) => command.name),
        ["app-review"],
      );
      assert.deepEqual(
        app.skills.map((skill) => skill.name),
        ["app-deploy"],
      );
      assert.deepEqual(
        worktree.slashCommands.map((command) => command.name),
        ["worktree-review"],
      );
      assert.deepEqual(
        worktree.skills.map((skill) => skill.name),
        ["worktree-deploy"],
      );
    }).pipe(Effect.scoped),
  );

  it.effect("provides auxiliary text generation for enabled instances", () =>
    Effect.gen(function* () {
      const instance = yield* CopilotDriver.create({
        instanceId: ProviderInstanceId.make("copilot_work"),
        displayName: "Copilot Work",
        environment: [],
        enabled: true,
        config: { enabled: true, binaryPath: "copilot" },
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, versionSpawner("1.0.80")),
      );

      const generated = yield* instance.textGeneration.generateThreadTitle({
        cwd: "/repo/app",
        message: "Add Copilot text generation",
        modelSelection: {
          instanceId: ProviderInstanceId.make("copilot_work"),
          model: "gpt-5.4",
        },
      });

      assert.deepEqual(generated, { title: "Copilot helper" });
    }).pipe(Effect.scoped),
  );
});
