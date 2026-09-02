import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

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
      models: unused("models"),
      createSession: () => unused("createSession"),
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
  it.effect("scopes commands and skills to their working directory", () =>
    Effect.gen(function* () {
      const instance = yield* CopilotDriver.create({
        instanceId: ProviderInstanceId.make("copilot_work"),
        displayName: "Copilot Work",
        environment: [],
        enabled: true,
        // A path that cannot resolve keeps the background health probe from
        // reaching a real Copilot CLI; the snapshot under test is the initial
        // one either way.
        config: { enabled: true, binaryPath: "/nonexistent/copilot" },
      });

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
});
