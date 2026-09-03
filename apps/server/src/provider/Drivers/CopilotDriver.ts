import {
  CopilotSettings,
  ProviderDriverKind,
  type ServerProviderModel,
  TextGenerationError,
} from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { makeCopilotTextGeneration } from "../../textGeneration/CopilotTextGeneration.ts";
import type { CopilotSdkConnection } from "../CopilotSdkRuntime.ts";
import { CopilotSdkRuntime } from "../CopilotSdkRuntime.ts";
import { discoverCopilotWorkspaceCapabilities } from "../CopilotWorkspace.ts";
import { ProviderAdapterRequestError, ProviderDriverError } from "../Errors.ts";
import {
  buildInitialCopilotProviderSnapshot,
  checkCopilotProviderStatus,
} from "../CopilotProvider.ts";
import { makeCopilotAdapter } from "../Layers/CopilotAdapter.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { makeProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
  type ProviderSnapshotSettings,
} from "../providerUpdateSettings.ts";
import { withInstanceIdentity } from "./instanceIdentity.ts";

const DRIVER_KIND = ProviderDriverKind.make("copilot");
const decodeSettings = Schema.decodeSync(CopilotSettings);
const maintenanceCapabilities = makeProviderMaintenanceCapabilities({
  provider: DRIVER_KIND,
  packageName: null,
  updateExecutable: "copilot",
  updateArgs: ["update"],
  updateLockKey: "copilot-update",
});

function discoveryOnlyAdapter(): ProviderAdapterShape<ProviderAdapterRequestError> {
  const unavailable = (method: string) =>
    Effect.fail(
      new ProviderAdapterRequestError({
        provider: DRIVER_KIND,
        method,
        detail: "Copilot thread execution is not available in this discovery-only provider slice.",
      }),
    );
  return {
    provider: DRIVER_KIND,
    capabilities: { sessionModelSwitch: "unsupported" },
    startSession: () => unavailable("startSession"),
    sendTurn: () => unavailable("sendTurn"),
    interruptTurn: () => unavailable("interruptTurn"),
    respondToRequest: () => unavailable("respondToRequest"),
    respondToUserInput: () => unavailable("respondToUserInput"),
    stopSession: () => unavailable("stopSession"),
    listSessions: () => Effect.succeed([]),
    hasSession: () => Effect.succeed(false),
    readThread: () => unavailable("readThread"),
    rollbackThread: () => unavailable("rollbackThread"),
    stopAll: () => Effect.void,
    streamEvents: Stream.empty,
  };
}

function discoveryOnlyTextGeneration(): TextGeneration.TextGeneration["Service"] {
  const unavailable = (operation: string) =>
    Effect.fail(
      new TextGenerationError({
        operation,
        detail: "Copilot text generation is not available in this discovery-only provider slice.",
      }),
    );
  return {
    generateCommitMessage: () => unavailable("generateCommitMessage"),
    generatePrContent: () => unavailable("generatePrContent"),
    generateBranchName: () => unavailable("generateBranchName"),
    generateThreadTitle: () => unavailable("generateThreadTitle"),
  };
}

export type CopilotDriverEnv =
  | BackgroundPolicy.BackgroundPolicy
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | ProviderEventLoggers
  | ServerConfig
  | ServerSettingsService;

export const CopilotDriver: ProviderDriver<CopilotSettings, CopilotDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "GitHub Copilot",
    supportsMultipleInstances: true,
  },
  configSchema: CopilotSettings,
  defaultConfig: () => decodeSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const runtime = yield* CopilotSdkRuntime;
      const serverSettings = yield* ServerSettingsService;
      const eventLoggers = yield* ProviderEventLoggers;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        driverKind: DRIVER_KIND,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies CopilotSettings;
      // The instance's one SDK connection serves its adapter, auxiliary text
      // generation, and per-workspace capability reads, so none of those paths
      // starts a second Copilot process behind the running one.
      const connection: CopilotSdkConnection | undefined = enabled
        ? yield* runtime
            .connect({
              binaryPath: effectiveConfig.binaryPath,
              environment: processEnv,
              platform: yield* HostProcessPlatform,
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderDriverError({
                    driver: DRIVER_KIND,
                    instanceId,
                    detail: "Failed to connect the GitHub Copilot SDK runtime.",
                    cause,
                  }),
              ),
            )
        : undefined;
      const adapter = connection
        ? yield* makeCopilotAdapter(connection, {
            instanceId,
            ...(eventLoggers.native ? { nativeEventLogger: eventLoggers.native } : {}),
          })
        : discoveryOnlyAdapter();
      const textGeneration = connection
        ? makeCopilotTextGeneration(connection)
        : discoveryOnlyTextGeneration();
      const lastModels = yield* Ref.make<ReadonlyArray<ServerProviderModel>>([]);
      const checkProvider = checkCopilotProviderStatus(effectiveConfig, processEnv, {
        lastModels,
      }).pipe(
        Effect.map(stampIdentity),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provideService(CopilotSdkRuntime, runtime),
      );
      const snapshotSettings = makeProviderSnapshotSettingsSource(effectiveConfig, serverSettings);
      const snapshot = yield* makeManagedServerProvider<ProviderSnapshotSettings<CopilotSettings>>({
        maintenanceCapabilities,
        getSettings: snapshotSettings.getSettings,
        streamSettings: snapshotSettings.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: (settings) =>
          buildInitialCopilotProviderSnapshot(settings.provider).pipe(Effect.map(stampIdentity)),
        checkProvider,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: "Failed to build the GitHub Copilot provider snapshot.",
              cause,
            }),
        ),
      );

      // Workspace capabilities stay out of the instance-wide snapshot: they
      // describe one directory, and the registry keys them by `cwd`.
      const snapshotForCwd = (cwd: string) =>
        connection
          ? Effect.all([
              snapshot.getSnapshot,
              discoverCopilotWorkspaceCapabilities(connection, cwd),
            ]).pipe(
              Effect.map(([machineSnapshot, capabilities]) => ({
                ...machineSnapshot,
                slashCommands: capabilities.slashCommands,
                skills: capabilities.skills,
              })),
            )
          : snapshot.getSnapshot;

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        snapshotForCwd,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
