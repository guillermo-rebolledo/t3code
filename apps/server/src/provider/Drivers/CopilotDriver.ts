import {
  CopilotSettings,
  ProviderDriverKind,
  type ServerProviderModel,
  TextGenerationError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { CopilotSdkRuntime } from "../CopilotSdkRuntime.ts";
import { ProviderDriverError } from "../Errors.ts";
import {
  buildInitialCopilotProviderSnapshot,
  checkCopilotProviderStatus,
} from "../CopilotProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { makeProviderMaintenanceCapabilities } from "../providerMaintenance.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import { makeCopilotAdapter } from "../Layers/CopilotAdapter.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
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
      const platform = yield* HostProcessPlatform;
      const serverSettings = yield* ServerSettingsService;
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
      const adapter = yield* makeCopilotAdapter(
        () =>
          runtime.connect({
            binaryPath: effectiveConfig.binaryPath,
            environment: processEnv,
            platform,
          }),
        { instanceId },
      );
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

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration: discoveryOnlyTextGeneration(),
      } satisfies ProviderInstance;
    }),
};
