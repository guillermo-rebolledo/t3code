import type { CopilotSettings, ServerProviderAuth, ServerProviderModel } from "@t3tools/contracts";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as DateTime from "effect/DateTime";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { resolveCopilotExecutable } from "./CopilotExecutable.ts";
import { buildCopilotModels } from "./CopilotSdkModels.ts";
import { CopilotSdkRuntime, type CopilotSdkRuntimeError } from "./CopilotSdkRuntime.ts";
import {
  buildServerProvider,
  collectStreamAsString,
  isCommandMissingCause,
  type CommandResult,
  type ServerProviderDraft,
} from "./providerSnapshot.ts";

const PRESENTATION = {
  displayName: "GitHub Copilot",
  showInteractionModeToggle: true,
  supportsThreadExecution: true,
} as const;
const VERSION_TIMEOUT = "8 seconds";
const SDK_TIMEOUT = "10 seconds";
const POLICY_FAILURE_PATTERN = /policy|entitlement|forbidden|organization|403/iu;

export type CopilotVersionResult =
  | { readonly kind: "ready"; readonly version: string }
  | { readonly kind: "missing" | "failed" | "unrecognized"; readonly message: string };

export function parseCopilotVersionOutput(result: CommandResult): CopilotVersionResult {
  const output = `${result.stdout}\n${result.stderr}`;
  const lower = output.toLowerCase();
  if (result.code !== 0) {
    if (lower.includes("not found") || lower.includes("enoent")) {
      return {
        kind: "missing",
        message:
          "GitHub Copilot CLI is not installed or not on PATH. Install it, then run `copilot auth login`.",
      };
    }
    return {
      kind: "failed",
      message: `GitHub Copilot CLI health check failed with exit code ${result.code}.`,
    };
  }
  const version = output.match(/GitHub Copilot CLI\s+(\d+\.\d+\.\d+)/i)?.[1];
  return version
    ? { kind: "ready", version }
    : {
        kind: "unrecognized",
        message:
          "The configured executable did not return recognizable GitHub Copilot CLI version output.",
      };
}

const versionCommand = Effect.fn("versionCommand")(function* (
  binaryPath: string,
  environment: NodeJS.ProcessEnv,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const platform = yield* HostProcessPlatform;
  const resolved = resolveCopilotExecutable(binaryPath, environment, { platform });
  const child = yield* spawner.spawn(
    ChildProcess.make(resolved, ["version"], {
      env: environment,
      shell: platform === "win32",
    }),
  );
  const [stdout, stderr, code] = yield* Effect.all(
    [
      collectStreamAsString(child.stdout),
      collectStreamAsString(child.stderr),
      child.exitCode.pipe(Effect.map(Number)),
    ],
    { concurrency: "unbounded" },
  );
  return { stdout, stderr, code } satisfies CommandResult;
}, Effect.scoped);

function authDescriptor(auth: {
  readonly isAuthenticated: boolean;
  readonly authType?: string;
  readonly login?: string;
}): ServerProviderAuth {
  return auth.isAuthenticated
    ? {
        status: "authenticated",
        ...(auth.authType ? { type: auth.authType } : {}),
        ...(auth.login ? { label: auth.login } : {}),
      }
    : { status: "unauthenticated" };
}

function failureMessage(error: CopilotSdkRuntimeError): {
  readonly status: "warning" | "error";
  readonly auth: ServerProviderAuth;
  readonly message: string;
} {
  const policyFailure = POLICY_FAILURE_PATTERN.test(error.detail);
  if (policyFailure) {
    return {
      status: "error",
      auth: { status: "unauthenticated" },
      message: `GitHub Copilot access was rejected by organization policy or account entitlement: ${error.detail}`,
    };
  }
  return {
    status: "error",
    auth: { status: "unknown" },
    message:
      error.kind === "timeout"
        ? "GitHub Copilot SDK timed out while checking authentication and models."
        : `GitHub Copilot SDK failed while checking the account: ${error.detail}`,
  };
}

function snapshot(input: {
  readonly settings: CopilotSettings;
  readonly checkedAt: string;
  readonly installed: boolean;
  readonly version: string | null;
  readonly status: "ready" | "warning" | "error";
  readonly auth: ServerProviderAuth;
  readonly models: ReadonlyArray<ServerProviderModel>;
  readonly message?: string;
}): ServerProviderDraft {
  return buildServerProvider({
    presentation: PRESENTATION,
    enabled: input.settings.enabled,
    checkedAt: input.checkedAt,
    models: input.models,
    probe: {
      installed: input.installed,
      version: input.version,
      status: input.status,
      auth: input.auth,
      ...(input.message ? { message: input.message } : {}),
    },
  });
}

export const buildInitialCopilotProviderSnapshot = (
  settings: CopilotSettings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.map(DateTime.now, (now) =>
    snapshot({
      settings,
      checkedAt: DateTime.formatIso(now),
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      models: [],
      message: settings.enabled
        ? "Checking GitHub Copilot CLI availability..."
        : "GitHub Copilot is disabled in T3 Code settings.",
    }),
  );

export const checkCopilotProviderStatus = Effect.fn("checkCopilotProviderStatus")(function* (
  settings: CopilotSettings,
  environment: NodeJS.ProcessEnv = process.env,
  options: {
    readonly lastModels?: Ref.Ref<ReadonlyArray<ServerProviderModel>>;
    readonly sdkTimeout?: Duration.Input;
  } = {},
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const previousModels = options.lastModels ? yield* Ref.get(options.lastModels) : [];
  if (!settings.enabled) {
    return snapshot({
      settings,
      checkedAt,
      installed: false,
      version: null,
      status: "warning",
      auth: { status: "unknown" },
      models: previousModels,
      message: "GitHub Copilot is disabled in T3 Code settings.",
    });
  }

  const versionResult = yield* versionCommand(settings.binaryPath, environment).pipe(
    Effect.timeoutOption(VERSION_TIMEOUT),
    Effect.result,
  );
  if (Result.isFailure(versionResult)) {
    const missing = isCommandMissingCause(versionResult.failure);
    return snapshot({
      settings,
      checkedAt,
      installed: !missing,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      models: previousModels,
      message: missing
        ? "GitHub Copilot CLI is not installed or not on PATH. Install it, then run `copilot auth login`."
        : "GitHub Copilot CLI failed before its version could be checked.",
    });
  }
  if (Option.isNone(versionResult.success)) {
    return snapshot({
      settings,
      checkedAt,
      installed: true,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      models: previousModels,
      message: "GitHub Copilot CLI timed out while reporting its version.",
    });
  }

  const parsed = parseCopilotVersionOutput(versionResult.success.value);
  if (parsed.kind !== "ready") {
    return snapshot({
      settings,
      checkedAt,
      installed: parsed.kind !== "missing",
      version: null,
      status: parsed.kind === "unrecognized" ? "warning" : "error",
      auth: { status: "unknown" },
      models: previousModels,
      message: parsed.message,
    });
  }

  const runtime = yield* CopilotSdkRuntime;
  const platform = yield* HostProcessPlatform;
  const accountResult = yield* runtime
    .connect({
      binaryPath: settings.binaryPath,
      environment,
      platform,
      startTimeoutMs: 8_000,
    })
    .pipe(
      Effect.flatMap((client) =>
        client.authStatus.pipe(
          Effect.flatMap((auth) =>
            auth.isAuthenticated
              ? client.models.pipe(Effect.map((models) => ({ auth, models })))
              : Effect.succeed({ auth, models: [] }),
          ),
        ),
      ),
      Effect.scoped,
      Effect.timeoutOption(options.sdkTimeout ?? SDK_TIMEOUT),
      Effect.result,
    );

  if (Result.isFailure(accountResult)) {
    const failure = failureMessage(accountResult.failure);
    return snapshot({
      settings,
      checkedAt,
      installed: true,
      version: parsed.version,
      status:
        previousModels.length > 0 && failure.status === "error" && failure.auth.status === "unknown"
          ? "warning"
          : failure.status,
      auth: failure.auth,
      models: previousModels,
      message:
        previousModels.length > 0 && failure.auth.status === "unknown"
          ? `${failure.message} Using the last known models.`
          : failure.message,
    });
  }
  if (Option.isNone(accountResult.success)) {
    return snapshot({
      settings,
      checkedAt,
      installed: true,
      version: parsed.version,
      status: previousModels.length > 0 ? "warning" : "error",
      auth: { status: "unknown" },
      models: previousModels,
      message:
        previousModels.length > 0
          ? "GitHub Copilot SDK timed out; using the last known models."
          : "GitHub Copilot SDK timed out while checking authentication and models.",
    });
  }

  const account = accountResult.success.value;
  const auth = authDescriptor(account.auth);
  if (!account.auth.isAuthenticated) {
    const policyFailure = POLICY_FAILURE_PATTERN.test(account.auth.statusMessage ?? "");
    return snapshot({
      settings,
      checkedAt,
      installed: true,
      version: parsed.version,
      status: "error",
      auth,
      models: previousModels,
      message: policyFailure
        ? `GitHub Copilot access was rejected by organization policy or account entitlement: ${account.auth.statusMessage}`
        : "GitHub Copilot is not authenticated. Run `copilot auth login` on this environment.",
    });
  }

  const models = buildCopilotModels(account.models);
  if (options.lastModels) yield* Ref.set(options.lastModels, models);
  return snapshot({
    settings,
    checkedAt,
    installed: true,
    version: parsed.version,
    status: models.length > 0 ? "ready" : "warning",
    auth,
    models,
    ...(models.length === 0
      ? { message: "GitHub Copilot returned no models allowed by this account." }
      : {}),
  });
});
