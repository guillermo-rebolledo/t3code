import type { GetAuthStatusResponse, ModelInfo } from "@github/copilot-sdk";
import type { ServerProviderModel } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  CopilotSdkRuntime,
  CopilotSdkRuntimeError,
  type CopilotSdkRuntimeShape,
} from "./CopilotSdkRuntime.ts";
import { checkCopilotProviderStatus, parseCopilotVersionOutput } from "./CopilotProvider.ts";

const encoder = new TextEncoder();
const settings = { enabled: true, binaryPath: "copilot" } as const;
const authenticated: GetAuthStatusResponse = {
  isAuthenticated: true,
  authType: "user",
  login: "octocat",
};
const inventory: ModelInfo[] = [
  {
    id: "gpt-5.4",
    name: "GPT-5.4",
    capabilities: {
      supports: { vision: true, reasoningEffort: false },
      limits: { max_context_window_tokens: 128_000 },
    },
  },
];

function versionSpawner(result = { stdout: "GitHub Copilot CLI 1.0.80\n", stderr: "", code: 0 }) {
  return Layer.succeed(
    ChildProcessSpawner.ChildProcessSpawner,
    ChildProcessSpawner.make(() =>
      Effect.succeed(
        ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(result.code)),
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          unref: Effect.succeed(Effect.void),
          stdin: Sink.drain,
          stdout: Stream.make(encoder.encode(result.stdout)),
          stderr: Stream.make(encoder.encode(result.stderr)),
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
        }),
      ),
    ),
  );
}

function runtimeLayer(input: {
  readonly auth?: Effect.Effect<GetAuthStatusResponse, CopilotSdkRuntimeError>;
  readonly models?: Effect.Effect<ReadonlyArray<ModelInfo>, CopilotSdkRuntimeError>;
  readonly starts?: number[];
  readonly stops?: number[];
  readonly connections?: Array<Parameters<CopilotSdkRuntimeShape["connect"]>[0]>;
}) {
  const runtime: CopilotSdkRuntimeShape = {
    connect: (connection) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          input.connections?.push(connection);
          input.starts?.push(1);
          return {
            authStatus: input.auth ?? Effect.succeed(authenticated),
            models: input.models ?? Effect.succeed(inventory),
            createSession: () =>
              Effect.fail(
                new CopilotSdkRuntimeError({
                  operation: "createSession",
                  kind: "failure",
                  detail: "not used by provider discovery tests",
                }),
              ),
          };
        }),
        () => Effect.sync(() => input.stops?.push(1)),
      ),
  };
  return Layer.succeed(CopilotSdkRuntime, runtime);
}

const provide = (runtime: ReturnType<typeof runtimeLayer>) =>
  Layer.mergeAll(runtime, versionSpawner(), NodeServices.layer);

describe("parseCopilotVersionOutput", () => {
  it("distinguishes missing, failed, and unrecognized executables", () => {
    expect(
      parseCopilotVersionOutput({ stdout: "", stderr: "command not found", code: 127 }).kind,
    ).toBe("missing");
    expect(parseCopilotVersionOutput({ stdout: "", stderr: "crashed", code: 2 }).kind).toBe(
      "failed",
    );
    expect(parseCopilotVersionOutput({ stdout: "another tool", stderr: "", code: 0 }).kind).toBe(
      "unrecognized",
    );
  });
});

describe("checkCopilotProviderStatus", () => {
  it.effect("recognizes keychain-backed authentication and cleans up the SDK client", () =>
    Effect.gen(function* () {
      const starts: number[] = [];
      const stops: number[] = [];
      const connections: Array<Parameters<CopilotSdkRuntimeShape["connect"]>[0]> = [];
      const snapshot = yield* checkCopilotProviderStatus(
        settings,
        { PATH: "/provider/bin" },
        {},
      ).pipe(Effect.provide(provide(runtimeLayer({ starts, stops, connections }))));

      expect(snapshot.status).toBe("ready");
      expect(snapshot.supportsThreadExecution).toBe(true);
      expect(snapshot.auth).toMatchObject({ status: "authenticated", label: "octocat" });
      expect(snapshot.models.map((model) => model.slug)).toEqual(["gpt-5.4"]);
      expect(starts).toHaveLength(1);
      expect(stops).toHaveLength(1);
      expect(connections[0]?.environment.PATH).toBe("/provider/bin");
    }),
  );

  it.effect("reports authentication and organization-policy failures actionably", () =>
    Effect.gen(function* () {
      const loggedOut = yield* checkCopilotProviderStatus(settings, {}, {}).pipe(
        Effect.provide(
          provide(
            runtimeLayer({
              auth: Effect.succeed({ isAuthenticated: false, statusMessage: "Sign in" }),
            }),
          ),
        ),
      );
      expect(loggedOut.status).toBe("error");
      expect(loggedOut.auth.status).toBe("unauthenticated");
      expect(loggedOut.message).toContain("copilot auth login");

      const policyDenied = yield* checkCopilotProviderStatus(settings, {}, {}).pipe(
        Effect.provide(
          provide(
            runtimeLayer({
              auth: Effect.succeed({
                isAuthenticated: false,
                statusMessage: "Blocked by organization policy",
              }),
            }),
          ),
        ),
      );
      expect(policyDenied.status).toBe("error");
      expect(policyDenied.message).toContain("organization policy");

      const policyFailure = yield* checkCopilotProviderStatus(settings, {}, {}).pipe(
        Effect.provide(
          provide(
            runtimeLayer({
              models: Effect.fail(
                new CopilotSdkRuntimeError({
                  operation: "listModels",
                  kind: "failure",
                  detail: "Organization policy forbids Copilot access",
                }),
              ),
            }),
          ),
        ),
      );
      expect(policyFailure.status).toBe("error");
      expect(policyFailure.message).toContain("organization policy");
    }),
  );

  it.effect("retains the last successful catalog after a transient SDK failure", () =>
    Effect.gen(function* () {
      const lastModels = yield* Ref.make<ReadonlyArray<ServerProviderModel>>([]);
      const first = yield* checkCopilotProviderStatus(settings, {}, { lastModels }).pipe(
        Effect.provide(provide(runtimeLayer({}))),
      );
      expect(first.models.map((model) => model.slug)).toEqual(["gpt-5.4"]);

      const second = yield* checkCopilotProviderStatus(settings, {}, { lastModels }).pipe(
        Effect.provide(
          provide(
            runtimeLayer({
              models: Effect.fail(
                new CopilotSdkRuntimeError({
                  operation: "listModels",
                  kind: "failure",
                  detail: "temporary network failure",
                }),
              ),
            }),
          ),
        ),
      );
      expect(second.status).toBe("warning");
      expect(second.models.map((model) => model.slug)).toEqual(["gpt-5.4"]);
      expect(second.message).toContain("last known models");
    }),
  );

  it.effect("times out SDK discovery without fabricating a model", () =>
    Effect.gen(function* () {
      const discoveryStarted = yield* Deferred.make<void>();
      const fiber = yield* checkCopilotProviderStatus(
        settings,
        {},
        { sdkTimeout: "1 second" },
      ).pipe(
        Effect.provide(
          provide(
            runtimeLayer({
              models: Deferred.succeed(discoveryStarted, undefined).pipe(
                Effect.andThen(Effect.never),
              ),
            }),
          ),
        ),
        Effect.forkChild,
      );
      yield* Deferred.await(discoveryStarted);
      yield* TestClock.adjust("1 second");
      const snapshot = yield* Fiber.join(fiber);
      expect(snapshot.status).toBe("error");
      expect(snapshot.message).toContain("timed out");
      expect(snapshot.models).toEqual([]);
    }),
  );
});
