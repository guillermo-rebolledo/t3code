import {
  CopilotClient,
  RuntimeConnection,
  type GetAuthStatusResponse,
  type ModelInfo,
} from "@github/copilot-sdk";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import { resolveCopilotExecutable } from "./CopilotExecutable.ts";

const DEFAULT_START_TIMEOUT_MS = 8_000;
const STOP_TIMEOUT_MS = 2_000;

export class CopilotSdkRuntimeError extends Schema.TaggedErrorClass<CopilotSdkRuntimeError>()(
  "CopilotSdkRuntimeError",
  {
    operation: Schema.String,
    kind: Schema.Literals(["timeout", "failure"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return `GitHub Copilot SDK ${this.operation} ${this.kind}: ${this.detail}`;
  }
}
const isCopilotSdkRuntimeError = Schema.is(CopilotSdkRuntimeError);

function detailFromCause(cause: unknown): string {
  return cause instanceof Error && cause.message.trim() ? cause.message.trim() : String(cause);
}

function sdkEffect<A>(operation: string, run: () => Promise<A>) {
  return Effect.tryPromise({
    try: run,
    catch: (cause) =>
      new CopilotSdkRuntimeError({
        operation,
        kind: "failure",
        detail: detailFromCause(cause),
        cause,
      }),
  });
}

function stringEnvironment(environment: NodeJS.ProcessEnv): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [name, value] of Object.entries(environment)) {
    if (typeof value === "string") result[name] = value;
  }
  return result;
}

function promiseWithTimeout<A>(
  operation: string,
  promise: Promise<A>,
  timeoutMs: number,
): Promise<A> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    // This is deliberately a JS-level timeout: Effect acquisition is
    // uninterruptible, while the SDK's start handshake is a plain promise.
    // @effect-diagnostics-next-line globalTimers:off
    timer = setTimeout(() => {
      reject(
        new CopilotSdkRuntimeError({
          operation,
          kind: "timeout",
          detail: `Timed out after ${timeoutMs}ms.`,
        }),
      );
    }, timeoutMs);
    timer.unref?.();
  });
  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

async function stopClient(client: CopilotClient): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timedOut = Symbol("timed-out");
  const timeout = new Promise<typeof timedOut>((resolve) => {
    // @effect-diagnostics-next-line globalTimers:off
    timer = setTimeout(() => resolve(timedOut), STOP_TIMEOUT_MS);
    timer.unref?.();
  });
  try {
    const result = await Promise.race([
      client.stop().then(
        () => undefined,
        () => undefined,
      ),
      timeout,
    ]);
    if (result === timedOut) await client.forceStop().catch(() => undefined);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export interface CopilotSdkConnection {
  readonly authStatus: Effect.Effect<GetAuthStatusResponse, CopilotSdkRuntimeError>;
  readonly models: Effect.Effect<ReadonlyArray<ModelInfo>, CopilotSdkRuntimeError>;
}

export interface CopilotSdkRuntimeShape {
  /**
   * The single injection seam for Copilot SDK behavior. Tests replace this
   * service to control acquisition, cleanup, inventories, failures, and hangs
   * without starting a process or contacting GitHub.
   */
  readonly connect: (input: {
    readonly binaryPath: string;
    readonly environment: NodeJS.ProcessEnv;
    readonly platform: NodeJS.Platform;
    readonly startTimeoutMs?: number;
  }) => Effect.Effect<CopilotSdkConnection, CopilotSdkRuntimeError, Scope.Scope>;
}

const makeCopilotSdkRuntime: CopilotSdkRuntimeShape = {
  connect: Effect.fn("CopilotSdkRuntime.connect")(function* (input) {
    const acquire = Effect.tryPromise({
      try: async () => {
        const path = resolveCopilotExecutable(input.binaryPath, input.environment, {
          platform: input.platform,
        });
        const client = new CopilotClient({
          connection: RuntimeConnection.forStdio({
            path,
            env: stringEnvironment(input.environment),
          }),
        });
        try {
          await promiseWithTimeout(
            "start",
            client.start(),
            input.startTimeoutMs ?? DEFAULT_START_TIMEOUT_MS,
          );
          return client;
        } catch (cause) {
          await stopClient(client);
          throw cause;
        }
      },
      catch: (cause) =>
        isCopilotSdkRuntimeError(cause)
          ? cause
          : new CopilotSdkRuntimeError({
              operation: "start",
              kind: "failure",
              detail: detailFromCause(cause),
              cause,
            }),
    });

    const client = yield* Effect.acquireRelease(acquire, (client) =>
      Effect.promise(() => stopClient(client)).pipe(Effect.asVoid),
    );
    return {
      authStatus: sdkEffect("getAuthStatus", () => client.getAuthStatus()),
      models: sdkEffect("listModels", () => client.listModels()),
    };
  }),
};

export const CopilotSdkRuntime = Context.Reference<CopilotSdkRuntimeShape>(
  "t3/provider/CopilotSdkRuntime",
  { defaultValue: () => makeCopilotSdkRuntime },
);

export const CopilotSdkRuntimeLive = Layer.succeed(CopilotSdkRuntime, makeCopilotSdkRuntime);
