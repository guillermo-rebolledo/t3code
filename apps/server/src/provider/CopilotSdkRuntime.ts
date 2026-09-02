import {
  CopilotClient,
  RuntimeConnection,
  type GetAuthStatusResponse,
  type MessageOptions,
  type ModelInfo,
  type PermissionRequest,
  type PermissionRequestResult,
  type ResumeSessionConfig,
  type SessionConfig,
  type SessionConfigBase,
  type SessionEvent,
} from "@github/copilot-sdk";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

import type {
  CopilotCommandInfo,
  CopilotCommandInvocationResult,
  CopilotSlashCommandInvocation,
} from "./CopilotCommands.ts";
import { resolveCopilotExecutable } from "./CopilotExecutable.ts";

/** The SDK does not re-export these unions from its entrypoint. */
type ReasoningEffort = NonNullable<SessionConfigBase["reasoningEffort"]>;
type ContextTier = NonNullable<SessionConfigBase["contextTier"]>;

const DEFAULT_START_TIMEOUT_MS = 8_000;
/**
 * Commands an SDK client registered are never listed: T3 registers none, and a
 * command it cannot run has no business in the composer.
 *
 * The two reads differ on skills on purpose. Copilot repeats every
 * user-invocable skill as a command, so publishing them would send the skill
 * inventory twice in one snapshot - measured at 142 commands against 32 - for
 * rows the client already drops in favour of the skill entry. Routing still
 * needs them: `/deploy`, and the `$deploy` the composer rewrites into it, are
 * only invocable if the live session knows the name.
 */
const PUBLISHED_COMMAND_FILTERS = {
  includeBuiltins: true,
  includeSkills: false,
  includeClientCommands: false,
} as const;
const ROUTABLE_COMMAND_FILTERS = {
  includeBuiltins: true,
  includeSkills: true,
  includeClientCommands: false,
} as const;
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
  readonly createSession: (
    input: CopilotSdkSessionStartInput,
  ) => Effect.Effect<CopilotSdkSession, CopilotSdkRuntimeError>;
  /**
   * Re-attaches to a remote session the runtime still holds on disk. Fails
   * when the id is unknown, so a stale cursor can never present itself as a
   * successful resume.
   */
  readonly resumeSession: (
    input: CopilotSdkSessionResumeInput,
  ) => Effect.Effect<CopilotSdkSession, CopilotSdkRuntimeError>;
  /**
   * The slash commands Copilot advertises for one working directory. A
   * throwaway session is what scopes the answer: the runtime resolves a
   * project's own commands from the directory the session opened in.
   */
  readonly workspaceCommands: (
    cwd: string,
  ) => Effect.Effect<ReadonlyArray<CopilotCommandInfo>, CopilotSdkRuntimeError>;
  /**
   * The skill inventory Copilot discovers for one project directory, returned
   * unparsed so malformed entries are rejected by T3's own mapping.
   */
  readonly workspaceSkills: (
    cwd: string,
  ) => Effect.Effect<ReadonlyArray<unknown>, CopilotSdkRuntimeError>;
}

/** Model and per-model settings the runtime accepts at start, resume, and switch. */
export interface CopilotSdkModelOptions {
  readonly model: string;
  readonly reasoningEffort?: ReasoningEffort;
  readonly contextTier?: ContextTier;
}

export interface CopilotSdkSessionStartInput {
  readonly workingDirectory: string;
  readonly modelOptions?: CopilotSdkModelOptions;
  readonly onEvent: (event: SessionEvent) => void;
  readonly onPermissionRequest: (request: unknown) => Promise<PermissionRequestResult>;
}

export interface CopilotSdkSessionResumeInput extends CopilotSdkSessionStartInput {
  readonly sessionId: string;
}

/** One user turn the runtime can rewind to, oldest first. */
export interface CopilotSdkRewindPoint {
  readonly eventId: string;
  readonly userMessage: string;
  readonly timestamp: string;
}

export interface CopilotSdkRewindPoints {
  readonly points: ReadonlyArray<CopilotSdkRewindPoint>;
  /** Set when the runtime cannot answer the read at all (busy, remote session). */
  readonly unavailableReason?: string;
}

export interface CopilotSdkRewindResult {
  readonly outcome: string;
  readonly eventsRemoved?: number;
  readonly error?: string;
}

export interface CopilotSdkSession {
  readonly sessionId: string;
  readonly send: (input: MessageOptions) => Effect.Effect<string, CopilotSdkRuntimeError>;
  /** Aborts the message the session is currently processing. The session stays usable. */
  readonly abort: Effect.Effect<void, CopilotSdkRuntimeError>;
  /** Applies a model change to the live session; it takes effect on the next message. */
  readonly setModel: (
    options: CopilotSdkModelOptions,
  ) => Effect.Effect<void, CopilotSdkRuntimeError>;
  /** The commands this session can run, used to route the user's slash text. */
  readonly listCommands: Effect.Effect<ReadonlyArray<CopilotCommandInfo>, CopilotSdkRuntimeError>;
  readonly invokeCommand: (
    input: CopilotSlashCommandInvocation,
  ) => Effect.Effect<CopilotCommandInvocationResult, CopilotSdkRuntimeError>;
  readonly rewindPoints: Effect.Effect<CopilotSdkRewindPoints, CopilotSdkRuntimeError>;
  readonly rewind: (
    eventId: string,
  ) => Effect.Effect<CopilotSdkRewindResult, CopilotSdkRuntimeError>;
  readonly disconnect: Effect.Effect<void, CopilotSdkRuntimeError>;
}

function wrapSession(
  session: Awaited<ReturnType<CopilotClient["createSession"]>>,
): CopilotSdkSession {
  return {
    sessionId: session.sessionId,
    send: (input) => sdkEffect("send", () => session.send(input)),
    abort: sdkEffect("abort", () => session.abort()).pipe(Effect.asVoid),
    setModel: (options) =>
      sdkEffect("setModel", () => session.setModel(options.model, modelSettings(options))),
    listCommands: sdkEffect("listCommands", () =>
      session.rpc.commands.list(ROUTABLE_COMMAND_FILTERS),
    ).pipe(Effect.map((result) => result.commands)),
    invokeCommand: (input) =>
      sdkEffect("invokeCommand", () => session.rpc.commands.invoke({ ...input })),
    rewindPoints: sdkEffect("listRewindPoints", () => session.rpc.history.listRewindPoints()).pipe(
      Effect.map((result) => ({
        // Autopilot continuations are turns the runtime injected on its own;
        // they are not turns a user took, so counting them would shift every
        // rollback boundary.
        points: result.points
          .filter((point) => !point.isAutopilotContinuation)
          .map((point) => ({
            eventId: point.eventId,
            userMessage: point.userMessage,
            timestamp: point.timestamp,
          })),
        ...(result.unavailableReason ? { unavailableReason: result.unavailableReason } : {}),
      })),
    ),
    // Conversation-only: T3 owns workspace restore through its own checkpoints,
    // so the runtime must never touch files behind our back.
    rewind: (eventId) =>
      sdkEffect("rewind", () => session.rpc.history.rewind({ eventId, mode: "conversation" })),
    disconnect: sdkEffect("disconnect", () => session.disconnect()),
  };
}

/** The per-model settings both session configs and `setModel` accept. */
function modelSettings(options: CopilotSdkModelOptions | undefined) {
  if (!options) return {};
  return {
    ...(options.reasoningEffort ? { reasoningEffort: options.reasoningEffort } : {}),
    ...(options.contextTier ? { contextTier: options.contextTier } : {}),
  };
}

/** Start and resume take the same shape; only the id the runtime looks up differs. */
function sessionConfig(input: CopilotSdkSessionStartInput): SessionConfig & ResumeSessionConfig {
  return {
    workingDirectory: input.workingDirectory,
    ...(input.modelOptions
      ? { model: input.modelOptions.model, ...modelSettings(input.modelOptions) }
      : {}),
    streaming: true,
    onEvent: input.onEvent,
    onPermissionRequest: (request: PermissionRequest) => input.onPermissionRequest(request),
  };
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
      createSession: (input) =>
        sdkEffect("createSession", () => client.createSession(sessionConfig(input))).pipe(
          Effect.map(wrapSession),
        ),
      resumeSession: (input) =>
        sdkEffect("resumeSession", () =>
          client.resumeSession(input.sessionId, sessionConfig(input)),
        ).pipe(Effect.map(wrapSession)),
      workspaceCommands: (cwd) =>
        sdkEffect("workspaceCommands", async () => {
          const session = await client.createSession({
            workingDirectory: cwd,
            enableConfigDiscovery: true,
            enableSkills: true,
            // A discovery session never runs a turn; refusing outright keeps a
            // runtime that asks anyway from waiting on nobody.
            onPermissionRequest: () => Promise.resolve({ kind: "reject" as const }),
          });
          try {
            const result = await session.rpc.commands.list(PUBLISHED_COMMAND_FILTERS);
            return result.commands;
          } finally {
            await session.disconnect().catch(() => undefined);
          }
        }),
      workspaceSkills: (cwd) =>
        sdkEffect("workspaceSkills", () =>
          client.rpc.skills.discover({ projectPaths: [cwd] }),
        ).pipe(Effect.map((result) => result.skills)),
    };
  }),
};

export const CopilotSdkRuntime = Context.Reference<CopilotSdkRuntimeShape>(
  "t3/provider/CopilotSdkRuntime",
  { defaultValue: () => makeCopilotSdkRuntime },
);

export const CopilotSdkRuntimeLive = Layer.succeed(CopilotSdkRuntime, makeCopilotSdkRuntime);
