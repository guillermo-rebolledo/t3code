import type { ModelInfo, SessionEvent } from "@github/copilot-sdk";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import { it } from "@effect/vitest";
import { describe, expect } from "vite-plus/test";

import type {
  CopilotSdkConnection,
  CopilotSdkSession,
  CopilotSdkSessionStartInput,
} from "../provider/CopilotSdkRuntime.ts";
import { CopilotSdkRuntimeError } from "../provider/CopilotSdkRuntime.ts";
import { makeCopilotTextGeneration } from "./CopilotTextGeneration.ts";

const modelSelection = {
  instanceId: ProviderInstanceId.make("copilot_work"),
  model: "gpt-5.4",
  options: [
    { id: "reasoning_effort", value: "high" },
    { id: "context_tier", value: "long_context" },
  ],
};

const inventory: ReadonlyArray<ModelInfo> = [
  {
    id: "gpt-5.4",
    name: "GPT-5.4",
    supportedReasoningEfforts: ["low", "high"],
    defaultReasoningEffort: "low",
    billing: { tokenPrices: { longContext: {} } },
    capabilities: { supports: { reasoningEffort: true } },
  } as ModelInfo,
];

function event<T extends SessionEvent>(value: Omit<T, "id" | "parentId" | "timestamp">): T {
  return {
    id: "event-1",
    parentId: null,
    timestamp: "2026-01-01T00:00:00.000Z",
    ...value,
  } as T;
}

type SessionBehavior =
  | { readonly kind: "response"; readonly content: string }
  | { readonly kind: "sendFailure" }
  | { readonly kind: "providerFailure" }
  | { readonly kind: "createPending" }
  | { readonly kind: "pending" };

function connectionFor(
  behaviors: ReadonlyArray<SessionBehavior>,
  onDisconnect: () => void,
): CopilotSdkConnection {
  let index = 0;
  return {
    authStatus: Effect.succeed({ isAuthenticated: true }),
    models: Effect.succeed(inventory),
    createSession: (input) => {
      const behavior = behaviors[index++] ?? { kind: "pending" };
      if (behavior.kind === "createPending") return Effect.never;
      return Effect.succeed({
        sessionId: `aux-session-${index}`,
        send: () => {
          if (behavior.kind === "sendFailure") {
            return Effect.fail(
              new CopilotSdkRuntimeError({
                operation: "send",
                kind: "failure",
                detail: "connection closed",
              }),
            );
          }
          return Effect.sync(() => {
            if (behavior.kind === "response") {
              input.onEvent(
                event({
                  type: "assistant.message",
                  data: { messageId: `message-${index}`, content: behavior.content },
                }),
              );
              input.onEvent(event({ type: "session.idle", data: {} }));
            } else if (behavior.kind === "providerFailure") {
              input.onEvent(
                event({
                  type: "session.error",
                  data: { errorType: "provider", message: "model service unavailable" },
                }),
              );
            }
            return `message-${index}`;
          });
        },
        abort: Effect.void,
        setModel: () => Effect.void,
        listCommands: Effect.succeed([]),
        invokeCommand: () => Effect.die("not used"),
        rewindPoints: Effect.succeed({ points: [] }),
        rewind: () => Effect.die("not used"),
        disconnect: Effect.sync(onDisconnect),
      });
    },
    resumeSession: () => Effect.die("not used"),
    workspaceCommands: () => Effect.succeed([]),
    workspaceSkills: () => Effect.succeed([]),
  };
}

const branchInput = {
  cwd: "/repo",
  message: "Add Copilot text generation",
  modelSelection,
};

describe("CopilotTextGeneration", () => {
  it.effect(
    "generates and sanitizes a commit message with the selected live model and no tools",
    () =>
      Effect.gen(function* () {
        let startInput: CopilotSdkSessionStartInput | undefined;
        let permissionResult: unknown;
        let disconnects = 0;
        const session: CopilotSdkSession = {
          sessionId: "aux-session-1",
          send: () =>
            Effect.promise(async () => {
              permissionResult = await startInput!.onPermissionRequest({ kind: "shell" });
              startInput!.onEvent(
                event({
                  type: "assistant.message",
                  data: {
                    messageId: "message-1",
                    content:
                      '{"subject":"Update the Copilot integration.\\nignored","body":"  Details  ","branch":"Feature/Copilot text!"}',
                  },
                }),
              );
              startInput!.onEvent(event({ type: "session.idle", data: {} }));
              return "message-1";
            }),
          abort: Effect.void,
          setModel: () => Effect.void,
          listCommands: Effect.succeed([]),
          invokeCommand: () => Effect.die("not used"),
          rewindPoints: Effect.succeed({ points: [] }),
          rewind: () => Effect.die("not used"),
          disconnect: Effect.sync(() => {
            disconnects += 1;
          }),
        };
        const connection: CopilotSdkConnection = {
          authStatus: Effect.succeed({ isAuthenticated: true }),
          models: Effect.succeed(inventory),
          createSession: (input) => {
            startInput = input;
            return Effect.succeed(session);
          },
          resumeSession: () => Effect.die("not used"),
          workspaceCommands: () => Effect.succeed([]),
          workspaceSkills: () => Effect.succeed([]),
        };

        const generated = yield* makeCopilotTextGeneration(connection).generateCommitMessage({
          cwd: "/repo",
          branch: "main",
          stagedSummary: "M file.ts",
          stagedPatch: "+changed",
          includeBranch: true,
          modelSelection,
        });

        expect(generated).toEqual({
          subject: "Update the Copilot integration",
          body: "Details",
          branch: "feature/copilot-text",
        });
        expect(startInput).toMatchObject({
          workingDirectory: "/repo",
          modelOptions: {
            model: "gpt-5.4",
            reasoningEffort: "high",
            contextTier: "long_context",
          },
          availableTools: [],
        });
        expect(permissionResult).toEqual({ kind: "reject" });
        expect(disconnects).toBe(1);
      }),
  );

  it.effect("generates and sanitizes pull-request, branch, and thread-title content", () =>
    Effect.gen(function* () {
      let disconnects = 0;
      const textGeneration = makeCopilotTextGeneration(
        connectionFor(
          [
            {
              kind: "response",
              content: '{"title":"Ship Copilot text\\nignored","body":"  ## Summary\\n- Ready  "}',
            },
            { kind: "response", content: '{"branch":"Feature/Copilot text!"}' },
            {
              kind: "response",
              content:
                '{"title":"  \\"A much too long Copilot thread title that is deliberately over fifty characters\\"  "}',
            },
          ],
          () => {
            disconnects += 1;
          },
        ),
      );

      const pullRequest = yield* textGeneration.generatePrContent({
        cwd: "/repo",
        baseBranch: "main",
        headBranch: "feature/copilot-text",
        commitSummary: "Add Copilot text generation",
        diffSummary: "1 file changed",
        diffPatch: "+change",
        modelSelection,
      });
      const branch = yield* textGeneration.generateBranchName(branchInput);
      const title = yield* textGeneration.generateThreadTitle({
        cwd: "/repo",
        message: "Add Copilot text generation",
        modelSelection,
      });

      expect(pullRequest).toEqual({
        title: "Ship Copilot text",
        body: "## Summary\n- Ready",
      });
      expect(branch).toEqual({ branch: "feature/copilot-text" });
      expect(title.title).toBe("A much too long Copilot thread title that is de...");
      expect(disconnects).toBe(3);
    }),
  );

  for (const [name, content, detail] of [
    ["empty", "", "returned empty output"],
    ["malformed", "not json", "returned invalid structured output"],
  ] as const) {
    it.effect(`returns typed failures for ${name} responses and disconnects`, () =>
      Effect.gen(function* () {
        let disconnects = 0;
        const textGeneration = makeCopilotTextGeneration(
          connectionFor([{ kind: "response", content }], () => {
            disconnects += 1;
          }),
        );

        const failure = yield* Effect.flip(textGeneration.generateBranchName(branchInput));

        expect(failure._tag).toBe("TextGenerationError");
        expect(failure.operation).toBe("generateBranchName");
        expect(failure.detail).toContain(detail);
        expect(disconnects).toBe(1);
      }),
    );
  }

  it.effect("returns a typed SDK failure and disconnects", () =>
    Effect.gen(function* () {
      let disconnects = 0;
      const textGeneration = makeCopilotTextGeneration(
        connectionFor([{ kind: "sendFailure" }], () => {
          disconnects += 1;
        }),
      );

      const failure = yield* Effect.flip(textGeneration.generateBranchName(branchInput));

      expect(failure._tag).toBe("TextGenerationError");
      expect(failure.detail).toContain("connection closed");
      expect(disconnects).toBe(1);
    }),
  );

  it.effect("returns a typed provider failure and disconnects", () =>
    Effect.gen(function* () {
      let disconnects = 0;
      const textGeneration = makeCopilotTextGeneration(
        connectionFor([{ kind: "providerFailure" }], () => {
          disconnects += 1;
        }),
      );

      const failure = yield* Effect.flip(textGeneration.generateBranchName(branchInput));

      expect(failure._tag).toBe("TextGenerationError");
      expect(failure.detail).toContain("model service unavailable");
      expect(disconnects).toBe(1);
    }),
  );
});

it.effect("disconnects a timed-out auxiliary session", () =>
  Effect.gen(function* () {
    let disconnects = 0;
    const textGeneration = makeCopilotTextGeneration(
      connectionFor([{ kind: "pending" }], () => {
        disconnects += 1;
      }),
      { timeoutMs: 1_000 },
    );
    const fiber = yield* Effect.flip(textGeneration.generateBranchName(branchInput)).pipe(
      Effect.forkScoped,
    );

    yield* Effect.yieldNow;
    yield* TestClock.adjust("1 second");
    const failure = yield* Fiber.join(fiber);

    expect(failure.detail).toContain("timed out");
    expect(disconnects).toBe(1);
  }).pipe(Effect.scoped),
);

for (const phase of ["model inventory", "session creation"] as const) {
  it.effect(`returns a typed timeout when ${phase} does not answer`, () =>
    Effect.gen(function* () {
      let disconnects = 0;
      const baseConnection = connectionFor(
        [{ kind: phase === "session creation" ? "createPending" : "pending" }],
        () => {
          disconnects += 1;
        },
      );
      const connection =
        phase === "model inventory" ? { ...baseConnection, models: Effect.never } : baseConnection;
      const textGeneration = makeCopilotTextGeneration(connection, { timeoutMs: 1_000 });
      const fiber = yield* Effect.flip(textGeneration.generateBranchName(branchInput)).pipe(
        Effect.forkScoped,
      );

      yield* Effect.yieldNow;
      yield* TestClock.adjust("1 second");
      const failure = yield* Fiber.join(fiber);

      expect(failure.detail).toContain("timed out");
      expect(disconnects).toBe(0);
    }).pipe(Effect.scoped),
  );
}

it.effect("disconnects an interrupted auxiliary session", () =>
  Effect.gen(function* () {
    let disconnects = 0;
    const textGeneration = makeCopilotTextGeneration(
      connectionFor([{ kind: "pending" }], () => {
        disconnects += 1;
      }),
    );
    const fiber = yield* textGeneration.generateBranchName(branchInput).pipe(Effect.forkScoped);

    yield* Effect.yieldNow;
    yield* Fiber.interrupt(fiber);
    expect(disconnects).toBe(1);
  }).pipe(Effect.scoped),
);
