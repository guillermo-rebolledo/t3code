import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Logger from "effect/Logger";
import * as TestClock from "effect/testing/TestClock";

import type { CopilotCommandInfo } from "./CopilotCommands.ts";
import type { CopilotSdkConnection } from "./CopilotSdkRuntime.ts";
import { CopilotSdkRuntimeError } from "./CopilotSdkRuntime.ts";
import { discoverCopilotWorkspaceCapabilities } from "./CopilotWorkspace.ts";

/** Discovery failures log a warning by design; tests assert the result, not the noise. */
const silent = Effect.provide(Logger.layer([], { mergeWithExisting: false }));

function connectionWith(
  perCwd: Record<
    string,
    {
      readonly commands?: ReadonlyArray<CopilotCommandInfo>;
      readonly skills?: ReadonlyArray<unknown>;
    }
  >,
  options: {
    readonly commands?: CopilotSdkConnection["workspaceCommands"];
    readonly skills?: CopilotSdkConnection["workspaceSkills"];
  } = {},
): CopilotSdkConnection {
  const unused = (operation: string) =>
    Effect.fail(
      new CopilotSdkRuntimeError({
        operation,
        kind: "failure",
        detail: "not used by workspace discovery tests",
      }),
    );
  return {
    authStatus: unused("authStatus"),
    models: unused("models"),
    createSession: () => unused("createSession"),
    resumeSession: () => unused("resumeSession"),
    workspaceCommands: options.commands ?? ((cwd) => Effect.succeed(perCwd[cwd]?.commands ?? [])),
    workspaceSkills: options.skills ?? ((cwd) => Effect.succeed(perCwd[cwd]?.skills ?? [])),
  };
}

const failing = (operation: string) => () =>
  Effect.fail(
    new CopilotSdkRuntimeError({ operation, kind: "failure", detail: "runtime refused" }),
  );

it.effect("publishes the commands and skills Copilot reports for one directory", () =>
  Effect.gen(function* () {
    const connection = connectionWith({
      "/repo/app": {
        commands: [{ name: "review", description: "Review the diff" }, { name: "model" }],
        skills: [{ name: "deploy", enabled: true, path: "/repo/app/skill.md", source: "project" }],
      },
    });

    const capabilities = yield* discoverCopilotWorkspaceCapabilities(connection, "/repo/app");

    assert.deepEqual(capabilities.slashCommands, [
      { name: "review", description: "Review the diff" },
    ]);
    assert.deepEqual(capabilities.skills, [
      {
        name: "deploy",
        path: "/repo/app/skill.md",
        enabled: true,
        scope: "project",
      },
    ]);
  }),
);

it.effect("keeps one working directory's capabilities out of another's", () =>
  Effect.gen(function* () {
    const connection = connectionWith({
      "/repo/app": {
        commands: [{ name: "app-review" }],
        skills: [{ name: "app-deploy", enabled: true, path: "/repo/app/skill.md" }],
      },
      "/repo/worktree": {
        commands: [{ name: "worktree-review" }],
        skills: [{ name: "worktree-deploy", enabled: true, path: "/repo/worktree/skill.md" }],
      },
    });

    const app = yield* discoverCopilotWorkspaceCapabilities(connection, "/repo/app");
    const worktree = yield* discoverCopilotWorkspaceCapabilities(connection, "/repo/worktree");

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
  }),
);

it.effect("degrades to an empty catalog when a discovery read fails", () =>
  Effect.gen(function* () {
    const connection = connectionWith(
      { "/repo/app": { skills: [{ name: "deploy", enabled: true, path: "/repo/app/skill.md" }] } },
      { commands: failing("workspaceCommands") },
    );

    const capabilities = yield* discoverCopilotWorkspaceCapabilities(connection, "/repo/app").pipe(
      silent,
    );

    assert.isEmpty(capabilities.slashCommands);
    assert.deepEqual(
      capabilities.skills.map((skill) => skill.name),
      ["deploy"],
    );
  }),
);

it.effect("degrades when skill discovery fails and keeps the commands it did read", () =>
  Effect.gen(function* () {
    const connection = connectionWith(
      { "/repo/app": { commands: [{ name: "review" }] } },
      { skills: failing("workspaceSkills") },
    );

    const capabilities = yield* discoverCopilotWorkspaceCapabilities(connection, "/repo/app").pipe(
      silent,
    );

    assert.deepEqual(
      capabilities.slashCommands.map((command) => command.name),
      ["review"],
    );
    assert.isEmpty(capabilities.skills);
  }),
);

it.effect("degrades when a discovery read never answers", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const connection = connectionWith(
      { "/repo/app": { commands: [{ name: "review" }] } },
      { skills: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)) },
    );

    const fiber = yield* discoverCopilotWorkspaceCapabilities(connection, "/repo/app", {
      timeout: "1 second",
    }).pipe(silent, Effect.forkChild);
    yield* Deferred.await(started);
    yield* TestClock.adjust("1 second");
    const capabilities = yield* Fiber.join(fiber);

    assert.deepEqual(
      capabilities.slashCommands.map((command) => command.name),
      ["review"],
    );
    assert.isEmpty(capabilities.skills);
  }),
);
