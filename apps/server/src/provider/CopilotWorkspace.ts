/**
 * CopilotWorkspace — the commands and skills Copilot offers in one working
 * directory.
 *
 * Copilot resolves both from the directory it opens in, so this is workspace
 * data, not machine data: it belongs to a project's snapshot and must not leak
 * into another project's or into the instance-wide one. Neither read can fail
 * the caller. A runtime that will not answer costs the user the catalog for
 * that directory, never their ability to talk to Copilot.
 *
 * @module provider/CopilotWorkspace
 */
import type { ServerProviderSkill, ServerProviderSlashCommand } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";

import { mapCopilotSlashCommands } from "./CopilotCommands.ts";
import { mapCopilotSkills } from "./CopilotSkills.ts";
import type { CopilotSdkConnection } from "./CopilotSdkRuntime.ts";

const DISCOVERY_TIMEOUT = Duration.seconds(20);

export interface CopilotWorkspaceCapabilities {
  readonly slashCommands: ReadonlyArray<ServerProviderSlashCommand>;
  readonly skills: ReadonlyArray<ServerProviderSkill>;
}

const EMPTY_COMMANDS: ReadonlyArray<ServerProviderSlashCommand> = [];
const EMPTY_SKILLS: ReadonlyArray<ServerProviderSkill> = [];

const degrade = <A>(what: string, cwd: string, empty: A) =>
  Effect.catchCause((cause: Cause.Cause<unknown>) =>
    Cause.hasInterrupts(cause)
      ? Effect.interrupt
      : Effect.logWarning(`GitHub Copilot ${what} discovery failed for '${cwd}'.`, {
          cause: Cause.pretty(cause),
        }).pipe(Effect.as(empty)),
  );

export const discoverCopilotWorkspaceCapabilities = Effect.fn(
  "discoverCopilotWorkspaceCapabilities",
)(function* (
  connection: CopilotSdkConnection,
  cwd: string,
  options: { readonly timeout?: Duration.Input } = {},
): Effect.fn.Return<CopilotWorkspaceCapabilities> {
  const timeout = options.timeout ?? DISCOVERY_TIMEOUT;
  const [slashCommands, skills] = yield* Effect.all(
    [
      connection
        .workspaceCommands(cwd)
        .pipe(
          Effect.map(mapCopilotSlashCommands),
          Effect.timeout(timeout),
          degrade("slash command", cwd, EMPTY_COMMANDS),
        ),
      connection
        .workspaceSkills(cwd)
        .pipe(
          Effect.map(mapCopilotSkills),
          Effect.timeout(timeout),
          degrade("skill", cwd, EMPTY_SKILLS),
        ),
    ],
    { concurrency: "unbounded" },
  );
  return { slashCommands, skills };
});
