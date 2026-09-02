/**
 * CopilotCommands — the Copilot slash-command catalog T3 advertises and the
 * routing decision for a prompt the user typed.
 *
 * Copilot's runtime owns its own command surface. T3 publishes that catalog in
 * the workspace-scoped provider snapshot so the composer can offer the commands
 * the installed CLI actually supports, and routes a prompt naming one of them
 * through the SDK's command RPC instead of sending it as prose.
 *
 * @module provider/CopilotCommands
 */
import type { ServerProviderSlashCommand } from "@t3tools/contracts";

/** The subset of Copilot's `SlashCommandInfo` T3 can present. */
export interface CopilotCommandInfo {
  readonly name: string;
  readonly description?: string;
  readonly input?: { readonly hint?: string };
}

export interface CopilotSlashCommandInvocation {
  readonly name: string;
  /** Raw text after the command name, exactly as Copilot's RPC expects it. */
  readonly input?: string;
}

/**
 * The structural form of Copilot's `SlashCommandInvocationResult`. Declared
 * here so the adapter and its tests never depend on the SDK's generated union.
 */
export type CopilotCommandInvocationResult =
  | { readonly kind: "text"; readonly text: string }
  | { readonly kind: "agent-prompt"; readonly prompt: string; readonly mode?: string }
  | { readonly kind: "completed"; readonly message?: string }
  | {
      readonly kind: "select-subcommand";
      readonly title: string;
      readonly options: ReadonlyArray<{ readonly name: string; readonly description?: string }>;
    };

/**
 * What T3 does with an invocation result: hand Copilot a prompt and let the
 * turn run, or show the command's own output and settle the turn.
 */
export type CopilotCommandOutcome =
  | { readonly kind: "prompt"; readonly prompt: string }
  | { readonly kind: "text"; readonly text: string };

/**
 * Commands T3 Code owns through its own interface. Copilot's versions would
 * move state T3 already controls - the model picker and the thread's runtime
 * mode - behind T3's back, so they never reach the composer.
 */
const T3_OWNED_COMMANDS: ReadonlySet<string> = new Set(["default", "model", "plan"]);

export function mapCopilotSlashCommands(
  commands: ReadonlyArray<CopilotCommandInfo>,
): ReadonlyArray<ServerProviderSlashCommand> {
  const seen = new Set<string>();
  const result: ServerProviderSlashCommand[] = [];

  for (const command of commands) {
    const name = typeof command?.name === "string" ? command.name.trim() : "";
    const key = name.toLowerCase();
    if (!name || T3_OWNED_COMMANDS.has(key) || seen.has(key)) continue;
    seen.add(key);

    const description = command.description?.trim();
    const hint = command.input?.hint?.trim();
    result.push({
      name,
      ...(description ? { description } : {}),
      ...(hint ? { input: { hint } } : {}),
    });
  }

  return result.toSorted((left, right) => left.name.localeCompare(right.name));
}

/** The lookup the adapter routes against, built from the advertised catalog. */
export function copilotSlashCommandNames(
  commands: ReadonlyArray<ServerProviderSlashCommand>,
): ReadonlySet<string> {
  return new Set(commands.map((command) => command.name.trim().toLowerCase()));
}

/**
 * Split a prompt into a Copilot command invocation, or return `undefined` when
 * it is ordinary text. Slash text naming a command Copilot did not advertise
 * stays a prompt: T3 must not invent a command the runtime cannot run.
 */
export function parseCopilotSlashCommand(
  prompt: string,
  commandNames: ReadonlySet<string>,
): CopilotSlashCommandInvocation | undefined {
  const match = /^\/([^\s/]+)(?:\s+([\s\S]*))?$/u.exec(prompt.trim());
  const name = match?.[1]?.trim().toLowerCase();
  if (!name || !commandNames.has(name)) return undefined;

  const input = match?.[2]?.trim();
  return { name, ...(input ? { input } : {}) };
}

/**
 * Same token shape the composer inserts and the timeline chips render
 * (`packages/shared/src/composerInlineTokens.ts`), widened to end-of-string so
 * a prompt that is nothing but one mention still matches.
 */
const SKILL_MENTION_PATTERN = /(^|\s)\$([a-zA-Z][a-zA-Z0-9:_-]*)(?=\s|$)/gu;

/**
 * Rewrite the composer's `$skill` mentions into the `/skill` form Copilot
 * understands. Copilot advertises its user-invocable skills as commands, so a
 * prompt that is just one mention goes on to route through the command RPC,
 * and a mention inside a sentence reaches the agent as legible command text it
 * can act on itself. A mention Copilot did not advertise is left alone: `$HOME`
 * in prose must stay `$HOME`.
 */
export function rewriteCopilotSkillMentions(
  prompt: string,
  commandNames: ReadonlySet<string>,
): string {
  return prompt.replace(SKILL_MENTION_PATTERN, (match, prefix: string, name: string) =>
    commandNames.has(name.toLowerCase()) ? `${prefix}/${name}` : match,
  );
}

/**
 * Reduce an invocation result to the two shapes a turn can take. A
 * subcommand selection is rendered as text: T3 has no picker for it, and
 * showing the options lets the user name one on the next message. The mode a
 * command asks for is ignored - the thread's runtime mode is T3's, and a
 * command must not move it where the user cannot see it.
 */
export function resolveCopilotCommandOutcome(
  result: CopilotCommandInvocationResult,
): CopilotCommandOutcome {
  switch (result.kind) {
    case "agent-prompt":
      return { kind: "prompt", prompt: result.prompt };
    case "text":
      return { kind: "text", text: result.text };
    case "completed":
      return { kind: "text", text: result.message ?? "" };
    case "select-subcommand":
      return {
        kind: "text",
        text: [
          result.title,
          ...result.options.map((option) =>
            option.description ? `- ${option.name}: ${option.description}` : `- ${option.name}`,
          ),
        ].join("\n"),
      };
  }
}
