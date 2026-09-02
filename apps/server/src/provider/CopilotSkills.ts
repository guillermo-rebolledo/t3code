/**
 * CopilotSkills — maps Copilot's skill inventory onto T3's skill contract.
 *
 * Copilot discovers skills per project, so the result belongs in a
 * workspace-scoped provider snapshot. The inventory is parsed defensively:
 * one malformed entry must never cost the user the rest of the catalog, and a
 * source T3 does not recognise is published without a scope rather than under
 * a guessed one.
 *
 * @module provider/CopilotSkills
 */
import type { ServerProviderSkill } from "@t3tools/contracts";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

const CopilotSkillEntry = Schema.Struct({
  name: Schema.String,
  description: Schema.optionalKey(Schema.String),
  enabled: Schema.Boolean,
  path: Schema.optionalKey(Schema.String),
  source: Schema.optionalKey(Schema.String),
  userInvocable: Schema.optionalKey(Schema.Boolean),
});

const decodeCopilotSkillEntry = Schema.decodeUnknownExit(CopilotSkillEntry);

/**
 * Copilot's source labels mapped onto the scopes T3's clients render. An
 * unknown label returns `undefined` so the skill still shows up, just without
 * a scope badge.
 */
function normalizeSkillScope(source: string | undefined): string | undefined {
  switch (source?.trim().toLowerCase()) {
    case "project":
      return "project";
    case "inherited":
      return "repo";
    case "personal-agents":
    case "personal-copilot":
    case "custom":
      return "personal";
    case "plugin":
      return "app";
    case "builtin":
      return "system";
    default:
      return undefined;
  }
}

export function mapCopilotSkills(
  entries: ReadonlyArray<unknown>,
): ReadonlyArray<ServerProviderSkill> {
  const seen = new Set<string>();
  const skills: ServerProviderSkill[] = [];

  for (const entry of entries) {
    const decoded = decodeCopilotSkillEntry(entry);
    if (Exit.isFailure(decoded)) continue;

    const skill = decoded.value;
    const name = skill.name.trim();
    // The contract requires a path, so an entry without one cannot be
    // represented and is dropped with the rest of the malformed input.
    const path = skill.path?.trim();
    const key = name.toLowerCase();
    if (!name || !path || seen.has(key)) continue;
    seen.add(key);

    const description = skill.description?.trim();
    const scope = normalizeSkillScope(skill.source);
    skills.push({
      name,
      path,
      enabled: skill.enabled,
      ...(description ? { description, shortDescription: description } : {}),
      ...(scope ? { scope } : {}),
      ...(typeof skill.userInvocable === "boolean" ? { userInvocable: skill.userInvocable } : {}),
    });
  }

  return skills.toSorted((left, right) => left.name.localeCompare(right.name));
}
