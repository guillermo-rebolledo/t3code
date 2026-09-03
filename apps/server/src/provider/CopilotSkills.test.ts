import { describe, expect, it } from "@effect/vitest";

import { mapCopilotSkills } from "./CopilotSkills.ts";

describe("mapCopilotSkills", () => {
  it("maps a discovered skill with its enabled state and description", () => {
    expect(
      mapCopilotSkills([
        {
          name: "deploy",
          description: " Ship the service ",
          enabled: true,
          userInvocable: true,
          path: "/repo/.github/skills/deploy/SKILL.md",
          source: "project",
        },
      ]),
    ).toEqual([
      {
        name: "deploy",
        path: "/repo/.github/skills/deploy/SKILL.md",
        enabled: true,
        description: "Ship the service",
        shortDescription: "Ship the service",
        scope: "project",
        userInvocable: true,
      },
    ]);
  });

  it("keeps a disabled skill so the client can show why it will not run", () => {
    const [skill] = mapCopilotSkills([
      { name: "deploy", enabled: false, path: "/repo/skill.md", source: "project" },
    ]);
    expect(skill?.enabled).toBe(false);
  });

  it("normalizes recognized sources and omits the scope for unknown ones", () => {
    const scopeFor = (source: string) =>
      mapCopilotSkills([{ name: "s", enabled: true, path: "/p", source }])[0]?.scope;
    expect(scopeFor("project")).toBe("project");
    expect(scopeFor("inherited")).toBe("repo");
    expect(scopeFor("personal-copilot")).toBe("personal");
    expect(scopeFor("personal-agents")).toBe("personal");
    expect(scopeFor("custom")).toBe("personal");
    expect(scopeFor("plugin")).toBe("app");
    expect(scopeFor("builtin")).toBe("system");
    expect(scopeFor("from-the-future")).toBeUndefined();
  });

  it("discards malformed, pathless, and duplicate entries without losing the rest", () => {
    expect(
      mapCopilotSkills([
        null,
        "deploy",
        { name: "missing-enabled", path: "/p" },
        { name: "no-path", enabled: true },
        { name: "  ", enabled: true, path: "/p" },
        { name: "deploy", enabled: true, path: "/first" },
        { name: "DEPLOY", enabled: false, path: "/second" },
        { name: "review", enabled: true, path: "/review" },
      ]).map((skill) => `${skill.name}@${skill.path}`),
    ).toEqual(["deploy@/first", "review@/review"]);
  });
});
