import { describe, expect, it } from "@effect/vitest";

import {
  copilotSlashCommandNames,
  mapCopilotSlashCommands,
  parseCopilotSlashCommand,
  resolveCopilotCommandOutcome,
} from "./CopilotCommands.ts";

describe("mapCopilotSlashCommands", () => {
  it("advertises named commands with their description and input hint", () => {
    expect(
      mapCopilotSlashCommands([
        { name: "review", description: " Review the diff ", input: { hint: " what to review " } },
        { name: "usage" },
      ]),
    ).toEqual([
      { name: "review", description: "Review the diff", input: { hint: "what to review" } },
      { name: "usage" },
    ]);
  });

  it("drops empty, malformed, duplicate, and T3-owned command names", () => {
    expect(
      mapCopilotSlashCommands([
        { name: "review" },
        { name: "   " },
        { name: undefined as unknown as string },
        { name: "REVIEW", description: "a later duplicate" },
        { name: "model" },
        { name: "plan" },
        { name: "default" },
      ]),
    ).toEqual([{ name: "review" }]);
  });

  it("sorts the catalog by name", () => {
    expect(mapCopilotSlashCommands([{ name: "usage" }, { name: "review" }]).map((c) => c.name)) //
      .toEqual(["review", "usage"]);
  });
});

describe("parseCopilotSlashCommand", () => {
  const names = copilotSlashCommandNames([{ name: "Review" }, { name: "usage" }]);

  it("routes an advertised command and keeps its arguments", () => {
    expect(parseCopilotSlashCommand("  /review the auth diff  ", names)).toEqual({
      name: "review",
      input: "the auth diff",
    });
  });

  it("routes an advertised command with no arguments", () => {
    expect(parseCopilotSlashCommand("/usage", names)).toEqual({ name: "usage" });
  });

  it("matches the advertised name case-insensitively", () => {
    expect(parseCopilotSlashCommand("/REVIEW", names)).toEqual({ name: "review" });
  });

  it("leaves unknown slash text and ordinary prompts alone", () => {
    expect(parseCopilotSlashCommand("/deploy staging", names)).toBeUndefined();
    expect(parseCopilotSlashCommand("please /review this", names)).toBeUndefined();
    expect(parseCopilotSlashCommand("/", names)).toBeUndefined();
    expect(parseCopilotSlashCommand("//review", names)).toBeUndefined();
    expect(parseCopilotSlashCommand("fix the failing test", names)).toBeUndefined();
  });
});

describe("resolveCopilotCommandOutcome", () => {
  it("hands an agent prompt back to the session and ignores the mode it asks for", () => {
    expect(
      resolveCopilotCommandOutcome({ kind: "agent-prompt", prompt: "Summarize", mode: "plan" }),
    ).toEqual({ kind: "prompt", prompt: "Summarize" });
    expect(resolveCopilotCommandOutcome({ kind: "agent-prompt", prompt: "Summarize" })).toEqual({
      kind: "prompt",
      prompt: "Summarize",
    });
  });

  it("renders text, completion, and subcommand results as command output", () => {
    expect(resolveCopilotCommandOutcome({ kind: "text", text: "42 requests left" })).toEqual({
      kind: "text",
      text: "42 requests left",
    });
    expect(resolveCopilotCommandOutcome({ kind: "completed", message: "Done" })).toEqual({
      kind: "text",
      text: "Done",
    });
    expect(resolveCopilotCommandOutcome({ kind: "completed" })).toEqual({ kind: "text", text: "" });
    expect(
      resolveCopilotCommandOutcome({
        kind: "select-subcommand",
        title: "Pick one",
        options: [{ name: "add", description: "Add a server" }, { name: "list" }],
      }),
    ).toEqual({ kind: "text", text: "Pick one\n- add: Add a server\n- list" });
  });
});
