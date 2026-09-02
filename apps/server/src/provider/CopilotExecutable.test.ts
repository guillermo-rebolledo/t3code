import { describe, expect, it } from "vite-plus/test";

import { resolveCopilotExecutable } from "./CopilotExecutable.ts";

describe("resolveCopilotExecutable", () => {
  it("uses an explicit path verbatim", () => {
    expect(
      resolveCopilotExecutable(
        "/custom/bin/copilot",
        {},
        { platform: "darwin", isFile: () => false },
      ),
    ).toBe("/custom/bin/copilot");
  });

  it("uses the provider instance PATH and skips directories shadowing the executable", () => {
    const files = new Set(["/real/bin/copilot"]);
    expect(
      resolveCopilotExecutable(
        "copilot",
        { PATH: "/shadow/bin:/real/bin" },
        { platform: "linux", isFile: (candidate) => files.has(candidate) },
      ),
    ).toBe("/real/bin/copilot");
  });

  it("checks conservative GUI install locations", () => {
    expect(
      resolveCopilotExecutable(
        "copilot",
        { PATH: "" },
        {
          platform: "darwin",
          isFile: (candidate) => candidate === "/opt/homebrew/bin/copilot",
        },
      ),
    ).toBe("/opt/homebrew/bin/copilot");
  });

  it("resolves Windows executable extensions from PATHEXT", () => {
    expect(
      resolveCopilotExecutable(
        "copilot",
        { PATH: "C:\\npm;C:\\bin", PATHEXT: ".EXE;.CMD" },
        {
          platform: "win32",
          isFile: (candidate) => candidate.toLowerCase() === "c:\\npm\\copilot.cmd",
        },
      ),
    ).toBe("C:\\npm\\copilot.CMD");
  });

  it("returns the bare command when no executable exists", () => {
    expect(
      resolveCopilotExecutable(
        "copilot",
        { PATH: "/empty" },
        {
          platform: "linux",
          isFile: () => false,
        },
      ),
    ).toBe("copilot");
  });
});
