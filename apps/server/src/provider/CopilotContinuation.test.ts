import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { buildCopilotContinuation, resolveCopilotContinuation } from "./CopilotContinuation.ts";

const instanceId = ProviderInstanceId.make("copilot_work");

describe("resolveCopilotContinuation", () => {
  it("resumes a cursor written by the same provider instance", () => {
    const cursor = buildCopilotContinuation({ instanceId, sessionId: "copilot-session-1" });

    expect(resolveCopilotContinuation(cursor, instanceId)).toEqual({
      kind: "resume",
      sessionId: "copilot-session-1",
    });
  });

  it("treats an absent cursor as a fresh start rather than a failure", () => {
    expect(resolveCopilotContinuation(undefined, instanceId).kind).toBe("none");
    expect(resolveCopilotContinuation(null, instanceId).kind).toBe("none");
  });

  it("refuses a cursor from an older metadata version", () => {
    const resolved = resolveCopilotContinuation(
      { schemaVersion: 0, provider: "copilot", instanceId, sessionId: "old" },
      instanceId,
    );

    expect(resolved.kind).toBe("invalid");
    expect(resolved.kind === "invalid" && resolved.issue).toContain("version");
  });

  it("refuses a cursor another provider instance owns", () => {
    const cursor = buildCopilotContinuation({
      instanceId: ProviderInstanceId.make("copilot_personal"),
      sessionId: "other-session",
    });

    const resolved = resolveCopilotContinuation(cursor, instanceId);

    expect(resolved.kind).toBe("invalid");
    expect(resolved.kind === "invalid" && resolved.issue).toContain("copilot_personal");
  });

  it("refuses a malformed cursor", () => {
    expect(resolveCopilotContinuation("copilot-session-1", instanceId).kind).toBe("invalid");
    expect(
      resolveCopilotContinuation(
        { ...buildCopilotContinuation({ instanceId, sessionId: "x" }), sessionId: "  " },
        instanceId,
      ).kind,
    ).toBe("invalid");
  });
});
