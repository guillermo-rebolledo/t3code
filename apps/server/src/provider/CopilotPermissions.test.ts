import { describe, expect, it } from "@effect/vitest";

import {
  approvalDecisionResult,
  automaticPermissionResult,
  canonicalPermissionRequest,
  parseCopilotPermission,
} from "./CopilotPermissions.ts";

const shell = {
  kind: "shell",
  fullCommandText: "pnpm test",
  intention: "Run tests",
  commands: [{ identifier: "pnpm", readOnly: true }],
  canOfferSessionApproval: true,
} as const;
const write = {
  kind: "write",
  fileName: "src/index.ts",
  intention: "Update source",
  diff: "+export const ready = true",
  canOfferSessionApproval: true,
} as const;
const genericTool = {
  kind: "custom-tool",
  toolName: "deploy_preview",
  toolDescription: "Deploy a preview build",
} as const;

describe("Copilot permissions", () => {
  it("classifies file, shell, and generic tool requests canonically", () => {
    expect(canonicalPermissionRequest(write)).toMatchObject({
      requestType: "file_change_approval",
      detail: "src/index.ts",
    });
    expect(canonicalPermissionRequest(shell)).toMatchObject({
      requestType: "exec_command_approval",
      detail: "pnpm test",
    });
    expect(canonicalPermissionRequest(genericTool)).toMatchObject({
      requestType: "dynamic_tool_call",
      appName: "deploy_preview",
      detail: "Deploy a preview build",
    });
  });

  it("applies every runtime mode without silently broadening auto approval", () => {
    expect(automaticPermissionResult("approval-required", write)).toBeUndefined();
    expect(automaticPermissionResult("approval-required", shell)).toBeUndefined();
    expect(automaticPermissionResult("auto-accept-edits", write)).toEqual({
      kind: "approve-once",
    });
    expect(automaticPermissionResult("auto-accept-edits", shell)).toBeUndefined();
    expect(automaticPermissionResult("auto", write)).toBeUndefined();
    expect(automaticPermissionResult("auto", genericTool)).toBeUndefined();
    expect(automaticPermissionResult("full-access", write)).toEqual({ kind: "approve-once" });
    expect(automaticPermissionResult("full-access", shell)).toEqual({ kind: "approve-once" });
    expect(automaticPermissionResult("full-access", genericTool)).toEqual({
      kind: "approve-once",
    });
  });

  it("keeps managed approvals interactive even in full access", () => {
    expect(
      automaticPermissionResult("full-access", {
        ...write,
        managedApprovalRequired: true,
      }),
    ).toBeUndefined();
  });

  it("translates allow, deny, and scoped session allow decisions", () => {
    expect(approvalDecisionResult("accept", shell)).toEqual({
      kind: "approve-once",
      approvedInteractively: true,
    });
    expect(approvalDecisionResult("decline", shell)).toEqual({
      kind: "reject",
      feedback: "The user denied this request.",
    });
    expect(approvalDecisionResult("acceptForSession", shell)).toEqual({
      kind: "approve-for-session",
      approval: { kind: "commands", commandIdentifiers: ["pnpm"] },
    });
    expect(approvalDecisionResult("acceptForSession", genericTool)).toEqual({
      kind: "approve-for-session",
      approval: { kind: "custom-tool", toolName: "deploy_preview" },
    });
  });

  it("rejects malformed and unknown permission payloads during decoding", () => {
    expect(parseCopilotPermission({ kind: "shell", fullCommandText: 42 })).toBeUndefined();
    expect(parseCopilotPermission({ kind: "future-permission", value: true })).toBeUndefined();
  });
});
