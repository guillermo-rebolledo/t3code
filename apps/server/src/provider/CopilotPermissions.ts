import type { PermissionRequestResult } from "@github/copilot-sdk";
import type {
  CanonicalRequestType,
  ProviderApprovalDecision,
  ProviderApprovalOption,
  RuntimeMode,
} from "@t3tools/contracts";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";

const ToolCallId = Schema.optional(Schema.String);
const ManagedApprovalRequired = Schema.optional(Schema.Boolean);

const ShellPermission = Schema.Struct({
  kind: Schema.Literal("shell"),
  fullCommandText: Schema.String,
  intention: Schema.String,
  commands: Schema.Array(Schema.Struct({ identifier: Schema.String, readOnly: Schema.Boolean })),
  canOfferSessionApproval: Schema.Boolean,
  toolCallId: ToolCallId,
  managedApprovalRequired: ManagedApprovalRequired,
});

const WritePermission = Schema.Struct({
  kind: Schema.Literal("write"),
  fileName: Schema.String,
  intention: Schema.String,
  diff: Schema.String,
  canOfferSessionApproval: Schema.Boolean,
  toolCallId: ToolCallId,
  managedApprovalRequired: ManagedApprovalRequired,
});

const ReadPermission = Schema.Struct({
  kind: Schema.Literal("read"),
  path: Schema.String,
  intention: Schema.String,
  toolCallId: ToolCallId,
  managedApprovalRequired: ManagedApprovalRequired,
});

const McpPermission = Schema.Struct({
  kind: Schema.Literal("mcp"),
  serverName: Schema.String,
  toolName: Schema.String,
  toolTitle: Schema.String,
  readOnly: Schema.Boolean,
  toolCallId: ToolCallId,
});

const CustomToolPermission = Schema.Struct({
  kind: Schema.Literal("custom-tool"),
  toolName: Schema.String,
  toolDescription: Schema.String,
  toolCallId: ToolCallId,
});

const UrlPermission = Schema.Struct({
  kind: Schema.Literal("url"),
  url: Schema.String,
  intention: Schema.String,
  toolCallId: ToolCallId,
  managedApprovalRequired: ManagedApprovalRequired,
});

const MemoryPermission = Schema.Struct({
  kind: Schema.Literal("memory"),
  fact: Schema.String,
  subject: Schema.optional(Schema.String),
  toolCallId: ToolCallId,
});

const HookPermission = Schema.Struct({
  kind: Schema.Literal("hook"),
  toolName: Schema.String,
  hookMessage: Schema.optional(Schema.String),
  toolCallId: ToolCallId,
});

const ExtensionManagementPermission = Schema.Struct({
  kind: Schema.Literal("extension-management"),
  operation: Schema.String,
  extensionName: Schema.optional(Schema.String),
  toolCallId: ToolCallId,
});

const FactoryPermission = Schema.Struct({
  kind: Schema.Literal("factory"),
  name: Schema.String,
  description: Schema.String,
  operation: Schema.Literals(["run", "author"]),
  approvalKey: Schema.String,
  canPersistApproval: Schema.Boolean,
  toolCallId: ToolCallId,
});

const ExtensionPermissionAccess = Schema.Struct({
  kind: Schema.Literal("extension-permission-access"),
  extensionName: Schema.String,
  capabilities: Schema.Array(Schema.String),
  toolCallId: ToolCallId,
});

export const CopilotPermission = Schema.Union([
  ShellPermission,
  WritePermission,
  ReadPermission,
  McpPermission,
  CustomToolPermission,
  UrlPermission,
  MemoryPermission,
  HookPermission,
  ExtensionManagementPermission,
  FactoryPermission,
  ExtensionPermissionAccess,
]);
export type CopilotPermission = typeof CopilotPermission.Type;
const decodeCopilotPermission = Schema.decodeUnknownExit(CopilotPermission);

export function parseCopilotPermission(input: unknown): CopilotPermission | undefined {
  const decoded = decodeCopilotPermission(input);
  return Exit.isSuccess(decoded) ? decoded.value : undefined;
}

export function canonicalPermissionRequest(permission: CopilotPermission): {
  readonly requestType: CanonicalRequestType;
  readonly detail: string;
  readonly appName?: string;
} {
  switch (permission.kind) {
    case "shell":
      return {
        requestType: "exec_command_approval",
        detail: permission.fullCommandText.trim() || permission.intention.trim() || "Run command",
      };
    case "write":
      return {
        requestType: "file_change_approval",
        detail: permission.fileName.trim() || permission.intention.trim() || "Change file",
      };
    case "read":
      return {
        requestType: "file_read_approval",
        detail: permission.path.trim() || permission.intention.trim() || "Read file",
      };
    case "mcp":
      return {
        requestType: "dynamic_tool_call",
        detail: `${permission.serverName}/${permission.toolName}`,
        appName: permission.toolTitle.trim() || permission.toolName,
      };
    case "custom-tool":
      return {
        requestType: "dynamic_tool_call",
        detail: permission.toolDescription.trim() || permission.toolName,
        appName: permission.toolName,
      };
    case "url":
      return {
        requestType: "dynamic_tool_call",
        detail: permission.url,
        appName: "Open URL",
      };
    case "memory":
      return {
        requestType: "dynamic_tool_call",
        detail: permission.subject?.trim() || permission.fact,
        appName: "Copilot memory",
      };
    case "hook":
      return {
        requestType: "dynamic_tool_call",
        detail: permission.hookMessage?.trim() || permission.toolName,
        appName: permission.toolName,
      };
    case "extension-management":
      return {
        requestType: "dynamic_tool_call",
        detail: permission.extensionName
          ? `${permission.operation}: ${permission.extensionName}`
          : permission.operation,
        appName: "Copilot extension",
      };
    case "factory":
      return {
        requestType: "dynamic_tool_call",
        detail: permission.description.trim() || permission.name,
        appName: permission.name,
      };
    case "extension-permission-access":
      return {
        requestType: "dynamic_tool_call",
        detail: permission.capabilities.join(", ") || "Access extension capabilities",
        appName: permission.extensionName,
      };
  }
}

function sessionResultForPermission(
  permission: CopilotPermission,
): Extract<PermissionRequestResult, { kind: "approve-for-session" }> | undefined {
  switch (permission.kind) {
    case "shell": {
      if (!permission.canOfferSessionApproval) return undefined;
      const commandIdentifiers = [
        ...new Set(permission.commands.map(({ identifier }) => identifier.trim()).filter(Boolean)),
      ];
      return commandIdentifiers.length > 0
        ? { kind: "approve-for-session", approval: { kind: "commands", commandIdentifiers } }
        : undefined;
    }
    case "write":
      return permission.canOfferSessionApproval
        ? { kind: "approve-for-session", approval: { kind: "write" } }
        : undefined;
    case "read":
      return { kind: "approve-for-session", approval: { kind: "read" } };
    case "mcp":
      return {
        kind: "approve-for-session",
        approval: {
          kind: "mcp",
          serverName: permission.serverName,
          toolName: permission.toolName,
        },
      };
    case "custom-tool":
      return {
        kind: "approve-for-session",
        approval: { kind: "custom-tool", toolName: permission.toolName },
      };
    case "url": {
      const domain = URL.canParse(permission.url) ? new URL(permission.url).hostname : undefined;
      return domain ? { kind: "approve-for-session", domain } : undefined;
    }
    case "memory":
      return { kind: "approve-for-session", approval: { kind: "memory" } };
    case "hook":
      return undefined;
    case "extension-management":
      return {
        kind: "approve-for-session",
        approval: { kind: "extension-management", operation: permission.operation },
      };
    case "factory":
      return permission.canPersistApproval
        ? {
            kind: "approve-for-session",
            approval: { kind: "factory", approvalKey: permission.approvalKey },
          }
        : undefined;
    case "extension-permission-access":
      return {
        kind: "approve-for-session",
        approval: {
          kind: "extension-permission-access",
          extensionName: permission.extensionName,
        },
      };
  }
}

export function approvalOptionsForPermission(
  permission: CopilotPermission,
): ReadonlyArray<ProviderApprovalOption> {
  return [
    { decision: "accept", label: "Allow once" },
    ...(sessionResultForPermission(permission)
      ? [{ decision: "acceptForSession" as const, label: "Always allow this session" }]
      : []),
    { decision: "decline", label: "Deny" },
  ];
}

export function automaticPermissionResult(
  runtimeMode: RuntimeMode,
  permission: CopilotPermission,
): PermissionRequestResult | undefined {
  if ("managedApprovalRequired" in permission && permission.managedApprovalRequired === true) {
    return undefined;
  }
  if (runtimeMode === "full-access") return { kind: "approve-once" };
  if (runtimeMode === "auto-accept-edits" && permission.kind === "write") {
    return { kind: "approve-once" };
  }
  return undefined;
}

export function approvalDecisionResult(
  decision: ProviderApprovalDecision,
  permission: CopilotPermission,
): PermissionRequestResult {
  if (decision === "accept") {
    return { kind: "approve-once", approvedInteractively: true };
  }
  if (decision === "acceptForSession") {
    const result = sessionResultForPermission(permission);
    if (result) return result;
  }
  return {
    kind: "reject",
    feedback:
      decision === "cancel" ? "The user cancelled this request." : "The user denied this request.",
  };
}
