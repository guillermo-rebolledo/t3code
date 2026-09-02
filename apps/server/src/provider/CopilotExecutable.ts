// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

export type CopilotExecutableFileCheck = (candidate: string) => boolean;

function isExecutableFile(candidate: string, platform: NodeJS.Platform): boolean {
  try {
    if (!NodeFS.statSync(candidate).isFile()) return false;
    if (platform !== "win32") NodeFS.accessSync(candidate, NodeFS.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function hasPathSeparator(value: string): boolean {
  return value.includes("/") || value.includes("\\");
}

/**
 * Resolve a Copilot command before handing it to the SDK, which requires a
 * concrete executable path and does not perform the shell's PATH lookup.
 */
export function resolveCopilotExecutable(
  configuredPath: string,
  environment: Readonly<Record<string, string | undefined>>,
  options: {
    readonly platform: NodeJS.Platform;
    readonly isFile?: CopilotExecutableFileCheck;
  },
): string {
  const binary = configuredPath.trim() || "copilot";
  if (hasPathSeparator(binary)) return binary;

  const platform = options.platform;
  const path = platform === "win32" ? NodePath.win32 : NodePath.posix;
  const delimiter = platform === "win32" ? ";" : ":";
  const isFile = options.isFile ?? ((candidate) => isExecutableFile(candidate, platform));
  const pathDirectories = (environment.PATH ?? process.env.PATH ?? "")
    .split(delimiter)
    .map((directory) => directory.trim())
    .filter((directory) => directory.length > 0);
  const home = environment.HOME ?? process.env.HOME;
  const commonDirectories =
    platform === "win32"
      ? [environment.APPDATA ? path.join(environment.APPDATA, "npm") : undefined]
      : [
          "/opt/homebrew/bin",
          "/usr/local/bin",
          "/usr/bin",
          "/bin",
          ...(home
            ? [
                path.join(home, ".local/bin"),
                path.join(home, ".bun/bin"),
                path.join(home, ".volta/bin"),
                path.join(home, ".npm-global/bin"),
              ]
            : []),
        ];
  const extensions =
    platform === "win32"
      ? (environment.PATHEXT ?? process.env.PATHEXT ?? ".COM;.EXE;.BAT;.CMD")
          .split(";")
          .map((extension) => extension.trim())
          .filter(Boolean)
      : [];
  const names = [
    binary,
    ...extensions
      .filter((extension) => !binary.toLowerCase().endsWith(extension.toLowerCase()))
      .map((extension) => `${binary}${extension}`),
  ];

  const seen = new Set<string>();
  for (const directory of [...pathDirectories, ...commonDirectories]) {
    if (!directory || seen.has(directory)) continue;
    seen.add(directory);
    for (const name of names) {
      const candidate = path.join(directory, name);
      if (isFile(candidate)) return candidate;
    }
  }
  return binary;
}
