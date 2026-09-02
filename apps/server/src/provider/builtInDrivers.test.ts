import { DEFAULT_SERVER_SETTINGS, ProviderDriverKind } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { BUILT_IN_DRIVERS } from "./builtInDrivers.ts";
import { deriveProviderInstanceConfigMap } from "./Layers/ProviderInstanceRegistryHydration.ts";

describe("Copilot built-in driver registration", () => {
  it("registers the canonical copilot kind without creating a legacy default instance", () => {
    expect(
      BUILT_IN_DRIVERS.some((driver) => driver.driverKind === ProviderDriverKind.make("copilot")),
    ).toBe(true);
    expect(deriveProviderInstanceConfigMap(DEFAULT_SERVER_SETTINGS)).not.toHaveProperty("copilot");
  });
});
