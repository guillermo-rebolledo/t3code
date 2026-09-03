import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { expect } from "vite-plus/test";

import { interruptibleSdkEffect } from "./CopilotSdkRuntime.ts";

it.effect("disposes an SDK value that arrives after its caller is interrupted", () =>
  Effect.gen(function* () {
    let resolveOperation!: (value: string) => void;
    const operation = new Promise<string>((resolve) => {
      resolveOperation = resolve;
    });
    let resolveCleanup!: () => void;
    const cleanup = new Promise<void>((resolve) => {
      resolveCleanup = resolve;
    });
    const fiber = yield* interruptibleSdkEffect(
      "createSession",
      () => operation,
      async (value) => {
        expect(value).toBe("late-session");
        resolveCleanup();
      },
    ).pipe(Effect.forkScoped);

    yield* Effect.yieldNow;
    yield* Fiber.interrupt(fiber);
    resolveOperation("late-session");
    yield* Effect.promise(() => cleanup);
  }).pipe(Effect.scoped),
);
