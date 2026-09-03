import type { SessionEvent } from "@github/copilot-sdk";
import { type ModelSelection, TextGenerationError } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import type {
  CopilotSdkConnection,
  CopilotSdkRuntimeError,
} from "../provider/CopilotSdkRuntime.ts";
import { resolveCopilotModelOptions } from "../provider/CopilotSdkModels.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

const COPILOT_TIMEOUT_MS = 180_000;
const isTextGenerationError = Schema.is(TextGenerationError);

type Operation =
  | "generateCommitMessage"
  | "generatePrContent"
  | "generateBranchName"
  | "generateThreadTitle";

function runtimeFailure(operation: Operation, cause: CopilotSdkRuntimeError): TextGenerationError {
  return new TextGenerationError({
    operation,
    detail: `GitHub Copilot SDK ${cause.operation} failed: ${cause.detail}`,
    cause,
  });
}

function timeoutFailure(operation: Operation): TextGenerationError {
  return new TextGenerationError({
    operation,
    detail: "GitHub Copilot request timed out.",
  });
}

function makeResponseCollector(operation: Operation) {
  let output = "";
  let result: Effect.Effect<string, TextGenerationError> | undefined;
  let resume: ((effect: Effect.Effect<string, TextGenerationError>) => void) | undefined;
  const settle = (effect: Effect.Effect<string, TextGenerationError>) => {
    if (result) return;
    result = effect;
    resume?.(effect);
    resume = undefined;
  };
  return {
    response: Effect.callback<string, TextGenerationError>((callback) => {
      if (result) {
        callback(result);
      } else {
        resume = callback;
      }
      return Effect.sync(() => {
        resume = undefined;
      });
    }),
    onEvent: (event: SessionEvent) => {
      if (event.type === "assistant.message" && !event.agentId) {
        output = event.data.content;
      } else if (event.type === "session.error") {
        settle(
          Effect.fail(
            new TextGenerationError({
              operation,
              detail: event.data.message || "GitHub Copilot reported an error.",
            }),
          ),
        );
      } else if (event.type === "session.shutdown") {
        settle(
          Effect.fail(
            new TextGenerationError({
              operation,
              detail: "GitHub Copilot session shut down before completing.",
            }),
          ),
        );
      } else if (event.type === "session.idle") {
        settle(Effect.succeed(output));
      }
    },
  };
}

export function makeCopilotTextGeneration(
  connection: CopilotSdkConnection,
  options: { readonly timeoutMs?: number } = {},
): TextGeneration.TextGeneration["Service"] {
  const timeoutMs = options.timeoutMs ?? COPILOT_TIMEOUT_MS;

  const runCopilotJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchema,
    modelSelection,
  }: {
    operation: Operation;
    cwd: string;
    prompt: string;
    outputSchema: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const timeOperation = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
        effect.pipe(
          Effect.timeoutOption(timeoutMs),
          Effect.flatMap(
            Option.match({
              onNone: () => Effect.fail(timeoutFailure(operation)),
              onSome: Effect.succeed,
            }),
          ),
        );
      const inventory = yield* timeOperation(
        connection.models.pipe(Effect.mapError((cause) => runtimeFailure(operation, cause))),
      );
      const resolved = resolveCopilotModelOptions({
        model: modelSelection.model,
        selections: modelSelection.options,
        inventory,
      });
      if (resolved.kind === "invalid") {
        return yield* new TextGenerationError({ operation, detail: resolved.issue });
      }

      const events = makeResponseCollector(operation);
      const request = Effect.acquireUseRelease(
        timeOperation(
          connection
            .createSession({
              workingDirectory: cwd,
              modelOptions: resolved.options,
              availableTools: [],
              onEvent: events.onEvent,
              onPermissionRequest: () => Promise.resolve({ kind: "reject" as const }),
            })
            .pipe(Effect.mapError((cause) => runtimeFailure(operation, cause))),
        ),
        (session) =>
          session.send({ prompt }).pipe(
            Effect.mapError((cause) => runtimeFailure(operation, cause)),
            Effect.andThen(events.response),
          ),
        (session) => session.disconnect.pipe(Effect.ignore),
      );

      const raw = yield* timeOperation(request);
      if (!raw.trim()) {
        return yield* new TextGenerationError({
          operation,
          detail: "GitHub Copilot returned empty output.",
        });
      }

      return yield* Schema.decodeEffect(Schema.fromJsonString(outputSchema))(
        extractJsonObject(raw),
      ).pipe(
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(
              new TextGenerationError({
                operation,
                detail: "GitHub Copilot returned invalid structured output.",
                cause,
              }),
            ),
        }),
      );
    }).pipe(
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : new TextGenerationError({
              operation,
              detail: "GitHub Copilot text generation failed.",
              cause,
            }),
      ),
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("CopilotTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });
      const generated = yield* runCopilotJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...(input.includeBranch === true &&
        "branch" in generated &&
        typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("CopilotTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt(input);
      const generated = yield* runCopilotJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { title: sanitizePrTitle(generated.title), body: generated.body.trim() };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("CopilotTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt(input);
      const generated = yield* runCopilotJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { branch: sanitizeBranchFragment(generated.branch) };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("CopilotTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt(input);
      const generated = yield* runCopilotJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchema,
        modelSelection: input.modelSelection,
      });
      return { title: sanitizeThreadTitle(generated.title) };
    });

  return TextGeneration.TextGeneration.of({
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  });
}
