import type { ModelCapability, ModelMetadata, ModelRegistryEntry, ModelRegistryProvider } from "../../types.js";

const GPT5_FAMILY = /^gpt-5\.(1|2|3|4|5)(-(mini|nano|pro|codex|codex-max))?$/;

export function fallbackMetadataForEntry(input: { entry: ModelRegistryEntry; provider: ModelRegistryProvider }): ModelMetadata {
  const { entry, provider } = input;
  const pm = entry.providerModelId;

  if (provider.type === "openai" && GPT5_FAMILY.test(pm)) {
    const suffix = pm.match(GPT5_FAMILY)?.[3] ?? "";
    let qualityTier: ModelMetadata["qualityTier"] = "frontier";
    let reasoningTier: ModelMetadata["reasoningTier"] = "strong";
    let speedTier: ModelMetadata["speedTier"] = "balanced";
    let costTier: ModelMetadata["costTier"] = "$$$";

    if (suffix === "mini") {
      qualityTier = "advanced";
      reasoningTier = "light";
      speedTier = "fast";
      costTier = "$$";
    } else if (suffix === "nano") {
      qualityTier = "standard";
      reasoningTier = "light";
      speedTier = "fast";
      costTier = "$";
    } else if (suffix === "pro" || suffix === "codex-max") {
      speedTier = "deep";
    }

    const caps: ModelCapability[] = ["text", "vision", "tool-use", "reasoning", "json"];

    return {
      displayName: humanizeOpenAiId(pm),
      providerDisplayName: provider.displayName,
      description: `OpenAI ${pm} model configured for this stack.`,
      contextWindow: null,
      maxOutputTokens: null,
      inputModalities: ["text", "image"],
      outputModalities: ["text"],
      supportedParameters: ["tools", "reasoning", "response_format", "structured_outputs"],
      capabilities: caps,
      pricing: {
        inputPerMillionTokens: null,
        outputPerMillionTokens: null,
        currency: "USD",
      },
      qualityTier,
      costTier,
      speedTier,
      reasoningTier,
      metadataSources: ["fallback-pattern"],
    };
  }

  if (provider.type === "anthropic" && /^claude-(sonnet|opus|haiku)-/.test(pm)) {
    return {
      displayName: humanizeAnthropicId(pm),
      providerDisplayName: provider.displayName,
      description: `Anthropic ${pm} model configured for this stack.`,
      contextWindow: null,
      maxOutputTokens: null,
      inputModalities: ["text", "image"],
      outputModalities: ["text"],
      supportedParameters: ["tools"],
      capabilities: ["text", "vision", "tool-use", "reasoning", "json"],
      pricing: {
        inputPerMillionTokens: null,
        outputPerMillionTokens: null,
        currency: "USD",
      },
      qualityTier: "advanced",
      costTier: "unknown",
      speedTier: "balanced",
      reasoningTier: "strong",
      metadataSources: ["fallback-pattern"],
    };
  }

  return {
    displayName: entry.metadataOverrides?.displayName ?? humanizeGenericId(pm),
    providerDisplayName: provider.displayName,
    description: entry.metadataOverrides?.description ?? `Model ${entry.id} configured for this stack.`,
    contextWindow: null,
    maxOutputTokens: null,
    inputModalities: ["text"],
    outputModalities: ["text"],
    supportedParameters: [],
    capabilities: ["text"],
    pricing: {
      inputPerMillionTokens: null,
      outputPerMillionTokens: null,
      currency: "USD",
    },
    qualityTier: "unknown",
    costTier: "unknown",
    speedTier: "unknown",
    reasoningTier: "unknown",
    metadataSources: ["fallback-pattern"],
  };
}

function humanizeOpenAiId(pm: string): string {
  const base = pm.replace(/^gpt-5\./, "GPT-5.").replace(/-/g, " ");
  return base
    .split(" ")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ")
    .replace(/Codex Max/i, "Codex Max");
}

function humanizeAnthropicId(pm: string): string {
  const words: string[] = [];
  const segments = pm.split("-");
  for (let index = 0; index < segments.length; index += 1) {
    const segment = segments[index] ?? "";
    if (/^\d+$/.test(segment)) {
      const versionSegments = [segment];
      while (/^\d+$/.test(segments[index + 1] ?? "")) {
        index += 1;
        versionSegments.push(segments[index]);
      }
      words.push(versionSegments.join("."));
    } else {
      words.push(segment.charAt(0).toUpperCase() + segment.slice(1));
    }
  }
  return words.join(" ");
}

function humanizeGenericId(pm: string): string {
  return pm.length > 0 ? pm.charAt(0).toUpperCase() + pm.slice(1) : pm;
}
