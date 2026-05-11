export type ModelSelectionErrorCode = "unknown_model" | "model_disabled" | "model_not_allowed_for_mode" | "provider_not_implemented" | "provider_api_key_missing";

export class ModelSelectionError extends Error {
  readonly code: ModelSelectionErrorCode;

  constructor(code: ModelSelectionErrorCode, message: string) {
    super(message);
    this.name = "ModelSelectionError";
    this.code = code;
  }
}
