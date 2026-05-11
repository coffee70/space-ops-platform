"""Service helpers for AI Engineer model registry YAML (models.local.yaml)."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
import yaml.nodes
from yaml.constructor import ConstructorError

from app.models.schemas import (
    AiEngineerModelConfigFetchResponse,
    AiEngineerModelConfigParsedSummary,
    AiEngineerModelConfigSaveResponse,
    AiEngineerModelConfigValidationError,
    AiEngineerModelConfigValidationResponse,
)

_ENV_VAR_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")
# Provider/apiKeyEnv must reference env vars, not pasted secrets (OpenAI/Anthropic-style keys).
_SK_LIKE_SECRET_PATTERN = re.compile(r"^sk-[a-zA-Z0-9_-]{8,}$")


class _DuplicateKeySafeLoader(yaml.SafeLoader):
    """Reject duplicate YAML mapping keys (safe_load would silently keep the last value)."""


def _construct_duplicate_safe_mapping(loader: yaml.Loader, node: yaml.nodes.MappingNode, deep: bool = False) -> dict[str, Any]:
    loader.flatten_mapping(node)
    mapping: dict[str, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"Duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_DuplicateKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_duplicate_safe_mapping,
)
_PROVIDER_TYPES = frozenset(
    {
        "openai",
        "anthropic",
        "openai-compatible",
        "google",
        "azure-openai",
        "bedrock",
        "vertex",
        "vercel-gateway",
    }
)
_EXECUTION_MODES = frozenset({"read_only", "suggest", "execute", "governed_execute"})
_DATA_BOUNDARIES = frozenset({"external_api", "private_cloud", "local_airgapped", "unknown"})


class AiEngineerModelConfigServiceError(ValueError):
    """Raised when a model config request cannot be fulfilled."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = 400,
        errors: list[AiEngineerModelConfigValidationError] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.errors = errors or []


def _config_file_path() -> Path:
    raw = (os.environ.get("AI_ENGINEER_MODELS_CONFIG_PATH") or os.environ.get("AGENT_RUNTIME_MODELS_CONFIG_PATH") or "").strip()
    if not raw:
        raise AiEngineerModelConfigServiceError(
            "Model registry path is not configured (set AI_ENGINEER_MODELS_CONFIG_PATH or AGENT_RUNTIME_MODELS_CONFIG_PATH).",
            status_code=500,
        )
    return Path(raw).expanduser().resolve()


def _normalize_line_endings(content: str) -> str:
    return content.replace("\r\n", "\n").replace("\r", "\n")


def _yaml_error(message: str, *, type_name: str = "yaml_error") -> AiEngineerModelConfigValidationError:
    return AiEngineerModelConfigValidationError(loc=[], message=message, type=type_name)


def _loc_error(loc: list[str], message: str, *, type_name: str = "value_error") -> AiEngineerModelConfigValidationError:
    return AiEngineerModelConfigValidationError(loc=loc, message=message, type=type_name)


def _load_yaml_object(content: str) -> tuple[dict[str, Any] | None, list[AiEngineerModelConfigValidationError]]:
    try:
        parsed = yaml.load(content, Loader=_DuplicateKeySafeLoader)
    except (yaml.YAMLError, ConstructorError) as exc:
        return None, [_yaml_error(str(exc), type_name=exc.__class__.__name__)]
    if not isinstance(parsed, dict):
        return None, [_yaml_error("Top-level content must be an object", type_name="type_error.object")]
    return parsed, []


def _is_plausible_env_reference(name: str) -> bool:
    return bool(_ENV_VAR_NAME_PATTERN.fullmatch(name.strip()))


def _looks_like_literal_api_key_value(name: str) -> bool:
    """True if apiKeyEnv looks like an embedded provider secret rather than an env var name."""
    s = name.strip()
    if _SK_LIKE_SECRET_PATTERN.fullmatch(s):
        return True
    return False


def _collect_env_references(payload: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    providers = payload.get("providers")
    if isinstance(providers, dict):
        for _pid, prov in providers.items():
            if not isinstance(prov, dict):
                continue
            env_name = prov.get("apiKeyEnv")
            if isinstance(env_name, str) and env_name.strip():
                refs.append(env_name.strip())

    resolvers = payload.get("metadataResolvers")
    if isinstance(resolvers, dict):
        or_conf = resolvers.get("openrouter")
        if isinstance(or_conf, dict):
            env_name = or_conf.get("apiKeyEnv")
            if isinstance(env_name, str) and env_name.strip():
                refs.append(env_name.strip())
    return refs


def _missing_api_key_envs(payload: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    for name in sorted(set(_collect_env_references(payload))):
        val = os.environ.get(name)
        if val is None or (isinstance(val, str) and val.strip() == ""):
            missing.append(name)
    return missing


def _validate_model_registry_payload(payload: dict[str, Any]) -> tuple[AiEngineerModelConfigParsedSummary | None, list[AiEngineerModelConfigValidationError]]:
    errors: list[AiEngineerModelConfigValidationError] = []
    warnings: list[str] = []

    version = payload.get("version")
    if version != 1:
        errors.append(_loc_error(["version"], "version must be 1", type_name="value_error.const"))

    defaults = payload.get("defaults")
    if not isinstance(defaults, dict):
        errors.append(_loc_error(["defaults"], "defaults must be an object", type_name="type_error.object"))
        defaults = {}

    required_default_keys = ("chatModel", "codingModel", "fastModel", "restrictedModel")
    for key in required_default_keys:
        if key not in defaults:
            errors.append(_loc_error(["defaults", key], f"defaults.{key} is required", type_name="value_error.missing"))

    providers = payload.get("providers")
    if not isinstance(providers, dict) or len(providers) == 0:
        errors.append(_loc_error(["providers"], "providers must be a non-empty object", type_name="value_error"))

    if isinstance(providers, dict):
        for pid, prov in providers.items():
            if not isinstance(prov, dict):
                errors.append(_loc_error(["providers", str(pid)], "Provider entry must be an object", type_name="type_error.object"))
                continue
            ptype = prov.get("type")
            if ptype not in _PROVIDER_TYPES:
                errors.append(
                    _loc_error(
                        ["providers", str(pid), "type"],
                        f"Unsupported provider type: {ptype!r}",
                        type_name="value_error.type",
                    )
                )
            display = prov.get("displayName")
            if not isinstance(display, str) or not display.strip():
                errors.append(
                    _loc_error(
                        ["providers", str(pid), "displayName"],
                        "displayName is required",
                        type_name="value_error.missing",
                    )
                )
            base_url = prov.get("baseUrl")
            if base_url is not None and (not isinstance(base_url, str) or not base_url.strip()):
                errors.append(
                    _loc_error(
                        ["providers", str(pid), "baseUrl"],
                        "baseUrl must be a non-empty string when set",
                        type_name="value_error",
                    )
                )
            api_key_env = prov.get("apiKeyEnv")
            if api_key_env is not None:
                if not isinstance(api_key_env, str) or not api_key_env.strip():
                    errors.append(
                        _loc_error(
                            ["providers", str(pid), "apiKeyEnv"],
                            "apiKeyEnv must be a non-empty string when set",
                            type_name="value_error",
                        )
                    )
                else:
                    name = api_key_env.strip()
                    if _looks_like_literal_api_key_value(name):
                        errors.append(
                            _loc_error(
                                ["providers", str(pid), "apiKeyEnv"],
                                "Do not paste provider API keys into YAML; set apiKeyEnv to an environment variable name (for example OPENAI_API_KEY)",
                                type_name="value_error.literal_secret",
                            )
                        )
                    elif not _is_plausible_env_reference(name):
                        errors.append(
                            _loc_error(
                                ["providers", str(pid), "apiKeyEnv"],
                                "apiKeyEnv should look like an environment variable name (e.g. OPENAI_API_KEY), not a secret literal",
                                type_name="value_error.pattern",
                            )
                        )

    models_raw = payload.get("models")
    if not isinstance(models_raw, list) or len(models_raw) == 0:
        errors.append(_loc_error(["models"], "models must be a non-empty array", type_name="value_error"))

    provider_ids: set[str] = set(providers.keys()) if isinstance(providers, dict) else set()
    model_id_set: set[str] = set()

    if isinstance(models_raw, list):
        for index, entry in enumerate(models_raw):
            loc_prefix = ["models", str(index)]
            if not isinstance(entry, dict):
                errors.append(_loc_error(loc_prefix, "Model entry must be an object", type_name="type_error.object"))
                continue
            mid = entry.get("id")
            if not isinstance(mid, str) or not mid.strip():
                errors.append(_loc_error([*loc_prefix, "id"], "Model id is required", type_name="value_error.missing"))
                continue
            mid_s = mid.strip()
            if mid_s in model_id_set:
                errors.append(_loc_error([*loc_prefix, "id"], f"Duplicate model id: {mid_s}", type_name="value_error.duplicate"))
                continue
            model_id_set.add(mid_s)

            pref = entry.get("providerRef")
            if not isinstance(pref, str) or not pref.strip():
                errors.append(_loc_error([*loc_prefix, "providerRef"], "providerRef is required", type_name="value_error.missing"))
            elif pref.strip() not in provider_ids:
                errors.append(
                    _loc_error(
                        [*loc_prefix, "providerRef"],
                        f"Unknown providerRef {pref!r}",
                        type_name="value_error",
                    )
                )

            pmid = entry.get("providerModelId")
            if not isinstance(pmid, str) or not pmid.strip():
                errors.append(
                    _loc_error(
                        [*loc_prefix, "providerModelId"],
                        "providerModelId is required",
                        type_name="value_error.missing",
                    )
                )

            enabled = entry.get("enabled")
            if enabled is not None and not isinstance(enabled, bool):
                errors.append(
                    _loc_error(
                        [*loc_prefix, "enabled"],
                        "enabled must be a boolean when provided",
                        type_name="type_error.bool",
                    )
                )

            gov = entry.get("governance")
            if gov is not None:
                if not isinstance(gov, dict):
                    errors.append(_loc_error([*loc_prefix, "governance"], "governance must be an object", type_name="type_error.object"))
                else:
                    modes = gov.get("allowedModes")
                    if modes is not None:
                        if not isinstance(modes, list):
                            errors.append(
                                _loc_error(
                                    [*loc_prefix, "governance", "allowedModes"],
                                    "allowedModes must be an array",
                                    type_name="type_error",
                                )
                            )
                        else:
                            for mi, mode in enumerate(modes):
                                if mode not in _EXECUTION_MODES:
                                    errors.append(
                                        _loc_error(
                                            [
                                                *loc_prefix,
                                                "governance",
                                                "allowedModes",
                                                str(mi),
                                            ],
                                            f"Invalid execution mode: {mode!r}",
                                            type_name="value_error",
                                        )
                                    )
                    boundary = gov.get("dataBoundary")
                    if boundary is not None and boundary not in _DATA_BOUNDARIES:
                        errors.append(
                            _loc_error(
                                [*loc_prefix, "governance", "dataBoundary"],
                                f"Invalid data boundary: {boundary!r}",
                                type_name="value_error",
                            )
                        )

    if isinstance(defaults, dict) and isinstance(models_raw, list) and model_id_set:
        for key in required_default_keys:
            mid = defaults.get(key)
            if isinstance(mid, str) and mid.strip() and mid.strip() not in model_id_set:
                errors.append(
                    _loc_error(
                        ["defaults", key],
                        f"defaults.{key} ({mid}) is not a configured model id",
                        type_name="value_error",
                    )
                )

        chat_model = defaults.get("chatModel")
        if isinstance(chat_model, str) and chat_model.strip() in model_id_set:
            with_chat = [
                m
                for m in models_raw
                if isinstance(m, dict)
                and isinstance(m.get("defaultFor"), list)
                and "chat" in m.get("defaultFor", [])
            ]
            if len(with_chat) != 1:
                errors.append(
                    _loc_error(
                        ["models"],
                        f'Expected exactly one model with defaultFor containing "chat", found {len(with_chat)}',
                        type_name="value_error",
                    )
                )
            elif with_chat and isinstance(with_chat[0], dict):
                cm = with_chat[0].get("id")
                if isinstance(cm, str) and cm.strip() != chat_model.strip():
                    errors.append(
                        _loc_error(
                            ["defaults", "chatModel"],
                            f'defaults.chatModel must match the model whose defaultFor includes "chat"',
                            type_name="value_error",
                        )
                    )

    if isinstance(models_raw, list):
        enabled_models = [
            m
            for m in models_raw
            if isinstance(m, dict) and m.get("enabled") is True
        ]
        if len(enabled_models) == 0 and len(errors) == 0:
            errors.append(_loc_error(["models"], "At least one model must be enabled", type_name="value_error"))

    resolvers = payload.get("metadataResolvers")
    if resolvers is not None:
        if not isinstance(resolvers, dict):
            errors.append(_loc_error(["metadataResolvers"], "metadataResolvers must be an object", type_name="type_error.object"))
        else:
            or_conf = resolvers.get("openrouter")
            if or_conf is not None:
                if not isinstance(or_conf, dict):
                    errors.append(
                        _loc_error(
                            ["metadataResolvers", "openrouter"],
                            "openrouter resolver must be an object",
                            type_name="type_error.object",
                        )
                    )
                else:
                    for field in ("enabled", "allowUnauthenticated"):
                        if field in or_conf and not isinstance(or_conf[field], bool):
                            errors.append(
                                _loc_error(
                                    ["metadataResolvers", "openrouter", field],
                                    f"{field} must be a boolean",
                                    type_name="type_error.bool",
                                )
                            )
                    if "baseUrl" in or_conf:
                        bu = or_conf["baseUrl"]
                        if not isinstance(bu, str) or not bu.startswith(("http://", "https://")):
                            errors.append(
                                _loc_error(
                                    ["metadataResolvers", "openrouter", "baseUrl"],
                                    "baseUrl must be an http(s) URL",
                                    type_name="value_error.url",
                                )
                            )
                    if "apiKeyEnv" in or_conf:
                        ake = or_conf["apiKeyEnv"]
                        if not isinstance(ake, str) or not ake.strip():
                            errors.append(
                                _loc_error(
                                    ["metadataResolvers", "openrouter", "apiKeyEnv"],
                                    "apiKeyEnv must be a non-empty string when set",
                                    type_name="value_error",
                                )
                            )
                        elif _looks_like_literal_api_key_value(ake.strip()):
                            errors.append(
                                _loc_error(
                                    ["metadataResolvers", "openrouter", "apiKeyEnv"],
                                    "Do not paste provider API keys into YAML; set apiKeyEnv to an environment variable name",
                                    type_name="value_error.literal_secret",
                                )
                            )
                        elif not _is_plausible_env_reference(ake.strip()):
                            errors.append(
                                _loc_error(
                                    ["metadataResolvers", "openrouter", "apiKeyEnv"],
                                    "apiKeyEnv should look like an environment variable name",
                                    type_name="value_error.pattern",
                                )
                            )
                    if "cacheTtlSeconds" in or_conf and (
                        not isinstance(or_conf["cacheTtlSeconds"], int) or or_conf["cacheTtlSeconds"] < 0
                    ):
                        errors.append(
                            _loc_error(
                                ["metadataResolvers", "openrouter", "cacheTtlSeconds"],
                                "cacheTtlSeconds must be a non-negative integer",
                                type_name="value_error",
                            )
                        )

    if errors:
        return None, errors

    assert isinstance(providers, dict) and isinstance(models_raw, list)

    provider_types: list[str] = []
    for prov in providers.values():
        if isinstance(prov, dict):
            t = prov.get("type")
            if isinstance(t, str):
                provider_types.append(t)
    provider_types = sorted(set(provider_types))

    enabled_ct = sum(1 for m in models_raw if isinstance(m, dict) and m.get("enabled") is True)
    chat_default = defaults.get("chatModel") if isinstance(defaults.get("chatModel"), str) else None

    summary = AiEngineerModelConfigParsedSummary(
        provider_count=len(providers),
        model_count=len(models_raw),
        enabled_model_count=enabled_ct,
        default_model_id=chat_default.strip() if isinstance(chat_default, str) and chat_default.strip() else None,
        provider_types=provider_types,
        missing_api_key_envs=_missing_api_key_envs(payload),
        warnings=warnings,
    )
    return summary, []


def validate_model_registry_content(content: str) -> AiEngineerModelConfigValidationResponse:
    parsed_obj, yaml_errors = _load_yaml_object(content)
    if parsed_obj is None:
        return AiEngineerModelConfigValidationResponse(valid=False, parsed=None, errors=yaml_errors)

    summary, errors = _validate_model_registry_payload(parsed_obj)
    if errors:
        return AiEngineerModelConfigValidationResponse(valid=False, parsed=None, errors=errors)
    return AiEngineerModelConfigValidationResponse(valid=True, parsed=summary, errors=[])


def load_model_registry_config() -> AiEngineerModelConfigFetchResponse:
    path = _config_file_path()
    if not path.is_file():
        hint = ""
        example = path.parent / "models.local.yaml.example"
        if example.is_file():
            hint = f" Copy or symlink from {example} if you are bootstrapping a new environment."
        raise AiEngineerModelConfigServiceError(
            f"Model registry file not found at {path}.{hint}",
            status_code=404,
        )

    raw_content = path.read_text(encoding="utf-8")
    validation = validate_model_registry_content(raw_content)
    return AiEngineerModelConfigFetchResponse(
        path=str(path),
        content=raw_content,
        format="yaml",
        parsed=validation.parsed if validation.valid else None,
        validation_errors=list(validation.errors),
    )


def save_model_registry_config(content: str) -> AiEngineerModelConfigSaveResponse:
    path = _config_file_path()
    validation = validate_model_registry_content(content)
    if not validation.valid or validation.parsed is None:
        raise AiEngineerModelConfigServiceError(
            "Model registry validation failed",
            status_code=400,
            errors=list(validation.errors),
        )

    normalized = _normalize_line_endings(content)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(normalized, encoding="utf-8")

    return AiEngineerModelConfigSaveResponse(path=str(path), parsed=validation.parsed, saved=True)
