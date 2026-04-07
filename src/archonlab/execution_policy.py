from __future__ import annotations

from typing import Any

from .models import (
    ActionPhase,
    AppConfig,
    BenchmarkManifest,
    ExecutionPhaseOverride,
    ExecutionPolicy,
    ExecutorConfig,
    ExecutorKind,
    ProviderConfig,
    ProviderKind,
)


def build_execution_policy(
    *,
    base_executor: ExecutorConfig,
    base_provider: ProviderConfig,
    phase_executor_raw: dict[str, object] | None = None,
    phase_provider_raw: dict[str, object] | None = None,
) -> ExecutionPolicy:
    phase_executor_raw = phase_executor_raw or {}
    phase_provider_raw = phase_provider_raw or {}
    phase_names = sorted(set(phase_executor_raw) | set(phase_provider_raw))
    phases: dict[ActionPhase, ExecutionPhaseOverride] = {}
    for phase_name in phase_names:
        phase = ActionPhase(phase_name)
        executor_patch = phase_executor_raw.get(phase_name)
        provider_patch = phase_provider_raw.get(phase_name)
        phases[phase] = ExecutionPhaseOverride(
            executor=(
                apply_executor_patch(base_executor, executor_patch)
                if isinstance(executor_patch, dict)
                else None
            ),
            provider=(
                apply_provider_patch(base_provider, provider_patch)
                if isinstance(provider_patch, dict)
                else None
            ),
        )
    return ExecutionPolicy(phases=phases)


def resolve_phase_configs(
    *,
    executor: ExecutorConfig,
    provider: ProviderConfig,
    execution_policy: ExecutionPolicy,
    phase: str | ActionPhase,
) -> tuple[ExecutorConfig, ProviderConfig]:
    resolved_phase = ActionPhase(str(phase))
    override = execution_policy.phases.get(resolved_phase)
    if override is None:
        return executor, provider
    return override.executor or executor, override.provider or provider


def resolve_app_phase_configs(
    config: AppConfig,
    *,
    phase: str | ActionPhase,
) -> tuple[ExecutorConfig, ProviderConfig]:
    return resolve_phase_configs(
        executor=config.executor,
        provider=config.provider,
        execution_policy=config.execution_policy,
        phase=phase,
    )


def resolve_manifest_phase_configs(
    manifest: BenchmarkManifest,
    *,
    phase: str | ActionPhase,
) -> tuple[ExecutorConfig, ProviderConfig]:
    return resolve_phase_configs(
        executor=manifest.executor,
        provider=manifest.provider,
        execution_policy=manifest.execution_policy,
        phase=phase,
    )


def apply_executor_patch(base: ExecutorConfig, raw_patch: object) -> ExecutorConfig:
    if not isinstance(raw_patch, dict):
        return base
    patch: dict[str, Any] = {}
    if "kind" in raw_patch:
        patch["kind"] = ExecutorKind(str(raw_patch["kind"]))
    for field in [
        "command",
        "profile",
        "auto_approve",
        "skip_git_repo_check",
        "sandbox",
        "color",
        "extra_args",
        "timeout_seconds",
    ]:
        if field in raw_patch:
            patch[field] = raw_patch[field]
    return base.model_copy(update=patch)


def apply_provider_patch(base: ProviderConfig, raw_patch: object) -> ProviderConfig:
    if not isinstance(raw_patch, dict):
        return base
    patch: dict[str, Any] = {}
    if "kind" in raw_patch:
        patch["kind"] = ProviderKind(str(raw_patch["kind"]))
    for field in [
        "model",
        "base_url",
        "api_key_env",
        "endpoint_path",
        "headers",
    ]:
        if field in raw_patch:
            patch[field] = raw_patch[field]
    return base.model_copy(update=patch)
