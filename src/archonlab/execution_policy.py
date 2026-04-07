from __future__ import annotations

import re
from typing import Any

from .models import (
    ActionPhase,
    AdapterAction,
    AppConfig,
    BenchmarkManifest,
    ExecutionCapability,
    ExecutionPhaseOverride,
    ExecutionPolicy,
    ExecutionTaskMatcher,
    ExecutionTaskOverride,
    ExecutorConfig,
    ExecutorKind,
    ProviderConfig,
    ProviderKind,
    TaskSource,
    TaskStatus,
)


def build_execution_policy(
    *,
    base_executor: ExecutorConfig,
    base_provider: ProviderConfig,
    phase_executor_raw: dict[str, object] | None = None,
    phase_provider_raw: dict[str, object] | None = None,
    task_matcher_raw: dict[str, object] | None = None,
    task_executor_raw: dict[str, object] | None = None,
    task_provider_raw: dict[str, object] | None = None,
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
    task_matcher_raw = task_matcher_raw or {}
    task_executor_raw = task_executor_raw or {}
    task_provider_raw = task_provider_raw or {}
    task_rules: list[ExecutionTaskOverride] = []
    for rule_name in _ordered_union(
        task_matcher_raw,
        task_executor_raw,
        task_provider_raw,
    ):
        matcher_patch = task_matcher_raw.get(rule_name)
        executor_patch = task_executor_raw.get(rule_name)
        provider_patch = task_provider_raw.get(rule_name)
        task_rules.append(
            ExecutionTaskOverride(
                name=rule_name,
                matcher=build_task_matcher(rule_name, matcher_patch),
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
        )
    return ExecutionPolicy(phases=phases, task_rules=task_rules)


def resolve_phase_configs(
    *,
    executor: ExecutorConfig,
    provider: ProviderConfig,
    execution_policy: ExecutionPolicy,
    phase: str | ActionPhase,
    action: AdapterAction | None = None,
) -> tuple[ExecutorConfig, ProviderConfig]:
    resolved_phase = ActionPhase(str(phase))
    resolved_executor = executor
    resolved_provider = provider
    override = execution_policy.phases.get(resolved_phase)
    if override is not None:
        resolved_executor = override.executor or resolved_executor
        resolved_provider = override.provider or resolved_provider
    task_override = find_matching_task_override(
        execution_policy=execution_policy,
        phase=resolved_phase,
        action=action,
    )
    if task_override is not None:
        resolved_executor = task_override.executor or resolved_executor
        resolved_provider = task_override.provider or resolved_provider
    return resolved_executor, resolved_provider


def resolve_app_phase_configs(
    config: AppConfig,
    *,
    phase: str | ActionPhase,
    action: AdapterAction | None = None,
) -> tuple[ExecutorConfig, ProviderConfig]:
    return resolve_phase_configs(
        executor=config.executor,
        provider=config.provider,
        execution_policy=config.execution_policy,
        phase=phase,
        action=action,
    )


def resolve_manifest_phase_configs(
    manifest: BenchmarkManifest,
    *,
    phase: str | ActionPhase,
    action: AdapterAction | None = None,
) -> tuple[ExecutorConfig, ProviderConfig]:
    return resolve_phase_configs(
        executor=manifest.executor,
        provider=manifest.provider,
        execution_policy=manifest.execution_policy,
        phase=phase,
        action=action,
    )


def collect_required_execution_kinds(
    *,
    executor: ExecutorConfig,
    provider: ProviderConfig,
    execution_policy: ExecutionPolicy,
) -> tuple[list[ExecutorKind], list[ProviderKind], list[str], list[str], list[str]]:
    capabilities = collect_required_execution_capabilities(
        executor=executor,
        provider=provider,
        execution_policy=execution_policy,
    )
    executor_kinds = {capability.executor_kind for capability in capabilities}
    provider_kinds = {capability.provider_kind for capability in capabilities}
    models = {
        capability.model
        for capability in capabilities
        if capability.model is not None
    }
    cost_tiers = {
        capability.cost_tier
        for capability in capabilities
        if capability.cost_tier is not None
    }
    endpoint_classes = {
        capability.endpoint_class
        for capability in capabilities
        if capability.endpoint_class is not None
    }
    return (
        sorted(executor_kinds),
        sorted(provider_kinds),
        sorted(models),
        sorted(cost_tiers),
        sorted(endpoint_classes),
    )


def collect_required_execution_capabilities(
    *,
    executor: ExecutorConfig,
    provider: ProviderConfig,
    execution_policy: ExecutionPolicy,
) -> list[ExecutionCapability]:
    capabilities: dict[str, ExecutionCapability] = {}

    def register(
        resolved_executor: ExecutorConfig,
        resolved_provider: ProviderConfig,
    ) -> None:
        capability = ExecutionCapability.from_configs(
            executor=resolved_executor,
            provider=resolved_provider,
        )
        capabilities[capability.capability_id] = capability

    register(executor, provider)
    phase_variants: dict[ActionPhase, tuple[ExecutorConfig, ProviderConfig]] = {}
    for phase in (ActionPhase.PLAN, ActionPhase.PROVER, ActionPhase.REVIEW):
        phase_executor, phase_provider = resolve_phase_configs(
            executor=executor,
            provider=provider,
            execution_policy=execution_policy,
            phase=phase,
            action=None,
        )
        phase_variants[phase] = (phase_executor, phase_provider)
        register(phase_executor, phase_provider)

    fallback_phases = tuple(phase_variants) or (
        ActionPhase.PLAN,
        ActionPhase.PROVER,
        ActionPhase.REVIEW,
    )
    for rule in execution_policy.task_rules:
        candidate_phases = (
            (rule.matcher.phase,)
            if rule.matcher.phase is not None
            else fallback_phases
        )
        for phase in candidate_phases:
            base_executor, base_provider = phase_variants.get(phase, (executor, provider))
            register(
                rule.executor or base_executor,
                rule.provider or base_provider,
            )

    return sorted(capabilities.values(), key=lambda capability: capability.capability_id)


def build_task_matcher(name: str, raw_matcher: object) -> ExecutionTaskMatcher:
    if not isinstance(raw_matcher, dict):
        raw_matcher = {}
    task_sources = raw_matcher.get("task_sources", [])
    blockers = raw_matcher.get("blockers", [])
    if "task_source" in raw_matcher:
        task_sources = [raw_matcher["task_source"]]
    elif not isinstance(task_sources, list):
        task_sources = [task_sources]
    if "blocker" in raw_matcher:
        blockers = [raw_matcher["blocker"]]
    elif not isinstance(blockers, list):
        blockers = [blockers]
    return ExecutionTaskMatcher(
        name=name,
        phase=(
            ActionPhase(str(raw_matcher["phase"]))
            if "phase" in raw_matcher
            else None
        ),
        task_id=raw_matcher.get("task_id"),
        task_title=raw_matcher.get("task_title"),
        theorem_name=raw_matcher.get("theorem_name"),
        file_path=raw_matcher.get("file_path"),
        task_status=(
            TaskStatus(str(raw_matcher["task_status"]))
            if "task_status" in raw_matcher
            else None
        ),
        task_sources=[TaskSource(str(source)) for source in task_sources],
        min_priority=(
            int(raw_matcher["min_priority"])
            if "min_priority" in raw_matcher
            else None
        ),
        max_priority=(
            int(raw_matcher["max_priority"])
            if "max_priority" in raw_matcher
            else None
        ),
        blockers=[str(blocker) for blocker in blockers],
        blocker_pattern=raw_matcher.get("blocker_pattern"),
        objective_relevant=(
            bool(raw_matcher["objective_relevant"])
            if "objective_relevant" in raw_matcher
            else None
        ),
        task_id_pattern=raw_matcher.get("task_id_pattern"),
        task_title_pattern=raw_matcher.get("task_title_pattern"),
        theorem_pattern=raw_matcher.get("theorem_pattern"),
        file_path_pattern=raw_matcher.get("file_path_pattern"),
    )


def find_matching_task_override(
    *,
    execution_policy: ExecutionPolicy,
    phase: ActionPhase,
    action: AdapterAction | None,
) -> ExecutionTaskOverride | None:
    for rule in execution_policy.task_rules:
        if matches_task_rule(rule.matcher, phase=phase, action=action):
            return rule
    return None


def matches_task_rule(
    matcher: ExecutionTaskMatcher,
    *,
    phase: ActionPhase,
    action: AdapterAction | None,
) -> bool:
    if matcher.phase is not None and matcher.phase is not phase:
        return False
    if action is None:
        return _matcher_has_no_task_constraints(matcher)
    if matcher.task_id is not None and matcher.task_id != action.task_id:
        return False
    if matcher.task_title is not None and matcher.task_title != action.task_title:
        return False
    if matcher.theorem_name is not None and matcher.theorem_name != action.theorem_name:
        return False
    if matcher.file_path is not None and _normalize_path(matcher.file_path) != _normalize_path(
        action.file_path
    ):
        return False
    if matcher.task_status is not None and matcher.task_status is not action.task_status:
        return False
    if matcher.task_sources and not set(matcher.task_sources).issubset(set(action.task_sources)):
        return False
    if matcher.min_priority is not None and (
        action.task_priority is None or action.task_priority < matcher.min_priority
    ):
        return False
    if matcher.max_priority is not None and (
        action.task_priority is None or action.task_priority > matcher.max_priority
    ):
        return False
    if matcher.blockers and not set(matcher.blockers).issubset(set(action.task_blockers)):
        return False
    if matcher.blocker_pattern is not None and not any(
        re.search(matcher.blocker_pattern, blocker) is not None
        for blocker in action.task_blockers
    ):
        return False
    if (
        matcher.objective_relevant is not None
        and matcher.objective_relevant is not action.objective_relevant
    ):
        return False
    if not _matches_pattern(matcher.task_id_pattern, action.task_id):
        return False
    if not _matches_pattern(matcher.task_title_pattern, action.task_title):
        return False
    if not _matches_pattern(matcher.theorem_pattern, action.theorem_name):
        return False
    return _matches_pattern(
        matcher.file_path_pattern,
        _normalize_path(action.file_path),
    )


def _ordered_union(*collections: dict[str, object]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for collection in collections:
        for name in collection:
            if name in seen:
                continue
            seen.add(name)
            names.append(name)
    return names


def _matcher_has_no_task_constraints(matcher: ExecutionTaskMatcher) -> bool:
    return all(
        value is None or value == []
        for value in (
            matcher.task_id,
            matcher.task_title,
            matcher.theorem_name,
            matcher.file_path,
            matcher.task_status,
            matcher.task_sources,
            matcher.min_priority,
            matcher.max_priority,
            matcher.blockers,
            matcher.blocker_pattern,
            matcher.objective_relevant,
            matcher.task_id_pattern,
            matcher.task_title_pattern,
            matcher.theorem_pattern,
            matcher.file_path_pattern,
        )
    )


def _normalize_path(path: object) -> str | None:
    if path is None:
        return None
    return str(path).replace("\\", "/")


def _matches_pattern(pattern: str | None, value: str | None) -> bool:
    if pattern is None:
        return True
    if value is None:
        return False
    return re.search(pattern, value) is not None


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
        "pool",
        "member_name",
        "model",
        "cost_tier",
        "endpoint_class",
        "base_url",
        "api_key_env",
        "endpoint_path",
        "headers",
        "input_cost_per_1k_tokens",
        "output_cost_per_1k_tokens",
    ]:
        if field in raw_patch:
            patch[field] = raw_patch[field]
    return base.model_copy(update=patch)
