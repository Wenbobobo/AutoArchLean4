from __future__ import annotations

import json
from pathlib import Path

from archonlab.config import load_config
from archonlab.execution_policy import (
    collect_required_execution_capabilities,
    collect_required_execution_kinds,
)
from archonlab.models import (
    ExecutionCapability,
    ExecutionRequirement,
    ExecutorKind,
    ProviderKind,
    ProviderPoolConfig,
)
from archonlab.services import RunService


def test_load_config_parses_executor_and_provider_sections(tmp_path: Path) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'artifact_root = "./artifacts"\n'
        "dry_run = false\n\n"
        "[executor]\n"
        'kind = "codex_exec"\n'
        'command = "codex"\n'
        'profile = "research"\n'
        "auto_approve = true\n\n"
        "[provider]\n"
        'model = "gpt-5.4-mini"\n'
        'cost_tier = "cheap"\n'
        'endpoint_class = "lab"\n'
        'base_url = "http://localhost:8000/v1"\n'
        'api_key_env = "LAB_KEY"\n',
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.executor.kind is ExecutorKind.CODEX_EXEC
    assert config.executor.profile == "research"
    assert config.executor.auto_approve is True
    assert config.provider.model == "gpt-5.4-mini"
    assert config.provider.cost_tier == "cheap"
    assert config.provider.endpoint_class == "lab"
    assert config.provider.base_url == "http://localhost:8000/v1"
    assert config.provider.api_key_env == "LAB_KEY"


def test_load_config_parses_named_provider_pools(tmp_path: Path) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'artifact_root = "./artifacts"\n'
        "dry_run = false\n\n"
        "[provider]\n"
        'pool = "research"\n'
        "\n"
        "[provider_pool.research]\n"
        "quarantine_seconds = 120\n"
        "max_consecutive_failures = 1\n"
        "\n"
        "[[provider_pool.research.members]]\n"
        'name = "primary"\n'
        'base_url = "http://localhost:8000/v1"\n'
        'model = "gpt-5.4-mini"\n'
        "\n"
        "[[provider_pool.research.members]]\n"
        'name = "backup"\n'
        'base_url = "http://localhost:9000/v1"\n'
        'model = "gpt-5.4"\n',
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.provider.pool == "research"
    assert isinstance(config.provider_pools["research"], ProviderPoolConfig)
    assert [member.name for member in config.provider_pools["research"].members] == [
        "primary",
        "backup",
    ]


def test_run_service_execute_supports_provider_pool_routing(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "archonlab.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = false\n\n"
        "[executor]\n"
        'kind = "dry_run"\n'
        "\n"
        "[provider]\n"
        'pool = "lab"\n'
        "\n"
        "[provider_pool.lab]\n"
        "max_consecutive_failures = 1\n"
        "quarantine_seconds = 60\n"
        "\n"
        "[[provider_pool.lab.members]]\n"
        'name = "member-a"\n'
        'model = "gpt-5.4-mini"\n',
        encoding="utf-8",
    )

    result = RunService(load_config(config_path)).start(dry_run=False)

    assert result.execution is not None
    assert result.execution.telemetry is not None
    assert result.execution.telemetry.provider_pool == "lab"
    assert result.execution.telemetry.provider_member == "member-a"
    assert result.execution.telemetry.retry_count == 0


def test_run_service_execute_uses_configured_executor(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "archonlab.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = false\n\n"
        "[executor]\n"
        'kind = "dry_run"\n',
        encoding="utf-8",
    )

    result = RunService(load_config(config_path)).start(dry_run=False)

    assert result.execution is not None
    assert result.execution.executor is ExecutorKind.DRY_RUN
    assert result.execution.output_path is not None
    execution_payload = json.loads(
        (result.artifact_dir / "execution.json").read_text(encoding="utf-8")
    )
    assert execution_payload["executor"] == "dry_run"
    assert execution_payload["status"] == "completed"


def test_collect_required_execution_kinds_includes_phase_and_task_overrides(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'artifact_root = "./artifacts"\n'
        "dry_run = false\n\n"
        "[executor]\n"
        'kind = "codex_exec"\n'
        "\n"
        "[provider]\n"
        'model = "gpt-5.4-mini"\n'
        'cost_tier = "cheap"\n'
        'endpoint_class = "lab"\n'
        "\n"
        "[phase_executor.plan]\n"
        'kind = "dry_run"\n'
        "\n"
        "[phase_provider.plan]\n"
        'model = "gpt-5.4-nano"\n'
        'cost_tier = "nano"\n'
        'endpoint_class = "fast"\n'
        "\n"
        "[task_matcher.core_focus]\n"
        'phase = "prover"\n'
        'theorem_pattern = "^foo$"\n'
        "\n"
        "[task_executor.core_focus]\n"
        'kind = "openai_compatible"\n',
        encoding="utf-8",
    )

    config = load_config(config_path)
    (
        executor_kinds,
        provider_kinds,
        models,
        cost_tiers,
        endpoint_classes,
    ) = collect_required_execution_kinds(
        executor=config.executor,
        provider=config.provider,
        execution_policy=config.execution_policy,
    )

    assert set(executor_kinds) == {
        ExecutorKind.CODEX_EXEC,
        ExecutorKind.DRY_RUN,
        ExecutorKind.OPENAI_COMPATIBLE,
    }
    assert provider_kinds == [ProviderKind.OPENAI_COMPATIBLE]
    assert set(models) == {"gpt-5.4-mini", "gpt-5.4-nano"}
    assert set(cost_tiers) == {"cheap", "nano"}
    assert set(endpoint_classes) == {"fast", "lab"}


def test_collect_required_execution_capabilities_preserves_valid_tuples(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'artifact_root = "./artifacts"\n'
        "dry_run = false\n\n"
        "[executor]\n"
        'kind = "codex_exec"\n'
        "\n"
        "[provider]\n"
        'model = "gpt-5.4-mini"\n'
        'cost_tier = "cheap"\n'
        'endpoint_class = "lab"\n'
        "\n"
        "[phase_executor.plan]\n"
        'kind = "openai_compatible"\n'
        "\n"
        "[phase_provider.plan]\n"
        'model = "gpt-5.4"\n'
        'cost_tier = "premium"\n'
        'endpoint_class = "priority"\n',
        encoding="utf-8",
    )

    config = load_config(config_path)
    capabilities = collect_required_execution_capabilities(
        executor=config.executor,
        provider=config.provider,
        execution_policy=config.execution_policy,
    )

    assert capabilities == sorted(capabilities, key=lambda capability: capability.capability_id)
    assert capabilities == [
        ExecutionCapability(
            executor_kind=ExecutorKind.CODEX_EXEC,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4-mini",
            cost_tier="cheap",
            endpoint_class="lab",
        ),
        ExecutionCapability(
            executor_kind=ExecutorKind.OPENAI_COMPATIBLE,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4",
            cost_tier="premium",
            endpoint_class="priority",
        ),
    ]


def test_execution_requirement_matches_capability_axes_with_wildcard_worker_dimensions() -> None:
    requirement = ExecutionRequirement(
        executor_kinds=[ExecutorKind.OPENAI_COMPATIBLE],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        models=["gpt-5.4-mini"],
        cost_tiers=["cheap"],
        endpoint_classes=["lab"],
    )

    assert requirement.matches_axes(
        executor_kinds=[ExecutorKind.OPENAI_COMPATIBLE],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        models=["gpt-5.4-mini"],
        cost_tiers=["cheap"],
        endpoint_classes=["lab"],
    )
    assert requirement.matches_axes(
        executor_kinds=[ExecutorKind.OPENAI_COMPATIBLE],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        models=[],
        cost_tiers=[],
        endpoint_classes=[],
    )
    assert requirement.matches_capability(
        ExecutionCapability(
            executor_kind=ExecutorKind.OPENAI_COMPATIBLE,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4-mini",
            cost_tier="cheap",
            endpoint_class="lab",
        )
    )
    assert requirement.matches_capability(
        ExecutionCapability(
            executor_kind=ExecutorKind.OPENAI_COMPATIBLE,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model=None,
            cost_tier=None,
            endpoint_class=None,
        )
    )
    assert requirement.matches_capability(
        ExecutionCapability(
            executor_kind=ExecutorKind.OPENAI_COMPATIBLE,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4",
            cost_tier="premium",
            endpoint_class="lab",
        ),
        allow_capability_wildcards=False,
    ) is False
