from __future__ import annotations

from pathlib import Path

from archonlab.config import load_config
from archonlab.models import (
    AdapterAction,
    ExecutionRequest,
    ExecutionResult,
    ExecutionStatus,
    ExecutorKind,
    TaskSource,
    TaskStatus,
)
from archonlab.services import RunService


def test_run_service_can_route_phase_specific_executor_and_provider(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
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
        'kind = "codex_exec"\n'
        'command = "codex"\n'
        "\n"
        "[provider]\n"
        'model = "baseline-model"\n'
        'base_url = "http://localhost:8000/v1"\n'
        "[phase_executor.plan]\n"
        'kind = "dry_run"\n'
        "\n"
        "[phase_provider.plan]\n"
        'model = "plan-model"\n'
        "\n"
        "[phase_executor.prover]\n"
        'kind = "openai_compatible"\n'
        "\n"
        "[phase_provider.prover]\n"
        'model = "prover-model"\n'
        "\n"
        "[phase_executor.review]\n"
        'kind = "codex_exec"\n'
        "\n"
        "[phase_provider.review]\n"
        'model = "review-model"\n',
        encoding="utf-8",
    )
    config = load_config(config_path)
    assert config.executor.kind is ExecutorKind.CODEX_EXEC
    assert config.provider.model == "baseline-model"

    phase_policy = {
        "plan": {
            "executor_kind": ExecutorKind.DRY_RUN,
            "provider": "dry_run",
            "model": "plan-model",
        },
        "prover": {
            "executor_kind": ExecutorKind.OPENAI_COMPATIBLE,
            "provider": "openai_compatible_http",
            "model": "prover-model",
        },
        "review": {
            "executor_kind": ExecutorKind.CODEX_EXEC,
            "provider": "codex_exec",
            "model": "review-model",
        },
    }

    observed: list[dict[str, object]] = []
    resolved: list[dict[str, object]] = []

    class PhaseDispatchExecutor:
        def execute(
            self,
            request_or_prompt: ExecutionRequest | str,
            system_prompt: str | None = None,
        ) -> ExecutionResult:
            del system_prompt
            assert isinstance(request_or_prompt, ExecutionRequest)
            policy = phase_policy[request_or_prompt.phase]
            observed.append(
                {
                    "phase": request_or_prompt.phase,
                    "executor_kind": policy["executor_kind"],
                    "provider": policy["provider"],
                    "model": policy["model"],
                }
            )
            output_path = (
                request_or_prompt.artifact_dir
                / "executor"
                / f"{request_or_prompt.phase}.txt"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                f"{request_or_prompt.phase}:{policy['model']}",
                encoding="utf-8",
            )
            return ExecutionResult(
                executor=policy["executor_kind"],
                status=ExecutionStatus.COMPLETED,
                response_text=f"{request_or_prompt.phase}:{policy['model']}",
                output_path=output_path,
                metadata={
                    "provider": policy["provider"],
                    "model": policy["model"],
                },
            )

    monkeypatch.setattr(
        "archonlab.services.create_executor",
        lambda **kwargs: resolved.append(
            {
                "resolved_executor": kwargs["executor_config"].kind,
                "resolved_model": kwargs["provider_config"].model,
            }
        )
        or PhaseDispatchExecutor(),
    )

    service = RunService(config)

    for phase in ("plan", "prover", "review"):
        monkeypatch.setattr(
            "archonlab.services.select_next_action",
            lambda *args, phase=phase, **kwargs: AdapterAction(
                phase=phase,
                reason=f"{phase}_policy",
                stage="prover",
                prompt_preview=f"{phase} prompt",
            ),
        )
        result = service.start(dry_run=False)
        assert result.action.phase == phase
        assert result.execution is not None
        assert result.execution.executor is phase_policy[phase]["executor_kind"]
        assert result.execution.provider == phase_policy[phase]["provider"]
        assert result.execution.metadata["model"] == phase_policy[phase]["model"]

    assert [entry["phase"] for entry in observed] == ["plan", "prover", "review"]
    assert {entry["executor_kind"] for entry in observed} == {
        ExecutorKind.DRY_RUN,
        ExecutorKind.OPENAI_COMPATIBLE,
        ExecutorKind.CODEX_EXEC,
    }
    assert resolved[0]["resolved_executor"] is ExecutorKind.DRY_RUN
    assert resolved[0]["resolved_model"] == "plan-model"
    assert resolved[1]["resolved_executor"] is ExecutorKind.OPENAI_COMPATIBLE
    assert resolved[1]["resolved_model"] == "prover-model"
    assert resolved[2]["resolved_executor"] is ExecutorKind.CODEX_EXEC
    assert resolved[2]["resolved_model"] == "review-model"


def test_run_service_can_route_task_specific_executor_and_provider(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
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
        'kind = "openai_compatible"\n'
        "\n"
        "[provider]\n"
        'pool = "lab"\n'
        'model = "baseline-model"\n'
        "[phase_provider.prover]\n"
        'model = "phase-model"\n'
        "\n"
        "[task_matcher.core_focus]\n"
        'phase = "prover"\n'
        'file_path_pattern = "Core\\\\.lean$"\n'
        'theorem_pattern = "^foo$"\n'
        'task_status = "blocked"\n'
        'task_sources = ["lean_declaration"]\n'
        "min_priority = 1\n"
        'blocker_pattern = "contains_sorry"\n'
        "objective_relevant = true\n"
        "\n"
        "[task_executor.core_focus]\n"
        'kind = "codex_exec"\n'
        "\n"
        "[task_provider.core_focus]\n"
        'model = "task-model"\n'
        'member_name = "member-b"\n',
        encoding="utf-8",
    )
    config = load_config(config_path)

    resolved: list[dict[str, object]] = []

    class CapturingExecutor:
        def execute(
            self,
            request_or_prompt: ExecutionRequest | str,
            system_prompt: str | None = None,
        ) -> ExecutionResult:
            del system_prompt
            assert isinstance(request_or_prompt, ExecutionRequest)
            return ExecutionResult(
                executor=resolved[-1]["executor_kind"],
                status=ExecutionStatus.COMPLETED,
                response_text=str(resolved[-1]["model"]),
                metadata={
                    "provider": "captured",
                    "model": resolved[-1]["model"],
                },
            )

    monkeypatch.setattr(
        "archonlab.services.create_executor",
        lambda **kwargs: resolved.append(
            {
                "executor_kind": kwargs["executor_config"].kind,
                "pool": kwargs["provider_config"].pool,
                "member_name": kwargs["provider_config"].member_name,
                "model": kwargs["provider_config"].model,
            }
        )
        or CapturingExecutor(),
    )

    service = RunService(config)

    monkeypatch.setattr(
        "archonlab.services.select_next_action",
        lambda *args, **kwargs: AdapterAction(
            phase="prover",
            reason="task_specific_policy",
            stage="prover",
            prompt_preview="target foo",
            task_id="lean:Core.lean:foo",
            task_title="foo",
            theorem_name="foo",
            file_path=Path("Core.lean"),
            task_status=TaskStatus.BLOCKED,
            task_sources=[TaskSource.LEAN_DECLARATION],
            task_priority=2,
            task_blockers=["contains_sorry"],
            objective_relevant=True,
        ),
    )
    first_result = service.start(dry_run=False)

    monkeypatch.setattr(
        "archonlab.services.select_next_action",
        lambda *args, **kwargs: AdapterAction(
            phase="prover",
            reason="phase_fallback_policy",
            stage="prover",
            prompt_preview="target bar",
            task_id="lean:Aux.lean:bar",
            task_title="bar",
            theorem_name="bar",
            file_path=Path("Aux.lean"),
            task_status=TaskStatus.BLOCKED,
            task_sources=[TaskSource.LEAN_DECLARATION],
            task_priority=0,
            task_blockers=["contains_sorry"],
            objective_relevant=False,
        ),
    )
    second_result = service.start(dry_run=False)

    assert first_result.execution is not None
    assert first_result.execution.executor is ExecutorKind.CODEX_EXEC
    assert first_result.execution.metadata["model"] == "task-model"

    assert second_result.execution is not None
    assert second_result.execution.executor is ExecutorKind.OPENAI_COMPATIBLE
    assert second_result.execution.metadata["model"] == "phase-model"

    assert resolved[0]["executor_kind"] is ExecutorKind.CODEX_EXEC
    assert resolved[0]["pool"] == "lab"
    assert resolved[0]["member_name"] == "member-b"
    assert resolved[0]["model"] == "task-model"
    assert resolved[1]["executor_kind"] is ExecutorKind.OPENAI_COMPATIBLE
    assert resolved[1]["pool"] == "lab"
    assert resolved[1]["member_name"] is None
    assert resolved[1]["model"] == "phase-model"
