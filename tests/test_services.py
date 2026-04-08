from __future__ import annotations

import json
from pathlib import Path

import pytest

from archonlab.config import load_config
from archonlab.control import ControlService
from archonlab.models import (
    LeanAnalysisSnapshot,
    LeanDeclaration,
    LeanDiagnostic,
    LeanProofGap,
    ProjectSession,
    SessionStatus,
    WorkflowMode,
)
from archonlab.services import RunService


def test_run_service_uses_history_to_reroute_repeated_no_progress(
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
        "dry_run = true\n",
        encoding="utf-8",
    )

    service = RunService(load_config(config_path))
    first = service.start(dry_run=True)
    second = service.start(dry_run=True)
    third = service.start(dry_run=True)
    fourth = service.start(dry_run=True)

    assert first.action.reason == "bootstrap_first_iteration"
    assert second.action.reason == "bootstrap_first_iteration"
    assert third.action.reason == "bootstrap_first_iteration"
    assert fourth.action.reason == "supervisor_repeated_no_progress"

    summary = json.loads((fourth.artifact_dir / "supervisor.json").read_text(encoding="utf-8"))
    assert summary["reason"] == "repeated_no_progress"


def test_run_service_prefers_prover_for_pure_sorry_blocker_with_review_history(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = tmp_path / "archonlab.toml"
    (fake_archon_project / "Core.lean").write_text(
        "theorem foo : True := by\n"
        "  sorry\n",
        encoding="utf-8",
    )
    (fake_archon_project / ".archon" / "proof-journal" / "sessions" / "session-1").mkdir(
        parents=True
    )
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n",
        encoding="utf-8",
    )

    preview = RunService(load_config(config_path)).preview()

    assert preview.supervisor.action.name.lower() == "continue"
    assert preview.supervisor.reason.name.lower() == "healthy"
    assert preview.action.phase == "prover"
    assert preview.action.reason == "task_graph_focus"


def test_run_service_respects_control_workflow_override_and_cleared_spec(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    workflow_spec = tmp_path / "workflow.toml"
    workflow_spec.write_text(
        "[workflow]\n"
        'name = "force-review"\n'
        "\n"
        "[[rules]]\n"
        'name = "force_review_without_sessions"\n'
        "when_has_review_sessions = false\n"
        'phase = "review"\n'
        'reason = "forced_review"\n',
        encoding="utf-8",
    )
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'workflow_spec = "{workflow_spec}"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n",
        encoding="utf-8",
    )

    app_config = load_config(config_path)
    ControlService(app_config.run.artifact_root).set_workflow(
        app_config.project,
        workflow=WorkflowMode.FIXED_LOOP,
        clear_workflow_spec=True,
    )

    service = RunService(app_config)
    preview = service.preview()

    assert preview.workflow is WorkflowMode.FIXED_LOOP
    assert preview.workflow_spec_path is None
    assert preview.action.reason == "fixed_loop_baseline"

    result = service.start(dry_run=True)
    runs = service.event_store.list_runs(limit=1)
    assert runs[0].workflow is WorkflowMode.FIXED_LOOP
    events = service.event_store.get_run_events(result.run_id)
    started = next(event for event in events if event.kind == "run.started")
    assert started.payload["workflow"] == "fixed_loop"
    assert started.payload["workflow_spec_path"] is None


def test_run_service_uses_control_workflow_override(
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
        "dry_run = true\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    ControlService(artifact_root).set_workflow(
        config.project,
        workflow=WorkflowMode.FIXED_LOOP,
        clear_workflow_spec=True,
    )

    service = RunService(config)
    preview = service.preview()

    assert preview.workflow is WorkflowMode.FIXED_LOOP
    assert preview.workflow_spec_path is None
    assert preview.action.reason == "fixed_loop_baseline"

    result = service.start(dry_run=True)
    run_summary = json.loads((result.artifact_dir / "run-summary.json").read_text(encoding="utf-8"))
    assert run_summary["effective_workflow"] == "fixed_loop"
    assert run_summary["effective_workflow_spec_path"] is None


def test_run_service_uses_control_workflow_spec_override(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "archonlab.toml"
    artifact_root = tmp_path / "artifacts"
    workflow_spec = tmp_path / "workflow-override.toml"
    workflow_spec.write_text(
        "[workflow]\n"
        'name = "control-override"\n'
        'description = "Override planning reason."\n\n'
        "[[rules]]\n"
        'name = "rewrite_plan_reason"\n'
        'when_phase = "plan"\n'
        "when_has_review_sessions = false\n"
        'phase = "plan"\n'
        'reason = "from_control_spec"\n',
        encoding="utf-8",
    )
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n",
        encoding="utf-8",
    )

    config = load_config(config_path)
    ControlService(artifact_root).set_workflow(
        config.project,
        workflow_spec_override=workflow_spec,
    )

    preview = RunService(config).preview()

    assert preview.workflow is WorkflowMode.ADAPTIVE_LOOP
    assert preview.workflow_spec_path == workflow_spec.resolve()
    assert preview.workflow_spec is not None
    assert preview.workflow_spec.name == "control-override"
    assert preview.action.reason == "from_control_spec"


def test_run_service_preview_reuses_analysis_and_persists_artifact(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    (fake_archon_project / "Core.lean").write_text(
        "theorem helper : True := by\n"
        "  trivial\n\n"
        "theorem foo : True := by\n"
        "  exact helper\n",
        encoding="utf-8",
    )
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
        "dry_run = true\n",
        encoding="utf-8",
    )

    class CountingAnalyzer:
        def __init__(self) -> None:
            self.calls = 0

        def analyze(self, *, project_path: Path, archon_path: Path) -> LeanAnalysisSnapshot:
            del archon_path
            self.calls += 1
            return LeanAnalysisSnapshot(
                project_id=project_path.name,
                project_path=project_path,
                backend="structured-test",
                lean_file_count=1,
                theorem_count=2,
                sorry_count=0,
                axiom_count=0,
                declarations=[
                    LeanDeclaration(
                        name="helper",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        dependencies=[],
                    ),
                    LeanDeclaration(
                        name="foo",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        dependencies=["helper"],
                    ),
                ],
                proof_gaps=[
                    LeanProofGap(
                        kind="unsolved_goal",
                        theorem_name="foo",
                        file_path=Path("Core.lean"),
                        message="stuck on final goal",
                    )
                ],
                diagnostics=[
                    LeanDiagnostic(
                        severity="error",
                        message="type mismatch",
                        theorem_name="foo",
                        file_path=Path("Core.lean"),
                        code="lean.type_mismatch",
                    )
                ],
            )

    analyzer = CountingAnalyzer()
    monkeypatch.setattr("archonlab.services.build_lean_analyzer", lambda config: analyzer)

    service = RunService(load_config(config_path))
    preview = service.preview()

    assert analyzer.calls == 1
    assert preview.analysis.backend == "structured-test"
    assert preview.snapshot.analysis_backend == "structured-test"
    assert preview.snapshot.proof_gap_count == 1
    assert preview.snapshot.diagnostic_count == 1
    foo_node = next(node for node in preview.task_graph.nodes if node.theorem_name == "foo")
    assert foo_node.blockers == ["proof_gap:unsolved_goal", "diagnostic:lean.type_mismatch"]

    result = service.start(dry_run=True)
    assert analyzer.calls == 2
    analysis_path = result.artifact_dir / "lean-analysis.json"
    assert analysis_path.exists()

    analysis_payload = json.loads(analysis_path.read_text(encoding="utf-8"))
    run_summary = json.loads((result.artifact_dir / "run-summary.json").read_text(encoding="utf-8"))
    events = service.event_store.get_run_events(result.run_id)
    analysis_event = next(event for event in events if event.kind == "lean_analysis.generated")

    assert analysis_payload["backend"] == "structured-test"
    assert analysis_payload["proof_gaps"][0]["kind"] == "unsolved_goal"
    assert analysis_payload["diagnostics"][0]["code"] == "lean.type_mismatch"
    assert run_summary["analysis"]["backend"] == "structured-test"
    assert run_summary["analysis"]["proof_gaps"][0]["kind"] == "unsolved_goal"
    assert analysis_event.payload["analysis_path"] == str(analysis_path)
    assert analysis_event.payload["backend"] == "structured-test"
    assert analysis_event.payload["proof_gap_count"] == 1
    assert analysis_event.payload["diagnostic_count"] == 1


def test_run_service_loop_records_project_session_iterations(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
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
        "dry_run = true\n"
        "max_iterations = 2\n",
        encoding="utf-8",
    )

    service = RunService(load_config(config_path))
    result = service.run_loop(dry_run=True, max_iterations=2, workspace_id="standalone")
    session = service.event_store.get_session(result.session_id)
    iterations = service.event_store.list_session_iterations(result.session_id)

    assert result.completed_iterations == 2
    assert result.status is SessionStatus.PAUSED
    assert session is not None
    assert session.last_run_id is not None
    assert len(iterations) == 2
    assert all(iteration.run_id is not None for iteration in iterations)


def test_run_service_loop_persists_summary_and_iteration_artifacts(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
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
        "dry_run = true\n"
        "max_iterations = 2\n",
        encoding="utf-8",
    )

    service = RunService(load_config(config_path))
    result = service.run_loop(
        dry_run=True,
        max_iterations=2,
        workspace_id="standalone",
        note="persist-me",
        config_path=config_path,
    )

    assert result.loop_run_id.startswith("run-loop-")
    assert result.config_path == config_path.resolve()
    assert result.artifact_dir is not None
    assert result.artifact_dir.exists()

    summary_path = result.artifact_dir / "summary.json"
    request_path = result.artifact_dir / "request.json"
    config_snapshot_path = result.artifact_dir / "archonlab.toml"
    iteration_paths = sorted(result.artifact_dir.glob("iteration-*.json"))

    assert summary_path.exists()
    assert request_path.exists()
    assert config_snapshot_path.exists()
    assert len(iteration_paths) == 2

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    request_payload = json.loads(request_path.read_text(encoding="utf-8"))
    first_iteration = json.loads(iteration_paths[0].read_text(encoding="utf-8"))
    persisted = service.event_store.get_run_loop_run(result.loop_run_id)

    assert summary["loop_run_id"] == result.loop_run_id
    assert summary["artifact_dir"] == str(result.artifact_dir)
    assert summary["completed_iterations"] == 2
    assert request_payload["workspace_id"] == "standalone"
    assert request_payload["note"] == "persist-me"
    assert first_iteration["iteration_index"] == 1
    assert first_iteration["run_id"] is not None
    assert persisted is not None
    assert persisted.project_id == "demo"
    assert persisted.run_ids == result.run_ids


def test_run_service_loop_persists_failure_artifacts_before_reraising(
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
        "dry_run = true\n"
        "max_iterations = 2\n",
        encoding="utf-8",
    )

    service = RunService(load_config(config_path))

    def fail_start(*, dry_run: bool | None = None) -> object:
        del dry_run
        raise RuntimeError("loop exploded")

    monkeypatch.setattr(service, "start", fail_start)

    try:
        service.run_loop(
            dry_run=True,
            max_iterations=2,
            workspace_id="standalone",
            config_path=config_path,
        )
    except RuntimeError as error:
        assert str(error) == "loop exploded"
    else:
        raise AssertionError("RunService.run_loop should re-raise loop failures")

    loop_runs = service.event_store.list_run_loop_runs(project_id="demo", limit=10)
    assert len(loop_runs) == 1
    failed_run = loop_runs[0]
    assert failed_run.stop_reason == "run_failed"
    assert failed_run.error_message == "loop exploded"
    assert failed_run.artifact_dir is not None

    summary_path = failed_run.artifact_dir / "summary.json"
    error_path = failed_run.artifact_dir / "error.json"
    assert summary_path.exists()
    assert error_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    error_payload = json.loads(error_path.read_text(encoding="utf-8"))
    assert summary["stop_reason"] == "run_failed"
    assert summary["error_message"] == "loop exploded"
    assert error_payload["error"] == "loop exploded"


def test_run_service_session_quantum_advances_one_iteration_at_a_time(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
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
        "dry_run = true\n"
        "max_iterations = 3\n",
        encoding="utf-8",
    )

    service = RunService(load_config(config_path))
    session = ProjectSession(
        session_id="session-demo-1",
        workspace_id="standalone",
        project_id="demo",
        dry_run=True,
        max_iterations=2,
    )
    service.event_store.register_session(session)

    first = service.run_session_quantum(session.session_id)
    second = service.run_session_quantum(session.session_id)
    stored = service.event_store.get_session(session.session_id)
    iterations = service.event_store.list_session_iterations(session.session_id)

    assert first.completed_iterations == 1
    assert first.status is SessionStatus.PENDING
    assert first.run_id is not None
    assert first.stop_reason == "quantum_complete"
    assert second.completed_iterations == 2
    assert second.status is SessionStatus.PAUSED
    assert second.stop_reason == "max_iterations_reached"
    assert stored is not None
    assert stored.status is SessionStatus.PAUSED
    assert stored.last_stop_reason == "max_iterations_reached"
    assert len(iterations) == 2
    assert all(iteration.run_id is not None for iteration in iterations)


def test_run_service_session_quantum_control_pause_is_resumable_and_budget_neutral(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
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
        "dry_run = true\n"
        "max_iterations = 3\n",
        encoding="utf-8",
    )

    service = RunService(load_config(config_path))
    ControlService(artifact_root).pause(service.config.project, reason="operator_pause")
    session = ProjectSession(
        session_id="session-demo-control-pause",
        workspace_id="standalone",
        project_id="demo",
        dry_run=True,
        max_iterations=3,
    )
    service.event_store.register_session(session)

    result = service.run_session_quantum(session.session_id)
    stored = service.event_store.get_session(session.session_id)
    iterations = service.event_store.list_session_iterations(session.session_id)

    assert result.status is SessionStatus.PAUSED
    assert result.completed_iterations == 0
    assert result.run_id is not None
    assert result.action_reason == "control_paused"
    assert result.stop_reason == "stop:control_paused"
    assert stored is not None
    assert stored.status is SessionStatus.PAUSED
    assert stored.completed_iterations == 0
    assert stored.last_stop_reason == "stop:control_paused"
    assert stored.last_run_id == result.run_id
    assert iterations == []


def test_run_service_session_quantum_failure_sets_cooldown_and_failure_streak(
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
        "dry_run = true\n"
        "max_iterations = 3\n",
        encoding="utf-8",
    )

    service = RunService(load_config(config_path))
    session = ProjectSession(
        session_id="session-demo-failure",
        workspace_id="standalone",
        project_id="demo",
        dry_run=True,
        max_iterations=3,
        max_consecutive_failures=2,
        failure_cooldown_seconds=60,
    )
    service.event_store.register_session(session)

    def fail_start(*, dry_run: bool | None = None) -> object:
        del dry_run
        raise RuntimeError("quantum exploded")

    monkeypatch.setattr(service, "start", fail_start)

    with pytest.raises(RuntimeError, match="quantum exploded"):
        service.run_session_quantum(session.session_id)

    stored = service.event_store.get_session(session.session_id)
    iterations = service.event_store.list_session_iterations(session.session_id)

    assert stored is not None
    assert stored.status is SessionStatus.FAILED
    assert stored.consecutive_failures == 1
    assert stored.max_consecutive_failures == 2
    assert stored.failure_cooldown_seconds == 60
    assert stored.error_message == "quantum exploded"
    assert stored.last_failure_at is not None
    assert stored.cooldown_until is not None
    assert stored.cooldown_until > stored.last_failure_at
    assert iterations == []


def test_run_service_loop_control_pause_does_not_consume_session_budget(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
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
        "dry_run = true\n"
        "max_iterations = 2\n",
        encoding="utf-8",
    )

    service = RunService(load_config(config_path))
    ControlService(artifact_root).pause(service.config.project, reason="operator_pause")

    result = service.run_loop(dry_run=True, max_iterations=2, workspace_id="standalone")
    session = service.event_store.get_session(result.session_id)
    iterations = service.event_store.list_session_iterations(result.session_id)

    assert result.status is SessionStatus.PAUSED
    assert result.completed_iterations == 0
    assert result.stop_reason == "stop:control_paused"
    assert len(result.run_ids) == 1
    assert session is not None
    assert session.status is SessionStatus.PAUSED
    assert session.completed_iterations == 0
    assert session.last_stop_reason == "stop:control_paused"
    assert iterations == []
