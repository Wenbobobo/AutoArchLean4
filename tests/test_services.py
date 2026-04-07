from __future__ import annotations

import json
from pathlib import Path

from archonlab.config import load_config
from archonlab.control import ControlService
from archonlab.models import ProjectSession, SessionStatus, WorkflowMode
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

    assert first.action.reason == "bootstrap_first_iteration"
    assert second.action.reason == "bootstrap_first_iteration"
    assert third.action.reason == "supervisor_repeated_no_progress"

    summary = json.loads((third.artifact_dir / "supervisor.json").read_text(encoding="utf-8"))
    assert summary["reason"] == "repeated_no_progress"


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
