from __future__ import annotations

from pathlib import Path

from archonlab.events import EventStore
from archonlab.models import QueueJobStatus, SessionStatus
from archonlab.queue import QueueStore
from archonlab.workspace_loop import WorkspaceLoopController


def _write_workspace_config(
    path: Path,
    *,
    artifact_root: Path,
    project_path: Path,
    archon_path: Path,
) -> Path:
    path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n"
        "max_parallel = 2\n\n"
        "[[projects]]\n"
        'id = "alpha"\n'
        f'project_path = "{project_path}"\n'
        f'archon_path = "{archon_path}"\n',
        encoding="utf-8",
    )
    return path


def test_workspace_loop_controller_restarts_completed_project_in_followup_cycles(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )

    def fake_run(self, **kwargs) -> object:
        del kwargs
        store = EventStore(artifact_root / "archonlab.db")
        session = store.list_sessions(
            workspace_id="demo-workspace",
            project_id="alpha",
            limit=1,
        )[0]
        job = self.queue_store.get_active_session_job(session.session_id)
        assert job is not None
        self.queue_store.finish_job(job.id, status=QueueJobStatus.COMPLETED)
        store.update_session(
            session.session_id,
            status=SessionStatus.COMPLETED,
            completed_iterations=session.max_iterations,
            stop_reason="max_iterations_reached",
            clear_error_message=True,
            clear_owner_claim=True,
        )
        return self.result_model(
            cycles_completed=1,
            stop_reason="queue_drained",
            total_processed_jobs=1,
            total_paused_jobs=0,
            total_failed_jobs=0,
            total_workers_launched=1,
            cycles=[],
            final_plan=self.queue_store.plan_fleet(),
        )

    monkeypatch.setattr("archonlab.workspace_loop.FleetController.run", fake_run)

    controller = WorkspaceLoopController(config_path)
    result = controller.run(
        project_id="alpha",
        max_cycles=2,
        idle_cycles=1,
        sleep_seconds=0.0,
        fleet_max_cycles=1,
        fleet_idle_cycles=1,
        queue_poll_seconds=0.01,
        queue_idle_timeout_seconds=0.01,
    )

    sessions = EventStore(artifact_root / "archonlab.db").list_sessions(
        workspace_id="demo-workspace",
        project_id="alpha",
        limit=10,
    )

    assert result.cycles_completed == 2
    assert result.stop_reason == "max_cycles_reached"
    assert result.total_processed_jobs == 2
    assert len(result.cycles) == 2
    assert len(sessions) == 2
    assert {session.status for session in sessions} == {SessionStatus.COMPLETED}
    assert sessions[0].session_id != sessions[1].session_id


def test_workspace_loop_controller_stops_after_idle_budget_without_dispatching(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )

    def fail_run(self, **kwargs) -> object:
        raise AssertionError("FleetController.run should not be called while workspace is idle")

    monkeypatch.setattr("archonlab.workspace_loop.FleetController.run", fail_run)

    controller = WorkspaceLoopController(config_path)
    result = controller.run(
        project_id="missing",
        max_cycles=3,
        idle_cycles=2,
        sleep_seconds=0.0,
    )

    queue_store = QueueStore(artifact_root / "archonlab.db")
    assert result.cycles_completed == 2
    assert result.stop_reason == "idle_cycles_exhausted"
    assert result.total_processed_jobs == 0
    assert queue_store.list_jobs(limit=10) == []


def test_workspace_loop_controller_filters_projects_by_tags(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = tmp_path / "workspace.toml"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n"
        "max_parallel = 2\n\n"
        "[[projects]]\n"
        'id = "alpha"\n'
        'tags = ["core"]\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[[projects]]\n"
        'id = "beta"\n'
        'tags = ["geometry", "batch"]\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )

    def fake_run(self, **kwargs) -> object:
        del kwargs
        store = EventStore(artifact_root / "archonlab.db")
        session = store.list_sessions(
            workspace_id="demo-workspace",
            project_id="beta",
            limit=1,
        )[0]
        job = self.queue_store.get_active_session_job(session.session_id)
        assert job is not None
        self.queue_store.finish_job(job.id, status=QueueJobStatus.COMPLETED)
        store.update_session(
            session.session_id,
            status=SessionStatus.COMPLETED,
            completed_iterations=session.max_iterations,
            stop_reason="max_iterations_reached",
            clear_error_message=True,
            clear_owner_claim=True,
        )
        return self.result_model(
            cycles_completed=1,
            stop_reason="queue_drained",
            total_processed_jobs=1,
            total_paused_jobs=0,
            total_failed_jobs=0,
            total_workers_launched=1,
            cycles=[],
            final_plan=self.queue_store.plan_fleet(),
        )

    monkeypatch.setattr("archonlab.workspace_loop.FleetController.run", fake_run)

    controller = WorkspaceLoopController(config_path)
    result = controller.run(
        project_tags=["geometry", "batch"],
        max_cycles=1,
        idle_cycles=1,
        sleep_seconds=0.0,
        fleet_max_cycles=1,
        fleet_idle_cycles=1,
        queue_poll_seconds=0.01,
        queue_idle_timeout_seconds=0.01,
    )

    sessions = EventStore(artifact_root / "archonlab.db").list_sessions(
        workspace_id="demo-workspace",
        limit=10,
    )

    assert result.total_processed_jobs == 1
    assert len(sessions) == 1
    assert sessions[0].project_id == "beta"
