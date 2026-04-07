from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from archonlab.events import EventStore
from archonlab.models import (
    ProjectSession,
    SessionStatus,
    WorkflowMode,
    WorkspaceLoopResult,
)
from archonlab.workspace_daemon import (
    WorkspaceDaemonRunner,
    WorkspaceDaemonState,
    load_workspace_daemon_state,
    request_workspace_daemon_stop,
    workspace_daemon_lock_path,
    write_workspace_daemon_state,
)


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


def test_workspace_daemon_runner_executes_repeated_workspace_loop_ticks(
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
    calls: list[dict[str, object]] = []

    def fake_run(self, **kwargs) -> WorkspaceLoopResult:
        calls.append(dict(kwargs))
        index = len(calls)
        loop_id = f"loop-daemon-{index}"
        return WorkspaceLoopResult(
            workspace_id="demo-workspace",
            loop_run_id=loop_id,
            loop_id=loop_id,
            stop_reason="idle_cycles_exhausted",
            cycles_completed=1,
        )

    monkeypatch.setattr("archonlab.workspace_daemon.WorkspaceLoopController.run", fake_run)

    state = WorkspaceDaemonRunner(config_path).run(
        max_ticks=2,
        poll_seconds=0.0,
        project_id="alpha",
        fleet_max_cycles=1,
        queue_poll_seconds=0.01,
        queue_idle_timeout_seconds=0.01,
    )
    persisted = load_workspace_daemon_state(artifact_root)

    assert len(calls) == 2
    assert calls[0]["project_id"] == "alpha"
    assert calls[0]["max_cycles"] == 1
    assert calls[0]["idle_cycles"] == 1
    assert state.daemon_run_id.startswith("workspace-daemon-")
    assert state.status == "idle"
    assert state.tick_count == 2
    assert state.last_loop_run_id == "loop-daemon-2"
    assert state.last_loop_stop_reason == "idle_cycles_exhausted"
    assert state.exit_reason == "max_ticks_reached"
    assert persisted.tick_count == 2
    assert persisted.last_loop_run_id == "loop-daemon-2"
    assert not workspace_daemon_lock_path(artifact_root).exists()


def test_workspace_daemon_runner_honors_stop_request_between_ticks(
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
    calls: list[dict[str, object]] = []

    def fake_run(self, **kwargs) -> WorkspaceLoopResult:
        calls.append(dict(kwargs))
        request_workspace_daemon_stop(
            artifact_root,
            reason="operator_stop_requested",
            requested_by="test",
        )
        return WorkspaceLoopResult(
            workspace_id="demo-workspace",
            loop_run_id="loop-daemon-stop-1",
            loop_id="loop-daemon-stop-1",
            stop_reason="idle_cycles_exhausted",
            cycles_completed=1,
        )

    monkeypatch.setattr("archonlab.workspace_daemon.WorkspaceLoopController.run", fake_run)

    state = WorkspaceDaemonRunner(config_path).run(
        max_ticks=4,
        poll_seconds=0.0,
        fleet_max_cycles=1,
        queue_poll_seconds=0.01,
        queue_idle_timeout_seconds=0.01,
    )

    assert len(calls) == 1
    assert state.status == "stopped"
    assert state.stop_requested is True
    assert state.request_reason == "operator_stop_requested"
    assert state.exit_reason == "operator_stop_requested"
    assert state.last_loop_run_id == "loop-daemon-stop-1"
    assert not workspace_daemon_lock_path(artifact_root).exists()


def test_workspace_daemon_runner_rejects_existing_live_lock(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )
    write_workspace_daemon_state(
        artifact_root,
        WorkspaceDaemonState(
            workspace_id="demo-workspace",
            daemon_run_id="workspace-daemon-live",
            status="running",
            pid=os.getpid(),
        ),
    )
    workspace_daemon_lock_path(artifact_root).parent.mkdir(parents=True, exist_ok=True)
    workspace_daemon_lock_path(artifact_root).write_text(
        (
            '{"workspace_id":"demo-workspace",'
            '"daemon_run_id":"workspace-daemon-live",'
            f'"pid":{os.getpid()}}}'
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="workspace-daemon-live"):
        WorkspaceDaemonRunner(config_path).run(
            max_ticks=1,
            poll_seconds=0.0,
            fleet_max_cycles=1,
            queue_poll_seconds=0.01,
            queue_idle_timeout_seconds=0.01,
        )

    persisted = load_workspace_daemon_state(artifact_root)
    assert persisted.daemon_run_id == "workspace-daemon-live"
    assert persisted.status == "running"


def test_workspace_daemon_runner_releases_lock_after_failure(
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

    def fail_run(self, **kwargs) -> WorkspaceLoopResult:
        del self, kwargs
        raise RuntimeError("daemon loop boom")

    monkeypatch.setattr("archonlab.workspace_daemon.WorkspaceLoopController.run", fail_run)

    with pytest.raises(RuntimeError, match="daemon loop boom"):
        WorkspaceDaemonRunner(config_path).run(
            max_ticks=1,
            poll_seconds=0.0,
            fleet_max_cycles=1,
            queue_poll_seconds=0.01,
            queue_idle_timeout_seconds=0.01,
        )

    persisted = load_workspace_daemon_state(artifact_root)
    assert persisted.status == "failed"
    assert persisted.exit_reason == "daemon_failed"
    assert persisted.error_message == "daemon loop boom"
    assert not workspace_daemon_lock_path(artifact_root).exists()


def test_workspace_daemon_runner_stops_before_next_tick_when_stop_requested_during_sleep(
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
    calls: list[dict[str, object]] = []

    def fake_run(self, **kwargs) -> WorkspaceLoopResult:
        calls.append(dict(kwargs))
        return WorkspaceLoopResult(
            workspace_id="demo-workspace",
            loop_run_id="loop-daemon-sleep-stop-1",
            loop_id="loop-daemon-sleep-stop-1",
            stop_reason="idle_cycles_exhausted",
            cycles_completed=1,
        )

    def fake_sleep(seconds: float) -> None:
        del seconds
        request_workspace_daemon_stop(
            artifact_root,
            reason="stop_during_sleep",
            requested_by="test",
        )

    monkeypatch.setattr("archonlab.workspace_daemon.WorkspaceLoopController.run", fake_run)
    monkeypatch.setattr("archonlab.workspace_daemon.time.sleep", fake_sleep)

    state = WorkspaceDaemonRunner(config_path).run(
        max_ticks=4,
        poll_seconds=5.0,
        fleet_max_cycles=1,
        queue_poll_seconds=0.01,
        queue_idle_timeout_seconds=0.01,
    )

    assert len(calls) == 1
    assert state.status == "stopped"
    assert state.exit_reason == "stop_during_sleep"
    assert state.request_reason == "stop_during_sleep"


def test_workspace_daemon_runner_waits_for_failure_cooldown_before_retrying(
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
    EventStore(artifact_root / "archonlab.db").register_session(
        ProjectSession(
            session_id="session-alpha-cooling",
            workspace_id="demo-workspace",
            project_id="alpha",
            status=SessionStatus.FAILED,
            workflow=WorkflowMode.ADAPTIVE_LOOP,
            dry_run=True,
            max_iterations=2,
            completed_iterations=1,
            last_stop_reason="run_failed",
            consecutive_failures=1,
            max_consecutive_failures=3,
            failure_cooldown_seconds=45,
            last_failure_at=datetime.now(UTC) - timedelta(seconds=5),
            cooldown_until=datetime.now(UTC) + timedelta(seconds=45),
        )
    )
    calls: list[dict[str, object]] = []
    sleep_calls: list[float] = []

    def fake_run(self, **kwargs) -> WorkspaceLoopResult:
        calls.append(dict(kwargs))
        return WorkspaceLoopResult(
            workspace_id="demo-workspace",
            loop_run_id="loop-daemon-cooldown-1",
            loop_id="loop-daemon-cooldown-1",
            stop_reason="failure_cooldown_active",
            cycles_completed=1,
        )

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        request_workspace_daemon_stop(
            artifact_root,
            reason="stop_after_backoff",
            requested_by="test",
        )

    monkeypatch.setattr("archonlab.workspace_daemon.WorkspaceLoopController.run", fake_run)
    monkeypatch.setattr("archonlab.workspace_daemon.time.sleep", fake_sleep)

    state = WorkspaceDaemonRunner(config_path).run(
        max_ticks=4,
        poll_seconds=5.0,
        fleet_max_cycles=1,
        queue_poll_seconds=0.01,
        queue_idle_timeout_seconds=0.01,
    )

    assert len(calls) == 1
    assert len(sleep_calls) == 1
    assert sleep_calls[0] > 30.0
    assert state.status == "stopped"
    assert state.exit_reason == "stop_after_backoff"
