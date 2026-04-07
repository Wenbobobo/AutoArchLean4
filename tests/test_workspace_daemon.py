from __future__ import annotations

from pathlib import Path

from archonlab.models import WorkspaceLoopResult
from archonlab.workspace_daemon import (
    WorkspaceDaemonRunner,
    load_workspace_daemon_state,
    request_workspace_daemon_stop,
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
