from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

from archonlab.app import app
from archonlab.checks import ToolStatus
from archonlab.events import EventStore
from archonlab.models import (
    ActionPhase,
    BatchRunReport,
    EventRecord,
    ExecutorKind,
    FleetControllerResult,
    ProjectSession,
    ProviderKind,
    ProviderPoolHealthReport,
    ProviderPoolHealthStatus,
    ProviderPoolMemberHealth,
    ProviderPoolMemberHealthStatus,
    QueueJobPreview,
    RunLoopResult,
    RunStatus,
    RunSummary,
    SessionStatus,
    SupervisorAction,
    SupervisorReason,
    WorkflowMode,
    WorkspaceLoopResult,
)
from archonlab.queue import QueueStore

runner = CliRunner()


def _init_git_repo(repo_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "ArchonLab"], cwd=repo_path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "archonlab@example.com"],
        cwd=repo_path,
        check=True,
    )
    (repo_path / "README.md").write_text("# demo\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "commit", "-m", "initial"],
        cwd=repo_path,
        check=True,
        capture_output=True,
    )


def _write_workspace_cli_config(
    path: Path,
    *,
    artifact_root: Path,
    project_path: Path,
    archon_path: Path,
    project_ids: tuple[str, ...] = ("alpha",),
) -> Path:
    content = (
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n"
        "max_parallel = 2\n\n"
    )
    for project_id in project_ids:
        content += (
            "[[projects]]\n"
            f'id = "{project_id}"\n'
            f'project_path = "{project_path}"\n'
            f'archon_path = "{archon_path}"\n\n'
        )
    path.write_text(content, encoding="utf-8")
    return path


def test_project_init_command_writes_config(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "project",
            "init",
            "--project-path",
            str(tmp_path / "LeanProject"),
            "--archon-path",
            str(tmp_path / "Archon"),
            "--config-path",
            str(tmp_path / "archonlab.toml"),
        ],
    )

    assert result.exit_code == 0
    assert (tmp_path / "archonlab.toml").exists()


def test_workspace_init_command_writes_config(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "workspace",
            "init",
            "--name",
            "demo-workspace",
            "--project-id",
            "alpha",
            "--project-path",
            str(tmp_path / "LeanProject"),
            "--archon-path",
            str(tmp_path / "Archon"),
            "--config-path",
            str(tmp_path / "workspace.toml"),
        ],
    )

    assert result.exit_code == 0
    assert (tmp_path / "workspace.toml").exists()


def test_doctor_command_reports_failed_command_lean_analyzer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("archonlab.checks._read_version", lambda executable: "stub-version")
    monkeypatch.setattr(
        "archonlab.checks._tool_status",
        lambda name, *, required: ToolStatus(
            name=name,
            required=required,
            available=True,
            path=f"/mock/{name}",
            version="stub-version",
        ),
    )
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    artifact_root = tmp_path / "artifacts"
    analyzer_script = tmp_path / "failing_sidecar.py"
    state_dir = project_path / ".archon"
    project_path.mkdir()
    archon_path.mkdir()
    state_dir.mkdir(parents=True)
    (project_path / "Core.lean").write_text(
        "theorem demo : True := by\n  sorry\n",
        encoding="utf-8",
    )
    (state_dir / "PROGRESS.md").write_text("# progress\n", encoding="utf-8")
    (archon_path / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    analyzer_script.write_text("raise SystemExit(1)\n", encoding="utf-8")
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{project_path}"\n'
        f'archon_path = "{archon_path}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n\n"
        "[lean_analyzer]\n"
        'kind = "command"\n'
        f'command = ["{analyzer_script}"]\n',
        encoding="utf-8",
    )

    result = runner.invoke(app, ["doctor", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "[FAIL] lean_analyzer:" in result.output
    assert "fallback=yes" in result.output


def test_workspace_status_and_start_session_commands(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 10\n\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )
    store = EventStore(artifact_root / "archonlab.db")
    store.register_session(
        ProjectSession(
            session_id="session-demo-project-1",
            workspace_id="demo-workspace",
            project_id="demo-project",
            status=SessionStatus.RUNNING,
            max_iterations=10,
        )
    )
    store.upsert_workspace_loop_run(
        WorkspaceLoopResult(
            loop_run_id="loop-demo-1",
            workspace_id="demo-workspace",
            project_id="demo-project",
            stop_reason="idle_cycles_exhausted",
            cycles_completed=3,
            total_scheduled_jobs=2,
            total_processed_jobs=2,
        )
    )
    store.upsert_fleet_run(
        FleetControllerResult(
            fleet_run_id="fleet-demo-1",
            workspace_id="demo-workspace",
            launcher="subprocess",
            stop_reason="queue_drained",
            cycles_completed=2,
            total_processed_jobs=2,
            total_workers_launched=1,
        )
    )

    status_result = runner.invoke(
        app,
        ["workspace", "status", "--config", str(config_path)],
    )
    status_json_result = runner.invoke(
        app,
        ["workspace", "status", "--config", str(config_path), "--json"],
    )
    start_result = runner.invoke(
        app,
        [
            "workspace",
            "start-session",
            "--config",
            str(config_path),
            "--project-id",
            "demo-project",
            "--max-iterations",
            "3",
        ],
    )

    assert status_result.exit_code == 0
    assert "Workspace: demo-workspace" in status_result.output
    assert "demo-project | enabled=True" in status_result.output
    assert "sessions=1 | running=1" in status_result.output
    assert "Latest loop: loop-demo-1 | stop=idle_cycles_exhausted | cycles=3 | processed=2" in (
        status_result.output
    )
    assert "Latest fleet: fleet-demo-1 | stop=queue_drained | cycles=2 | processed=2" in (
        status_result.output
    )
    assert status_json_result.exit_code == 0
    status_payload = json.loads(status_json_result.output)
    assert status_payload["latest_loop"]["loop_run_id"] == "loop-demo-1"
    assert status_payload["latest_loop"]["stop_reason"] == "idle_cycles_exhausted"
    assert status_payload["latest_fleet"]["fleet_run_id"] == "fleet-demo-1"
    assert status_payload["latest_fleet"]["launcher"] == "subprocess"
    assert start_result.exit_code == 0
    assert "Session: session-demo-project-" in start_result.output


def test_workspace_loops_command_lists_recent_runs(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 10\n\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )
    store = EventStore(artifact_root / "archonlab.db")
    store.upsert_workspace_loop_run(
        WorkspaceLoopResult(
            loop_run_id="loop-demo-2",
            workspace_id="demo-workspace",
            project_id="demo-project",
            stop_reason="max_cycles_reached",
            cycles_completed=2,
            total_scheduled_jobs=2,
            total_processed_jobs=1,
        )
    )

    result = runner.invoke(
        app,
        ["workspace", "loops", "--config", str(config_path), "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert len(payload["loops"]) == 1
    assert payload["loops"][0]["loop_run_id"] == "loop-demo-2"
    assert payload["loops"][0]["cycles_completed"] == 2


def test_workspace_fleets_command_lists_recent_runs(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 10\n\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )
    store = EventStore(artifact_root / "archonlab.db")
    store.upsert_fleet_run(
        FleetControllerResult(
            fleet_run_id="fleet-demo-2",
            workspace_id="demo-workspace",
            launcher="in_process",
            stop_reason="idle_cycles_exhausted",
            cycles_completed=1,
            total_processed_jobs=0,
            total_workers_launched=1,
        )
    )

    result = runner.invoke(
        app,
        ["workspace", "fleets", "--config", str(config_path), "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert len(payload["fleets"]) == 1
    assert payload["fleets"][0]["fleet_run_id"] == "fleet-demo-2"
    assert payload["fleets"][0]["launcher"] == "in_process"


def test_workspace_stop_loop_command_requests_stop(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 10\n\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )
    loop_id = "workspace-loop-stop-demo"
    artifact_dir = artifact_root / "workspace-loops" / loop_id
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "control.json").write_text(
        json.dumps({"stop_requested": False}, indent=2),
        encoding="utf-8",
    )
    store = EventStore(artifact_root / "archonlab.db")
    store.upsert_workspace_loop_run(
        WorkspaceLoopResult(
            loop_run_id=loop_id,
            loop_id=loop_id,
            workspace_id="demo-workspace",
            artifact_dir=artifact_dir,
            stop_reason="running",
        )
    )

    result = runner.invoke(
        app,
        [
            "workspace",
            "stop-loop",
            "--config",
            str(config_path),
            "--loop-run-id",
            loop_id,
            "--reason",
            "operator_stop_requested",
        ],
    )

    assert result.exit_code == 0
    assert f"Stop requested: {loop_id}" in result.output
    control_payload = json.loads((artifact_dir / "control.json").read_text(encoding="utf-8"))
    assert control_payload["stop_requested"] is True
    assert control_payload["reason"] == "operator_stop_requested"


def test_workspace_daemon_commands_run_status_and_stop(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 10\n\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )

    def fake_run(self, **kwargs) -> WorkspaceLoopResult:
        del kwargs
        return WorkspaceLoopResult(
            workspace_id="demo-workspace",
            loop_run_id="loop-daemon-cli-1",
            loop_id="loop-daemon-cli-1",
            stop_reason="idle_cycles_exhausted",
            cycles_completed=1,
        )

    monkeypatch.setattr("archonlab.workspace_daemon.WorkspaceLoopController.run", fake_run)

    run_result = runner.invoke(
        app,
        [
            "workspace",
            "daemon",
            "run",
            "--config",
            str(config_path),
            "--max-ticks",
            "1",
            "--poll-seconds",
            "0",
        ],
    )
    status_result = runner.invoke(
        app,
        ["workspace", "daemon", "status", "--config", str(config_path), "--json"],
    )
    stop_result = runner.invoke(
        app,
        [
            "workspace",
            "daemon",
            "stop",
            "--config",
            str(config_path),
            "--reason",
            "operator_pause",
        ],
    )
    stopped_status_result = runner.invoke(
        app,
        ["workspace", "daemon", "status", "--config", str(config_path), "--json"],
    )

    assert run_result.exit_code == 0
    assert "Daemon:" in run_result.output
    assert "Ticks: 1" in run_result.output
    assert status_result.exit_code == 0
    status_payload = json.loads(status_result.output)
    assert status_payload["tick_count"] == 1
    assert status_payload["last_loop_run_id"] == "loop-daemon-cli-1"
    assert status_payload["status"] == "idle"
    assert stop_result.exit_code == 0
    assert "Stop requested: demo-workspace" in stop_result.output
    stopped_payload = json.loads(stopped_status_result.output)
    assert stopped_payload["stop_requested"] is True
    assert stopped_payload["request_reason"] == "operator_pause"


def test_workspace_run_project_command_executes_loop(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "workspace",
            "run-project",
            "--config",
            str(config_path),
            "--project-id",
            "demo-project",
            "--max-iterations",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert "Loop: run-loop-" in result.output
    assert "Artifacts:" in result.output
    assert "Project: demo-project" in result.output
    assert "Completed iterations: 2" in result.output


def test_workspace_run_command_enqueues_sessions_and_runs_autoscaler(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )
    captured: dict[str, object] = {}

    def fake_run(self, **kwargs) -> object:
        captured.update(kwargs)
        return self.result_model(
            cycles_completed=2,
            stop_reason="queue_drained",
            total_processed_jobs=1,
            total_paused_jobs=0,
            total_failed_jobs=0,
            total_workers_launched=1,
            cycles=[],
            final_plan=self.queue_store.plan_fleet(),
        )

    monkeypatch.setattr("archonlab.app.FleetController.run", fake_run)

    result = runner.invoke(
        app,
        [
            "workspace",
            "run",
            "--config",
            str(config_path),
            "--project-id",
            "alpha",
            "--max-iterations",
            "4",
            "--execute",
            "--max-cycles",
            "5",
            "--idle-cycles",
            "2",
            "--target-jobs-per-worker",
            "3",
            "--note",
            "workspace-run",
        ],
    )

    assert result.exit_code == 0
    assert "Enqueued sessions: 1" in result.output
    assert "Stop reason: queue_drained" in result.output
    assert "Processed: 1" in result.output
    assert captured["max_cycles"] == 5
    assert captured["idle_cycles"] == 2
    assert captured["target_jobs_per_worker"] == 3
    jobs = QueueStore(artifact_root / "archonlab.db").list_jobs(limit=10)
    assert len(jobs) == 1
    assert jobs[0].project_id == "alpha"
    session = EventStore(artifact_root / "archonlab.db").get_session(jobs[0].session_id or "")
    assert session is not None
    assert session.dry_run is False
    assert session.max_iterations == 4
    assert session.note == "workspace-run"


def test_workspace_run_command_resumes_failed_session_before_autoscaling(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )
    store = EventStore(artifact_root / "archonlab.db")
    store.register_session(
        ProjectSession(
            session_id="session-alpha-1",
            workspace_id="demo-workspace",
            project_id="alpha",
            status=SessionStatus.FAILED,
            max_iterations=3,
            completed_iterations=1,
            error_message="executor failed",
            last_stop_reason="run_failed",
        )
    )
    captured: dict[str, object] = {}

    def fake_run(self, **kwargs) -> object:
        captured.update(kwargs)
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

    monkeypatch.setattr("archonlab.app.FleetController.run", fake_run)

    result = runner.invoke(
        app,
        [
            "workspace",
            "run",
            "--config",
            str(config_path),
            "--project-id",
            "alpha",
        ],
    )

    assert result.exit_code == 0
    assert "Enqueued sessions: 1" in result.output
    session = store.get_session("session-alpha-1")
    assert session is not None
    assert session.status is SessionStatus.PENDING
    assert session.last_resume_reason == "workspace_enqueue_resume"
    jobs = QueueStore(artifact_root / "archonlab.db").list_jobs(limit=10)
    assert len(jobs) == 1
    assert jobs[0].session_id == "session-alpha-1"
    assert captured["max_cycles"] == 10


def test_workspace_run_command_skips_control_paused_session_without_autoscaling(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )
    store = EventStore(artifact_root / "archonlab.db")
    store.register_session(
        ProjectSession(
            session_id="session-alpha-control",
            workspace_id="demo-workspace",
            project_id="alpha",
            status=SessionStatus.PAUSED,
            max_iterations=3,
            completed_iterations=0,
            last_stop_reason="stop:control_paused",
        )
    )

    def fail_run(self, **kwargs) -> object:
        raise AssertionError(
            "FleetController.run should not be called when no sessions were enqueued"
        )

    monkeypatch.setattr("archonlab.app.FleetController.run", fail_run)

    result = runner.invoke(
        app,
        [
            "workspace",
            "run",
            "--config",
            str(config_path),
            "--project-id",
            "alpha",
        ],
    )

    assert result.exit_code == 0
    assert "Enqueued sessions: 0" in result.output
    assert "Stop reason: no_sessions_enqueued" in result.output
    session = store.get_session("session-alpha-control")
    assert session is not None
    assert session.status is SessionStatus.PAUSED


def test_workspace_run_command_selects_subprocess_launcher(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )

    class FakeLauncher:
        pass

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "archonlab.app.create_worker_launcher",
        lambda kind, config_path: (
            captured.update({"kind": kind, "config_path": config_path}) or FakeLauncher()
        ),
    )

    def fake_run(self, **kwargs) -> object:
        captured["launcher_type"] = type(self.worker_launcher).__name__
        return self.result_model(
            cycles_completed=0,
            stop_reason="queue_drained",
            total_processed_jobs=0,
            total_paused_jobs=0,
            total_failed_jobs=0,
            total_workers_launched=0,
            cycles=[],
            final_plan=self.queue_store.plan_fleet(),
        )

    monkeypatch.setattr("archonlab.app.FleetController.run", fake_run)

    result = runner.invoke(
        app,
        [
            "workspace",
            "run",
            "--config",
            str(config_path),
            "--launcher",
            "subprocess",
        ],
    )

    assert result.exit_code == 0
    assert captured["kind"] == "subprocess"
    assert captured["config_path"] == config_path
    assert captured["launcher_type"] == "FakeLauncher"


def test_workspace_loop_command_selects_subprocess_launcher(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )

    class FakeLauncher:
        pass

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "archonlab.app.create_worker_launcher",
        lambda kind, config_path: (
            captured.update({"kind": kind, "config_path": config_path}) or FakeLauncher()
        ),
    )

    def fake_run(self, **kwargs) -> object:
        captured["launcher_type"] = type(self.worker_launcher).__name__
        captured.update(kwargs)
        return self.result_model(
            workspace_id="demo-workspace",
            loop_id="workspace-loop-test",
            artifact_dir=artifact_root / "workspace-loops" / "workspace-loop-test",
            cycles_completed=1,
            stop_reason="max_cycles_reached",
            total_processed_jobs=2,
            total_paused_jobs=0,
            total_failed_jobs=0,
            total_workers_launched=1,
            cycles=[],
        )

    monkeypatch.setattr("archonlab.app.WorkspaceLoopController.run", fake_run)

    result = runner.invoke(
        app,
        [
            "workspace",
            "loop",
            "--config",
            str(config_path),
            "--launcher",
            "subprocess",
            "--max-cycles",
            "2",
            "--fleet-max-cycles",
            "3",
        ],
    )

    assert result.exit_code == 0
    assert captured["kind"] == "subprocess"
    assert captured["config_path"] == config_path
    assert captured["launcher_type"] == "FakeLauncher"
    assert captured["max_cycles"] == 2
    assert captured["fleet_max_cycles"] == 3
    assert "Loop: workspace-loop-test" in result.output
    expected_artifacts = artifact_root / "workspace-loops" / "workspace-loop-test"
    assert f"Artifacts: {expected_artifacts}" in result.output
    assert "Stop reason: max_cycles_reached" in result.output
    assert "Processed: 2" in result.output


def test_workspace_loop_command_passes_tag_filters(
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

    captured: dict[str, object] = {}

    def fake_run(self, **kwargs) -> object:
        captured.update(kwargs)
        return self.result_model(
            workspace_id="demo-workspace",
            loop_id="workspace-loop-tagged",
            artifact_dir=artifact_root / "workspace-loops" / "workspace-loop-tagged",
            cycles_completed=1,
            stop_reason="idle_cycles_exhausted",
            total_scheduled_jobs=1,
            total_processed_jobs=0,
            total_paused_jobs=0,
            total_failed_jobs=0,
            total_workers_launched=0,
            cycles=[],
        )

    monkeypatch.setattr("archonlab.app.WorkspaceLoopController.run", fake_run)

    result = runner.invoke(
        app,
        [
            "workspace",
            "loop",
            "--config",
            str(config_path),
            "--tag",
            "geometry",
            "--tag",
            "batch",
        ],
    )

    assert result.exit_code == 0
    assert captured["project_tags"] == ["geometry", "batch"]


def test_workspace_run_command_filters_projects_by_tags(
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

    monkeypatch.setattr(
        "archonlab.app.FleetController.run",
        lambda self, **kwargs: self.result_model(
            cycles_completed=1,
            stop_reason="queue_drained",
            total_processed_jobs=1,
            total_paused_jobs=0,
            total_failed_jobs=0,
            total_workers_launched=1,
            cycles=[],
            final_plan=self.queue_store.plan_fleet(),
        ),
    )

    result = runner.invoke(
        app,
        [
            "workspace",
            "run",
            "--config",
            str(config_path),
            "--tag",
            "geometry",
            "--tag",
            "batch",
        ],
    )

    assert result.exit_code == 0
    assert "Enqueued sessions: 1" in result.output
    jobs = QueueStore(artifact_root / "archonlab.db").list_jobs(limit=10)
    assert len(jobs) == 1
    assert jobs[0].project_id == "beta"


def test_run_start_creates_artifacts(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{tmp_path / "artifacts"}"\n'
        "dry_run = true\n",
        encoding="utf-8",
    )

    result = runner.invoke(app, ["run", "start", "--config", str(config_path), "--dry-run"])

    assert result.exit_code == 0
    db_path = tmp_path / "artifacts" / "archonlab.db"
    assert db_path.exists()
    runs_dir = tmp_path / "artifacts" / "runs"
    run_dirs = [path for path in runs_dir.iterdir() if path.is_dir()]
    assert run_dirs
    run_dir = run_dirs[0]
    assert (run_dir / "task-graph.json").exists()
    assert (run_dir / "supervisor.json").exists()


def test_run_loop_command_creates_session_iterations(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{tmp_path / "artifacts"}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n",
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "run",
            "loop",
            "--config",
            str(config_path),
            "--max-iterations",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert "Loop: run-loop-" in result.output
    assert "Artifacts:" in result.output
    assert "Session: session-" in result.output
    assert "Completed iterations: 2" in result.output


def test_run_loops_command_lists_recent_runs(
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
    store = EventStore(artifact_root / "archonlab.db")
    store.upsert_run_loop_run(
        RunLoopResult(
            loop_run_id="run-loop-demo-1",
            session_id="session-demo-1",
            workspace_id="standalone",
            project_id="demo",
            status=SessionStatus.PAUSED,
            dry_run=True,
            max_iterations=2,
            completed_iterations=2,
            run_ids=["run-1", "run-2"],
            stop_reason="max_iterations_reached",
        )
    )

    result = runner.invoke(
        app,
        ["run", "loops", "--config", str(config_path), "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["project"] == "demo"
    assert len(payload["loops"]) == 1
    assert payload["loops"][0]["loop_run_id"] == "run-loop-demo-1"


def test_benchmark_run_creates_summary(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    manifest_path = tmp_path / "benchmark.toml"
    manifest_path.write_text(
        "[benchmark]\n"
        'name = "smoke"\n'
        'artifact_root = "./artifacts/benchmarks/smoke"\n\n'
        "[[projects]]\n"
        'id = "demo"\n'
        f'path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n'
        'workflow = "adaptive_loop"\n',
        encoding="utf-8",
    )

    result = runner.invoke(app, ["benchmark", "run", "--manifest", str(manifest_path)])

    assert result.exit_code == 0
    summary_files = list((tmp_path / "artifacts" / "benchmarks" / "smoke").rglob("summary.json"))
    assert summary_files

    ledger_result = runner.invoke(
        app,
        [
            "benchmark",
            "ledger",
            "--summary",
            str(summary_files[0]),
            "--json",
        ],
    )

    assert ledger_result.exit_code == 0
    ledger_payload = json.loads(ledger_result.output)
    assert ledger_payload["benchmark_name"] == "smoke"
    assert ledger_payload["summary"]["total_projects"] == 1
    assert ledger_payload["outcomes"][0]["project_id"] == "demo"

    experiment_ledger_result = runner.invoke(
        app,
        [
            "benchmark",
            "experiment-ledger",
            "--summary",
            str(summary_files[0]),
            "--json",
        ],
    )

    assert experiment_ledger_result.exit_code == 0
    experiment_ledger_payload = json.loads(experiment_ledger_result.output)
    assert experiment_ledger_payload["benchmark_name"] == "smoke"
    assert experiment_ledger_payload["summary"]["total_projects"] == 1
    assert experiment_ledger_payload["outcomes"][0]["project_id"] == "demo"

    replay_result = runner.invoke(
        app,
        [
            "benchmark",
            "replay",
            "--summary",
            str(summary_files[0]),
            "--project-id",
            "demo",
            "--json",
        ],
    )

    assert replay_result.exit_code == 0
    replay_payload = json.loads(replay_result.output)
    assert replay_payload["benchmark_name"] == "smoke"
    assert replay_payload["project_id"] == "demo"
    assert replay_payload["artifact_dir"] is not None
    assert replay_payload["run_summary_path"].endswith("run-summary.json")
    assert isinstance(replay_payload["theorem_outcomes"], list)


def test_benchmark_compare_reports_theorem_level_deltas(tmp_path: Path) -> None:
    baseline_ledger = tmp_path / "baseline-ledger.json"
    candidate_ledger = tmp_path / "candidate-ledger.json"
    baseline_ledger.write_text(
        json.dumps(
            {
                "benchmark_name": "baseline-smoke",
                "benchmark_run_id": "baseline-run",
                "generated_at": datetime.now(UTC).isoformat(),
                "summary": {
                    "total_projects": 1,
                    "total_theorems": 2,
                    "unchanged": 2,
                    "improved": 0,
                    "regressed": 0,
                    "new": 0,
                    "removed": 0,
                    "failure_taxonomy": [
                        {
                            "category": "contains_sorry",
                            "count": 1,
                            "samples": ["foo"],
                        }
                    ],
                },
                "outcomes": [
                    {
                        "project_id": "demo",
                        "run_id": "baseline-run",
                        "run_status": "completed",
                        "theorem_outcomes": [
                            {
                                "theorem_name": "foo",
                                "file_path": "Core.lean",
                                "declaration_kind": "theorem",
                                "before_state": "contains_sorry",
                                "after_state": "contains_sorry",
                                "outcome": "unchanged",
                                "failure_categories": ["contains_sorry"],
                            },
                            {
                                "theorem_name": "helper",
                                "file_path": "Core.lean",
                                "declaration_kind": "theorem",
                                "before_state": "proved",
                                "after_state": "proved",
                                "outcome": "unchanged",
                                "failure_categories": [],
                            },
                        ],
                        "failure_taxonomy": [
                            {
                                "category": "contains_sorry",
                                "count": 1,
                                "samples": ["foo"],
                            }
                        ],
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    candidate_ledger.write_text(
        json.dumps(
            {
                "benchmark_name": "candidate-smoke",
                "benchmark_run_id": "candidate-run",
                "generated_at": datetime.now(UTC).isoformat(),
                "summary": {
                    "total_projects": 1,
                    "total_theorems": 3,
                    "unchanged": 1,
                    "improved": 1,
                    "regressed": 0,
                    "new": 1,
                    "removed": 0,
                    "failure_taxonomy": [
                        {
                            "category": "contains_sorry",
                            "count": 1,
                            "samples": ["bar"],
                        }
                    ],
                },
                "outcomes": [
                    {
                        "project_id": "demo",
                        "run_id": "candidate-run",
                        "run_status": "completed",
                        "theorem_outcomes": [
                            {
                                "theorem_name": "foo",
                                "file_path": "Core.lean",
                                "declaration_kind": "theorem",
                                "before_state": "contains_sorry",
                                "after_state": "proved",
                                "outcome": "improved",
                                "failure_categories": [],
                            },
                            {
                                "theorem_name": "helper",
                                "file_path": "Core.lean",
                                "declaration_kind": "theorem",
                                "before_state": "proved",
                                "after_state": "proved",
                                "outcome": "unchanged",
                                "failure_categories": [],
                            },
                            {
                                "theorem_name": "bar",
                                "file_path": "Extra.lean",
                                "declaration_kind": "lemma",
                                "before_state": "missing",
                                "after_state": "contains_sorry",
                                "outcome": "new",
                                "failure_categories": ["contains_sorry"],
                            },
                        ],
                        "failure_taxonomy": [
                            {
                                "category": "contains_sorry",
                                "count": 1,
                                "samples": ["bar"],
                            }
                        ],
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    compare_result = runner.invoke(
        app,
        [
            "benchmark",
            "compare",
            "--baseline-ledger",
            str(baseline_ledger),
            "--candidate-ledger",
            str(candidate_ledger),
            "--json",
        ],
    )

    assert compare_result.exit_code == 0
    compare_payload = json.loads(compare_result.output)
    assert compare_payload["baseline_benchmark"] == "baseline-smoke"
    assert compare_payload["candidate_benchmark"] == "candidate-smoke"
    assert compare_payload["summary"]["improved"] == 1
    assert compare_payload["summary"]["new"] == 1
    changes = {
        (item["project_id"], item["theorem_name"]): item
        for item in compare_payload["changes"]
    }
    assert changes[("demo", "foo")]["change"] == "improved"
    assert changes[("demo", "foo")]["candidate_state"] == "proved"
    assert changes[("demo", "bar")]["change"] == "new"


def test_worktree_create_and_remove_commands(tmp_path: Path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    _init_git_repo(repo_path)
    worktree_root = tmp_path / "worktrees"

    create_result = runner.invoke(
        app,
        [
            "worktree",
            "create",
            "--repo-path",
            str(repo_path),
            "--root",
            str(worktree_root),
            "--name",
            "phase4-run",
        ],
    )

    assert create_result.exit_code == 0
    lease_path = worktree_root / ".leases" / "phase4-run.json"
    assert lease_path.exists()
    lease_data = json.loads(lease_path.read_text(encoding="utf-8"))
    worktree_path = Path(lease_data["worktree_path"])
    assert worktree_path.exists()

    remove_result = runner.invoke(
        app,
        ["worktree", "remove", "--lease", str(lease_path)],
    )

    assert remove_result.exit_code == 0
    assert not worktree_path.exists()
    assert not lease_path.exists()


def test_control_commands_pause_resume_and_hint(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "archonlab.toml"
    workflow_spec = tmp_path / "workflow.toml"
    workflow_spec.write_text("[workflow]\nname = \"demo\"\n", encoding="utf-8")
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{tmp_path / "artifacts"}"\n'
        "dry_run = true\n",
        encoding="utf-8",
    )

    pause_result = runner.invoke(
        app,
        ["control", "pause", "--config", str(config_path), "--reason", "manual_hold"],
    )
    hint_result = runner.invoke(
        app,
        [
            "control",
            "hint",
            "--text",
            "Try `rw` before `simp`.",
            "--config",
            str(config_path),
        ],
    )
    workflow_result = runner.invoke(
        app,
        [
            "control",
            "workflow",
            "--config",
            str(config_path),
            "--workflow",
            "fixed_loop",
            "--workflow-spec",
            str(workflow_spec),
        ],
    )
    resume_result = runner.invoke(app, ["control", "resume", "--config", str(config_path)])
    clear_workflow_result = runner.invoke(
        app,
        ["control", "clear-workflow", "--config", str(config_path)],
    )

    assert pause_result.exit_code == 0
    assert hint_result.exit_code == 0
    assert workflow_result.exit_code == 0
    assert resume_result.exit_code == 0
    assert clear_workflow_result.exit_code == 0
    assert (fake_archon_project / ".archon" / "USER_HINTS.md").exists()


def test_control_workflow_commands_apply_and_clear_override(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "archonlab.toml"
    artifact_root = tmp_path / "artifacts"
    workflow_spec = tmp_path / "workflow-override.toml"
    workflow_spec.write_text(
        "[workflow]\n"
        'name = "cli-override"\n'
        'description = "Override"\n',
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

    workflow_result = runner.invoke(
        app,
        [
            "control",
            "workflow",
            "--config",
            str(config_path),
            "--workflow",
            "fixed_loop",
            "--workflow-spec",
            str(workflow_spec),
        ],
    )
    status_result = runner.invoke(app, ["control", "status", "--config", str(config_path)])
    reset_result = runner.invoke(
        app,
        ["control", "clear-workflow", "--config", str(config_path)],
    )

    assert workflow_result.exit_code == 0
    assert "workflow=fixed_loop" in workflow_result.output
    assert status_result.exit_code == 0
    assert '"workflow_override": "fixed_loop"' in status_result.output
    assert '"workflow_spec_override":' in status_result.output
    assert reset_result.exit_code == 0
    assert "Workflow override cleared: demo" in reset_result.output


def test_queue_worker_command_processes_benchmark_jobs(
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
    manifest_path = tmp_path / "benchmark.toml"
    manifest_path.write_text(
        "[benchmark]\n"
        'name = "smoke"\n'
        'artifact_root = "./artifacts/benchmarks/smoke"\n\n'
        "[[projects]]\n"
        'id = "demo"\n'
        f'path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n'
        'workflow = "adaptive_loop"\n',
        encoding="utf-8",
    )

    enqueue_result = runner.invoke(
        app,
        [
            "queue",
            "enqueue-benchmark",
            "--config",
            str(config_path),
            "--manifest",
            str(manifest_path),
            "--dry-run",
        ],
    )
    worker_result = runner.invoke(
        app,
        [
            "queue",
            "worker",
            "--config",
            str(config_path),
            "--slot-index",
            "1",
            "--executor-kinds",
            "dry_run",
            "--models",
            "gpt-5.4-mini",
            "--cost-tiers",
            "cheap",
            "--max-jobs",
            "1",
            "--idle-timeout-seconds",
            "0.1",
        ],
    )

    assert enqueue_result.exit_code == 0
    assert worker_result.exit_code == 0
    assert "Processed: 1" in worker_result.output

    workers_result = runner.invoke(
        app,
        ["queue", "workers", "--config", str(config_path)],
    )
    assert workers_result.exit_code == 0
    assert "slot=1" in workers_result.output
    assert "executors=dry_run" in workers_result.output
    assert "models=gpt-5.4-mini" in workers_result.output
    assert "cost_tiers=cheap" in workers_result.output


def test_queue_worker_command_can_auto_assign_slot(
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
    manifest_path = tmp_path / "benchmark.toml"
    manifest_path.write_text(
        "[benchmark]\n"
        'name = "smoke"\n'
        'artifact_root = "./artifacts/benchmarks/smoke"\n\n'
        "[[projects]]\n"
        'id = "demo"\n'
        f'path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n'
        'workflow = "adaptive_loop"\n',
        encoding="utf-8",
    )

    enqueue_result = runner.invoke(
        app,
        [
            "queue",
            "enqueue-benchmark",
            "--config",
            str(config_path),
            "--manifest",
            str(manifest_path),
            "--dry-run",
        ],
    )
    worker_result = runner.invoke(
        app,
        [
            "queue",
            "worker",
            "--config",
            str(config_path),
            "--auto-slot",
            "--executor-kinds",
            "dry_run",
            "--max-jobs",
            "1",
            "--idle-timeout-seconds",
            "0.1",
        ],
    )

    assert enqueue_result.exit_code == 0
    assert worker_result.exit_code == 0
    assert "Worker slot: 1" in worker_result.output


def test_queue_sweep_workers_command_reaps_stale_worker(
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

    queue_store = QueueStore(artifact_root / "archonlab.db")
    queue_store.register_worker(slot_index=1, worker_id="worker-stale")
    queue_store._conn.execute(
        "UPDATE queue_workers SET heartbeat_at = ? WHERE worker_id = ?",
        (
            (datetime.now(UTC) - timedelta(seconds=300)).isoformat(),
            "worker-stale",
        ),
    )
    queue_store._conn.commit()

    sweep_result = runner.invoke(
        app,
        [
            "queue",
            "sweep-workers",
            "--config",
            str(config_path),
            "--stale-after-seconds",
            "60",
        ],
    )
    workers_result = runner.invoke(
        app,
        [
            "queue",
            "workers",
            "--config",
            str(config_path),
            "--stale-after-seconds",
            "60",
        ],
    )

    assert sweep_result.exit_code == 0
    assert "Reaped: 1" in sweep_result.output
    assert workers_result.exit_code == 0
    assert "worker-stale | slot=1 | failed" in workers_result.output


def test_queue_requeue_command_requeues_terminal_job(
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

    queue_store = QueueStore(artifact_root / "archonlab.db")
    job = queue_store.enqueue("benchmark", {"manifest_path": "demo.toml"}, priority=4)
    queue_store.cancel(job.id, reason="obsolete_manifest")

    result = runner.invoke(
        app,
        [
            "queue",
            "requeue",
            "--config",
            str(config_path),
            "--job-id",
            job.id,
        ],
    )

    assert result.exit_code == 0
    assert f"Requeued: {job.id}" in result.output
    requeued = queue_store.get_job(job.id)
    assert requeued is not None
    assert requeued.status.value == "queued"
    assert requeued.priority == 4


def test_queue_enqueue_workspace_and_session_status_commands(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )

    enqueue_result = runner.invoke(
        app,
        [
            "queue",
            "enqueue-workspace",
            "--config",
            str(config_path),
        ],
    )
    status_result = runner.invoke(
        app,
        [
            "queue",
            "session-status",
            "--config",
            str(config_path),
        ],
    )

    assert enqueue_result.exit_code == 0
    assert "Enqueued sessions: 1" in enqueue_result.output
    assert status_result.exit_code == 0
    assert "demo-project" in status_result.output
    assert "pending" in status_result.output


def test_queue_enqueue_workspace_command_filters_projects_by_tags(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n\n"
        "[[projects]]\n"
        'id = "alpha"\n'
        'tags = ["core", "batch"]\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[[projects]]\n"
        'id = "beta"\n'
        'tags = ["core"]\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "queue",
            "enqueue-workspace",
            "--config",
            str(config_path),
            "--tag",
            "core",
            "--tag",
            "batch",
        ],
    )

    assert result.exit_code == 0
    assert "Enqueued sessions: 1" in result.output
    jobs = QueueStore(artifact_root / "archonlab.db").list_jobs(limit=10)
    assert len(jobs) == 1
    assert jobs[0].project_id == "alpha"


def test_queue_resume_session_command_extends_budget_and_enqueues_quantum(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )
    store = EventStore(artifact_root / "archonlab.db")
    session = ProjectSession(
        session_id="session-demo-project-1",
        workspace_id="demo-workspace",
        project_id="demo-project",
        status=SessionStatus.PAUSED,
        max_iterations=1,
    )
    store.register_session(session)

    result = runner.invoke(
        app,
        [
            "queue",
            "resume-session",
            "--config",
            str(config_path),
            "--session-id",
            session.session_id,
            "--max-iterations",
            "3",
            "--resume-reason",
            "manual_extend_budget",
        ],
    )
    status_result = runner.invoke(
        app,
        [
            "queue",
            "session-status",
            "--config",
            str(config_path),
            "--session-id",
            session.session_id,
        ],
    )

    assert result.exit_code == 0
    assert status_result.exit_code == 0
    assert f"Session: {session.session_id}" in result.output
    assert "Enqueued job:" in result.output

    updated = store.get_session(session.session_id)
    assert updated is not None
    assert updated.max_iterations == 3
    assert updated.last_resume_reason == "manual_extend_budget"
    assert "resume=manual_extend_budget" in status_result.output

    jobs = QueueStore(artifact_root / "archonlab.db").list_jobs(limit=10)
    assert len(jobs) == 1
    assert jobs[0].session_id == session.session_id


def test_queue_resume_workspace_command_resumes_workspace_sessions(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n\n"
        "[[projects]]\n"
        'id = "alpha"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[[projects]]\n"
        'id = "beta"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )
    store = EventStore(artifact_root / "archonlab.db")
    store.register_session(
        ProjectSession(
            session_id="session-alpha-1",
            workspace_id="demo-workspace",
            project_id="alpha",
            status=SessionStatus.PAUSED,
            max_iterations=1,
            completed_iterations=1,
            last_stop_reason="max_iterations_reached",
        )
    )
    store.register_session(
        ProjectSession(
            session_id="session-beta-1",
            workspace_id="demo-workspace",
            project_id="beta",
            status=SessionStatus.FAILED,
            max_iterations=2,
            completed_iterations=1,
            error_message="executor failed",
            last_stop_reason="run_failed",
        )
    )

    result = runner.invoke(
        app,
        [
            "queue",
            "resume-workspace",
            "--config",
            str(config_path),
            "--max-iterations",
            "4",
            "--resume-reason",
            "workspace_recover",
        ],
    )

    assert result.exit_code == 0
    assert "Resumed sessions: 2" in result.output
    assert "project=alpha" in result.output
    assert "project=beta" in result.output

    alpha = store.get_session("session-alpha-1")
    beta = store.get_session("session-beta-1")
    assert alpha is not None
    assert beta is not None
    assert alpha.status is SessionStatus.PENDING
    assert beta.status is SessionStatus.PENDING
    assert alpha.max_iterations == 4
    assert beta.max_iterations == 4
    assert alpha.last_resume_reason == "workspace_recover"
    assert beta.last_resume_reason == "workspace_recover"

    jobs = QueueStore(artifact_root / "archonlab.db").list_jobs(limit=10)
    assert len(jobs) == 2
    assert {job.session_id for job in jobs} == {"session-alpha-1", "session-beta-1"}


def test_queue_resume_workspace_command_filters_projects_by_tags(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n\n"
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
    store = EventStore(artifact_root / "archonlab.db")
    store.register_session(
        ProjectSession(
            session_id="session-alpha-1",
            workspace_id="demo-workspace",
            project_id="alpha",
            status=SessionStatus.PAUSED,
            max_iterations=1,
            completed_iterations=1,
            last_stop_reason="max_iterations_reached",
        )
    )
    store.register_session(
        ProjectSession(
            session_id="session-beta-1",
            workspace_id="demo-workspace",
            project_id="beta",
            status=SessionStatus.FAILED,
            max_iterations=2,
            completed_iterations=1,
            error_message="executor failed",
            last_stop_reason="run_failed",
        )
    )

    result = runner.invoke(
        app,
        [
            "queue",
            "resume-workspace",
            "--config",
            str(config_path),
            "--tag",
            "geometry",
            "--tag",
            "batch",
            "--resume-reason",
            "tagged_recover",
        ],
    )

    assert result.exit_code == 0
    assert "Resumed sessions: 1" in result.output
    assert "project=beta" in result.output
    assert "project=alpha" not in result.output
    alpha = store.get_session("session-alpha-1")
    beta = store.get_session("session-beta-1")
    assert alpha is not None
    assert beta is not None
    assert alpha.status is SessionStatus.PAUSED
    assert beta.status is SessionStatus.PENDING
    assert beta.last_resume_reason == "tagged_recover"
    jobs = QueueStore(artifact_root / "archonlab.db").list_jobs(limit=10)
    assert len(jobs) == 1
    assert jobs[0].project_id == "beta"


def test_queue_resume_session_command_resumes_control_paused_session_without_budget_extension(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "workspace.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n',
        encoding="utf-8",
    )
    store = EventStore(artifact_root / "archonlab.db")
    session = ProjectSession(
        session_id="session-demo-project-control-paused",
        workspace_id="demo-workspace",
        project_id="demo-project",
        status=SessionStatus.PAUSED,
        max_iterations=2,
        completed_iterations=0,
        last_stop_reason="stop:control_paused",
    )
    store.register_session(session)

    result = runner.invoke(
        app,
        [
            "queue",
            "resume-session",
            "--config",
            str(config_path),
            "--session-id",
            session.session_id,
        ],
    )

    assert result.exit_code == 0
    updated = store.get_session(session.session_id)
    assert updated is not None
    assert updated.status is SessionStatus.PENDING
    jobs = QueueStore(artifact_root / "archonlab.db").list_jobs(limit=10)
    assert len(jobs) == 1
    assert jobs[0].session_id == session.session_id


def test_queue_run_and_status_commands_accept_workspace_config(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )

    enqueue_result = runner.invoke(
        app,
        ["queue", "enqueue-workspace", "--config", str(config_path)],
    )
    status_result = runner.invoke(
        app,
        ["queue", "status", "--config", str(config_path)],
    )
    run_result = runner.invoke(
        app,
        [
            "queue",
            "run",
            "--config",
            str(config_path),
            "--max-jobs",
            "1",
            "--slots",
            "1",
        ],
    )

    assert enqueue_result.exit_code == 0
    assert status_result.exit_code == 0
    assert run_result.exit_code == 0
    assert "alpha" in status_result.output
    assert "queued" in status_result.output
    assert "Processed: 1" in run_result.output


def test_queue_worker_and_workers_commands_accept_workspace_config(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )

    enqueue_result = runner.invoke(
        app,
        ["queue", "enqueue-workspace", "--config", str(config_path)],
    )
    worker_result = runner.invoke(
        app,
        [
            "queue",
            "worker",
            "--config",
            str(config_path),
            "--auto-slot",
            "--executor-kinds",
            "dry_run",
            "--max-jobs",
            "1",
            "--idle-timeout-seconds",
            "0.1",
        ],
    )
    workers_result = runner.invoke(
        app,
        ["queue", "workers", "--config", str(config_path)],
    )

    assert enqueue_result.exit_code == 0
    assert worker_result.exit_code == 0
    assert workers_result.exit_code == 0
    assert "Worker slot: 1" in worker_result.output
    assert "slot=1" in workers_result.output


def test_queue_sweep_workers_command_accepts_workspace_config(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )

    queue_store = QueueStore(artifact_root / "archonlab.db")
    queue_store.register_worker(slot_index=1, worker_id="worker-stale")
    queue_store._conn.execute(
        "UPDATE queue_workers SET heartbeat_at = ? WHERE worker_id = ?",
        (
            (datetime.now(UTC) - timedelta(seconds=300)).isoformat(),
            "worker-stale",
        ),
    )
    queue_store._conn.commit()

    sweep_result = runner.invoke(
        app,
        [
            "queue",
            "sweep-workers",
            "--config",
            str(config_path),
            "--stale-after-seconds",
            "60",
        ],
    )
    workers_result = runner.invoke(
        app,
        [
            "queue",
            "workers",
            "--config",
            str(config_path),
            "--stale-after-seconds",
            "60",
        ],
    )

    assert sweep_result.exit_code == 0
    assert workers_result.exit_code == 0
    assert "Reaped: 1" in sweep_result.output
    assert "worker-stale | slot=1 | failed" in workers_result.output


def test_queue_plan_fleet_and_fleet_commands_accept_workspace_config(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )

    enqueue_result = runner.invoke(
        app,
        ["queue", "enqueue-workspace", "--config", str(config_path)],
    )
    plan_result = runner.invoke(
        app,
        ["queue", "plan-fleet", "--config", str(config_path)],
    )
    fleet_result = runner.invoke(
        app,
        [
            "queue",
            "fleet",
            "--config",
            str(config_path),
            "--workers",
            "1",
            "--max-jobs-per-worker",
            "1",
            "--executor-kinds",
            "dry_run",
            "--idle-timeout-seconds",
            "0.1",
            "--poll-seconds",
            "0.1",
        ],
    )

    assert enqueue_result.exit_code == 0
    assert plan_result.exit_code == 0
    assert fleet_result.exit_code == 0
    assert "Profiles: 1" in plan_result.output
    assert "Processed: 1" in fleet_result.output
    assert "Workers: 1" in fleet_result.output


def test_queue_provider_health_and_reset_commands_accept_workspace_config(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        + (
            "[provider]\n"
            'pool = "lab"\n\n'
            "[provider_pool.lab]\n"
            "max_consecutive_failures = 1\n"
            "quarantine_seconds = 60\n\n"
            "[[provider_pool.lab.members]]\n"
            'name = "primary"\n'
            'model = "gpt-5.4-mini"\n\n'
            "[[provider_pool.lab.members]]\n"
            'name = "backup"\n'
            'model = "gpt-5.4"\n'
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "archonlab.app.snapshot_provider_pool_health",
        lambda provider_pools, *, db_path=None: [
            ProviderPoolHealthReport(
                pool_name="lab",
                status=ProviderPoolHealthStatus.DEGRADED,
                strategy="ordered_failover",
                total_members=2,
                available_members=1,
                quarantined_members=1,
                members=[
                    ProviderPoolMemberHealth(
                        pool_name="lab",
                        member_name="primary",
                        status=ProviderPoolMemberHealthStatus.QUARANTINED,
                        consecutive_failures=2,
                        model="gpt-5.4-mini",
                    ),
                    ProviderPoolMemberHealth(
                        pool_name="lab",
                        member_name="backup",
                        status=ProviderPoolMemberHealthStatus.HEALTHY,
                        model="gpt-5.4",
                    ),
                ],
            )
        ],
    )

    captured: dict[str, object] = {}

    def fake_reset_provider_pool_health(
        *,
        pool_name: str | None = None,
        member_name: str | None = None,
        db_path: Path | None = None,
    ) -> int:
        captured["pool_name"] = pool_name
        captured["member_name"] = member_name
        captured["db_path"] = db_path
        return 1

    monkeypatch.setattr(
        "archonlab.app.reset_provider_pool_health",
        fake_reset_provider_pool_health,
    )

    health_result = runner.invoke(
        app,
        ["queue", "provider-health", "--config", str(config_path)],
    )
    reset_result = runner.invoke(
        app,
        [
            "queue",
            "reset-provider-health",
            "--config",
            str(config_path),
            "--pool",
            "lab",
            "--member",
            "primary",
        ],
    )

    assert health_result.exit_code == 0
    assert "lab default | degraded | available=1/2 | quarantined=1" in health_result.output
    assert "primary | quarantined | failures=2 | model=gpt-5.4-mini" in health_result.output
    assert "backup | healthy | failures=0 | model=gpt-5.4" in health_result.output
    assert reset_result.exit_code == 0
    assert "Reset health entries: 1" in reset_result.output
    assert captured == {
        "pool_name": "lab",
        "member_name": "primary",
        "db_path": artifact_root / "archonlab.db",
    }


def test_queue_runtime_summary_command_accepts_workspace_config(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )
    store = EventStore(artifact_root / "archonlab.db")
    started_at = datetime.now(UTC)
    store.register_run(
        RunSummary(
            run_id="run-runtime-1",
            project_id="alpha",
            workflow=WorkflowMode.ADAPTIVE_LOOP,
            status=RunStatus.COMPLETED,
            stage="prover",
            dry_run=False,
            started_at=started_at,
            artifact_dir=artifact_root / "runs" / "run-runtime-1",
        )
    )
    store.append(
        EventRecord(
            run_id="run-runtime-1",
            kind="executor.completed",
            project_id="alpha",
            payload={
                "telemetry": {
                    "provider_pool": "lab",
                    "provider_member": "member-a",
                    "retry_count": 1,
                    "cost_estimate": 0.25,
                    "health_status": "degraded",
                }
            },
        )
    )

    result = runner.invoke(
        app,
        [
            "queue",
            "runtime-summary",
            "--config",
            str(config_path),
            "--limit",
            "20",
        ],
    )

    assert result.exit_code == 0
    assert "lab | success=1 | failed=0" in result.output
    assert "member-a | success=1 | failed=0 | retries=1" in result.output


def test_queue_autoscale_command_accepts_workspace_config(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
    monkeypatch,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )

    captured: dict[str, object] = {}

    def fake_run(self, **kwargs) -> object:
        captured.update(kwargs)
        return self.result_model(
            fleet_run_id="fleet-cli-1",
            workspace_id="demo-workspace",
            artifact_dir=artifact_root / "fleet-runs" / "fleet-cli-1",
            cycles_completed=2,
            stop_reason="queue_drained",
            total_processed_jobs=3,
            total_paused_jobs=0,
            total_failed_jobs=0,
            total_workers_launched=2,
            cycles=[],
            final_plan=self.queue_store.plan_fleet(),
        )

    monkeypatch.setattr("archonlab.app.FleetController.run", fake_run)

    result = runner.invoke(
        app,
        [
            "queue",
            "autoscale",
            "--config",
            str(config_path),
            "--max-cycles",
            "5",
            "--idle-cycles",
            "2",
            "--target-jobs-per-worker",
            "3",
        ],
    )

    assert result.exit_code == 0
    assert "Fleet: fleet-cli-1" in result.output
    assert "Stop reason: queue_drained" in result.output
    assert "Processed: 3" in result.output
    assert captured["max_cycles"] == 5
    assert captured["idle_cycles"] == 2
    assert captured["target_jobs_per_worker"] == 3


def test_queue_fleet_runs_command_lists_persisted_workspace_runs(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    config_path = _write_workspace_cli_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
    )
    EventStore(artifact_root / "archonlab.db").upsert_fleet_run(
        FleetControllerResult(
            fleet_run_id="fleet-workspace-1",
            workspace_id="demo-workspace",
            launcher="subprocess",
            stop_reason="queue_drained",
            cycles_completed=2,
            total_processed_jobs=3,
            total_workers_launched=2,
        )
    )

    result = runner.invoke(
        app,
        ["queue", "fleet-runs", "--config", str(config_path), "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["fleet_run_id"] == "fleet-workspace-1"
    assert payload[0]["workspace_id"] == "demo-workspace"


def test_queue_fleet_command_processes_jobs_with_auto_slot_workers(
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
        "dry_run = true\n"
        "max_parallel = 2\n",
        encoding="utf-8",
    )
    manifest_path = tmp_path / "benchmark.toml"
    manifest_path.write_text(
        "[benchmark]\n"
        'name = "smoke"\n'
        'artifact_root = "./artifacts/benchmarks/smoke"\n\n'
        "[[projects]]\n"
        'id = "demo"\n'
        f'path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n'
        'workflow = "adaptive_loop"\n',
        encoding="utf-8",
    )

    enqueue_result = runner.invoke(
        app,
        [
            "queue",
            "enqueue-benchmark",
            "--config",
            str(config_path),
            "--manifest",
            str(manifest_path),
            "--dry-run",
        ],
    )
    fleet_result = runner.invoke(
        app,
        [
            "queue",
            "fleet",
            "--config",
            str(config_path),
            "--workers",
            "1",
            "--executor-kinds",
            "dry_run",
            "--models",
            "gpt-5.4-mini",
            "--cost-tiers",
            "cheap",
            "--idle-timeout-seconds",
            "0.1",
            "--poll-seconds",
            "0.1",
        ],
    )

    assert enqueue_result.exit_code == 0
    assert fleet_result.exit_code == 0
    assert "Processed: 1" in fleet_result.output
    assert "Workers: 1" in fleet_result.output


def test_queue_fleet_command_forwards_plan_driven_options(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path, monkeypatch
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
        "max_parallel = 2\n",
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def fake_run_fleet(self, **kwargs) -> BatchRunReport:
        captured.update(kwargs)
        return BatchRunReport(worker_ids=["worker-planned"])

    monkeypatch.setattr("archonlab.app.BatchRunner.run_fleet", fake_run_fleet)

    result = runner.invoke(
        app,
        [
            "queue",
            "fleet",
            "--config",
            str(config_path),
            "--plan-driven",
            "--target-jobs-per-worker",
            "3",
        ],
    )

    assert result.exit_code == 0
    assert captured["plan_driven"] is True
    assert captured["target_jobs_per_worker"] == 3
    assert "Workers: 1" in result.output


def test_queue_plan_fleet_command_summarizes_recommended_worker_profiles(
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

    queue_store = QueueStore(artifact_root / "archonlab.db")
    queue_store.enqueue(
        "benchmark",
        {"manifest_path": "cheap.toml"},
        project_id="demo-cheap",
        priority=5,
        required_executor_kinds=[ExecutorKind.DRY_RUN],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        required_models=["gpt-5.4-mini"],
        required_cost_tiers=["cheap"],
        required_endpoint_classes=["lab"],
        preview=QueueJobPreview(
            phase=ActionPhase.PLAN,
            reason="bootstrap_first_iteration",
            stage="plan",
            supervisor_action=SupervisorAction.CONTINUE,
            supervisor_reason=SupervisorReason.HEALTHY,
            theorem_name="cheap_goal",
            final_priority=5,
            executor_kind=ExecutorKind.DRY_RUN,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4-mini",
            cost_tier="cheap",
            endpoint_class="lab",
        ),
    )
    queue_store.enqueue(
        "benchmark",
        {"manifest_path": "premium.toml"},
        project_id="demo-premium",
        priority=9,
        required_executor_kinds=[ExecutorKind.DRY_RUN],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        required_models=["gpt-5.4"],
        required_cost_tiers=["premium"],
        required_endpoint_classes=["lab"],
        preview=QueueJobPreview(
            phase=ActionPhase.PROVER,
            reason="task_graph_focus",
            stage="prover",
            supervisor_action=SupervisorAction.CONTINUE,
            supervisor_reason=SupervisorReason.HEALTHY,
            theorem_name="premium_goal",
            final_priority=9,
            executor_kind=ExecutorKind.DRY_RUN,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4",
            cost_tier="premium",
            endpoint_class="lab",
        ),
    )
    queue_store.register_worker(
        slot_index=1,
        worker_id="worker-cheap",
        executor_kinds=[ExecutorKind.DRY_RUN],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        models=["gpt-5.4-mini"],
        cost_tiers=["cheap"],
        endpoint_classes=["lab"],
    )

    result = runner.invoke(
        app,
        [
            "queue",
            "plan-fleet",
            "--config",
            str(config_path),
            "--target-jobs-per-worker",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert "Profiles: 2" in result.output
    assert "Active workers: 1" in result.output
    assert "Dedicated workers: 1" in result.output
    assert "Additional workers: 1" in result.output
    assert "model=gpt-5.4-mini" in result.output
    assert "model=gpt-5.4" in result.output


def test_queue_autoscale_command_runs_fleet_controller(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path, monkeypatch
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
        "max_parallel = 2\n",
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def fake_run(self, **kwargs) -> object:
        captured.update(kwargs)
        return self.result_model(
            cycles_completed=2,
            stop_reason="queue_drained",
            total_processed_jobs=3,
            total_paused_jobs=0,
            total_failed_jobs=0,
            total_workers_launched=2,
            cycles=[],
            final_plan=self.queue_store.plan_fleet(),
        )

    monkeypatch.setattr("archonlab.app.FleetController.run", fake_run)

    result = runner.invoke(
        app,
        [
            "queue",
            "autoscale",
            "--config",
            str(config_path),
            "--max-cycles",
            "5",
            "--idle-cycles",
            "2",
            "--target-jobs-per-worker",
            "3",
        ],
    )

    assert result.exit_code == 0
    assert "Stop reason: queue_drained" in result.output
    assert "Processed: 3" in result.output
    assert captured["max_cycles"] == 5
    assert captured["idle_cycles"] == 2
    assert captured["target_jobs_per_worker"] == 3


def test_queue_autoscale_command_selects_subprocess_launcher(
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
        "dry_run = true\n",
        encoding="utf-8",
    )

    class FakeLauncher:
        pass

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "archonlab.app.create_worker_launcher",
        lambda kind, config_path: (
            captured.update({"kind": kind, "config_path": config_path}) or FakeLauncher()
        ),
    )

    def fake_run(self, **kwargs) -> object:
        captured["launcher_type"] = type(self.worker_launcher).__name__
        return self.result_model(
            cycles_completed=0,
            stop_reason="queue_drained",
            total_processed_jobs=0,
            total_paused_jobs=0,
            total_failed_jobs=0,
            total_workers_launched=0,
            cycles=[],
            final_plan=self.queue_store.plan_fleet(),
        )

    monkeypatch.setattr("archonlab.app.FleetController.run", fake_run)

    result = runner.invoke(
        app,
        [
            "queue",
            "autoscale",
            "--config",
            str(config_path),
            "--launcher",
            "subprocess",
        ],
    )

    assert result.exit_code == 0
    assert captured["kind"] == "subprocess"
    assert captured["config_path"] == config_path
    assert captured["launcher_type"] == "FakeLauncher"


def test_queue_worker_command_supports_json_output(
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
        "dry_run = true\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "archonlab.app.BatchRunner.run_worker",
        lambda self, **kwargs: BatchRunReport(
            processed_job_ids=["job-1"],
            paused_job_ids=[],
            failed_job_ids=[],
            worker_ids=["worker-1"],
        ),
    )

    result = runner.invoke(
        app,
        [
            "queue",
            "worker",
            "--config",
            str(config_path),
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["processed_job_ids"] == ["job-1"]
    assert payload["worker_ids"] == ["worker-1"]
