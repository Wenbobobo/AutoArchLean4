from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

from archonlab.app import app
from archonlab.events import EventStore
from archonlab.models import (
    ActionPhase,
    BatchRunReport,
    ExecutorKind,
    ProjectSession,
    ProviderKind,
    QueueJobPreview,
    SessionStatus,
    SupervisorAction,
    SupervisorReason,
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

    status_result = runner.invoke(
        app,
        ["workspace", "status", "--config", str(config_path)],
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
    assert start_result.exit_code == 0
    assert "Session: session-demo-project-" in start_result.output


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
    assert "Project: demo-project" in result.output
    assert "Completed iterations: 2" in result.output


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
    assert "Session: session-" in result.output
    assert "Completed iterations: 2" in result.output


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
        ],
    )

    assert result.exit_code == 0
    assert f"Session: {session.session_id}" in result.output
    assert "Enqueued job:" in result.output

    updated = store.get_session(session.session_id)
    assert updated is not None
    assert updated.max_iterations == 3

    jobs = QueueStore(artifact_root / "archonlab.db").list_jobs(limit=10)
    assert len(jobs) == 1
    assert jobs[0].session_id == session.session_id


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
