from __future__ import annotations

import json
import subprocess
from pathlib import Path

from typer.testing import CliRunner

from archonlab.app import app

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
    resume_result = runner.invoke(app, ["control", "resume", "--config", str(config_path)])

    assert pause_result.exit_code == 0
    assert hint_result.exit_code == 0
    assert resume_result.exit_code == 0
    assert (fake_archon_project / ".archon" / "USER_HINTS.md").exists()
