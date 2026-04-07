from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from archonlab.app import app

runner = CliRunner()


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
    assert any(path.is_dir() for path in runs_dir.iterdir())


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
