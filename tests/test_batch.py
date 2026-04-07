from __future__ import annotations

import json
from pathlib import Path

from archonlab.control import ControlService
from archonlab.models import ProjectConfig
from archonlab.queue import QueueStore


def _make_project(tmp_path: Path, name: str) -> Path:
    project_path = tmp_path / name
    state_dir = project_path / ".archon"
    prompts_dir = state_dir / "prompts"
    prompts_dir.mkdir(parents=True)
    (state_dir / "CLAUDE.md").write_text("# demo\n", encoding="utf-8")
    (prompts_dir / "plan.md").write_text("# plan\n", encoding="utf-8")
    (prompts_dir / "prover-prover.md").write_text("# prover\n", encoding="utf-8")
    (state_dir / "PROGRESS.md").write_text(
        "# Project Progress\n\n"
        "## Current Stage\n"
        "prover\n\n"
        "## Stages\n"
        "- [x] init\n"
        "- [ ] prover\n\n"
        "## Current Objectives\n\n"
        "1. **Core.lean** - fill theorem `foo`\n",
        encoding="utf-8",
    )
    (project_path / "Core.lean").write_text(
        "theorem foo : True := by\n"
        "  trivial\n",
        encoding="utf-8",
    )
    return project_path


def _make_archon(tmp_path: Path) -> Path:
    archon_path = tmp_path / "Archon"
    archon_path.mkdir()
    (archon_path / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    return archon_path


def _write_benchmark_manifest(tmp_path: Path, project_path: Path, archon_path: Path) -> Path:
    manifest_path = tmp_path / "benchmark.toml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        "[benchmark]\n"
        'name = "batch-smoke"\n'
        'artifact_root = "./artifacts/benchmarks/batch-smoke"\n\n'
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'path = "{project_path}"\n'
        f'archon_path = "{archon_path}"\n'
        'workflow = "adaptive_loop"\n',
        encoding="utf-8",
    )
    return manifest_path


def test_batch_runner_processes_jobs_serially_and_writes_job_summaries(
    tmp_path: Path,
) -> None:
    from archonlab.batch import BatchRunner

    project_path = _make_project(tmp_path, "DemoProject")
    archon_path = _make_archon(tmp_path)
    manifest_a = _write_benchmark_manifest(tmp_path / "a", project_path, archon_path)
    manifest_b = _write_benchmark_manifest(tmp_path / "b", project_path, archon_path)

    queue_store = QueueStore(tmp_path / "queue.db")
    job_a = queue_store.enqueue("benchmark", {"manifest_path": str(manifest_a)})
    job_b = queue_store.enqueue("benchmark", {"manifest_path": str(manifest_b)})

    calls: list[Path] = []

    class FakeBenchmarkRunService:
        def __init__(self, manifest_path: Path) -> None:
            self.manifest_path = manifest_path

        def run(self, *, dry_run: bool = True, use_worktrees: bool = False) -> object:
            calls.append(self.manifest_path)
            artifact_dir = tmp_path / "fake-artifacts" / self.manifest_path.stem
            artifact_dir.mkdir(parents=True, exist_ok=True)
            summary_path = artifact_dir / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "manifest_path": str(self.manifest_path),
                        "dry_run": dry_run,
                        "use_worktrees": use_worktrees,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            return type(
                "FakeBenchmarkResult",
                (),
                {
                    "benchmark": type("Benchmark", (), {"name": "batch-smoke"})(),
                    "run_id": f"run-{self.manifest_path.stem}",
                    "status": type("Status", (), {"value": "completed"})(),
                    "artifact_dir": artifact_dir,
                    "summary_path": summary_path,
                    "projects": [],
                },
            )()

    runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "batch-artifacts",
        benchmark_runner_cls=FakeBenchmarkRunService,
        slot_limit=1,
    )
    report = runner.run_pending()

    assert calls == [manifest_a, manifest_b]
    assert report.processed_job_ids == [job_a.id, job_b.id]
    assert queue_store.list_jobs()[0].status.value == "completed"
    assert queue_store.list_jobs()[1].status.value == "completed"
    assert (tmp_path / "batch-artifacts" / "jobs" / job_a.id / "summary.json").exists()
    assert (tmp_path / "batch-artifacts" / "jobs" / job_b.id / "summary.json").exists()


def test_batch_runner_skips_paused_projects_and_writes_status_artifact(
    tmp_path: Path,
) -> None:
    from archonlab.batch import BatchRunner

    project_path = _make_project(tmp_path, "DemoProject")
    archon_path = _make_archon(tmp_path)
    manifest = _write_benchmark_manifest(tmp_path, project_path, archon_path)

    queue_store = QueueStore(tmp_path / "queue.db")
    job = queue_store.enqueue("benchmark", {"manifest_path": str(manifest)})

    control_service = ControlService(tmp_path / "control")
    project_config = ProjectConfig(
        name="demo-project",
        project_path=project_path,
        archon_path=archon_path,
    )
    control_service.pause(project_config, reason="manual_hold")

    class FailingBenchmarkRunService:
        def __init__(self, manifest_path: Path) -> None:
            raise AssertionError("benchmark runner should not be called when paused")

    runner = BatchRunner(
        queue_store=queue_store,
        control_service=control_service,
        artifact_root=tmp_path / "batch-artifacts",
        benchmark_runner_cls=FailingBenchmarkRunService,
        slot_limit=1,
    )
    report = runner.run_pending()

    assert report.paused_job_ids == [job.id]
    assert queue_store.list_jobs()[0].status.value == "paused"
    status_path = tmp_path / "batch-artifacts" / "jobs" / job.id / "status.json"
    assert status_path.exists()
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["reason"] == "manual_hold"
