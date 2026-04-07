from __future__ import annotations

import threading
import time
from pathlib import Path

from archonlab.benchmark import BenchmarkRunService
from archonlab.models import (
    BenchmarkProjectResult,
    RunStatus,
    SnapshotDelta,
    WorkflowMode,
)
from archonlab.project_state import collect_project_snapshot, score_project_snapshot


def _write_manifest_with_two_projects(tmp_path: Path) -> Path:
    archon_root = tmp_path / "Archon"
    archon_root.mkdir()
    (archon_root / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    for name in ["DemoProjectA", "DemoProjectB"]:
        project_dir = tmp_path / name
        state_dir = project_dir / ".archon"
        prompts_dir = state_dir / "prompts"
        prompts_dir.mkdir(parents=True)
        (project_dir / "lakefile.lean").write_text("import Lake\n", encoding="utf-8")
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

    manifest_path = tmp_path / "benchmark.toml"
    manifest_path.write_text(
        "[benchmark]\n"
        'name = "parallel-smoke"\n'
        'artifact_root = "./artifacts/benchmarks/parallel-smoke"\n'
        "worker_slots = 2\n\n"
        "[[projects]]\n"
        'id = "demo-a"\n'
        'path = "./DemoProjectA"\n'
        'archon_path = "./Archon"\n\n'
        "[[projects]]\n"
        'id = "demo-b"\n'
        'path = "./DemoProjectB"\n'
        'archon_path = "./Archon"\n',
        encoding="utf-8",
    )
    return manifest_path


def test_benchmark_run_service_uses_worker_slots_for_parallel_projects(
    tmp_path: Path, monkeypatch
) -> None:
    manifest_path = _write_manifest_with_two_projects(tmp_path)
    active = 0
    max_active = 0
    lock = threading.Lock()

    def fake_run_benchmark_project(*args, **kwargs) -> BenchmarkProjectResult:
        nonlocal active, max_active
        benchmark_project = args[0]
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.1)
        snapshot = collect_project_snapshot(
            project_path=benchmark_project.project_path,
            archon_path=benchmark_project.archon_path,
        )
        with lock:
            active -= 1
        return BenchmarkProjectResult(
            id=benchmark_project.id,
            workflow=WorkflowMode.ADAPTIVE_LOOP,
            budget_minutes=benchmark_project.budget_minutes,
            run_id=f"run-{benchmark_project.id}",
            run_status=RunStatus.COMPLETED,
            snapshot=snapshot,
            score=score_project_snapshot(snapshot),
            delta=SnapshotDelta(
                sorry_delta=0,
                axiom_delta=0,
                review_session_delta=0,
                task_results_delta=0,
                checklist_done_delta=0,
                score_delta=0,
            ),
        )

    monkeypatch.setattr("archonlab.benchmark.run_benchmark_project", fake_run_benchmark_project)

    result = BenchmarkRunService(manifest_path).run(worker_slots=2)

    assert result.status.value == "completed"
    assert len(result.projects) == 2
    assert max_active >= 2
