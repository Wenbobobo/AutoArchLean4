from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

from archonlab.batch import BatchRunner
from archonlab.control import ControlService
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


def _write_manifest(tmp_path: Path, project_path: Path, archon_path: Path, name: str) -> Path:
    manifest_path = tmp_path / f"{name}.toml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        "[benchmark]\n"
        f'name = "{name}"\n'
        f'artifact_root = "./artifacts/{name}"\n\n'
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'path = "{project_path}"\n'
        f'archon_path = "{archon_path}"\n'
        'workflow = "adaptive_loop"\n',
        encoding="utf-8",
    )
    return manifest_path


def _list_worker_telemetry(source: object) -> list[object] | None:
    for attr_name in (
        "list_worker_leases",
        "get_worker_leases",
        "worker_leases",
        "worker_lease_telemetry",
    ):
        candidate = getattr(source, attr_name, None)
        if candidate is None:
            continue
        if callable(candidate):
            try:
                return list(candidate())
            except TypeError:
                return list(candidate)
        return list(candidate)
    return None


def test_batch_runner_slot_limit_two_keeps_worker_and_job_telemetry(
    tmp_path: Path,
) -> None:
    project_path = _make_project(tmp_path, "DemoProject")
    archon_path = _make_archon(tmp_path)
    manifest_a = _write_manifest(tmp_path / "a", project_path, archon_path, "bench-a")
    manifest_b = _write_manifest(tmp_path / "b", project_path, archon_path, "bench-b")

    queue_store = QueueStore(tmp_path / "queue.db")
    queue_store.enqueue("benchmark", {"manifest_path": str(manifest_a)})
    queue_store.enqueue("benchmark", {"manifest_path": str(manifest_b)})

    claims: list[dict[str, object]] = []
    claims_lock = threading.Lock()
    original_claim_next_job = queue_store.claim_next_job

    def claim_next_job_with_trace() -> object | None:
        job = original_claim_next_job()
        if job is not None:
            with claims_lock:
                claims.append(
                    {
                        "worker_id": threading.get_ident(),
                        "job_id": job.id,
                        "status": job.status.value,
                        "started_at": job.started_at,
                        "updated_at": job.updated_at,
                    }
                )
        return job

    queue_store.claim_next_job = claim_next_job_with_trace  # type: ignore[method-assign]

    class BlockingBenchmarkRunService:
        started_workers: set[int] = set()
        barrier = threading.Barrier(2)
        lock = threading.Lock()

        def __init__(self, manifest_path: Path) -> None:
            self.manifest_path = manifest_path

        def run(self, *, dry_run: bool = True, use_worktrees: bool = False) -> object:
            del dry_run, use_worktrees
            with BlockingBenchmarkRunService.lock:
                BlockingBenchmarkRunService.started_workers.add(threading.get_ident())
            BlockingBenchmarkRunService.barrier.wait(timeout=3)
            artifact_dir = tmp_path / "fake-artifacts" / self.manifest_path.stem
            artifact_dir.mkdir(parents=True, exist_ok=True)
            summary_path = artifact_dir / "summary.json"
            summary_path.write_text(
                json.dumps(
                    {
                        "manifest_path": str(self.manifest_path),
                        "worker_id": threading.get_ident(),
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            time.sleep(0.05)
            return SimpleNamespace(
                benchmark=SimpleNamespace(name="bench"),
                run_id=f"run-{self.manifest_path.stem}",
                status=SimpleNamespace(value="completed"),
                artifact_dir=artifact_dir,
                summary_path=summary_path,
                projects=[],
            )

    runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "batch-artifacts",
        benchmark_runner_cls=BlockingBenchmarkRunService,
        slot_limit=2,
    )
    report = runner.run_pending()

    assert set(report.processed_job_ids) == {
        queue_store.list_jobs()[0].id,
        queue_store.list_jobs()[1].id,
    }
    assert len(BlockingBenchmarkRunService.started_workers) == 2
    assert len({entry["worker_id"] for entry in claims}) == 2
    assert {entry["job_id"] for entry in claims} == {
        queue_store.list_jobs()[0].id,
        queue_store.list_jobs()[1].id,
    }

    jobs = queue_store.list_jobs()
    assert all(job.status.value == "completed" for job in jobs)
    assert all(job.started_at is not None for job in jobs)
    assert all(job.updated_at is not None for job in jobs)

    lease_telemetry = _list_worker_telemetry(runner) or _list_worker_telemetry(queue_store)
    if lease_telemetry is not None:
        for lease in lease_telemetry:
            if isinstance(lease, dict):
                worker_id = lease.get("worker_id")
                status = lease.get("status")
                current_job_id = lease.get("current_job_id")
                last_job_id = lease.get("last_job_id")
                heartbeat = lease.get("heartbeat")
                updated_at = lease.get("updated_at")
            else:
                worker_id = getattr(lease, "worker_id", None)
                status = getattr(lease, "status", None)
                current_job_id = getattr(lease, "current_job_id", None)
                last_job_id = getattr(lease, "last_job_id", None)
                heartbeat = getattr(lease, "heartbeat", None)
                updated_at = getattr(lease, "updated_at", None)
            assert worker_id is not None
            assert status is not None
            assert current_job_id is not None or last_job_id is not None
            assert heartbeat is not None or updated_at is not None
