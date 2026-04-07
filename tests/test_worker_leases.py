from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from archonlab.batch import BatchRunner
from archonlab.control import ControlService
from archonlab.models import (
    ActionPhase,
    BenchmarkProjectResult,
    ExecutorKind,
    ProjectConfig,
    ProviderKind,
    QueueJobStatus,
    RunStatus,
    SnapshotDelta,
    SupervisorAction,
    SupervisorReason,
    WorkerStatus,
    WorkflowMode,
)
from archonlab.project_state import collect_project_snapshot, score_project_snapshot
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


def test_batch_worker_uses_worker_specific_worktree_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_path = _make_project(tmp_path, "DemoProject")
    archon_path = _make_archon(tmp_path)
    manifest_path = _write_manifest(tmp_path, project_path, archon_path, "bench-a")

    queue_store = QueueStore(tmp_path / "queue.db")
    jobs = queue_store.enqueue_benchmark_manifest(
        manifest_path,
        dry_run=True,
        use_worktrees=True,
    )
    captured: dict[str, object] = {}

    def fake_run_benchmark_project(*args, **kwargs) -> BenchmarkProjectResult:
        benchmark_project = args[0]
        captured["worktree_root"] = kwargs["worktree_root"]
        snapshot = collect_project_snapshot(
            project_path=benchmark_project.project_path,
            archon_path=benchmark_project.archon_path,
        )
        return BenchmarkProjectResult(
            id=benchmark_project.id,
            workflow=WorkflowMode.ADAPTIVE_LOOP,
            budget_minutes=benchmark_project.budget_minutes,
            run_id="run-worker",
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

    monkeypatch.setattr("archonlab.batch.run_benchmark_project", fake_run_benchmark_project)

    runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "batch-artifacts",
        slot_limit=1,
    )
    report = runner.run_worker(
        slot_index=3,
        max_jobs=1,
        idle_timeout_seconds=0.1,
    )

    assert report.processed_job_ids == [jobs[0].id]
    worker_id = report.worker_ids[0]
    assert captured["worktree_root"] == (
        tmp_path / "batch-artifacts" / "queue-worktrees" / worker_id
    )


def test_queue_store_auto_assigns_next_available_worker_slot(tmp_path: Path) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")

    first = queue_store.register_worker(slot_index=1, worker_id="worker-a")
    second = queue_store.register_worker(slot_index=None, worker_id="worker-b")
    queue_store.stop_worker(first.worker_id)
    third = queue_store.register_worker(slot_index=None, worker_id="worker-c")

    assert first.slot_index == 1
    assert second.slot_index == 2
    assert third.slot_index == 1


def test_queue_store_reaps_stale_worker_and_requeues_running_job(tmp_path: Path) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    job = queue_store.enqueue("benchmark_project", {"manifest_path": "demo.toml"})
    worker = queue_store.register_worker(slot_index=1, worker_id="worker-stale")
    claimed = queue_store.claim_next_job(worker_id=worker.worker_id)
    assert claimed is not None
    queue_store.assign_job_to_worker(worker.worker_id, claimed.id)

    stale_heartbeat = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
    queue_store._conn.execute(
        "UPDATE queue_workers SET heartbeat_at = ? WHERE worker_id = ?",
        (stale_heartbeat, worker.worker_id),
    )
    queue_store._conn.commit()

    listed = queue_store.list_workers(stale_after_seconds=60)
    assert listed[0].stale is True

    reaped = queue_store.reap_stale_workers(stale_after_seconds=60)

    assert [lease.worker_id for lease in reaped] == [worker.worker_id]
    updated_worker = queue_store.get_worker(worker.worker_id)
    assert updated_worker is not None
    assert updated_worker.status is WorkerStatus.FAILED
    assert updated_worker.current_job_id is None

    updated_job = queue_store.get_job(job.id)
    assert updated_job is not None
    assert updated_job.status is QueueJobStatus.QUEUED
    assert updated_job.worker_id is None
    assert updated_job.error_message == f"Recovered from stale worker {worker.worker_id}"


def test_stale_worker_cannot_overwrite_requeued_job_owned_by_new_worker(
    tmp_path: Path,
) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    job = queue_store.enqueue("benchmark_project", {"manifest_path": "demo.toml"})
    worker_stale = queue_store.register_worker(slot_index=1, worker_id="worker-stale")
    claimed = queue_store.claim_next_job(worker_id=worker_stale.worker_id)
    assert claimed is not None
    queue_store.assign_job_to_worker(worker_stale.worker_id, claimed.id)

    stale_heartbeat = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
    queue_store._conn.execute(
        "UPDATE queue_workers SET heartbeat_at = ? WHERE worker_id = ?",
        (stale_heartbeat, worker_stale.worker_id),
    )
    queue_store._conn.commit()
    queue_store.reap_stale_workers(stale_after_seconds=60)

    worker_fresh = queue_store.register_worker(slot_index=2, worker_id="worker-fresh")
    reclaimed = queue_store.claim_next_job(worker_id=worker_fresh.worker_id)
    assert reclaimed is not None
    assert reclaimed.id == job.id
    queue_store.assign_job_to_worker(worker_fresh.worker_id, reclaimed.id)

    stale_finish = queue_store.finish_job(
        job.id,
        status=QueueJobStatus.COMPLETED,
        worker_id=worker_stale.worker_id,
        result_path=tmp_path / "stale-summary.json",
    )
    stale_release = queue_store.release_job_from_worker(
        worker_stale.worker_id,
        job_id=job.id,
        failed=False,
    )

    assert stale_finish.status is QueueJobStatus.RUNNING
    assert stale_finish.worker_id == worker_fresh.worker_id
    assert stale_release.status is WorkerStatus.FAILED

    current_job = queue_store.get_job(job.id)
    assert current_job is not None
    assert current_job.status is QueueJobStatus.RUNNING
    assert current_job.worker_id == worker_fresh.worker_id
    assert current_job.result_path is None


def test_queue_store_claim_next_job_respects_worker_executor_capabilities(
    tmp_path: Path,
) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    codex_job = queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": "codex.toml"},
        required_executor_kinds=[ExecutorKind.CODEX_EXEC],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
    )
    dry_run_job = queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": "dry-run.toml"},
        required_executor_kinds=[ExecutorKind.DRY_RUN],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
    )
    worker = queue_store.register_worker(
        slot_index=1,
        worker_id="worker-dry-run",
        executor_kinds=[ExecutorKind.DRY_RUN],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
    )

    claimed = queue_store.claim_next_job(worker_id=worker.worker_id)

    assert claimed is not None
    assert claimed.id == dry_run_job.id
    assert claimed.required_executor_kinds == [ExecutorKind.DRY_RUN]
    assert queue_store.get_job(codex_job.id).status is QueueJobStatus.QUEUED


def test_queue_store_claim_next_job_respects_model_and_cost_capabilities(
    tmp_path: Path,
) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    premium_job = queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": "premium.toml"},
        required_executor_kinds=[ExecutorKind.OPENAI_COMPATIBLE],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        required_models=["gpt-5.4"],
        required_cost_tiers=["premium"],
        required_endpoint_classes=["lab"],
    )
    cheap_job = queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": "cheap.toml"},
        required_executor_kinds=[ExecutorKind.OPENAI_COMPATIBLE],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        required_models=["gpt-5.4-mini"],
        required_cost_tiers=["cheap"],
        required_endpoint_classes=["lab"],
    )
    worker = queue_store.register_worker(
        slot_index=1,
        worker_id="worker-cheap",
        executor_kinds=[ExecutorKind.OPENAI_COMPATIBLE],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        models=["gpt-5.4-mini"],
        cost_tiers=["cheap"],
        endpoint_classes=["lab"],
    )

    claimed = queue_store.claim_next_job(worker_id=worker.worker_id)

    assert claimed is not None
    assert claimed.id == cheap_job.id
    assert claimed.required_models == ["gpt-5.4-mini"]
    assert queue_store.get_job(premium_job.id).status is QueueJobStatus.QUEUED


def test_enqueue_benchmark_manifest_uses_previewed_resource_requirements(
    tmp_path: Path,
) -> None:
    project_path = _make_project(tmp_path, "DemoProject")
    archon_path = _make_archon(tmp_path)
    (project_path / "Core.lean").write_text(
        "theorem foo : True := by\n"
        "  sorry\n"
        "\n"
        "theorem bar : True := by\n"
        "  trivial\n",
        encoding="utf-8",
    )
    (project_path / "Support.lean").write_text(
        "theorem helper : True := by\n"
        "  trivial\n",
        encoding="utf-8",
    )
    (project_path / ".archon" / "proof-journal" / "sessions" / "session-1").mkdir(parents=True)
    manifest_path = tmp_path / "previewed.toml"
    manifest_path.write_text(
        "[benchmark]\n"
        'name = "previewed"\n'
        'artifact_root = "./artifacts/previewed"\n\n'
        "[provider]\n"
        'model = "gpt-5.4-mini"\n'
        'cost_tier = "cheap"\n'
        'endpoint_class = "fast"\n'
        "\n"
        "[phase_provider.prover]\n"
        'model = "gpt-5.4"\n'
        'cost_tier = "premium"\n'
        'endpoint_class = "lab"\n'
        "\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'path = "{project_path}"\n'
        f'archon_path = "{archon_path}"\n'
        'workflow = "adaptive_loop"\n',
        encoding="utf-8",
    )

    queue_store = QueueStore(tmp_path / "queue.db")
    jobs = queue_store.enqueue_benchmark_manifest(manifest_path, priority=3)

    assert len(jobs) == 1
    job = jobs[0]
    assert job.required_models == ["gpt-5.4"]
    assert job.required_cost_tiers == ["premium"]
    assert job.required_endpoint_classes == ["lab"]
    assert job.priority == 10
    assert job.preview is not None
    assert job.preview.phase is ActionPhase.PROVER
    assert job.preview.reason == "task_graph_focus"
    assert job.preview.stage == "prover"
    assert job.preview.supervisor_action is SupervisorAction.CONTINUE
    assert job.preview.supervisor_reason is SupervisorReason.HEALTHY
    assert job.preview.theorem_name == "foo"
    assert job.preview.base_priority == 3
    assert job.preview.task_priority_bonus == 2
    assert job.preview.objective_relevance_bonus == 5
    assert job.preview.final_priority == 10

    stored = queue_store.get_job(job.id)
    assert stored is not None
    assert stored.preview is not None
    assert stored.preview.phase is ActionPhase.PROVER
    assert stored.preview.model == "gpt-5.4"
    assert stored.preview.cost_tier == "premium"
    assert stored.preview.endpoint_class == "lab"
    assert stored.preview.supervisor_summary is not None


def test_enqueue_benchmark_manifest_paused_project_keeps_base_priority_breakdown(
    tmp_path: Path,
) -> None:
    project_path = _make_project(tmp_path, "DemoProject")
    archon_path = _make_archon(tmp_path)
    (project_path / "Core.lean").write_text(
        "theorem foo : True := by\n"
        "  sorry\n"
        "\n"
        "theorem bar : True := by\n"
        "  trivial\n",
        encoding="utf-8",
    )
    (project_path / ".archon" / "proof-journal" / "sessions" / "session-1").mkdir(parents=True)
    manifest_path = tmp_path / "paused-previewed.toml"
    manifest_path.write_text(
        "[benchmark]\n"
        'name = "paused-previewed"\n'
        'artifact_root = "./artifacts/paused-previewed"\n\n'
        "[provider]\n"
        'model = "gpt-5.4-mini"\n'
        'cost_tier = "cheap"\n'
        'endpoint_class = "fast"\n'
        "\n"
        "[phase_provider.prover]\n"
        'model = "gpt-5.4"\n'
        'cost_tier = "premium"\n'
        'endpoint_class = "lab"\n'
        "\n"
        "[[projects]]\n"
        'id = "demo-project"\n'
        f'path = "{project_path}"\n'
        f'archon_path = "{archon_path}"\n'
        'workflow = "adaptive_loop"\n',
        encoding="utf-8",
    )
    ControlService(tmp_path / "artifacts" / "paused-previewed").pause(
        ProjectConfig(
            name="demo-project",
            project_path=project_path,
            archon_path=archon_path,
        ),
        reason="manual_hold",
    )

    queue_store = QueueStore(tmp_path / "queue.db")
    jobs = queue_store.enqueue_benchmark_manifest(manifest_path, priority=3)

    assert len(jobs) == 1
    job = jobs[0]
    assert job.priority == 3
    assert job.required_executor_kinds == []
    assert job.required_provider_kinds == []
    assert job.required_models == []
    assert job.required_cost_tiers == []
    assert job.required_endpoint_classes == []
    assert job.preview is not None
    assert job.preview.phase is ActionPhase.STOP
    assert job.preview.reason == "control_paused"
    assert job.preview.base_priority == 3
    assert job.preview.task_priority_bonus == 0
    assert job.preview.objective_relevance_bonus == 0
    assert job.preview.final_priority == 3

    queue_store.pause(job.id, reason="manual_hold")
    ControlService(tmp_path / "artifacts" / "paused-previewed").resume(
        ProjectConfig(
            name="demo-project",
            project_path=project_path,
            archon_path=archon_path,
        )
    )

    requeued = queue_store.requeue(job.id)

    assert requeued.status is QueueJobStatus.QUEUED
    assert requeued.priority == 10
    assert requeued.required_models == ["gpt-5.4"]
    assert requeued.required_cost_tiers == ["premium"]
    assert requeued.required_endpoint_classes == ["lab"]
    assert requeued.preview is not None
    assert requeued.preview.phase is ActionPhase.PROVER
    assert requeued.preview.reason == "task_graph_focus"
    assert requeued.preview.base_priority == 3
    assert requeued.preview.task_priority_bonus == 2
    assert requeued.preview.objective_relevance_bonus == 5
    assert requeued.preview.final_priority == 10
