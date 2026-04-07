from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace

from archonlab.batch import BatchRunner
from archonlab.control import ControlService
from archonlab.models import (
    BatchRunReport,
    ExecutorKind,
    ProviderKind,
    ProviderPoolConfig,
    ProviderPoolHealthReport,
    ProviderPoolHealthStatus,
    ProviderPoolMemberConfig,
    ProviderPoolMemberHealth,
    ProviderPoolMemberHealthStatus,
)
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


def _make_manifest(tmp_path: Path, project_path: Path, archon_path: Path, name: str) -> Path:
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


def test_batch_runner_slot_limit_dispatches_jobs_concurrently(tmp_path: Path) -> None:
    project_path = _make_project(tmp_path, "DemoProject")
    archon_path = _make_archon(tmp_path)
    manifest_a = _make_manifest(tmp_path / "a", project_path, archon_path, "bench-a")
    manifest_b = _make_manifest(tmp_path / "b", project_path, archon_path, "bench-b")

    queue_store = QueueStore(tmp_path / "queue.db")
    job_a = queue_store.enqueue("benchmark", {"manifest_path": str(manifest_a)})
    job_b = queue_store.enqueue("benchmark", {"manifest_path": str(manifest_b)})

    control_service = ControlService(tmp_path / "control")

    class BlockingBenchmarkRunService:
        active = 0
        max_active = 0
        started_manifests: list[Path] = []
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def __init__(self, manifest_path: Path) -> None:
            self.manifest_path = manifest_path

        def run(self, *, dry_run: bool = True, use_worktrees: bool = False) -> object:
            del dry_run, use_worktrees
            with BlockingBenchmarkRunService.lock:
                BlockingBenchmarkRunService.active += 1
                BlockingBenchmarkRunService.max_active = max(
                    BlockingBenchmarkRunService.max_active,
                    BlockingBenchmarkRunService.active,
                )
                BlockingBenchmarkRunService.started_manifests.append(self.manifest_path)
            try:
                BlockingBenchmarkRunService.barrier.wait(timeout=2)
                artifact_dir = tmp_path / "fake-artifacts" / self.manifest_path.stem
                artifact_dir.mkdir(parents=True, exist_ok=True)
                summary_path = artifact_dir / "summary.json"
                summary_path.write_text(
                    "{\"status\": \"completed\"}",
                    encoding="utf-8",
                )
                return SimpleNamespace(
                    benchmark=SimpleNamespace(name="bench"),
                    run_id=f"run-{self.manifest_path.stem}",
                    status=SimpleNamespace(value="completed"),
                    artifact_dir=artifact_dir,
                    summary_path=summary_path,
                    projects=[],
                )
            finally:
                with BlockingBenchmarkRunService.lock:
                    BlockingBenchmarkRunService.active -= 1

    runner = BatchRunner(
        queue_store=queue_store,
        control_service=control_service,
        artifact_root=tmp_path / "batch-artifacts",
        benchmark_runner_cls=BlockingBenchmarkRunService,
        slot_limit=2,
    )

    outcome: dict[str, object] = {}

    def run_batch() -> None:
        try:
            outcome["report"] = runner.run_pending()
        except BaseException as exc:  # noqa: BLE001
            outcome["error"] = exc

    thread = threading.Thread(target=run_batch, daemon=True)
    thread.start()
    thread.join(timeout=6)
    assert thread.is_alive() is False, "batch runner did not finish in time"
    assert "error" not in outcome, outcome["error"]
    report = outcome["report"]

    assert set(report.processed_job_ids) == {job_a.id, job_b.id}
    assert BlockingBenchmarkRunService.max_active >= 2
    assert set(BlockingBenchmarkRunService.started_manifests) == {manifest_a, manifest_b}
    assert queue_store.list_jobs()[0].status.value == "completed"
    assert queue_store.list_jobs()[1].status.value == "completed"


def test_batch_runner_fleet_launches_auto_slot_workers(tmp_path: Path) -> None:
    project_path = _make_project(tmp_path, "DemoProject")
    archon_path = _make_archon(tmp_path)
    manifest_a = _make_manifest(tmp_path / "a", project_path, archon_path, "bench-a")
    manifest_b = _make_manifest(tmp_path / "b", project_path, archon_path, "bench-b")

    queue_store = QueueStore(tmp_path / "queue.db")
    queue_store.enqueue("benchmark", {"manifest_path": str(manifest_a)})
    queue_store.enqueue("benchmark", {"manifest_path": str(manifest_b)})

    control_service = ControlService(tmp_path / "control")

    class BlockingBenchmarkRunService:
        active = 0
        max_active = 0
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def __init__(self, manifest_path: Path) -> None:
            self.manifest_path = manifest_path

        def run(self, *, dry_run: bool = True, use_worktrees: bool = False) -> object:
            del dry_run, use_worktrees
            with BlockingBenchmarkRunService.lock:
                BlockingBenchmarkRunService.active += 1
                BlockingBenchmarkRunService.max_active = max(
                    BlockingBenchmarkRunService.max_active,
                    BlockingBenchmarkRunService.active,
                )
            try:
                BlockingBenchmarkRunService.barrier.wait(timeout=2)
                artifact_dir = tmp_path / "fleet-artifacts" / self.manifest_path.stem
                artifact_dir.mkdir(parents=True, exist_ok=True)
                summary_path = artifact_dir / "summary.json"
                summary_path.write_text(
                    "{\"status\": \"completed\"}",
                    encoding="utf-8",
                )
                return SimpleNamespace(
                    benchmark=SimpleNamespace(name="bench"),
                    run_id=f"run-{self.manifest_path.stem}",
                    status=SimpleNamespace(value="completed"),
                    artifact_dir=artifact_dir,
                    summary_path=summary_path,
                    projects=[],
                )
            finally:
                with BlockingBenchmarkRunService.lock:
                    BlockingBenchmarkRunService.active -= 1

    runner = BatchRunner(
        queue_store=queue_store,
        control_service=control_service,
        artifact_root=tmp_path / "batch-artifacts",
        benchmark_runner_cls=BlockingBenchmarkRunService,
        slot_limit=2,
    )

    report = runner.run_fleet(
        worker_count=2,
        idle_timeout_seconds=0.1,
        poll_seconds=0.01,
        stale_after_seconds=60,
    )

    assert len(report.processed_job_ids) == 2
    assert len(report.worker_ids) == 2
    workers = queue_store.list_workers()
    assert {worker.slot_index for worker in workers} == {1, 2}
    assert all(worker.worker_id.startswith("worker-auto-") for worker in workers)
    assert BlockingBenchmarkRunService.max_active >= 2


def test_batch_runner_plan_driven_fleet_launches_profile_specific_workers(
    tmp_path: Path, monkeypatch
) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": "cheap.toml"},
        project_id="cheap-project",
        required_executor_kinds=[ExecutorKind.DRY_RUN],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        required_models=["gpt-5.4-mini"],
        required_cost_tiers=["cheap"],
        required_endpoint_classes=["lab"],
    )
    queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": "premium.toml"},
        project_id="premium-project",
        required_executor_kinds=[ExecutorKind.DRY_RUN],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        required_models=["gpt-5.4"],
        required_cost_tiers=["premium"],
        required_endpoint_classes=["lab"],
    )

    runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "batch-artifacts",
        slot_limit=1,
    )

    launched: list[dict[str, object]] = []

    def fake_run_worker(**kwargs) -> BatchRunReport:
        launched.append(kwargs)
        return BatchRunReport(
            worker_ids=[f"worker-{len(launched)}"],
        )

    monkeypatch.setattr(runner, "run_worker", fake_run_worker)

    report = runner.run_fleet(
        plan_driven=True,
        target_jobs_per_worker=2,
        idle_timeout_seconds=0.1,
        poll_seconds=0.01,
        stale_after_seconds=60,
    )

    assert report.worker_ids == ["worker-1", "worker-2"]
    assert len(launched) == 2
    assert {tuple(call["models"] or []) for call in launched} == {
        ("gpt-5.4-mini",),
        ("gpt-5.4",),
    }
    assert {tuple(call["cost_tiers"] or []) for call in launched} == {
        ("cheap",),
        ("premium",),
    }
    assert all(str(call["note"]).startswith("planned_fleet:") for call in launched)


def test_batch_runner_plan_driven_fleet_launches_generic_workers_for_unconstrained_jobs(
    tmp_path: Path, monkeypatch
) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": "generic-a.toml"},
        project_id="generic-a",
    )
    queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": "generic-b.toml"},
        project_id="generic-b",
    )

    runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "batch-artifacts",
        slot_limit=1,
    )

    launched: list[dict[str, object]] = []

    def fake_run_worker(**kwargs) -> BatchRunReport:
        launched.append(kwargs)
        return BatchRunReport(worker_ids=[f"worker-{len(launched)}"])

    monkeypatch.setattr(runner, "run_worker", fake_run_worker)

    report = runner.run_fleet(
        plan_driven=True,
        target_jobs_per_worker=2,
        idle_timeout_seconds=0.1,
        poll_seconds=0.01,
        stale_after_seconds=60,
    )

    assert report.worker_ids == ["worker-1"]
    assert len(launched) == 1
    assert launched[0]["executor_kinds"] is None
    assert launched[0]["provider_kinds"] is None
    assert launched[0]["models"] is None
    assert launched[0]["cost_tiers"] is None
    assert launched[0]["endpoint_classes"] is None
    assert str(launched[0]["note"]).startswith("planned_fleet:generic:")


def test_batch_runner_plan_driven_fleet_skips_profiles_without_available_provider_capacity(
    tmp_path: Path, monkeypatch
) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": "premium.toml"},
        project_id="premium-project",
        required_executor_kinds=[ExecutorKind.DRY_RUN],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        required_models=["gpt-5.4"],
        required_cost_tiers=["premium"],
        required_endpoint_classes=["lab"],
    )

    runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "batch-artifacts",
        slot_limit=1,
        provider_pools={
            "lab": ProviderPoolConfig(
                name="lab",
                members=[
                    ProviderPoolMemberConfig(
                        name="premium-a",
                        model="gpt-5.4",
                        cost_tier="premium",
                        endpoint_class="lab",
                    )
                ],
            )
        },
    )

    monkeypatch.setattr(
        "archonlab.batch.snapshot_provider_pool_health",
        lambda provider_pools, *, db_path=None: [
            ProviderPoolHealthReport(
                pool_name="lab",
                status=ProviderPoolHealthStatus.ALL_QUARANTINED,
                strategy="ordered_failover",
                total_members=1,
                available_members=0,
                quarantined_members=1,
                members=[
                    ProviderPoolMemberHealth(
                        pool_name="lab",
                        member_name="premium-a",
                        status=ProviderPoolMemberHealthStatus.QUARANTINED,
                        model="gpt-5.4",
                        cost_tier="premium",
                        endpoint_class="lab",
                    )
                ],
            )
        ],
    )

    launched: list[dict[str, object]] = []

    def fake_run_worker(**kwargs) -> BatchRunReport:
        launched.append(kwargs)
        return BatchRunReport(worker_ids=[f"worker-{len(launched)}"])

    monkeypatch.setattr(runner, "run_worker", fake_run_worker)

    report = runner.run_fleet(
        plan_driven=True,
        target_jobs_per_worker=1,
        idle_timeout_seconds=0.1,
        poll_seconds=0.01,
        stale_after_seconds=60,
    )

    assert report.worker_ids == []
    assert launched == []
