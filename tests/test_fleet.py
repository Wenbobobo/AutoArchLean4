from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from archonlab.batch import BatchRunner
from archonlab.control import ControlService
from archonlab.events import EventStore
from archonlab.fleet import (
    FleetController,
    InProcessWorkerLauncher,
    SubprocessWorkerLauncher,
    WorkerLaunchRequest,
    persist_batch_fleet_run,
)
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
    QueueJobStatus,
)
from archonlab.queue import QueueStore


class FakeWorkerLauncher:
    def __init__(self, queue_store: QueueStore, *, jobs_per_wave: int = 1) -> None:
        self.queue_store = queue_store
        self.jobs_per_wave = jobs_per_wave
        self.requests: list[WorkerLaunchRequest] = []

    def launch(
        self,
        *,
        batch_runner: BatchRunner,
        request: WorkerLaunchRequest,
    ) -> BatchRunReport:
        del batch_runner
        self.requests.append(request)
        processed: list[str] = []
        for _ in range(self.jobs_per_wave):
            job = self.queue_store.claim_next_job()
            if job is None:
                break
            self.queue_store.finish_job(
                job.id,
                status=QueueJobStatus.COMPLETED,
            )
            processed.append(job.id)
        return BatchRunReport(
            processed_job_ids=processed,
            worker_ids=["worker-fake"] if processed else [],
        )


def _healthy_provider_report() -> list[ProviderPoolHealthReport]:
    return [
        ProviderPoolHealthReport(
            pool_name="lab",
            status=ProviderPoolHealthStatus.HEALTHY,
            strategy="ordered_failover",
            total_members=1,
            available_members=1,
            quarantined_members=0,
            members=[
                ProviderPoolMemberHealth(
                    pool_name="lab",
                    member_name="premium-a",
                    status=ProviderPoolMemberHealthStatus.HEALTHY,
                    model="gpt-5.4",
                    cost_tier="premium",
                    endpoint_class="lab",
                )
            ],
        )
    ]


def _quarantined_provider_report() -> list[ProviderPoolHealthReport]:
    return [
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
    ]


def test_fleet_controller_runs_plan_driven_waves_until_queue_drains(tmp_path: Path) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    queue_store.enqueue("benchmark", {"manifest_path": "bench-a.toml"})
    queue_store.enqueue("benchmark", {"manifest_path": "bench-b.toml"})
    batch_runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "artifacts",
        slot_limit=2,
    )
    launcher = FakeWorkerLauncher(queue_store)
    controller = FleetController(
        queue_store=queue_store,
        batch_runner=batch_runner,
        worker_launcher=launcher,
    )

    result = controller.run(
        max_cycles=4,
        idle_cycles=1,
        poll_seconds=0.01,
        idle_timeout_seconds=0.01,
    )

    assert result.stop_reason == "queue_drained"
    assert result.cycles_completed == 2
    assert result.total_processed_jobs == 2
    assert len(result.cycles) == 2
    assert len(launcher.requests) == 2
    assert all(request.plan_driven for request in launcher.requests)


def test_fleet_controller_stops_after_idle_cycle_budget(tmp_path: Path) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    queue_store.enqueue("benchmark", {"manifest_path": "bench-a.toml"})
    batch_runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "artifacts",
        slot_limit=1,
    )

    class NoOpLauncher:
        def __init__(self) -> None:
            self.requests: list[WorkerLaunchRequest] = []

        def launch(
            self,
            *,
            batch_runner: BatchRunner,
            request: WorkerLaunchRequest,
        ) -> BatchRunReport:
            del batch_runner
            self.requests.append(request)
            return BatchRunReport()

    launcher = NoOpLauncher()
    controller = FleetController(
        queue_store=queue_store,
        batch_runner=batch_runner,
        worker_launcher=launcher,
    )

    result = controller.run(
        max_cycles=3,
        idle_cycles=1,
        poll_seconds=0.01,
        idle_timeout_seconds=0.01,
    )

    assert result.stop_reason == "idle_cycles_exhausted"
    assert result.cycles_completed == 1
    assert result.total_processed_jobs == 0
    assert len(launcher.requests) == 1


def test_fleet_controller_persists_run_artifacts_and_event_store(tmp_path: Path) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    queue_store.enqueue("benchmark", {"manifest_path": "bench-a.toml"})
    batch_runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "artifacts",
        slot_limit=1,
    )
    launcher = FakeWorkerLauncher(queue_store)
    controller = FleetController(
        queue_store=queue_store,
        batch_runner=batch_runner,
        worker_launcher=launcher,
        config_path=tmp_path / "workspace.toml",
        workspace_id="demo-workspace",
        note="dashboard_manual_fleet",
    )

    result = controller.run(
        max_cycles=2,
        idle_cycles=1,
        poll_seconds=0.01,
        idle_timeout_seconds=0.01,
    )

    assert result.fleet_run_id.startswith("fleet-run-")
    assert result.artifact_dir is not None
    assert result.workspace_id == "demo-workspace"
    assert result.launcher == "FakeWorkerLauncher"
    assert result.note == "dashboard_manual_fleet"
    assert (result.artifact_dir / "request.json").exists()
    assert (result.artifact_dir / "summary.json").exists()
    assert json.loads((result.artifact_dir / "summary.json").read_text(encoding="utf-8"))[
        "fleet_run_id"
    ] == result.fleet_run_id

    store = EventStore(tmp_path / "queue.db")
    persisted = store.get_fleet_run(result.fleet_run_id)
    assert persisted is not None
    assert persisted.total_processed_jobs == 1
    assert persisted.stop_reason == "queue_drained"


def test_fleet_controller_final_plan_includes_provider_capacity(
    tmp_path: Path,
    monkeypatch,
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
    monkeypatch.setattr(
        "archonlab.queue.snapshot_provider_pool_health",
        lambda provider_pools, *, db_path=None: _healthy_provider_report(),
    )

    class NoOpLauncher:
        def launch(
            self,
            *,
            batch_runner: BatchRunner,
            request: WorkerLaunchRequest,
        ) -> BatchRunReport:
            del batch_runner, request
            return BatchRunReport()

    batch_runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "artifacts",
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
    controller = FleetController(
        queue_store=queue_store,
        batch_runner=batch_runner,
        worker_launcher=NoOpLauncher(),
    )

    result = controller.run(
        max_cycles=1,
        idle_cycles=1,
        poll_seconds=0.01,
        idle_timeout_seconds=0.01,
    )

    assert result.stop_reason == "idle_cycles_exhausted"
    assert result.final_plan.profiles[0].provider_capacity_status == "healthy"
    assert result.final_plan.profiles[0].available_provider_members == 1


def test_persist_batch_fleet_run_uses_provider_aware_final_plan(
    tmp_path: Path,
    monkeypatch,
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
    monkeypatch.setattr(
        "archonlab.queue.snapshot_provider_pool_health",
        lambda provider_pools, *, db_path=None: _healthy_provider_report(),
    )

    initial_plan = queue_store.plan_fleet(target_jobs_per_worker=1)
    result = persist_batch_fleet_run(
        queue_store=queue_store,
        artifact_root=tmp_path / "artifacts",
        initial_plan=initial_plan,
        report=BatchRunReport(),
        started_at=datetime.now(UTC),
        target_jobs_per_worker=1,
        stale_after_seconds=60.0,
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
        provider_health_db_path=tmp_path / "queue.db",
    )

    assert result.final_plan.profiles[0].provider_capacity_status == "healthy"
    assert result.final_plan.profiles[0].available_provider_members == 1


def test_fleet_controller_stops_when_provider_capacity_is_unavailable(
    tmp_path: Path,
    monkeypatch,
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
    monkeypatch.setattr(
        "archonlab.queue.snapshot_provider_pool_health",
        lambda provider_pools, *, db_path=None: _quarantined_provider_report(),
    )

    class FailLauncher:
        def launch(
            self,
            *,
            batch_runner: BatchRunner,
            request: WorkerLaunchRequest,
        ) -> BatchRunReport:
            del batch_runner, request
            raise AssertionError("launch should not be called when provider capacity is blocked")

    batch_runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "artifacts",
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
    controller = FleetController(
        queue_store=queue_store,
        batch_runner=batch_runner,
        worker_launcher=FailLauncher(),
    )

    result = controller.run(
        max_cycles=3,
        idle_cycles=2,
        poll_seconds=0.01,
        idle_timeout_seconds=0.01,
    )

    assert result.stop_reason == "capacity_unavailable"
    assert result.cycles_completed == 1
    assert result.total_processed_jobs == 0
    assert result.total_workers_launched == 0
    assert len(result.cycles) == 1
    assert result.cycles[0].plan.profiles[0].provider_capacity_status == "unavailable"
    assert result.final_plan.profiles[0].provider_capacity_status == "unavailable"


def test_in_process_worker_launcher_delegates_to_batch_runner(tmp_path: Path, monkeypatch) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    batch_runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "artifacts",
        slot_limit=1,
    )
    captured: dict[str, object] = {}

    def fake_run_fleet(**kwargs) -> BatchRunReport:
        captured.update(kwargs)
        return BatchRunReport(worker_ids=["worker-1"])

    monkeypatch.setattr(batch_runner, "run_fleet", fake_run_fleet)

    launcher = InProcessWorkerLauncher()
    report = launcher.launch(
        batch_runner=batch_runner,
        request=WorkerLaunchRequest(
            worker_count=2,
            plan_driven=True,
            target_jobs_per_worker=3,
            max_jobs_per_worker=4,
            poll_seconds=0.5,
            idle_timeout_seconds=2.0,
            stale_after_seconds=30.0,
        ),
    )

    assert report.worker_ids == ["worker-1"]
    assert captured == {
        "worker_count": 2,
        "plan_driven": True,
        "target_jobs_per_worker": 3,
        "max_jobs_per_worker": 4,
        "poll_seconds": 0.5,
        "idle_timeout_seconds": 2.0,
        "stale_after_seconds": 30.0,
        "executor_kinds": None,
        "provider_kinds": None,
        "models": None,
        "cost_tiers": None,
        "endpoint_classes": None,
    }


def test_subprocess_worker_launcher_spawns_json_queue_workers_and_merges_reports(
    tmp_path: Path,
    monkeypatch,
) -> None:
    queue_store = QueueStore(tmp_path / "queue.db")
    batch_runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(tmp_path / "control"),
        artifact_root=tmp_path / "artifacts",
        slot_limit=2,
    )
    monkeypatch.setattr(
        batch_runner,
        "plan_fleet_launch_specs",
        lambda **kwargs: [
            {
                "slot_index": None,
                "max_jobs": 2,
                "poll_seconds": 0.5,
                "idle_timeout_seconds": 3.0,
                "note": "planned_fleet:cheap:1",
                "stale_after_seconds": 30.0,
                "executor_kinds": [],
                "provider_kinds": [],
                "models": ["gpt-5.4-mini"],
                "cost_tiers": ["cheap"],
                "endpoint_classes": ["lab"],
            },
            {
                "slot_index": None,
                "max_jobs": 1,
                "poll_seconds": 0.5,
                "idle_timeout_seconds": 3.0,
                "note": "planned_fleet:premium:1",
                "stale_after_seconds": 30.0,
                "executor_kinds": [],
                "provider_kinds": [],
                "models": ["gpt-5.4"],
                "cost_tiers": ["premium"],
                "endpoint_classes": ["lab"],
            },
        ],
    )
    calls: list[list[str]] = []

    def fake_run(
        command: list[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        capture_output: bool = False,
        text: bool = False,
        check: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        del cwd, env, capture_output, text, check
        calls.append(command)
        payload = (
            BatchRunReport(
                processed_job_ids=[f"job-{len(calls)}"],
                worker_ids=[f"worker-{len(calls)}"],
            )
            .model_dump_json()
        )
        return subprocess.CompletedProcess(command, 0, stdout=payload, stderr="")

    monkeypatch.setattr("archonlab.fleet.subprocess.run", fake_run)

    launcher = SubprocessWorkerLauncher(
        config_path=tmp_path / "workspace.toml",
        python_executable="/usr/bin/python3",
        repo_root=tmp_path,
        pythonpath_root=tmp_path / "src",
    )
    report = launcher.launch(
        batch_runner=batch_runner,
        request=WorkerLaunchRequest(
            worker_count=2,
            plan_driven=True,
            target_jobs_per_worker=2,
            max_jobs_per_worker=3,
            poll_seconds=0.5,
            idle_timeout_seconds=3.0,
            stale_after_seconds=30.0,
        ),
    )

    assert sorted(report.processed_job_ids) == ["job-1", "job-2"]
    assert sorted(report.worker_ids) == ["worker-1", "worker-2"]
    assert len(calls) == 2
    assert calls[0][:6] == [
        "/usr/bin/python3",
        "-m",
        "archonlab.app",
        "queue",
        "worker",
        "--config",
    ]
    assert "--json" in calls[0]
    assert "--auto-slot" in calls[0]
    assert "--models" in calls[0]
    assert "gpt-5.4-mini" in calls[0]
    assert "--cost-tiers" in calls[1]
    assert "premium" in calls[1]
