from __future__ import annotations

from pathlib import Path

from archonlab.batch import BatchRunner
from archonlab.control import ControlService
from archonlab.fleet import FleetController, InProcessWorkerLauncher, WorkerLaunchRequest
from archonlab.models import BatchRunReport, QueueJobStatus
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
