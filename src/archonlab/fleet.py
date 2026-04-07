from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from .batch import BatchRunner
from .models import BatchRunReport, FleetControllerCycle, FleetControllerResult
from .queue import QueueStore


@dataclass(slots=True)
class WorkerLaunchRequest:
    worker_count: int | None = None
    plan_driven: bool = True
    target_jobs_per_worker: int = 2
    max_jobs_per_worker: int | None = None
    poll_seconds: float = 2.0
    idle_timeout_seconds: float = 30.0
    stale_after_seconds: float | None = 120.0


class WorkerLauncher(Protocol):
    def launch(
        self,
        *,
        batch_runner: BatchRunner,
        request: WorkerLaunchRequest,
    ) -> BatchRunReport: ...


class InProcessWorkerLauncher:
    def launch(
        self,
        *,
        batch_runner: BatchRunner,
        request: WorkerLaunchRequest,
    ) -> BatchRunReport:
        return batch_runner.run_fleet(
            worker_count=request.worker_count,
            plan_driven=request.plan_driven,
            target_jobs_per_worker=request.target_jobs_per_worker,
            max_jobs_per_worker=request.max_jobs_per_worker,
            poll_seconds=request.poll_seconds,
            idle_timeout_seconds=request.idle_timeout_seconds,
            stale_after_seconds=request.stale_after_seconds,
            executor_kinds=None,
            provider_kinds=None,
            models=None,
            cost_tiers=None,
            endpoint_classes=None,
        )


class FleetController:
    result_model = FleetControllerResult

    def __init__(
        self,
        *,
        queue_store: QueueStore,
        batch_runner: BatchRunner,
        worker_launcher: WorkerLauncher | None = None,
    ) -> None:
        self.queue_store = queue_store
        self.batch_runner = batch_runner
        self.worker_launcher = worker_launcher or InProcessWorkerLauncher()

    def run(
        self,
        *,
        max_cycles: int = 10,
        idle_cycles: int = 1,
        worker_count: int | None = None,
        target_jobs_per_worker: int = 2,
        max_jobs_per_worker: int | None = None,
        poll_seconds: float = 2.0,
        idle_timeout_seconds: float = 30.0,
        stale_after_seconds: float | None = 120.0,
    ) -> FleetControllerResult:
        started_at = datetime.now(UTC)
        cycles: list[FleetControllerCycle] = []
        total_processed = 0
        total_paused = 0
        total_failed = 0
        total_workers = 0
        consecutive_idle_cycles = 0
        stop_reason = "max_cycles_reached"

        for cycle_index in range(1, max_cycles + 1):
            plan = self.queue_store.plan_fleet(
                target_jobs_per_worker=target_jobs_per_worker,
                stale_after_seconds=stale_after_seconds,
            )
            if plan.active_jobs == 0:
                stop_reason = "queue_drained"
                break

            report = self.worker_launcher.launch(
                batch_runner=self.batch_runner,
                request=WorkerLaunchRequest(
                    worker_count=worker_count,
                    plan_driven=True,
                    target_jobs_per_worker=target_jobs_per_worker,
                    max_jobs_per_worker=max_jobs_per_worker,
                    poll_seconds=poll_seconds,
                    idle_timeout_seconds=idle_timeout_seconds,
                    stale_after_seconds=stale_after_seconds,
                ),
            )
            cycles.append(
                FleetControllerCycle(
                    cycle_index=cycle_index,
                    plan=plan,
                    report=report,
                )
            )
            total_processed += len(report.processed_job_ids)
            total_paused += len(report.paused_job_ids)
            total_failed += len(report.failed_job_ids)
            total_workers += len(report.worker_ids)

            made_progress = bool(
                report.processed_job_ids or report.paused_job_ids or report.failed_job_ids
            )
            if made_progress:
                consecutive_idle_cycles = 0
            else:
                consecutive_idle_cycles += 1
                if consecutive_idle_cycles >= idle_cycles:
                    stop_reason = "idle_cycles_exhausted"
                    break

        final_plan = self.queue_store.plan_fleet(
            target_jobs_per_worker=target_jobs_per_worker,
            stale_after_seconds=stale_after_seconds,
        )
        return self.result_model(
            started_at=started_at,
            finished_at=datetime.now(UTC),
            cycles_completed=len(cycles),
            stop_reason=stop_reason,
            total_processed_jobs=total_processed,
            total_paused_jobs=total_paused,
            total_failed_jobs=total_failed,
            total_workers_launched=total_workers,
            cycles=cycles,
            final_plan=final_plan,
        )
