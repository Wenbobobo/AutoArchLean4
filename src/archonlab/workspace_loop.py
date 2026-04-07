from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path

from .batch import BatchRunner
from .config import load_workspace_config
from .control import ControlService
from .fleet import FleetController, WorkerLauncher
from .models import WorkspaceLoopCycle, WorkspaceLoopResult
from .queue import QueueStore


class WorkspaceLoopController:
    result_model = WorkspaceLoopResult

    def __init__(
        self,
        config_path: Path,
        *,
        worker_launcher: WorkerLauncher | None = None,
    ) -> None:
        self.config_path = config_path.resolve()
        self.workspace_config = load_workspace_config(self.config_path)
        self.queue_store = QueueStore(self.workspace_config.run.artifact_root / "archonlab.db")
        self.worker_launcher = worker_launcher

    def run(
        self,
        *,
        project_id: str | None = None,
        project_tags: list[str] | None = None,
        max_iterations: int | None = None,
        dry_run: bool | None = None,
        priority: int = 0,
        note: str | None = None,
        max_cycles: int = 10,
        idle_cycles: int = 1,
        sleep_seconds: float = 0.0,
        fleet_max_cycles: int = 10,
        fleet_idle_cycles: int = 1,
        workers: int | None = None,
        target_jobs_per_worker: int = 2,
        max_jobs_per_worker: int | None = None,
        queue_poll_seconds: float = 2.0,
        queue_idle_timeout_seconds: float = 30.0,
        stale_after_seconds: float | None = 120.0,
    ) -> WorkspaceLoopResult:
        started_at = datetime.now(UTC)
        cycles: list[WorkspaceLoopCycle] = []
        total_scheduled_jobs = 0
        total_processed_jobs = 0
        total_paused_jobs = 0
        total_failed_jobs = 0
        total_workers_launched = 0
        consecutive_idle_cycles = 0
        stop_reason = "max_cycles_reached"

        batch_runner = BatchRunner(
            queue_store=self.queue_store,
            control_service=ControlService(self.workspace_config.run.artifact_root),
            artifact_root=self.workspace_config.run.artifact_root,
            slot_limit=workers or self.workspace_config.run.max_parallel,
        )

        for cycle_index in range(1, max_cycles + 1):
            jobs = self.queue_store.enqueue_workspace_sessions(
                self.config_path,
                project_ids=[project_id] if project_id is not None else None,
                project_tags=project_tags,
                max_iterations=max_iterations,
                dry_run=dry_run,
                priority=priority,
                note=note,
            )
            plan = self.queue_store.plan_fleet(
                target_jobs_per_worker=target_jobs_per_worker,
                stale_after_seconds=stale_after_seconds,
            )
            fleet_result = None
            if plan.active_jobs > 0:
                fleet_result = FleetController(
                    queue_store=self.queue_store,
                    batch_runner=batch_runner,
                    worker_launcher=self.worker_launcher,
                ).run(
                    max_cycles=fleet_max_cycles,
                    idle_cycles=fleet_idle_cycles,
                    worker_count=workers,
                    target_jobs_per_worker=target_jobs_per_worker,
                    max_jobs_per_worker=max_jobs_per_worker,
                    poll_seconds=queue_poll_seconds,
                    idle_timeout_seconds=queue_idle_timeout_seconds,
                    stale_after_seconds=stale_after_seconds,
                )
                total_processed_jobs += fleet_result.total_processed_jobs
                total_paused_jobs += fleet_result.total_paused_jobs
                total_failed_jobs += fleet_result.total_failed_jobs
                total_workers_launched += fleet_result.total_workers_launched

            scheduled_job_ids = [job.id for job in jobs]
            scheduled_session_ids = [
                job.session_id for job in jobs if job.session_id is not None
            ]
            total_scheduled_jobs += len(scheduled_job_ids)
            cycles.append(
                WorkspaceLoopCycle(
                    cycle_index=cycle_index,
                    plan=plan,
                    scheduled_job_ids=scheduled_job_ids,
                    scheduled_session_ids=scheduled_session_ids,
                    fleet_result=fleet_result,
                )
            )

            made_progress = (
                fleet_result is not None
                and (
                    fleet_result.total_processed_jobs > 0
                    or fleet_result.total_paused_jobs > 0
                    or fleet_result.total_failed_jobs > 0
                )
            )
            if made_progress:
                consecutive_idle_cycles = 0
            else:
                consecutive_idle_cycles += 1
                if consecutive_idle_cycles >= idle_cycles:
                    stop_reason = "idle_cycles_exhausted"
                    break

            if sleep_seconds > 0 and cycle_index < max_cycles:
                time.sleep(sleep_seconds)

        return self.result_model(
            started_at=started_at,
            finished_at=datetime.now(UTC),
            cycles_completed=len(cycles),
            stop_reason=stop_reason,
            total_scheduled_jobs=total_scheduled_jobs,
            total_processed_jobs=total_processed_jobs,
            total_paused_jobs=total_paused_jobs,
            total_failed_jobs=total_failed_jobs,
            total_workers_launched=total_workers_launched,
            cycles=cycles,
        )
