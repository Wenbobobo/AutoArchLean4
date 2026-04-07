from __future__ import annotations

import concurrent.futures
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
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


class SubprocessWorkerLauncher:
    def __init__(
        self,
        *,
        config_path: Path,
        python_executable: str | None = None,
        repo_root: Path | None = None,
        pythonpath_root: Path | None = None,
    ) -> None:
        self.config_path = config_path
        self.python_executable = python_executable or sys.executable
        self.repo_root = repo_root or Path(__file__).resolve().parents[2]
        self.pythonpath_root = pythonpath_root or (self.repo_root / "src")

    def launch(
        self,
        *,
        batch_runner: BatchRunner,
        request: WorkerLaunchRequest,
    ) -> BatchRunReport:
        launch_specs: list[dict[str, object]]
        if request.plan_driven:
            launch_specs = [
                dict(spec)
                for spec in batch_runner.plan_fleet_launch_specs(
                    worker_count=request.worker_count,
                    target_jobs_per_worker=request.target_jobs_per_worker,
                    max_jobs_per_worker=request.max_jobs_per_worker,
                    poll_seconds=request.poll_seconds,
                    idle_timeout_seconds=request.idle_timeout_seconds,
                    stale_after_seconds=request.stale_after_seconds,
                )
            ]
        else:
            launch_specs = [
                {
                    "slot_index": None,
                    "max_jobs": request.max_jobs_per_worker,
                    "poll_seconds": request.poll_seconds,
                    "idle_timeout_seconds": request.idle_timeout_seconds,
                    "note": f"fleet_worker_{index}",
                    "stale_after_seconds": request.stale_after_seconds,
                    "executor_kinds": None,
                    "provider_kinds": None,
                    "models": None,
                    "cost_tiers": None,
                    "endpoint_classes": None,
                }
                for index in range(1, max(1, request.worker_count or batch_runner.slot_limit) + 1)
            ]
        if not launch_specs:
            return BatchRunReport()

        aggregate = BatchRunReport()
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(launch_specs)) as pool:
            futures = [pool.submit(self._launch_spec, spec) for spec in launch_specs]
            for future in futures:
                report = future.result()
                aggregate.processed_job_ids.extend(report.processed_job_ids)
                aggregate.paused_job_ids.extend(report.paused_job_ids)
                aggregate.failed_job_ids.extend(report.failed_job_ids)
                aggregate.worker_ids.extend(report.worker_ids)
        return aggregate

    def _launch_spec(self, spec: dict[str, object]) -> BatchRunReport:
        command = [
            self.python_executable,
            "-m",
            "archonlab.app",
            "queue",
            "worker",
            "--config",
            str(self.config_path),
            "--json",
        ]
        slot_index = spec.get("slot_index")
        if isinstance(slot_index, int):
            command.extend(["--slot-index", str(slot_index)])
        else:
            command.append("--auto-slot")
        self._append_optional_arg(command, "--max-jobs", spec.get("max_jobs"))
        self._append_optional_arg(command, "--poll-seconds", spec.get("poll_seconds"))
        self._append_optional_arg(
            command,
            "--idle-timeout-seconds",
            spec.get("idle_timeout_seconds"),
        )
        self._append_optional_arg(command, "--note", spec.get("note"))
        self._append_optional_arg(
            command,
            "--stale-after-seconds",
            spec.get("stale_after_seconds"),
        )
        self._append_csv_arg(command, "--executor-kinds", spec.get("executor_kinds"))
        self._append_csv_arg(command, "--provider-kinds", spec.get("provider_kinds"))
        self._append_csv_arg(command, "--models", spec.get("models"))
        self._append_csv_arg(command, "--cost-tiers", spec.get("cost_tiers"))
        self._append_csv_arg(command, "--endpoint-classes", spec.get("endpoint_classes"))

        env = os.environ.copy()
        existing_pythonpath = env.get("PYTHONPATH")
        env["PYTHONPATH"] = (
            str(self.pythonpath_root)
            if not existing_pythonpath
            else f"{self.pythonpath_root}{os.pathsep}{existing_pythonpath}"
        )
        completed = subprocess.run(
            command,
            cwd=self.repo_root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            error_detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
            raise RuntimeError(f"Worker launcher failed: {error_detail}")
        stdout = completed.stdout.strip()
        if not stdout:
            return BatchRunReport()
        return BatchRunReport.model_validate_json(stdout)

    @staticmethod
    def _append_optional_arg(command: list[str], flag: str, value: object) -> None:
        if value is None:
            return
        command.extend([flag, str(value)])

    @staticmethod
    def _append_csv_arg(command: list[str], flag: str, value: object) -> None:
        if not isinstance(value, list) or not value:
            return
        command.extend([flag, ",".join(str(item) for item in value)])


def create_worker_launcher(kind: str, config_path: Path) -> WorkerLauncher:
    if kind == "in_process":
        return InProcessWorkerLauncher()
    if kind == "subprocess":
        return SubprocessWorkerLauncher(config_path=config_path)
    raise ValueError(f"Unsupported worker launcher: {kind}")


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
