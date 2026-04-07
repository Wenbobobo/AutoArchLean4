from __future__ import annotations

import concurrent.futures
import json
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

from .batch import BatchRunner
from .events import EventStore
from .models import BatchRunReport, FleetControllerCycle, FleetControllerResult, QueueFleetPlan
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
                cast(dict[str, object], dict(spec))
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


def persist_batch_fleet_run(
    *,
    queue_store: QueueStore,
    artifact_root: Path,
    initial_plan: QueueFleetPlan,
    report: BatchRunReport,
    started_at: datetime,
    target_jobs_per_worker: int,
    stale_after_seconds: float | None,
    workspace_id: str = "standalone",
    config_path: Path | None = None,
    launcher: str = "batch_runner",
    note: str | None = None,
    request_payload: dict[str, object] | None = None,
) -> FleetControllerResult:
    fleet_run_id = FleetController._new_fleet_run_id()
    artifact_dir = artifact_root.resolve() / "fleet-runs" / fleet_run_id
    artifact_dir.mkdir(parents=True, exist_ok=False)
    resolved_config_path = config_path.resolve() if config_path is not None else None
    FleetController._write_json(
        artifact_dir / "request.json",
        {
            "fleet_run_id": fleet_run_id,
            "workspace_id": workspace_id,
            "config_path": str(resolved_config_path) if resolved_config_path is not None else None,
            "launcher": launcher,
            "note": note,
            "request": request_payload or {},
        },
    )
    finished_at = datetime.now(UTC)
    cycle = FleetControllerCycle(
        cycle_index=1,
        started_at=started_at,
        finished_at=finished_at,
        plan=initial_plan,
        report=report,
    )
    FleetController._write_json(
        artifact_dir / "cycle-001.json",
        cycle.model_dump(mode="json"),
    )
    final_plan = queue_store.plan_fleet(
        target_jobs_per_worker=target_jobs_per_worker,
        stale_after_seconds=stale_after_seconds,
    )
    stop_reason = "queue_drained" if final_plan.active_jobs == 0 else "manual_fleet_complete"
    result = FleetControllerResult(
        fleet_run_id=fleet_run_id,
        workspace_id=workspace_id,
        config_path=resolved_config_path,
        artifact_dir=artifact_dir,
        launcher=launcher,
        note=note,
        started_at=started_at,
        finished_at=finished_at,
        cycles_completed=1,
        stop_reason=stop_reason,
        total_processed_jobs=len(report.processed_job_ids),
        total_paused_jobs=len(report.paused_job_ids),
        total_failed_jobs=len(report.failed_job_ids),
        total_workers_launched=len(report.worker_ids),
        cycles=[cycle],
        final_plan=final_plan,
    )
    FleetController._persist_result(
        EventStore(queue_store.db_path),
        artifact_dir,
        result,
    )
    return result


class FleetController:
    result_model = FleetControllerResult

    def __init__(
        self,
        *,
        queue_store: QueueStore,
        batch_runner: BatchRunner,
        worker_launcher: WorkerLauncher | None = None,
        config_path: Path | None = None,
        workspace_id: str = "standalone",
        note: str | None = None,
    ) -> None:
        self.queue_store = queue_store
        self.batch_runner = batch_runner
        self.worker_launcher = worker_launcher or InProcessWorkerLauncher()
        self.config_path = config_path.resolve() if config_path is not None else None
        self.workspace_id = workspace_id
        self.note = note

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
        fleet_run_id = self._new_fleet_run_id()
        artifact_dir = self.batch_runner.artifact_root / "fleet-runs" / fleet_run_id
        artifact_dir.mkdir(parents=True, exist_ok=False)
        self._write_json(
            artifact_dir / "request.json",
            {
                "fleet_run_id": fleet_run_id,
                "workspace_id": self.workspace_id,
                "config_path": (
                    str(self.config_path) if self.config_path is not None else None
                ),
                "launcher": self._launcher_name(),
                "note": self.note,
                "max_cycles": max_cycles,
                "idle_cycles": idle_cycles,
                "worker_count": worker_count,
                "target_jobs_per_worker": target_jobs_per_worker,
                "max_jobs_per_worker": max_jobs_per_worker,
                "poll_seconds": poll_seconds,
                "idle_timeout_seconds": idle_timeout_seconds,
                "stale_after_seconds": stale_after_seconds,
            },
        )
        cycles: list[FleetControllerCycle] = []
        total_processed = 0
        total_paused = 0
        total_failed = 0
        total_workers = 0
        consecutive_idle_cycles = 0
        stop_reason = "max_cycles_reached"
        event_store = EventStore(self.queue_store.db_path)
        result = self.result_model(
            fleet_run_id=fleet_run_id,
            workspace_id=self.workspace_id,
            config_path=self.config_path,
            artifact_dir=artifact_dir,
            launcher=self._launcher_name(),
            note=self.note,
            started_at=started_at,
            stop_reason="running",
        )
        self._persist_result(event_store, artifact_dir, result)

        try:
            for cycle_index in range(1, max_cycles + 1):
                cycle_started_at = datetime.now(UTC)
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
                cycle = FleetControllerCycle(
                    cycle_index=cycle_index,
                    started_at=cycle_started_at,
                    finished_at=datetime.now(UTC),
                    plan=plan,
                    report=report,
                )
                cycles.append(cycle)
                self._write_json(
                    artifact_dir / f"cycle-{cycle_index:03d}.json",
                    cycle.model_dump(mode="json"),
                )
                total_processed += len(report.processed_job_ids)
                total_paused += len(report.paused_job_ids)
                total_failed += len(report.failed_job_ids)
                total_workers += len(report.worker_ids)
                result.cycles = list(cycles)
                result.cycles_completed = len(cycles)
                result.total_processed_jobs = total_processed
                result.total_paused_jobs = total_paused
                result.total_failed_jobs = total_failed
                result.total_workers_launched = total_workers
                self._persist_result(event_store, artifact_dir, result)

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
        except Exception as exc:  # noqa: BLE001
            result.finished_at = datetime.now(UTC)
            result.stop_reason = "fleet_failed"
            result.final_plan = self.queue_store.plan_fleet(
                target_jobs_per_worker=target_jobs_per_worker,
                stale_after_seconds=stale_after_seconds,
            )
            self._write_json(
                artifact_dir / "error.json",
                {
                    "fleet_run_id": fleet_run_id,
                    "error": str(exc),
                },
            )
            self._persist_result(event_store, artifact_dir, result)
            raise

        result.finished_at = datetime.now(UTC)
        result.stop_reason = stop_reason
        result.final_plan = self.queue_store.plan_fleet(
            target_jobs_per_worker=target_jobs_per_worker,
            stale_after_seconds=stale_after_seconds,
        )
        self._persist_result(event_store, artifact_dir, result)
        return result

    @staticmethod
    def _new_fleet_run_id() -> str:
        return f"fleet-run-{uuid.uuid4().hex[:10]}"

    @staticmethod
    def _write_json(path: Path, payload: object) -> None:
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _persist_result(
        event_store: EventStore,
        artifact_dir: Path,
        result: FleetControllerResult,
    ) -> None:
        FleetController._write_json(
            artifact_dir / "summary.json",
            result.model_dump(mode="json"),
        )
        event_store.upsert_fleet_run(result)

    def _launcher_name(self) -> str:
        if self.worker_launcher is None:
            return "in_process"
        launcher_name = type(self.worker_launcher).__name__
        if launcher_name == "InProcessWorkerLauncher":
            return "in_process"
        if launcher_name == "SubprocessWorkerLauncher":
            return "subprocess"
        return launcher_name
