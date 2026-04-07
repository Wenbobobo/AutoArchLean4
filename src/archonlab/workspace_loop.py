from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from .batch import BatchRunner
from .config import load_workspace_config
from .control import ControlService
from .events import EventStore
from .fleet import FleetController, WorkerLauncher
from .models import WorkspaceLoopControlState, WorkspaceLoopCycle, WorkspaceLoopResult
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
        loop_id = self._new_loop_id()
        artifact_dir = self.workspace_config.run.artifact_root / "workspace-loops" / loop_id
        artifact_dir.mkdir(parents=True, exist_ok=False)
        self._write_json(
            artifact_dir / "request.json",
            {
                "workspace_id": self.workspace_config.name,
                "config_path": str(self.config_path),
                "project_id": project_id,
                "project_tags": project_tags or [],
                "max_iterations": max_iterations,
                "dry_run": dry_run,
                "priority": priority,
                "note": note,
                "max_cycles": max_cycles,
                "idle_cycles": idle_cycles,
                "sleep_seconds": sleep_seconds,
                "fleet_max_cycles": fleet_max_cycles,
                "fleet_idle_cycles": fleet_idle_cycles,
                "workers": workers,
                "target_jobs_per_worker": target_jobs_per_worker,
                "max_jobs_per_worker": max_jobs_per_worker,
                "queue_poll_seconds": queue_poll_seconds,
                "queue_idle_timeout_seconds": queue_idle_timeout_seconds,
                "stale_after_seconds": stale_after_seconds,
                "worker_launcher": (
                    type(self.worker_launcher).__name__
                    if self.worker_launcher is not None
                    else None
                ),
            },
        )
        (artifact_dir / "workspace.toml").write_text(
            self.config_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        write_workspace_loop_control_state(artifact_dir, WorkspaceLoopControlState())
        cycles: list[WorkspaceLoopCycle] = []
        total_scheduled_jobs = 0
        total_processed_jobs = 0
        total_paused_jobs = 0
        total_failed_jobs = 0
        total_workers_launched = 0
        consecutive_idle_cycles = 0
        stop_reason = "max_cycles_reached"
        event_store = EventStore(self.workspace_config.run.artifact_root / "archonlab.db")
        result = self.result_model(
            workspace_id=self.workspace_config.name,
            config_path=self.config_path,
            loop_run_id=loop_id,
            loop_id=loop_id,
            artifact_dir=artifact_dir,
            project_id=project_id,
            project_tags=project_tags or [],
            note=note,
            launcher=self._launcher_name(),
            started_at=started_at,
            finished_at=None,
            stop_reason="running",
        )

        batch_runner = BatchRunner(
            queue_store=self.queue_store,
            control_service=ControlService(self.workspace_config.run.artifact_root),
            artifact_root=self.workspace_config.run.artifact_root,
            slot_limit=workers or self.workspace_config.run.max_parallel,
            provider_pools=(
                self.workspace_config.provider_pools
                if self.workspace_config.provider.pool is not None
                else None
            ),
        )
        self._persist_result(event_store, artifact_dir, result)

        try:
            for cycle_index in range(1, max_cycles + 1):
                control_state = load_workspace_loop_control_state(artifact_dir)
                if control_state.stop_requested:
                    stop_reason = control_state.reason or "operator_stop_requested"
                    break
                cycle_started_at = datetime.now(UTC)
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
                        config_path=self.config_path,
                        workspace_id=self.workspace_config.name,
                        note=note or f"workspace_loop:{loop_id}:cycle:{cycle_index}",
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
                cycle = WorkspaceLoopCycle(
                    cycle_index=cycle_index,
                    started_at=cycle_started_at,
                    finished_at=datetime.now(UTC),
                    plan=plan,
                    scheduled_job_ids=scheduled_job_ids,
                    scheduled_session_ids=scheduled_session_ids,
                    fleet_result=fleet_result,
                )
                cycles.append(cycle)
                self._write_json(
                    artifact_dir / f"cycle-{cycle_index:03d}.json",
                    cycle.model_dump(mode="json"),
                )
                result.cycles = list(cycles)
                result.cycles_completed = len(cycles)
                result.total_scheduled_jobs = total_scheduled_jobs
                result.total_processed_jobs = total_processed_jobs
                result.total_paused_jobs = total_paused_jobs
                result.total_failed_jobs = total_failed_jobs
                result.total_workers_launched = total_workers_launched
                self._persist_result(event_store, artifact_dir, result)

                control_state = load_workspace_loop_control_state(artifact_dir)
                if control_state.stop_requested:
                    stop_reason = control_state.reason or "operator_stop_requested"
                    break

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
        except Exception as exc:  # noqa: BLE001
            result.finished_at = datetime.now(UTC)
            result.stop_reason = "loop_failed"
            result.error_message = str(exc)
            self._write_json(
                artifact_dir / "error.json",
                {
                    "loop_run_id": result.loop_run_id or result.loop_id,
                    "error": str(exc),
                },
            )
            self._persist_result(event_store, artifact_dir, result)
            raise

        result.finished_at = datetime.now(UTC)
        result.stop_reason = stop_reason
        self._persist_result(event_store, artifact_dir, result)
        return result

    @staticmethod
    def _new_loop_id() -> str:
        return f"workspace-loop-{uuid.uuid4().hex[:10]}"

    @staticmethod
    def _write_json(path: Path, payload: object) -> None:
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _persist_result(
        self,
        event_store: EventStore,
        artifact_dir: Path,
        result: WorkspaceLoopResult,
    ) -> None:
        self._write_json(
            artifact_dir / "summary.json",
            result.model_dump(mode="json"),
        )
        event_store.upsert_workspace_loop_run(result)

    def _launcher_name(self) -> str:
        if self.worker_launcher is None:
            return "in_process"
        launcher_name = type(self.worker_launcher).__name__
        if launcher_name == "InProcessWorkerLauncher":
            return "in_process"
        if launcher_name == "SubprocessWorkerLauncher":
            return "subprocess"
        return launcher_name


def load_workspace_loop_control_state(artifact_dir: Path) -> WorkspaceLoopControlState:
    control_path = artifact_dir / "control.json"
    if not control_path.exists():
        return WorkspaceLoopControlState()
    return WorkspaceLoopControlState.model_validate_json(
        control_path.read_text(encoding="utf-8")
    )


def write_workspace_loop_control_state(
    artifact_dir: Path,
    state: WorkspaceLoopControlState,
) -> WorkspaceLoopControlState:
    control_path = artifact_dir / "control.json"
    control_path.write_text(
        state.model_dump_json(indent=2),
        encoding="utf-8",
    )
    return state


def request_workspace_loop_stop(
    artifact_dir: Path,
    *,
    reason: str = "operator_stop_requested",
    requested_by: str | None = "cli",
) -> WorkspaceLoopControlState:
    state = load_workspace_loop_control_state(artifact_dir).model_copy(
        update={
            "stop_requested": True,
            "reason": reason,
            "requested_at": datetime.now(UTC),
            "requested_by": requested_by,
        }
    )
    return write_workspace_loop_control_state(artifact_dir, state)
