from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from .config import load_workspace_config
from .fleet import WorkerLauncher
from .workspace_loop import WorkspaceLoopController


class WorkspaceDaemonState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workspace_id: str = ""
    config_path: Path | None = None
    daemon_run_id: str = ""
    pid: int | None = None
    status: str = "idle"
    started_at: datetime | None = None
    updated_at: datetime | None = None
    finished_at: datetime | None = None
    tick_count: int = 0
    last_loop_run_id: str | None = None
    last_loop_stop_reason: str | None = None
    exit_reason: str | None = None
    stop_requested: bool = False
    request_reason: str | None = None
    requested_at: datetime | None = None
    requested_by: str | None = None
    error_message: str | None = None


class WorkspaceDaemonLock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workspace_id: str = ""
    config_path: Path | None = None
    daemon_run_id: str = ""
    pid: int
    acquired_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


def workspace_daemon_state_path(artifact_root: Path) -> Path:
    return artifact_root.resolve() / "workspace-daemon" / "state.json"


def workspace_daemon_lock_path(artifact_root: Path) -> Path:
    return artifact_root.resolve() / "workspace-daemon" / "lock.json"


def load_workspace_daemon_state(
    artifact_root: Path,
    *,
    workspace_id: str = "",
    config_path: Path | None = None,
) -> WorkspaceDaemonState:
    state_path = workspace_daemon_state_path(artifact_root)
    if not state_path.exists():
        return WorkspaceDaemonState(
            workspace_id=workspace_id,
            config_path=config_path.resolve() if config_path is not None else None,
        )
    return WorkspaceDaemonState.model_validate_json(state_path.read_text(encoding="utf-8"))


def write_workspace_daemon_state(
    artifact_root: Path,
    state: WorkspaceDaemonState,
) -> WorkspaceDaemonState:
    state_path = workspace_daemon_state_path(artifact_root)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(state.model_dump_json(indent=2), encoding="utf-8")
    return state


def acquire_workspace_daemon_lock(
    artifact_root: Path,
    lock: WorkspaceDaemonLock,
) -> WorkspaceDaemonLock:
    lock_path = workspace_daemon_lock_path(artifact_root)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = lock.model_dump_json(indent=2)
    while True:
        try:
            descriptor = os.open(
                lock_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o644,
            )
        except FileExistsError:
            existing = _load_existing_daemon_lock(lock_path)
            if existing is not None and _process_is_alive(existing.pid):
                raise RuntimeError(
                    "Workspace daemon already running: "
                    f"{existing.daemon_run_id} (pid={existing.pid})"
                ) from None
            lock_path.unlink(missing_ok=True)
            continue
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
        return lock


def release_workspace_daemon_lock(
    artifact_root: Path,
    *,
    daemon_run_id: str,
    pid: int,
) -> None:
    lock_path = workspace_daemon_lock_path(artifact_root)
    existing = _load_existing_daemon_lock(lock_path)
    if existing is None:
        lock_path.unlink(missing_ok=True)
        return
    if existing.daemon_run_id == daemon_run_id and existing.pid == pid:
        lock_path.unlink(missing_ok=True)


def request_workspace_daemon_stop(
    artifact_root: Path,
    *,
    reason: str = "operator_stop_requested",
    requested_by: str | None = "cli",
    workspace_id: str = "",
    config_path: Path | None = None,
) -> WorkspaceDaemonState:
    state = load_workspace_daemon_state(
        artifact_root,
        workspace_id=workspace_id,
        config_path=config_path,
    ).model_copy(
        update={
            "stop_requested": True,
            "request_reason": reason,
            "requested_at": datetime.now(UTC),
            "requested_by": requested_by,
            "updated_at": datetime.now(UTC),
        }
    )
    return write_workspace_daemon_state(artifact_root, state)


class WorkspaceDaemonRunner:
    def __init__(
        self,
        config_path: Path,
        *,
        worker_launcher: WorkerLauncher | None = None,
    ) -> None:
        self.config_path = config_path.resolve()
        self.workspace_config = load_workspace_config(self.config_path)
        self.artifact_root = self.workspace_config.run.artifact_root.resolve()
        self.worker_launcher = worker_launcher

    def status(self) -> WorkspaceDaemonState:
        return load_workspace_daemon_state(
            self.artifact_root,
            workspace_id=self.workspace_config.name,
            config_path=self.config_path,
        )

    def request_stop(
        self,
        *,
        reason: str = "operator_stop_requested",
        requested_by: str | None = "cli",
    ) -> WorkspaceDaemonState:
        return request_workspace_daemon_stop(
            self.artifact_root,
            reason=reason,
            requested_by=requested_by,
            workspace_id=self.workspace_config.name,
            config_path=self.config_path,
        )

    def run(
        self,
        *,
        project_id: str | None = None,
        project_tags: list[str] | None = None,
        max_iterations: int | None = None,
        dry_run: bool | None = None,
        priority: int = 0,
        note: str | None = None,
        max_ticks: int | None = None,
        poll_seconds: float = 5.0,
        fleet_max_cycles: int = 10,
        fleet_idle_cycles: int = 1,
        workers: int | None = None,
        target_jobs_per_worker: int = 2,
        max_jobs_per_worker: int | None = None,
        queue_poll_seconds: float = 2.0,
        queue_idle_timeout_seconds: float = 30.0,
        stale_after_seconds: float | None = 120.0,
    ) -> WorkspaceDaemonState:
        started_at = datetime.now(UTC)
        daemon_run_id = self._new_daemon_run_id()
        pid = os.getpid()
        acquire_workspace_daemon_lock(
            self.artifact_root,
            WorkspaceDaemonLock(
                workspace_id=self.workspace_config.name,
                config_path=self.config_path,
                daemon_run_id=daemon_run_id,
                pid=pid,
            ),
        )
        state = self.status().model_copy(
            update={
                "daemon_run_id": daemon_run_id,
                "pid": pid,
                "status": "running",
                "started_at": started_at,
                "updated_at": started_at,
                "finished_at": None,
                "tick_count": 0,
                "last_loop_run_id": None,
                "last_loop_stop_reason": None,
                "exit_reason": None,
                "stop_requested": False,
                "request_reason": None,
                "requested_at": None,
                "requested_by": None,
                "error_message": None,
            }
        )
        write_workspace_daemon_state(self.artifact_root, state)

        controller = WorkspaceLoopController(
            self.config_path,
            worker_launcher=self.worker_launcher,
        )
        tick_count = 0
        try:
            while True:
                if max_ticks is not None and tick_count >= max_ticks:
                    return self._finalize_state(status="idle", exit_reason="max_ticks_reached")

                loop_result = controller.run(
                    project_id=project_id,
                    project_tags=project_tags,
                    max_iterations=max_iterations,
                    dry_run=dry_run,
                    priority=priority,
                    note=note,
                    max_cycles=1,
                    idle_cycles=1,
                    sleep_seconds=0.0,
                    fleet_max_cycles=fleet_max_cycles,
                    fleet_idle_cycles=fleet_idle_cycles,
                    workers=workers,
                    target_jobs_per_worker=target_jobs_per_worker,
                    max_jobs_per_worker=max_jobs_per_worker,
                    queue_poll_seconds=queue_poll_seconds,
                    queue_idle_timeout_seconds=queue_idle_timeout_seconds,
                    stale_after_seconds=stale_after_seconds,
                )
                tick_count += 1
                state = self.status().model_copy(
                    update={
                        "status": "running",
                        "updated_at": datetime.now(UTC),
                        "tick_count": tick_count,
                        "last_loop_run_id": loop_result.loop_run_id or loop_result.loop_id,
                        "last_loop_stop_reason": loop_result.stop_reason,
                    }
                )
                write_workspace_daemon_state(self.artifact_root, state)

                observed_state = self.status()
                if observed_state.stop_requested:
                    return self._finalize_state(
                        status="stopped",
                        exit_reason=observed_state.request_reason or "operator_stop_requested",
                    )
                if loop_result.stop_reason == "operator_stop_requested":
                    return self._finalize_state(
                        status="stopped",
                        exit_reason=loop_result.stop_reason,
                    )
                if max_ticks is not None and tick_count >= max_ticks:
                    return self._finalize_state(status="idle", exit_reason="max_ticks_reached")
                if poll_seconds > 0:
                    time.sleep(poll_seconds)
        except Exception as exc:  # noqa: BLE001
            self._finalize_state(
                status="failed",
                exit_reason="daemon_failed",
                error_message=str(exc),
            )
            raise
        finally:
            release_workspace_daemon_lock(
                self.artifact_root,
                daemon_run_id=daemon_run_id,
                pid=pid,
            )

    def _finalize_state(
        self,
        *,
        status: str,
        exit_reason: str,
        error_message: str | None = None,
    ) -> WorkspaceDaemonState:
        state = self.status().model_copy(
            update={
                "status": status,
                "updated_at": datetime.now(UTC),
                "finished_at": datetime.now(UTC),
                "exit_reason": exit_reason,
                "error_message": error_message,
            }
        )
        return write_workspace_daemon_state(self.artifact_root, state)

    @staticmethod
    def _new_daemon_run_id() -> str:
        return f"workspace-daemon-{uuid.uuid4().hex[:10]}"


def _load_existing_daemon_lock(lock_path: Path) -> WorkspaceDaemonLock | None:
    if not lock_path.exists():
        return None
    try:
        return WorkspaceDaemonLock.model_validate_json(lock_path.read_text(encoding="utf-8"))
    except ValueError:
        return None


def _process_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
