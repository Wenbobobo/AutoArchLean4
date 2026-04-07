from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from math import ceil
from pathlib import Path
from typing import TypedDict, cast

from pydantic import ValidationError

from .benchmark import load_benchmark_manifest
from .events import EventStore
from .models import (
    ActionPhase,
    ExecutionCapability,
    ExecutionPolicy,
    ExecutionRequirement,
    ExecutorConfig,
    ExecutorKind,
    ProjectConfig,
    ProjectSession,
    ProviderConfig,
    ProviderKind,
    QueueBenchmarkPayload,
    QueueFleetPlan,
    QueueFleetProfile,
    QueueJob,
    QueueJobKind,
    QueueJobPreview,
    QueueJobStatus,
    QueueSessionPayload,
    QueueWorkerLease,
    RunConfig,
    RunPreview,
    SessionStatus,
    WorkerStatus,
    WorkflowMode,
    WorkspaceProjectConfig,
)


class ProjectRequirements(TypedDict):
    priority: int
    required_executor_kinds: list[ExecutorKind]
    required_provider_kinds: list[ProviderKind]
    required_models: list[str]
    required_cost_tiers: list[str]
    required_endpoint_classes: list[str]
    resolved_capability: ExecutionCapability | None
    preview: QueueJobPreview


class FleetProfileAggregate(TypedDict):
    jobs: list[QueueJob]
    phase_counts: dict[str, int]
    stage_counts: dict[str, int]
    project_ids: list[str]
    focus_examples: list[str]


class QueueStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._initialize()

    def _initialize(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS queue_jobs (
                    job_id TEXT PRIMARY KEY,
                    batch_id TEXT,
                    kind TEXT NOT NULL,
                    project_id TEXT NOT NULL,
                    workspace_id TEXT,
                    session_id TEXT,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    artifact_dir TEXT,
                    result_path TEXT,
                    error_message TEXT,
                    pause_reason TEXT,
                    cancel_reason TEXT,
                    worker_id TEXT,
                    required_executor_kinds TEXT,
                    required_provider_kinds TEXT,
                    required_models TEXT,
                    required_cost_tiers TEXT,
                    required_endpoint_classes TEXT,
                    preview_json TEXT
                );

                CREATE TABLE IF NOT EXISTS queue_workers (
                    worker_id TEXT PRIMARY KEY,
                    slot_index INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    current_job_id TEXT,
                    last_job_id TEXT,
                    thread_name TEXT,
                    note TEXT,
                    worktree_root TEXT,
                    started_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL,
                    finished_at TEXT,
                    processed_jobs INTEGER NOT NULL,
                    failed_jobs INTEGER NOT NULL,
                    executor_kinds TEXT,
                    provider_kinds TEXT,
                    models TEXT,
                    cost_tiers TEXT,
                    endpoint_classes TEXT
                );
                """
            )
            for column in [
                "pause_reason",
                "cancel_reason",
                "worker_id",
                "workspace_id",
                "session_id",
                "required_executor_kinds",
                "required_provider_kinds",
                "required_models",
                "required_cost_tiers",
                "required_endpoint_classes",
                "preview_json",
            ]:
                with suppress(sqlite3.OperationalError):
                    self._conn.execute(f"ALTER TABLE queue_jobs ADD COLUMN {column} TEXT")
            for column in [
                "worktree_root",
                "executor_kinds",
                "provider_kinds",
                "models",
                "cost_tiers",
                "endpoint_classes",
            ]:
                with suppress(sqlite3.OperationalError):
                    self._conn.execute(f"ALTER TABLE queue_workers ADD COLUMN {column} TEXT")
            self._conn.commit()

    def enqueue(
        self,
        kind: QueueJobKind | str,
        payload: dict[str, object],
        *,
        priority: int = 0,
        project_id: str | None = None,
        workspace_id: str | None = None,
        session_id: str | None = None,
        batch_id: str | None = None,
        required_executor_kinds: list[ExecutorKind] | None = None,
        required_provider_kinds: list[ProviderKind] | None = None,
        required_models: list[str] | None = None,
        required_cost_tiers: list[str] | None = None,
        required_endpoint_classes: list[str] | None = None,
        preview: QueueJobPreview | None = None,
    ) -> QueueJob:
        normalized_kind = self._normalize_job_kind(kind)
        job = QueueJob(
            job_id=self._new_job_id(),
            batch_id=batch_id,
            kind=normalized_kind,
            project_id=project_id or self._infer_project_id(payload),
            workspace_id=workspace_id,
            session_id=session_id,
            status=QueueJobStatus.QUEUED,
            priority=priority,
            payload=payload,
            required_executor_kinds=required_executor_kinds or [],
            required_provider_kinds=required_provider_kinds or [],
            required_models=required_models or [],
            required_cost_tiers=required_cost_tiers or [],
            required_endpoint_classes=required_endpoint_classes or [],
            preview=preview,
        )
        self._insert(job)
        return job

    def enqueue_workspace_sessions(
        self,
        workspace_config_path: Path,
        *,
        project_ids: list[str] | None = None,
        max_iterations: int | None = None,
        dry_run: bool | None = None,
        priority: int = 0,
        note: str | None = None,
    ) -> list[QueueJob]:
        from .config import load_workspace_config

        resolved_workspace_config_path = workspace_config_path.resolve()
        workspace_config = load_workspace_config(resolved_workspace_config_path)
        event_store = EventStore(workspace_config.run.artifact_root / "archonlab.db")
        selected_project_ids = set(project_ids) if project_ids is not None else None
        batch_id = self._new_batch_id()
        jobs: list[QueueJob] = []
        for project in workspace_config.projects:
            if not project.enabled:
                continue
            if selected_project_ids is not None and project.id not in selected_project_ids:
                continue
            session = self._resolve_workspace_session(
                workspace_id=workspace_config.name,
                project=project,
                event_store=event_store,
                default_workflow=project.workflow or workspace_config.run.workflow,
                default_dry_run=(
                    project.dry_run
                    if project.dry_run is not None
                    else workspace_config.run.dry_run
                ),
                default_max_iterations=(
                    project.max_iterations
                    if project.max_iterations is not None
                    else workspace_config.run.max_iterations
                ),
                max_iterations=max_iterations,
                dry_run=dry_run,
                note=note,
            )
            jobs.append(
                self.enqueue_session_quantum(
                    resolved_workspace_config_path,
                    session_id=session.session_id,
                    priority=priority,
                    batch_id=batch_id,
                )
            )
        return jobs

    def enqueue_session_quantum(
        self,
        workspace_config_path: Path,
        *,
        session_id: str,
        priority: int = 0,
        batch_id: str | None = None,
    ) -> QueueJob:
        from .config import load_workspace_config

        resolved_workspace_config_path = workspace_config_path.resolve()
        active_job = self.get_active_session_job(session_id)
        if active_job is not None:
            return active_job

        workspace_config = load_workspace_config(resolved_workspace_config_path)
        event_store = EventStore(workspace_config.run.artifact_root / "archonlab.db")
        session = event_store.get_session(session_id)
        if session is None:
            raise KeyError(f"Unknown project session: {session_id}")

        requirements = self._derive_session_requirements(
            workspace_config_path=resolved_workspace_config_path,
            session=session,
            base_priority=priority,
        )
        payload = QueueSessionPayload(
            workspace_config_path=resolved_workspace_config_path,
            workspace_id=session.workspace_id,
            project_id=session.project_id,
            session_id=session.session_id,
        )
        return self.enqueue(
            QueueJobKind.SESSION_QUANTUM,
            payload.model_dump(mode="json"),
            priority=requirements["priority"],
            project_id=session.project_id,
            workspace_id=session.workspace_id,
            session_id=session.session_id,
            batch_id=batch_id,
            required_executor_kinds=requirements["required_executor_kinds"],
            required_provider_kinds=requirements["required_provider_kinds"],
            required_models=requirements["required_models"],
            required_cost_tiers=requirements["required_cost_tiers"],
            required_endpoint_classes=requirements["required_endpoint_classes"],
            preview=requirements["preview"],
        )

    def enqueue_benchmark_manifest(
        self,
        manifest_path: Path,
        *,
        dry_run: bool = True,
        use_worktrees: bool = False,
        cleanup_worktrees: bool = True,
        priority: int = 0,
    ) -> list[QueueJob]:
        manifest = load_benchmark_manifest(manifest_path)
        batch_id = self._new_batch_id()
        jobs: list[QueueJob] = []
        for project in manifest.projects:
            requireds = self._derive_project_requirements(
                project_id=project.id,
                project_path=project.project_path,
                archon_path=project.archon_path,
                workflow=project.workflow,
                max_iterations=project.max_iterations,
                artifact_root=manifest.benchmark.artifact_root,
                executor=manifest.executor,
                provider=manifest.provider,
                execution_policy=manifest.execution_policy,
                base_priority=priority,
            )
            payload = QueueBenchmarkPayload(
                benchmark_name=manifest.benchmark.name,
                manifest_path=manifest_path.resolve(),
                project=project,
                dry_run=dry_run,
                use_worktrees=use_worktrees,
                cleanup_worktrees=cleanup_worktrees,
            )
            jobs.append(
                self.enqueue(
                    "benchmark_project",
                    payload.model_dump(mode="json"),
                    priority=requireds["priority"],
                    project_id=project.id,
                    batch_id=batch_id,
                    required_executor_kinds=requireds["required_executor_kinds"],
                    required_provider_kinds=requireds["required_provider_kinds"],
                    required_models=requireds["required_models"],
                    required_cost_tiers=requireds["required_cost_tiers"],
                    required_endpoint_classes=requireds["required_endpoint_classes"],
                    preview=requireds["preview"],
                )
            )
        return jobs

    def list_jobs(
        self,
        *,
        limit: int = 50,
        status: QueueJobStatus | None = None,
        batch_id: str | None = None,
    ) -> list[QueueJob]:
        with self._lock:
            query = "SELECT * FROM queue_jobs"
            clauses: list[str] = []
            params: list[str | int] = []
            if status is not None:
                clauses.append("status = ?")
                params.append(status.value)
            if batch_id is not None:
                clauses.append("batch_id = ?")
                params.append(batch_id)
            if clauses:
                query += " WHERE " + " AND ".join(clauses)
            query += " ORDER BY priority DESC, created_at ASC LIMIT ?"
            params.append(limit)
            rows = self._conn.execute(query, params).fetchall()
            return [self._row_to_job(row) for row in rows]

    def get_job(self, job_id: str) -> QueueJob | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM queue_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                return None
            return self._row_to_job(row)

    def list_session_jobs(self, session_id: str, *, limit: int = 50) -> list[QueueJob]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM queue_jobs
                WHERE session_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (session_id, limit),
            ).fetchall()
            return [self._row_to_job(row) for row in rows]

    def get_active_session_job(self, session_id: str) -> QueueJob | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM queue_jobs
                WHERE session_id = ? AND status IN (?, ?, ?)
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (
                    session_id,
                    QueueJobStatus.QUEUED.value,
                    QueueJobStatus.PENDING.value,
                    QueueJobStatus.RUNNING.value,
                ),
            ).fetchone()
            if row is None:
                return None
            return self._row_to_job(row)

    def claim_next_job(self, *, worker_id: str | None = None) -> QueueJob | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            rows = self._conn.execute(
                """
                SELECT * FROM queue_jobs
                WHERE status IN (?, ?)
                ORDER BY priority DESC, created_at ASC
                """,
                (QueueJobStatus.QUEUED.value, QueueJobStatus.PENDING.value),
            ).fetchall()
            worker_row = self._get_worker_locked(worker_id) if worker_id is not None else None
            worker = self._row_to_worker(worker_row) if worker_row is not None else None
            row = next(
                (
                    candidate
                    for candidate in rows
                    if self._job_matches_worker(
                        self._row_to_job(candidate),
                        worker,
                    )
                ),
                None,
            )
            if row is None:
                self._conn.commit()
                return None
            started_at = datetime.now(UTC).isoformat()
            self._conn.execute(
                """
                UPDATE queue_jobs
                SET status = ?, started_at = ?, updated_at = ?, worker_id = ?
                WHERE job_id = ?
                """,
                (
                    QueueJobStatus.RUNNING.value,
                    started_at,
                    started_at,
                    worker_id,
                    row["job_id"],
                ),
            )
            self._conn.commit()
            claimed = self._conn.execute(
                "SELECT * FROM queue_jobs WHERE job_id = ?",
                (row["job_id"],),
            ).fetchone()
            if claimed is None:
                raise KeyError(f"Unknown queue job: {row['job_id']}")
            return self._row_to_job(claimed)

    def update_status(self, job_id: str, status: QueueJobStatus) -> QueueJob:
        if status in {QueueJobStatus.COMPLETED, QueueJobStatus.FAILED, QueueJobStatus.CANCELED}:
            return self.finish_job(job_id, status=status)
        updated_at = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                """
                UPDATE queue_jobs
                SET status = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (status.value, updated_at, job_id),
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM queue_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown queue job: {job_id}")
            return self._row_to_job(row)

    def pause(self, job_id: str, *, reason: str | None = None) -> QueueJob:
        return self.finish_job(
            job_id,
            status=QueueJobStatus.PAUSED,
            pause_reason=reason,
        )

    def cancel(self, job_id: str, *, reason: str | None = None) -> QueueJob:
        return self.finish_job(
            job_id,
            status=QueueJobStatus.CANCELED,
            cancel_reason=reason,
        )

    def requeue(self, job_id: str) -> QueueJob:
        job = self.get_job(job_id)
        if job is None:
            raise KeyError(f"Unknown queue job: {job_id}")
        if job.status in {
            QueueJobStatus.QUEUED,
            QueueJobStatus.PENDING,
            QueueJobStatus.RUNNING,
        }:
            raise ValueError("Only paused, failed, canceled, or completed jobs can be requeued.")
        refreshed = self._refresh_job_requirements(job)
        priority = refreshed["priority"] if refreshed is not None else job.priority
        required_executor_kinds = (
            refreshed["required_executor_kinds"]
            if refreshed is not None
            else job.required_executor_kinds
        )
        required_provider_kinds = (
            refreshed["required_provider_kinds"]
            if refreshed is not None
            else job.required_provider_kinds
        )
        required_models = (
            refreshed["required_models"]
            if refreshed is not None
            else job.required_models
        )
        required_cost_tiers = (
            refreshed["required_cost_tiers"]
            if refreshed is not None
            else job.required_cost_tiers
        )
        required_endpoint_classes = (
            refreshed["required_endpoint_classes"]
            if refreshed is not None
            else job.required_endpoint_classes
        )
        preview = refreshed["preview"] if refreshed is not None else job.preview
        updated_at = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                """
                UPDATE queue_jobs
                SET status = ?, priority = ?, updated_at = ?, started_at = NULL,
                    finished_at = NULL, artifact_dir = NULL, result_path = NULL,
                    error_message = NULL, pause_reason = NULL, cancel_reason = NULL,
                    worker_id = NULL, required_executor_kinds = ?,
                    required_provider_kinds = ?, required_models = ?, required_cost_tiers = ?,
                    required_endpoint_classes = ?, preview_json = ?
                WHERE job_id = ?
                """,
                (
                    QueueJobStatus.QUEUED.value,
                    priority,
                    updated_at,
                    json.dumps(
                        [kind.value for kind in required_executor_kinds],
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        [kind.value for kind in required_provider_kinds],
                        ensure_ascii=False,
                    ),
                    json.dumps(required_models, ensure_ascii=False),
                    json.dumps(required_cost_tiers, ensure_ascii=False),
                    json.dumps(required_endpoint_classes, ensure_ascii=False),
                    (
                        json.dumps(preview.model_dump(mode="json"), ensure_ascii=False)
                        if preview is not None
                        else None
                    ),
                    job_id,
                ),
            )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM queue_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown queue job: {job_id}")
            return self._row_to_job(row)

    def finish_job(
        self,
        job_id: str,
        *,
        status: QueueJobStatus,
        artifact_dir: Path | None = None,
        result_path: Path | None = None,
        error_message: str | None = None,
        pause_reason: str | None = None,
        cancel_reason: str | None = None,
        worker_id: str | None = None,
        expected_worker_id: str | None = None,
    ) -> QueueJob:
        finished_at = datetime.now(UTC).isoformat()
        with self._lock:
            current_row = self._conn.execute(
                "SELECT * FROM queue_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if current_row is None:
                raise KeyError(f"Unknown queue job: {job_id}")
            current_job = self._row_to_job(current_row)
            resolved_expected_worker_id = expected_worker_id
            if (
                resolved_expected_worker_id is None
                and worker_id is not None
                and current_job.status is QueueJobStatus.RUNNING
            ):
                resolved_expected_worker_id = worker_id
            params: list[str | None] = [
                status.value,
                finished_at,
                finished_at,
                str(artifact_dir) if artifact_dir is not None else None,
                str(result_path) if result_path is not None else None,
                error_message,
                pause_reason,
                cancel_reason,
                worker_id,
                job_id,
            ]
            query = """
                UPDATE queue_jobs
                SET status = ?, updated_at = ?, finished_at = ?, artifact_dir = ?, result_path = ?,
                    error_message = ?, pause_reason = ?, cancel_reason = ?, worker_id = ?
                WHERE job_id = ?
            """
            if resolved_expected_worker_id is not None:
                query += " AND worker_id = ?"
                params.append(resolved_expected_worker_id)
            self._conn.execute(query, params)
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM queue_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown queue job: {job_id}")
            return self._row_to_job(row)

    def reset_paused_jobs(self, *, project_id: str | None = None) -> None:
        clauses = ["status = ?"]
        params: list[str] = [QueueJobStatus.PAUSED.value]
        if project_id is not None:
            clauses.append("project_id = ?")
            params.append(project_id)
        updated_at = datetime.now(UTC).isoformat()
        params.extend([QueueJobStatus.QUEUED.value, updated_at])
        with self._lock:
            self._conn.execute(
                f"""
                UPDATE queue_jobs
                SET status = ?, updated_at = ?, pause_reason = NULL, worker_id = NULL
                WHERE {' AND '.join(clauses)}
                """,
                params,
            )
            self._conn.commit()

    def register_worker(
        self,
        *,
        slot_index: int | None,
        worker_id: str | None = None,
        thread_name: str | None = None,
        note: str | None = None,
        worktree_root: Path | None = None,
        worktree_root_factory: Callable[[str, int], Path] | None = None,
        stale_after_seconds: float | None = None,
        executor_kinds: list[ExecutorKind] | None = None,
        provider_kinds: list[ProviderKind] | None = None,
        models: list[str] | None = None,
        cost_tiers: list[str] | None = None,
        endpoint_classes: list[str] | None = None,
    ) -> QueueWorkerLease:
        resolved_worker_id = worker_id or self._new_worker_id()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            if stale_after_seconds is not None:
                self._reap_stale_workers_locked(
                    stale_after_seconds=stale_after_seconds,
                    requeue_running_jobs=True,
                )
            resolved_slot_index = slot_index or self._next_available_slot_locked()
            active_row = self._conn.execute(
                """
                SELECT worker_id FROM queue_workers
                WHERE slot_index = ? AND status IN (?, ?)
                LIMIT 1
                """,
                (
                    resolved_slot_index,
                    WorkerStatus.IDLE.value,
                    WorkerStatus.RUNNING.value,
                ),
            ).fetchone()
            if active_row is not None and str(active_row["worker_id"]) != resolved_worker_id:
                active_worker_id = str(active_row["worker_id"])
                self._conn.rollback()
                raise RuntimeError(
                    f"Slot {resolved_slot_index} is already held by {active_worker_id}"
                )
            resolved_worktree_root = worktree_root
            if resolved_worktree_root is None and worktree_root_factory is not None:
                resolved_worktree_root = worktree_root_factory(
                    resolved_worker_id,
                    resolved_slot_index,
                )
            lease = QueueWorkerLease(
                worker_id=resolved_worker_id,
                slot_index=resolved_slot_index,
                status=WorkerStatus.IDLE,
                thread_name=thread_name,
                note=note,
                worktree_root=resolved_worktree_root,
                executor_kinds=(
                    executor_kinds if executor_kinds is not None else list(ExecutorKind)
                ),
                provider_kinds=(
                    provider_kinds if provider_kinds is not None else list(ProviderKind)
                ),
                models=models or [],
                cost_tiers=cost_tiers or [],
                endpoint_classes=endpoint_classes or [],
            )
            self._conn.execute(
                """
                INSERT INTO queue_workers (
                    worker_id, slot_index, status, current_job_id, last_job_id,
                    thread_name, note, worktree_root, started_at, heartbeat_at, finished_at,
                    processed_jobs, failed_jobs, executor_kinds, provider_kinds,
                    models, cost_tiers, endpoint_classes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease.worker_id,
                    lease.slot_index,
                    lease.status.value,
                    lease.current_job_id,
                    lease.last_job_id,
                    lease.thread_name,
                    lease.note,
                    str(lease.worktree_root) if lease.worktree_root is not None else None,
                    lease.started_at.isoformat(),
                    lease.heartbeat_at.isoformat(),
                    lease.finished_at.isoformat() if lease.finished_at else None,
                    lease.processed_jobs,
                    lease.failed_jobs,
                    json.dumps(
                        [kind.value for kind in lease.executor_kinds],
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        [kind.value for kind in lease.provider_kinds],
                        ensure_ascii=False,
                    ),
                    json.dumps(lease.models, ensure_ascii=False),
                    json.dumps(lease.cost_tiers, ensure_ascii=False),
                    json.dumps(lease.endpoint_classes, ensure_ascii=False),
                ),
            )
            self._conn.commit()
        return lease

    def reap_stale_workers(
        self,
        *,
        stale_after_seconds: float,
        requeue_running_jobs: bool = True,
    ) -> list[QueueWorkerLease]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            reaped_ids, recovered_session_claims = self._reap_stale_workers_locked(
                stale_after_seconds=stale_after_seconds,
                requeue_running_jobs=requeue_running_jobs,
            )
            self._conn.commit()
        if recovered_session_claims:
            event_store = EventStore(self.db_path)
            for worker_id, job_id in recovered_session_claims:
                event_store.recover_session_claims(
                    owner_worker_id=worker_id,
                    owner_job_id=job_id,
                    stop_reason=f"recovered_from_stale_worker:{worker_id}",
                    note=f"recovered_from_stale_worker:{worker_id}",
                )
        workers = [
            worker
            for worker in (self.get_worker(worker_id) for worker_id in reaped_ids)
            if worker is not None
        ]
        return self._annotate_workers(
            workers,
            stale_after_seconds=stale_after_seconds,
        )

    def _next_available_slot_locked(self, *, start_at: int = 1) -> int:
        rows = self._conn.execute(
            """
            SELECT slot_index FROM queue_workers
            WHERE status IN (?, ?)
            ORDER BY slot_index ASC
            """,
            (
                WorkerStatus.IDLE.value,
                WorkerStatus.RUNNING.value,
            ),
        ).fetchall()
        occupied = {int(row["slot_index"]) for row in rows}
        candidate = start_at
        while candidate in occupied:
            candidate += 1
        return candidate

    def list_workers(
        self,
        *,
        include_stopped: bool = True,
        stale_after_seconds: float | None = None,
    ) -> list[QueueWorkerLease]:
        with self._lock:
            query = "SELECT * FROM queue_workers"
            params: list[str] = []
            if not include_stopped:
                query += " WHERE status != ?"
                params.append(WorkerStatus.STOPPED.value)
            query += " ORDER BY slot_index ASC, started_at ASC"
            rows = self._conn.execute(query, params).fetchall()
            workers = [self._row_to_worker(row) for row in rows]
        return self._annotate_workers(workers, stale_after_seconds=stale_after_seconds)

    def list_worker_leases(self) -> list[QueueWorkerLease]:
        return self.list_workers()

    def plan_fleet(
        self,
        *,
        target_jobs_per_worker: int = 2,
        stale_after_seconds: float | None = 120.0,
    ) -> QueueFleetPlan:
        if target_jobs_per_worker < 1:
            raise ValueError("target_jobs_per_worker must be >= 1")

        with self._lock:
            job_rows = self._conn.execute(
                """
                SELECT * FROM queue_jobs
                WHERE status IN (?, ?, ?)
                ORDER BY priority DESC, created_at ASC
                """,
                (
                    QueueJobStatus.QUEUED.value,
                    QueueJobStatus.PENDING.value,
                    QueueJobStatus.RUNNING.value,
                ),
            ).fetchall()
            worker_rows = self._conn.execute(
                """
                SELECT * FROM queue_workers
                WHERE status IN (?, ?)
                ORDER BY slot_index ASC, started_at ASC
                """,
                (
                    WorkerStatus.IDLE.value,
                    WorkerStatus.RUNNING.value,
                ),
            ).fetchall()

        jobs = [self._row_to_job(row) for row in job_rows]
        workers = self._annotate_workers(
            [self._row_to_worker(row) for row in worker_rows],
            stale_after_seconds=stale_after_seconds,
        )
        active_workers = [worker for worker in workers if not worker.stale]

        queued_jobs = sum(1 for job in jobs if job.status is QueueJobStatus.QUEUED)
        pending_jobs = sum(1 for job in jobs if job.status is QueueJobStatus.PENDING)
        running_jobs = sum(1 for job in jobs if job.status is QueueJobStatus.RUNNING)

        aggregates: dict[tuple[tuple[str, ...], ...], FleetProfileAggregate] = {}
        for job in jobs:
            key = self._fleet_profile_key(job)
            aggregate = aggregates.get(key)
            if aggregate is None:
                aggregate = {
                    "jobs": [],
                    "phase_counts": {},
                    "stage_counts": {},
                    "project_ids": [],
                    "focus_examples": [],
                }
                aggregates[key] = aggregate
            aggregate["jobs"].append(job)
            phase = (
                job.preview.phase.value
                if job.preview is not None and job.preview.phase is not None
                else "unknown"
            )
            stage = (
                job.preview.stage
                if job.preview is not None and job.preview.stage is not None
                else "unknown"
            )
            self._increment_count(aggregate["phase_counts"], phase)
            self._increment_count(aggregate["stage_counts"], stage)
            if job.project_id not in aggregate["project_ids"]:
                aggregate["project_ids"].append(job.project_id)
            focus = self._queue_job_focus(job)
            if (
                focus is not None
                and focus not in aggregate["focus_examples"]
                and len(aggregate["focus_examples"]) < 3
            ):
                aggregate["focus_examples"].append(focus)

        profiles: list[QueueFleetProfile] = []
        dedicated_worker_ids: set[str] = set()
        for aggregate in aggregates.values():
            profile_jobs = aggregate["jobs"]
            exemplar = profile_jobs[0]
            profile_running = sum(
                1 for job in profile_jobs if job.status is QueueJobStatus.RUNNING
            )
            profile_queued = sum(
                1 for job in profile_jobs if job.status is QueueJobStatus.QUEUED
            )
            profile_pending = sum(
                1 for job in profile_jobs if job.status is QueueJobStatus.PENDING
            )
            profile_active = len(profile_jobs)
            dedicated_workers = [
                worker
                for worker in active_workers
                if self._worker_matches_profile_exact(worker, exemplar)
            ]
            dedicated_worker_ids.update(worker.worker_id for worker in dedicated_workers)
            recommended_total_workers = max(
                profile_running,
                ceil(profile_active / target_jobs_per_worker),
            )
            profiles.append(
                QueueFleetProfile(
                    profile_id=exemplar.execution_requirement.profile_id,
                    required_executor_kinds=exemplar.required_executor_kinds,
                    required_provider_kinds=exemplar.required_provider_kinds,
                    required_models=exemplar.required_models,
                    required_cost_tiers=exemplar.required_cost_tiers,
                    required_endpoint_classes=exemplar.required_endpoint_classes,
                    queued_jobs=profile_queued,
                    pending_jobs=profile_pending,
                    running_jobs=profile_running,
                    active_jobs=profile_active,
                    dedicated_workers=len(dedicated_workers),
                    recommended_total_workers=recommended_total_workers,
                    recommended_additional_workers=max(
                        recommended_total_workers - len(dedicated_workers),
                        0,
                    ),
                    dominant_phase=self._dominant_phase(aggregate["phase_counts"]),
                    phase_counts=aggregate["phase_counts"],
                    stage_counts=aggregate["stage_counts"],
                    max_priority=max(job.priority for job in profile_jobs),
                    avg_priority=round(
                        sum(job.priority for job in profile_jobs) / profile_active,
                        2,
                    ),
                    project_ids=sorted(aggregate["project_ids"]),
                    focus_examples=sorted(aggregate["focus_examples"]),
                )
            )

        profiles.sort(
            key=lambda profile: (
                -profile.max_priority,
                -profile.active_jobs,
                -profile.running_jobs,
                profile.profile_id,
            )
        )

        return QueueFleetPlan(
            target_jobs_per_worker=target_jobs_per_worker,
            total_profiles=len(profiles),
            queued_jobs=queued_jobs,
            pending_jobs=pending_jobs,
            running_jobs=running_jobs,
            active_jobs=len(jobs),
            active_workers=len(active_workers),
            dedicated_workers=len(dedicated_worker_ids),
            generic_workers=max(len(active_workers) - len(dedicated_worker_ids), 0),
            recommended_total_workers=sum(
                profile.recommended_total_workers for profile in profiles
            ),
            recommended_additional_workers=sum(
                profile.recommended_additional_workers for profile in profiles
            ),
            profiles=profiles,
        )

    def _reap_stale_workers_locked(
        self,
        *,
        stale_after_seconds: float,
        requeue_running_jobs: bool,
    ) -> tuple[list[str], list[tuple[str, str]]]:
        cutoff = datetime.now(UTC).timestamp() - stale_after_seconds
        rows = self._conn.execute(
            """
            SELECT worker_id, current_job_id, heartbeat_at FROM queue_workers
            WHERE status IN (?, ?)
            """,
            (
                WorkerStatus.IDLE.value,
                WorkerStatus.RUNNING.value,
            ),
        ).fetchall()
        reaped_ids: list[str] = []
        recovered_session_claims: list[tuple[str, str]] = []
        now_iso = datetime.now(UTC).isoformat()
        for row in rows:
            heartbeat_ts = datetime.fromisoformat(row["heartbeat_at"]).timestamp()
            if heartbeat_ts >= cutoff:
                continue
            worker_id = str(row["worker_id"])
            current_job_id = row["current_job_id"]
            if requeue_running_jobs and current_job_id is not None:
                job_row = self._conn.execute(
                    "SELECT session_id FROM queue_jobs WHERE job_id = ?",
                    (current_job_id,),
                ).fetchone()
                self._conn.execute(
                    """
                    UPDATE queue_jobs
                    SET status = ?, updated_at = ?, started_at = NULL, worker_id = NULL,
                        error_message = ?
                    WHERE job_id = ? AND status = ? AND worker_id = ?
                    """,
                    (
                        QueueJobStatus.QUEUED.value,
                        now_iso,
                        f"Recovered from stale worker {worker_id}",
                        current_job_id,
                        QueueJobStatus.RUNNING.value,
                        worker_id,
                    ),
                )
                if job_row is not None and job_row["session_id"] is not None:
                    recovered_session_claims.append(
                        (worker_id, str(current_job_id))
                    )
            self._conn.execute(
                """
                UPDATE queue_workers
                SET status = ?, current_job_id = NULL, last_job_id = COALESCE(?, last_job_id),
                    note = ?, heartbeat_at = ?, finished_at = ?
                WHERE worker_id = ?
                """,
                (
                    WorkerStatus.FAILED.value,
                    current_job_id,
                    f"stale_worker_reaped_after_{stale_after_seconds:.1f}s",
                    now_iso,
                    now_iso,
                    worker_id,
                ),
            )
            reaped_ids.append(worker_id)
        return reaped_ids, recovered_session_claims

    @staticmethod
    def _annotate_workers(
        workers: list[QueueWorkerLease],
        *,
        stale_after_seconds: float | None,
    ) -> list[QueueWorkerLease]:
        now = datetime.now(UTC)
        annotated: list[QueueWorkerLease] = []
        for worker in workers:
            heartbeat_age_seconds = max(0.0, (now - worker.heartbeat_at).total_seconds())
            stale = (
                stale_after_seconds is not None
                and worker.status in {WorkerStatus.IDLE, WorkerStatus.RUNNING}
                and heartbeat_age_seconds >= stale_after_seconds
            )
            annotated.append(
                worker.model_copy(
                    update={
                        "heartbeat_age_seconds": heartbeat_age_seconds,
                        "stale": stale,
                    }
                )
            )
        return annotated

    def heartbeat_worker(
        self,
        worker_id: str,
        *,
        status: WorkerStatus | None = None,
        current_job_id: str | None = None,
        note: str | None = None,
    ) -> QueueWorkerLease:
        current = self.get_worker(worker_id)
        if current is None:
            raise KeyError(f"Unknown worker: {worker_id}")
        heartbeat_at = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                """
                UPDATE queue_workers
                SET status = ?, current_job_id = ?, note = ?, heartbeat_at = ?
                WHERE worker_id = ?
                """,
                (
                    (status or current.status).value,
                    current_job_id,
                    note if note is not None else current.note,
                    heartbeat_at,
                    worker_id,
                ),
            )
            self._conn.commit()
        updated = self.get_worker(worker_id)
        if updated is None:
            raise KeyError(f"Unknown worker: {worker_id}")
        return updated

    def assign_job_to_worker(self, worker_id: str, job_id: str) -> QueueWorkerLease:
        current = self.get_worker(worker_id)
        if current is None:
            raise KeyError(f"Unknown worker: {worker_id}")
        heartbeat_at = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            claimed = self._conn.execute(
                """
                UPDATE queue_jobs
                SET worker_id = ?, updated_at = ?
                WHERE job_id = ? AND status = ? AND (worker_id IS NULL OR worker_id = ?)
                """,
                (
                    worker_id,
                    heartbeat_at,
                    job_id,
                    QueueJobStatus.RUNNING.value,
                    worker_id,
                ),
            )
            if claimed.rowcount == 0:
                job_row = self._conn.execute(
                    "SELECT job_id FROM queue_jobs WHERE job_id = ?",
                    (job_id,),
                ).fetchone()
                self._conn.rollback()
                if job_row is None:
                    raise KeyError(f"Unknown queue job: {job_id}")
                raise RuntimeError(f"Job {job_id} is not assignable to worker {worker_id}")
            self._conn.execute(
                """
                UPDATE queue_workers
                SET status = ?, current_job_id = ?, heartbeat_at = ?
                WHERE worker_id = ?
                """,
                (
                    WorkerStatus.RUNNING.value,
                    job_id,
                    heartbeat_at,
                    worker_id,
                ),
            )
            self._conn.commit()
        updated = self.get_worker(worker_id)
        if updated is None:
            raise KeyError(f"Unknown worker: {worker_id}")
        return updated

    def release_job_from_worker(
        self,
        worker_id: str,
        *,
        job_id: str | None,
        failed: bool = False,
    ) -> QueueWorkerLease:
        current = self.get_worker(worker_id)
        if current is None:
            raise KeyError(f"Unknown worker: {worker_id}")
        heartbeat_at = datetime.now(UTC).isoformat()
        with self._lock:
            params: list[str | int | None] = [
                WorkerStatus.IDLE.value,
                job_id,
                heartbeat_at,
                1 if failed else 0,
                worker_id,
            ]
            query = """
                UPDATE queue_workers
                SET status = ?, current_job_id = NULL, last_job_id = ?, heartbeat_at = ?,
                    processed_jobs = processed_jobs + 1,
                    failed_jobs = failed_jobs + ?
                WHERE worker_id = ?
            """
            if job_id is None:
                query += " AND current_job_id IS NULL"
            else:
                query += " AND current_job_id = ?"
                params.append(job_id)
            self._conn.execute(query, params)
            self._conn.commit()
        updated = self.get_worker(worker_id)
        if updated is None:
            raise KeyError(f"Unknown worker: {worker_id}")
        return updated

    def stop_worker(
        self,
        worker_id: str,
        *,
        failed: bool = False,
        note: str | None = None,
    ) -> QueueWorkerLease:
        current = self.get_worker(worker_id)
        if current is None:
            raise KeyError(f"Unknown worker: {worker_id}")
        finished_at = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                """
                UPDATE queue_workers
                SET status = ?, current_job_id = NULL, note = ?, heartbeat_at = ?, finished_at = ?
                WHERE worker_id = ?
                """,
                (
                    WorkerStatus.FAILED.value if failed else WorkerStatus.STOPPED.value,
                    note if note is not None else current.note,
                    finished_at,
                    finished_at,
                    worker_id,
                ),
            )
            self._conn.commit()
        updated = self.get_worker(worker_id)
        if updated is None:
            raise KeyError(f"Unknown worker: {worker_id}")
        return updated

    def get_worker(self, worker_id: str) -> QueueWorkerLease | None:
        with self._lock:
            row = self._get_worker_locked(worker_id)
            if row is None:
                return None
            return self._row_to_worker(row)

    def _insert(self, job: QueueJob) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO queue_jobs (
                    job_id, batch_id, kind, project_id, workspace_id, session_id,
                    status, priority, payload_json,
                    created_at, updated_at, started_at, finished_at, artifact_dir, result_path,
                    error_message, pause_reason, cancel_reason
                    , worker_id, required_executor_kinds, required_provider_kinds,
                    required_models, required_cost_tiers, required_endpoint_classes, preview_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.job_id,
                    job.batch_id,
                    job.kind.value,
                    job.project_id,
                    job.workspace_id,
                    job.session_id,
                    job.status.value,
                    job.priority,
                    json.dumps(job.payload, ensure_ascii=False, sort_keys=True),
                    job.created_at.isoformat(),
                    job.updated_at.isoformat(),
                    job.started_at.isoformat() if job.started_at else None,
                    job.finished_at.isoformat() if job.finished_at else None,
                    str(job.artifact_dir) if job.artifact_dir is not None else None,
                    str(job.result_path) if job.result_path is not None else None,
                    job.error_message,
                    job.pause_reason,
                    job.cancel_reason,
                    job.worker_id,
                    json.dumps(
                        [kind.value for kind in job.required_executor_kinds],
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        [kind.value for kind in job.required_provider_kinds],
                        ensure_ascii=False,
                    ),
                    json.dumps(job.required_models, ensure_ascii=False),
                    json.dumps(job.required_cost_tiers, ensure_ascii=False),
                    json.dumps(job.required_endpoint_classes, ensure_ascii=False),
                    (
                        json.dumps(job.preview.model_dump(mode="json"), ensure_ascii=False)
                        if job.preview is not None
                        else None
                    ),
                ),
            )
            self._conn.commit()

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> QueueJob:
        return QueueJob(
            job_id=row["job_id"],
            batch_id=row["batch_id"],
            kind=QueueJobKind(row["kind"]),
            project_id=row["project_id"],
            workspace_id=row["workspace_id"],
            session_id=row["session_id"],
            status=QueueJobStatus(row["status"]),
            priority=row["priority"],
            payload=json.loads(row["payload_json"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            finished_at=datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None,
            artifact_dir=Path(row["artifact_dir"]) if row["artifact_dir"] else None,
            result_path=Path(row["result_path"]) if row["result_path"] else None,
            error_message=row["error_message"],
            pause_reason=row["pause_reason"],
            cancel_reason=row["cancel_reason"],
            worker_id=row["worker_id"],
            required_executor_kinds=[
                ExecutorKind(value)
                for value in json.loads(row["required_executor_kinds"] or "[]")
            ],
            required_provider_kinds=[
                ProviderKind(value)
                for value in json.loads(row["required_provider_kinds"] or "[]")
            ],
            required_models=json.loads(row["required_models"] or "[]"),
            required_cost_tiers=json.loads(row["required_cost_tiers"] or "[]"),
            required_endpoint_classes=json.loads(
                row["required_endpoint_classes"] or "[]"
            ),
            preview=(
                QueueJobPreview.model_validate(json.loads(row["preview_json"]))
                if row["preview_json"]
                else None
            ),
        )

    @staticmethod
    def _row_to_worker(row: sqlite3.Row) -> QueueWorkerLease:
        return QueueWorkerLease(
            worker_id=row["worker_id"],
            slot_index=row["slot_index"],
            status=WorkerStatus(row["status"]),
            current_job_id=row["current_job_id"],
            last_job_id=row["last_job_id"],
            thread_name=row["thread_name"],
            note=row["note"],
            worktree_root=Path(row["worktree_root"]) if row["worktree_root"] else None,
            started_at=datetime.fromisoformat(row["started_at"]),
            heartbeat_at=datetime.fromisoformat(row["heartbeat_at"]),
            finished_at=(
                datetime.fromisoformat(row["finished_at"])
                if row["finished_at"]
                else None
            ),
            processed_jobs=row["processed_jobs"],
            failed_jobs=row["failed_jobs"],
            executor_kinds=[
                ExecutorKind(value)
                for value in json.loads(row["executor_kinds"] or "[]")
            ],
            provider_kinds=[
                ProviderKind(value)
                for value in json.loads(row["provider_kinds"] or "[]")
            ],
            models=json.loads(row["models"] or "[]"),
            cost_tiers=json.loads(row["cost_tiers"] or "[]"),
            endpoint_classes=json.loads(row["endpoint_classes"] or "[]"),
        )

    def _get_worker_locked(self, worker_id: str | None) -> sqlite3.Row | None:
        if worker_id is None:
            return None
        return cast(
            sqlite3.Row | None,
            self._conn.execute(
                "SELECT * FROM queue_workers WHERE worker_id = ?",
                (worker_id,),
            ).fetchone(),
        )

    @staticmethod
    def _job_matches_worker(
        job: QueueJob,
        worker: QueueWorkerLease | None,
    ) -> bool:
        if worker is None:
            return True
        return job.execution_requirement.matches_axes(
            executor_kinds=worker.executor_kinds,
            provider_kinds=worker.provider_kinds,
            models=worker.models,
            cost_tiers=worker.cost_tiers,
            endpoint_classes=worker.endpoint_classes,
        )

    @staticmethod
    def _worker_matches_profile_exact(worker: QueueWorkerLease, job: QueueJob) -> bool:
        return worker.execution_requirement.profile_key == job.execution_requirement.profile_key

    @staticmethod
    def _fleet_profile_key(job: QueueJob) -> tuple[tuple[str, ...], ...]:
        return job.execution_requirement.profile_key

    @staticmethod
    def _profile_id(job: QueueJob) -> str:
        return job.execution_requirement.profile_id

    @staticmethod
    def _increment_count(counts: dict[str, int], key: str) -> None:
        counts[key] = counts.get(key, 0) + 1

    @staticmethod
    def _dominant_phase(phase_counts: dict[str, int]) -> ActionPhase | None:
        valid = [
            (phase, count)
            for phase, count in phase_counts.items()
            if phase in {item.value for item in ActionPhase}
        ]
        if not valid:
            return None
        dominant_phase = sorted(valid, key=lambda item: (-item[1], item[0]))[0][0]
        return ActionPhase(dominant_phase)

    @staticmethod
    def _queue_job_focus(job: QueueJob) -> str | None:
        if job.preview is None:
            return None
        return (
            job.preview.theorem_name
            or job.preview.task_title
            or job.preview.task_id
            or None
        )

    @staticmethod
    def _infer_project_id(payload: dict[str, object]) -> str:
        project_id = payload.get("project_id")
        if isinstance(project_id, str):
            return project_id
        manifest_path = payload.get("manifest_path")
        if isinstance(manifest_path, str):
            return Path(manifest_path).stem
        return "unknown"

    @staticmethod
    def _derive_priority(
        base_priority: int,
        *,
        task_priority: int | None,
        objective_relevant: bool | None,
    ) -> int:
        return base_priority + (task_priority or 0) + (5 if objective_relevant else 0)

    def _derive_project_requirements(
        self,
        *,
        project_id: str,
        project_path: Path,
        archon_path: Path,
        workflow: WorkflowMode,
        max_iterations: int,
        artifact_root: Path,
        executor: ExecutorConfig,
        provider: ProviderConfig,
        execution_policy: ExecutionPolicy,
        base_priority: int,
    ) -> ProjectRequirements:
        from .models import AppConfig
        from .services import RunService

        service = RunService(
            AppConfig(
                project=ProjectConfig(
                    name=project_id,
                    project_path=project_path,
                    archon_path=archon_path,
                ),
                run=RunConfig(
                    workflow=workflow,
                    max_iterations=max_iterations,
                    artifact_root=artifact_root,
                ),
                executor=executor,
                provider=provider,
                execution_policy=execution_policy,
            )
        )
        preview = service.preview()
        if preview.resolved_capability is None:
            final_priority = base_priority
            return {
                "priority": final_priority,
                "required_executor_kinds": [],
                "required_provider_kinds": [],
                "required_models": [],
                "required_cost_tiers": [],
                "required_endpoint_classes": [],
                "resolved_capability": None,
                "preview": self._build_queue_preview(
                    preview=preview,
                    base_priority=base_priority,
                    final_priority=final_priority,
                    include_dynamic_bonuses=False,
                ),
            }
        resolved_capability = preview.resolved_capability
        requirement = ExecutionRequirement.from_capability(resolved_capability)
        final_priority = self._derive_priority(
            base_priority,
            task_priority=preview.action.task_priority,
            objective_relevant=preview.action.objective_relevant,
        )
        return {
            "priority": final_priority,
            "required_executor_kinds": requirement.executor_kinds,
            "required_provider_kinds": requirement.provider_kinds,
            "required_models": requirement.models,
            "required_cost_tiers": requirement.cost_tiers,
            "required_endpoint_classes": requirement.endpoint_classes,
            "resolved_capability": resolved_capability,
            "preview": self._build_queue_preview(
                preview=preview,
                base_priority=base_priority,
                final_priority=final_priority,
            ),
        }

    def _derive_session_requirements(
        self,
        *,
        workspace_config_path: Path,
        session: ProjectSession,
        base_priority: int,
    ) -> ProjectRequirements:
        from .config import build_workspace_project_app_config, load_workspace_config
        from .services import RunService

        workspace_config = load_workspace_config(workspace_config_path)
        app_config = build_workspace_project_app_config(
            workspace_config,
            project_id=session.project_id,
        )
        app_config = app_config.model_copy(
            update={
                "run": app_config.run.model_copy(
                    update={
                        "max_iterations": session.max_iterations,
                        "dry_run": session.dry_run,
                    }
                )
            }
        )
        service = RunService(app_config)
        preview = service.preview()
        if preview.resolved_capability is None:
            final_priority = base_priority
            return {
                "priority": final_priority,
                "required_executor_kinds": [],
                "required_provider_kinds": [],
                "required_models": [],
                "required_cost_tiers": [],
                "required_endpoint_classes": [],
                "resolved_capability": None,
                "preview": self._build_queue_preview(
                    preview=preview,
                    base_priority=base_priority,
                    final_priority=final_priority,
                    include_dynamic_bonuses=False,
                ),
            }
        resolved_capability = preview.resolved_capability
        requirement = ExecutionRequirement.from_capability(resolved_capability)
        final_priority = self._derive_priority(
            base_priority,
            task_priority=preview.action.task_priority,
            objective_relevant=preview.action.objective_relevant,
        )
        return {
            "priority": final_priority,
            "required_executor_kinds": requirement.executor_kinds,
            "required_provider_kinds": requirement.provider_kinds,
            "required_models": requirement.models,
            "required_cost_tiers": requirement.cost_tiers,
            "required_endpoint_classes": requirement.endpoint_classes,
            "resolved_capability": resolved_capability,
            "preview": self._build_queue_preview(
                preview=preview,
                base_priority=base_priority,
                final_priority=final_priority,
            ),
        }

    def _refresh_job_requirements(self, job: QueueJob) -> ProjectRequirements | None:
        if job.kind is not QueueJobKind.BENCHMARK_PROJECT:
            if job.kind is not QueueJobKind.SESSION_QUANTUM:
                return None
            try:
                session_payload = QueueSessionPayload.model_validate(job.payload)
            except ValidationError:
                return None
            from .config import load_workspace_config

            workspace_config = load_workspace_config(session_payload.workspace_config_path)
            session = EventStore(workspace_config.run.artifact_root / "archonlab.db").get_session(
                session_payload.session_id
            )
            if session is None:
                return None
            base_priority = job.preview.base_priority if job.preview is not None else job.priority
            return self._derive_session_requirements(
                workspace_config_path=session_payload.workspace_config_path,
                session=session,
                base_priority=base_priority,
            )
        try:
            benchmark_payload = QueueBenchmarkPayload.model_validate(job.payload)
        except ValidationError:
            return None
        manifest = load_benchmark_manifest(benchmark_payload.manifest_path)
        base_priority = job.preview.base_priority if job.preview is not None else job.priority
        return self._derive_project_requirements(
            project_id=benchmark_payload.project.id,
            project_path=benchmark_payload.project.project_path,
            archon_path=benchmark_payload.project.archon_path,
            workflow=benchmark_payload.project.workflow,
            max_iterations=benchmark_payload.project.max_iterations,
            artifact_root=manifest.benchmark.artifact_root,
            executor=manifest.executor,
            provider=manifest.provider,
            execution_policy=manifest.execution_policy,
            base_priority=base_priority,
        )

    @staticmethod
    def _build_queue_preview(
        *,
        preview: RunPreview,
        base_priority: int,
        final_priority: int,
        include_dynamic_bonuses: bool = True,
    ) -> QueueJobPreview:
        task_priority_bonus = (
            preview.action.task_priority or 0
        ) if include_dynamic_bonuses else 0
        objective_bonus = (
            5 if preview.action.objective_relevant else 0
        ) if include_dynamic_bonuses else 0
        return QueueJobPreview(
            phase=ActionPhase(str(preview.action.phase)),
            reason=preview.action.reason,
            stage=preview.action.stage,
            supervisor_action=preview.action.supervisor_action,
            supervisor_reason=preview.action.supervisor_reason,
            supervisor_summary=preview.supervisor.summary,
            task_id=preview.action.task_id,
            task_title=preview.action.task_title,
            theorem_name=preview.action.theorem_name,
            file_path=preview.action.file_path,
            task_status=preview.action.task_status,
            task_priority=preview.action.task_priority,
            objective_relevant=preview.action.objective_relevant,
            base_priority=base_priority,
            task_priority_bonus=task_priority_bonus,
            objective_relevance_bonus=objective_bonus,
            final_priority=final_priority,
            executor_kind=(
                preview.resolved_capability.executor_kind
                if preview.resolved_capability is not None
                else None
            ),
            provider_kind=(
                preview.resolved_capability.provider_kind
                if preview.resolved_capability is not None
                else None
            ),
            model=(
                preview.resolved_capability.model
                if preview.resolved_capability is not None
                else None
            ),
            cost_tier=(
                preview.resolved_capability.cost_tier
                if preview.resolved_capability is not None
                else None
            ),
            endpoint_class=(
                preview.resolved_capability.endpoint_class
                if preview.resolved_capability is not None
                else None
            ),
            capability_id=(
                preview.resolved_capability.capability_id
                if preview.resolved_capability is not None
                else None
            ),
        )

    @staticmethod
    def _new_job_id() -> str:
        return f"job-{uuid.uuid4().hex[:10]}"

    @staticmethod
    def _new_batch_id() -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"batch-{timestamp}-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _new_worker_id() -> str:
        return f"worker-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _normalize_job_kind(kind: QueueJobKind | str) -> QueueJobKind:
        if isinstance(kind, QueueJobKind):
            return kind
        aliases = {
            "benchmark": QueueJobKind.BENCHMARK_PROJECT,
            QueueJobKind.BENCHMARK_PROJECT.value: QueueJobKind.BENCHMARK_PROJECT,
            "session": QueueJobKind.SESSION_QUANTUM,
            QueueJobKind.SESSION_QUANTUM.value: QueueJobKind.SESSION_QUANTUM,
        }
        resolved = aliases.get(kind, kind)
        return QueueJobKind(resolved)

    def _resolve_workspace_session(
        self,
        *,
        workspace_id: str,
        project: WorkspaceProjectConfig,
        event_store: EventStore,
        default_workflow: WorkflowMode,
        default_dry_run: bool,
        default_max_iterations: int,
        max_iterations: int | None,
        dry_run: bool | None,
        note: str | None,
    ) -> ProjectSession:
        existing = event_store.list_sessions(
            workspace_id=workspace_id,
            project_id=project.id,
            limit=1,
        )
        if existing and existing[0].status in {
            SessionStatus.PENDING,
            SessionStatus.RUNNING,
            SessionStatus.PAUSED,
        }:
            current = existing[0]
            if max_iterations is not None and current.max_iterations != max_iterations:
                return event_store.update_session(
                    current.session_id,
                    max_iterations=max_iterations,
                    note=note,
                )
            if note is not None and current.note != note:
                return event_store.update_session(
                    current.session_id,
                    note=note,
                )
            return current
        session = ProjectSession(
            session_id=self._new_session_id(project.id),
            workspace_id=workspace_id,
            project_id=project.id,
            workflow=default_workflow,
            dry_run=default_dry_run if dry_run is None else dry_run,
            max_iterations=max_iterations or default_max_iterations,
            note=note,
        )
        event_store.register_session(session)
        return session

    @staticmethod
    def _new_session_id(project_id: str) -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        safe_project_id = project_id.replace("/", "-")
        return f"session-{safe_project_id}-{timestamp}-{uuid.uuid4().hex[:8]}"
