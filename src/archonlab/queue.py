from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

from .benchmark import load_benchmark_manifest
from .models import (
    QueueBenchmarkPayload,
    QueueJob,
    QueueJobKind,
    QueueJobStatus,
    QueueWorkerLease,
    WorkerStatus,
)


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
                    worker_id TEXT
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
                    failed_jobs INTEGER NOT NULL
                );
                """
            )
            for column in ["pause_reason", "cancel_reason", "worker_id"]:
                with suppress(sqlite3.OperationalError):
                    self._conn.execute(f"ALTER TABLE queue_jobs ADD COLUMN {column} TEXT")
            with suppress(sqlite3.OperationalError):
                self._conn.execute("ALTER TABLE queue_workers ADD COLUMN worktree_root TEXT")
            self._conn.commit()

    def enqueue(
        self,
        kind: str,
        payload: dict[str, object],
        *,
        priority: int = 0,
        project_id: str | None = None,
        batch_id: str | None = None,
    ) -> QueueJob:
        job = QueueJob(
            job_id=self._new_job_id(),
            batch_id=batch_id,
            kind=QueueJobKind.BENCHMARK_PROJECT,
            project_id=project_id or self._infer_project_id(payload),
            status=QueueJobStatus.QUEUED,
            priority=priority,
            payload=payload,
        )
        self._insert(job)
        return job

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
                    priority=priority,
                    project_id=project.id,
                    batch_id=batch_id,
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

    def claim_next_job(self, *, worker_id: str | None = None) -> QueueJob | None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                """
                SELECT * FROM queue_jobs
                WHERE status IN (?, ?)
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                """,
                (QueueJobStatus.QUEUED.value, QueueJobStatus.PENDING.value),
            ).fetchone()
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
    ) -> QueueJob:
        finished_at = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                """
                UPDATE queue_jobs
                SET status = ?, updated_at = ?, finished_at = ?, artifact_dir = ?, result_path = ?,
                    error_message = ?, pause_reason = ?, cancel_reason = ?, worker_id = ?
                WHERE job_id = ?
                """,
                (
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
            )
            self._conn.execute(
                """
                INSERT INTO queue_workers (
                    worker_id, slot_index, status, current_job_id, last_job_id,
                    thread_name, note, worktree_root, started_at, heartbeat_at, finished_at,
                    processed_jobs, failed_jobs
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            reaped_ids = self._reap_stale_workers_locked(
                stale_after_seconds=stale_after_seconds,
                requeue_running_jobs=requeue_running_jobs,
            )
            self._conn.commit()
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

    def _reap_stale_workers_locked(
        self,
        *,
        stale_after_seconds: float,
        requeue_running_jobs: bool,
    ) -> list[str]:
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
        now_iso = datetime.now(UTC).isoformat()
        for row in rows:
            heartbeat_ts = datetime.fromisoformat(row["heartbeat_at"]).timestamp()
            if heartbeat_ts >= cutoff:
                continue
            worker_id = str(row["worker_id"])
            current_job_id = row["current_job_id"]
            if requeue_running_jobs and current_job_id is not None:
                self._conn.execute(
                    """
                    UPDATE queue_jobs
                    SET status = ?, updated_at = ?, started_at = NULL, worker_id = NULL,
                        error_message = ?
                    WHERE job_id = ? AND status = ?
                    """,
                    (
                        QueueJobStatus.QUEUED.value,
                        now_iso,
                        f"Recovered from stale worker {worker_id}",
                        current_job_id,
                        QueueJobStatus.RUNNING.value,
                    ),
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
        return reaped_ids

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
            self._conn.execute(
                """
                UPDATE queue_workers
                SET status = ?, current_job_id = NULL, last_job_id = ?, heartbeat_at = ?,
                    processed_jobs = processed_jobs + 1,
                    failed_jobs = failed_jobs + ?
                WHERE worker_id = ?
                """,
                (
                    WorkerStatus.IDLE.value,
                    job_id,
                    heartbeat_at,
                    1 if failed else 0,
                    worker_id,
                ),
            )
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
            row = self._conn.execute(
                "SELECT * FROM queue_workers WHERE worker_id = ?",
                (worker_id,),
            ).fetchone()
            if row is None:
                return None
            return self._row_to_worker(row)

    def _insert(self, job: QueueJob) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO queue_jobs (
                    job_id, batch_id, kind, project_id, status, priority, payload_json,
                    created_at, updated_at, started_at, finished_at, artifact_dir, result_path,
                    error_message, pause_reason, cancel_reason
                    , worker_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.job_id,
                    job.batch_id,
                    job.kind.value,
                    job.project_id,
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
        )

    @staticmethod
    def _infer_project_id(payload: dict[str, object]) -> str:
        manifest_path = payload.get("manifest_path")
        if isinstance(manifest_path, str):
            return Path(manifest_path).stem
        return "unknown"

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
