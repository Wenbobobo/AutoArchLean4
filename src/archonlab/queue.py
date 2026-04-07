from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

from .benchmark import load_benchmark_manifest
from .models import QueueBenchmarkPayload, QueueJob, QueueJobKind, QueueJobStatus


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
                    cancel_reason TEXT
                );
                """
            )
            for column in ["pause_reason", "cancel_reason"]:
                with suppress(sqlite3.OperationalError):
                    self._conn.execute(f"ALTER TABLE queue_jobs ADD COLUMN {column} TEXT")
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

    def claim_next_job(self) -> QueueJob | None:
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
                SET status = ?, started_at = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (
                    QueueJobStatus.RUNNING.value,
                    started_at,
                    started_at,
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
    ) -> QueueJob:
        finished_at = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                """
                UPDATE queue_jobs
                SET status = ?, updated_at = ?, finished_at = ?, artifact_dir = ?, result_path = ?,
                    error_message = ?, pause_reason = ?, cancel_reason = ?
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
                SET status = ?, updated_at = ?, pause_reason = NULL
                WHERE {' AND '.join(clauses)}
                """,
                params,
            )
            self._conn.commit()

    def _insert(self, job: QueueJob) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO queue_jobs (
                    job_id, batch_id, kind, project_id, status, priority, payload_json,
                    created_at, updated_at, started_at, finished_at, artifact_dir, result_path,
                    error_message, pause_reason, cancel_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
