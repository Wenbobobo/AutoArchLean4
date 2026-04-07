from __future__ import annotations

import json
import sqlite3
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

from .models import (
    EventRecord,
    ProjectSession,
    RunStatus,
    RunSummary,
    SessionIteration,
    SessionStatus,
    WorkflowMode,
)


class EventStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._initialize()

    def _initialize(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                workflow TEXT NOT NULL,
                status TEXT NOT NULL,
                stage TEXT NOT NULL,
                dry_run INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                artifact_dir TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                kind TEXT NOT NULL,
                project_id TEXT NOT NULL,
                task_id TEXT,
                worktree_id TEXT,
                policy_version TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS project_sessions (
                session_id TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                status TEXT NOT NULL,
                workflow TEXT NOT NULL,
                dry_run INTEGER NOT NULL,
                max_iterations INTEGER NOT NULL,
                completed_iterations INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                last_run_id TEXT,
                error_message TEXT,
                last_stop_reason TEXT,
                last_resume_reason TEXT,
                note TEXT
            );

            CREATE TABLE IF NOT EXISTS session_iterations (
                session_id TEXT NOT NULL,
                iteration_index INTEGER NOT NULL,
                project_id TEXT NOT NULL,
                run_id TEXT,
                status TEXT NOT NULL,
                action_phase TEXT,
                action_reason TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                error_message TEXT,
                PRIMARY KEY (session_id, iteration_index)
            );
            """
        )
        for column in ["last_stop_reason", "last_resume_reason"]:
            with suppress(sqlite3.OperationalError):
                self._conn.execute(
                    f"ALTER TABLE project_sessions ADD COLUMN {column} TEXT"
                )
        self._conn.commit()

    def register_run(self, summary: RunSummary) -> None:
        self._conn.execute(
            """
            INSERT INTO runs (
                run_id, project_id, workflow, status, stage,
                dry_run, started_at, finished_at, artifact_dir
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                summary.run_id,
                summary.project_id,
                summary.workflow.value,
                summary.status.value,
                summary.stage,
                int(summary.dry_run),
                summary.started_at.isoformat(),
                summary.finished_at.isoformat() if summary.finished_at else None,
                str(summary.artifact_dir),
            ),
        )
        self._conn.commit()

    def append(self, event: EventRecord, *, jsonl_path: Path | None = None) -> None:
        self._conn.execute(
            """
            INSERT INTO events (
                run_id, ts, kind, project_id, task_id, worktree_id, policy_version, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.run_id,
                event.ts.isoformat(),
                event.kind,
                event.project_id,
                event.task_id,
                event.worktree_id,
                event.policy_version,
                json.dumps(event.payload, ensure_ascii=False, sort_keys=True),
            ),
        )
        self._conn.commit()
        if jsonl_path is not None:
            jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            with jsonl_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event.model_dump(mode="json"), ensure_ascii=False) + "\n")

    def complete_run(self, run_id: str, status: RunStatus) -> None:
        self._conn.execute(
            """
            UPDATE runs
            SET status = ?, finished_at = ?
            WHERE run_id = ?
            """,
            (
                status.value,
                datetime.now(UTC).isoformat(),
                run_id,
            ),
        )
        self._conn.commit()

    def register_session(self, session: ProjectSession) -> None:
        self._conn.execute(
            """
            INSERT INTO project_sessions (
                session_id, workspace_id, project_id, status, workflow, dry_run,
                max_iterations, completed_iterations, created_at, updated_at,
                started_at, finished_at, last_run_id, error_message,
                last_stop_reason, last_resume_reason, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session.session_id,
                session.workspace_id,
                session.project_id,
                session.status.value,
                session.workflow.value,
                int(session.dry_run),
                session.max_iterations,
                session.completed_iterations,
                session.created_at.isoformat(),
                session.updated_at.isoformat(),
                session.started_at.isoformat() if session.started_at is not None else None,
                session.finished_at.isoformat() if session.finished_at is not None else None,
                session.last_run_id,
                session.error_message,
                session.last_stop_reason,
                session.last_resume_reason,
                session.note,
            ),
        )
        self._conn.commit()

    def get_session(self, session_id: str) -> ProjectSession | None:
        row = self._conn.execute(
            """
            SELECT *
            FROM project_sessions
            WHERE session_id = ?
            """,
            (session_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_project_session(row)

    def list_sessions(
        self,
        *,
        workspace_id: str | None = None,
        project_id: str | None = None,
        limit: int = 100,
    ) -> list[ProjectSession]:
        query = "SELECT * FROM project_sessions"
        clauses: list[str] = []
        params: list[str | int] = []
        if workspace_id is not None:
            clauses.append("workspace_id = ?")
            params.append(workspace_id)
        if project_id is not None:
            clauses.append("project_id = ?")
            params.append(project_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_project_session(row) for row in rows]

    def update_session(
        self,
        session_id: str,
        *,
        status: SessionStatus | None = None,
        completed_iterations: int | None = None,
        max_iterations: int | None = None,
        last_run_id: str | None = None,
        error_message: str | None = None,
        stop_reason: str | None = None,
        resume_reason: str | None = None,
        clear_error_message: bool = False,
        clear_stop_reason: bool = False,
        clear_resume_reason: bool = False,
        note: str | None = None,
    ) -> ProjectSession:
        current = self.get_session(session_id)
        if current is None:
            raise KeyError(f"Unknown project session: {session_id}")
        now = datetime.now(UTC)
        next_status = status or current.status
        started_at = current.started_at
        finished_at = current.finished_at
        if next_status is SessionStatus.RUNNING and started_at is None:
            started_at = now
        if next_status in {
            SessionStatus.COMPLETED,
            SessionStatus.FAILED,
            SessionStatus.CANCELED,
        }:
            finished_at = now
        elif next_status is SessionStatus.RUNNING:
            finished_at = None
        self._conn.execute(
            """
            UPDATE project_sessions
            SET status = ?, completed_iterations = ?, max_iterations = ?,
                updated_at = ?, started_at = ?,
                finished_at = ?, last_run_id = ?, error_message = ?,
                last_stop_reason = ?, last_resume_reason = ?, note = ?
            WHERE session_id = ?
            """,
            (
                next_status.value,
                (
                    completed_iterations
                    if completed_iterations is not None
                    else current.completed_iterations
                ),
                max_iterations if max_iterations is not None else current.max_iterations,
                now.isoformat(),
                started_at.isoformat() if started_at is not None else None,
                finished_at.isoformat() if finished_at is not None else None,
                last_run_id if last_run_id is not None else current.last_run_id,
                (
                    None
                    if clear_error_message
                    else (
                        error_message if error_message is not None else current.error_message
                    )
                ),
                (
                    None
                    if clear_stop_reason
                    else (stop_reason if stop_reason is not None else current.last_stop_reason)
                ),
                (
                    None
                    if clear_resume_reason
                    else (
                        resume_reason
                        if resume_reason is not None
                        else current.last_resume_reason
                    )
                ),
                note if note is not None else current.note,
                session_id,
            ),
        )
        self._conn.commit()
        updated = self.get_session(session_id)
        if updated is None:
            raise KeyError(f"Unknown project session: {session_id}")
        return updated

    def append_session_iteration(self, iteration: SessionIteration) -> None:
        self._conn.execute(
            """
            INSERT INTO session_iterations (
                session_id, iteration_index, project_id, run_id, status,
                action_phase, action_reason, started_at, finished_at, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                iteration.session_id,
                iteration.iteration_index,
                iteration.project_id,
                iteration.run_id,
                iteration.status.value,
                iteration.action_phase.value if iteration.action_phase is not None else None,
                iteration.action_reason,
                iteration.started_at.isoformat(),
                iteration.finished_at.isoformat()
                if iteration.finished_at is not None
                else None,
                iteration.error_message,
            ),
        )
        self._conn.commit()

    def list_session_iterations(self, session_id: str) -> list[SessionIteration]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM session_iterations
            WHERE session_id = ?
            ORDER BY iteration_index ASC
            """,
            (session_id,),
        ).fetchall()
        return [self._row_to_session_iteration(row) for row in rows]

    def list_runs(self, *, limit: int = 20) -> list[RunSummary]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM runs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [self._row_to_run_summary(row) for row in rows]

    def get_run(self, run_id: str) -> RunSummary | None:
        row = self._conn.execute(
            """
            SELECT *
            FROM runs
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_run_summary(row)

    def get_run_events(self, run_id: str) -> list[EventRecord]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM events
            WHERE run_id = ?
            ORDER BY seq ASC
            """,
            (run_id,),
        ).fetchall()
        return [
            EventRecord(
                run_id=row["run_id"],
                kind=row["kind"],
                project_id=row["project_id"],
                task_id=row["task_id"],
                worktree_id=row["worktree_id"],
                policy_version=row["policy_version"],
                payload=json.loads(row["payload_json"]),
                ts=datetime.fromisoformat(row["ts"]),
            )
            for row in rows
        ]

    def list_recent_project_events(self, project_id: str, *, limit: int = 20) -> list[EventRecord]:
        rows = self._conn.execute(
            """
            SELECT *
            FROM events
            WHERE project_id = ?
            ORDER BY seq DESC
            LIMIT ?
            """,
            (project_id, limit),
        ).fetchall()
        ordered_rows = reversed(rows)
        return [
            EventRecord(
                run_id=row["run_id"],
                kind=row["kind"],
                project_id=row["project_id"],
                task_id=row["task_id"],
                worktree_id=row["worktree_id"],
                policy_version=row["policy_version"],
                payload=json.loads(row["payload_json"]),
                ts=datetime.fromisoformat(row["ts"]),
            )
            for row in ordered_rows
        ]

    def _row_to_run_summary(self, row: sqlite3.Row) -> RunSummary:
        return RunSummary(
            run_id=row["run_id"],
            project_id=row["project_id"],
            workflow=WorkflowMode(row["workflow"]),
            status=RunStatus(row["status"]),
            stage=row["stage"],
            dry_run=bool(row["dry_run"]),
            started_at=datetime.fromisoformat(row["started_at"]),
            finished_at=datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None,
            artifact_dir=Path(row["artifact_dir"]),
        )

    def _row_to_project_session(self, row: sqlite3.Row) -> ProjectSession:
        return ProjectSession(
            session_id=row["session_id"],
            workspace_id=row["workspace_id"],
            project_id=row["project_id"],
            status=SessionStatus(row["status"]),
            workflow=WorkflowMode(row["workflow"]),
            dry_run=bool(row["dry_run"]),
            max_iterations=int(row["max_iterations"]),
            completed_iterations=int(row["completed_iterations"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            started_at=datetime.fromisoformat(row["started_at"])
            if row["started_at"]
            else None,
            finished_at=datetime.fromisoformat(row["finished_at"])
            if row["finished_at"]
            else None,
            last_run_id=row["last_run_id"],
            error_message=row["error_message"],
            last_stop_reason=row["last_stop_reason"],
            last_resume_reason=row["last_resume_reason"],
            note=row["note"],
        )

    def _row_to_session_iteration(self, row: sqlite3.Row) -> SessionIteration:
        return SessionIteration(
            session_id=row["session_id"],
            iteration_index=int(row["iteration_index"]),
            project_id=row["project_id"],
            run_id=row["run_id"],
            status=RunStatus(row["status"]),
            action_phase=row["action_phase"],
            action_reason=row["action_reason"],
            started_at=datetime.fromisoformat(row["started_at"]),
            finished_at=datetime.fromisoformat(row["finished_at"])
            if row["finished_at"]
            else None,
            error_message=row["error_message"],
        )
