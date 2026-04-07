from __future__ import annotations

import json
import sqlite3
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

from .models import (
    EventRecord,
    ProjectSession,
    ProviderMemberRuntimeSummary,
    ProviderPoolRuntimeSummary,
    RunStatus,
    RunSummary,
    SessionIteration,
    SessionStatus,
    WorkflowMode,
)


def _coerce_int(value: object) -> int:
    return value if isinstance(value, int) else 0


def _coerce_float(value: object) -> float:
    return float(value) if isinstance(value, (int, float)) else 0.0


def _coerce_datetime(value: object) -> datetime | None:
    return value if isinstance(value, datetime) else None


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
                owner_worker_id TEXT,
                owner_job_id TEXT,
                owner_claimed_at TEXT,
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
        for column in [
            "last_stop_reason",
            "last_resume_reason",
            "owner_worker_id",
            "owner_job_id",
            "owner_claimed_at",
        ]:
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
                last_stop_reason, last_resume_reason,
                owner_worker_id, owner_job_id, owner_claimed_at, note
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                session.owner_worker_id,
                session.owner_job_id,
                (
                    session.owner_claimed_at.isoformat()
                    if session.owner_claimed_at is not None
                    else None
                ),
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
        clear_owner_claim: bool = False,
        expected_owner_worker_id: str | None = None,
        expected_owner_job_id: str | None = None,
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
        params: list[str | int | None] = [
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
            None if clear_owner_claim else current.owner_worker_id,
            None if clear_owner_claim else current.owner_job_id,
            (
                None
                if clear_owner_claim or current.owner_claimed_at is None
                else current.owner_claimed_at.isoformat()
            ),
            note if note is not None else current.note,
            session_id,
        ]
        query = """
            UPDATE project_sessions
            SET status = ?, completed_iterations = ?, max_iterations = ?,
                updated_at = ?, started_at = ?,
                finished_at = ?, last_run_id = ?, error_message = ?,
                last_stop_reason = ?, last_resume_reason = ?,
                owner_worker_id = ?, owner_job_id = ?, owner_claimed_at = ?, note = ?
            WHERE session_id = ?
        """
        if expected_owner_worker_id is not None or expected_owner_job_id is not None:
            query += " AND owner_worker_id IS ? AND owner_job_id IS ?"
            params.extend([expected_owner_worker_id, expected_owner_job_id])
        cursor = self._conn.execute(query, params)
        self._conn.commit()
        if cursor.rowcount == 0 and (
            expected_owner_worker_id is not None or expected_owner_job_id is not None
        ):
            current = self.get_session(session_id)
            if current is None:
                raise KeyError(f"Unknown project session: {session_id}")
            raise RuntimeError(
                f"Session {session_id} is not owned by "
                f"{expected_owner_worker_id or '-'}:{expected_owner_job_id or '-'}"
            )
        updated = self.get_session(session_id)
        if updated is None:
            raise KeyError(f"Unknown project session: {session_id}")
        return updated

    def claim_session(
        self,
        session_id: str,
        *,
        owner_worker_id: str,
        owner_job_id: str,
        note: str | None = None,
    ) -> ProjectSession:
        current = self.get_session(session_id)
        if current is None:
            raise KeyError(f"Unknown project session: {session_id}")
        now = datetime.now(UTC).isoformat()
        cursor = self._conn.execute(
            """
            UPDATE project_sessions
            SET owner_worker_id = ?, owner_job_id = ?, owner_claimed_at = ?,
                updated_at = ?, note = ?
            WHERE session_id = ?
              AND (
                (owner_worker_id IS NULL AND owner_job_id IS NULL)
                OR (owner_worker_id = ? AND owner_job_id = ?)
              )
            """,
            (
                owner_worker_id,
                owner_job_id,
                now,
                now,
                note if note is not None else current.note,
                session_id,
                owner_worker_id,
                owner_job_id,
            ),
        )
        self._conn.commit()
        updated = self.get_session(session_id)
        if updated is None:
            raise KeyError(f"Unknown project session: {session_id}")
        if cursor.rowcount == 0:
            raise RuntimeError(
                f"Session {session_id} is already owned by "
                f"{updated.owner_worker_id or '-'}:{updated.owner_job_id or '-'}"
            )
        return updated

    def release_session_claim(
        self,
        session_id: str,
        *,
        owner_worker_id: str,
        owner_job_id: str,
        note: str | None = None,
    ) -> ProjectSession:
        try:
            return self.update_session(
                session_id,
                clear_owner_claim=True,
                expected_owner_worker_id=owner_worker_id,
                expected_owner_job_id=owner_job_id,
                note=note,
            )
        except RuntimeError as err:
            current = self.get_session(session_id)
            if current is None:
                raise KeyError(f"Unknown project session: {session_id}") from err
            return current

    def recover_session_claims(
        self,
        *,
        owner_worker_id: str | None = None,
        owner_job_id: str | None = None,
        stop_reason: str,
        note: str | None = None,
    ) -> list[ProjectSession]:
        if owner_worker_id is None and owner_job_id is None:
            raise ValueError("Specify owner_worker_id or owner_job_id to recover claims.")
        query = "SELECT session_id, status FROM project_sessions"
        clauses: list[str] = []
        params: list[str] = []
        if owner_worker_id is not None:
            clauses.append("owner_worker_id = ?")
            params.append(owner_worker_id)
        if owner_job_id is not None:
            clauses.append("owner_job_id = ?")
            params.append(owner_job_id)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        rows = self._conn.execute(query, params).fetchall()
        recovered: list[ProjectSession] = []
        for row in rows:
            current_status = SessionStatus(row["status"])
            recovered.append(
                self.update_session(
                    str(row["session_id"]),
                    status=(
                        SessionStatus.PENDING
                        if current_status is SessionStatus.RUNNING
                        else current_status
                    ),
                    stop_reason=stop_reason,
                    clear_owner_claim=True,
                    clear_error_message=True,
                    note=note,
                )
            )
        return recovered

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

    def summarize_provider_runtime(
        self,
        *,
        limit: int = 200,
    ) -> list[ProviderPoolRuntimeSummary]:
        rows = self._conn.execute(
            """
            SELECT ts, kind, payload_json
            FROM events
            WHERE kind IN ('executor.completed', 'executor.failed')
            ORDER BY seq DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        pool_totals: dict[str, dict[str, object]] = {}

        for row in reversed(rows):
            payload = json.loads(row["payload_json"])
            telemetry = payload.get("telemetry")
            if not isinstance(telemetry, dict):
                continue
            pool_name = telemetry.get("provider_pool")
            member_name = telemetry.get("provider_member")
            if not isinstance(pool_name, str) or not isinstance(member_name, str):
                continue
            retry_count = telemetry.get("retry_count", 0)
            cost_estimate = telemetry.get("cost_estimate", 0.0)
            health_status = telemetry.get("health_status")
            event_ts = datetime.fromisoformat(row["ts"])
            success = row["kind"] == "executor.completed"

            pool_state = pool_totals.setdefault(
                pool_name,
                {
                    "success_count": 0,
                    "failure_count": 0,
                    "total_retry_count": 0,
                    "total_cost_estimate": 0.0,
                    "last_health_status": None,
                    "last_seen_at": None,
                    "members": {},
                },
            )
            if success:
                pool_state["success_count"] = _coerce_int(pool_state["success_count"]) + 1
            else:
                pool_state["failure_count"] = _coerce_int(pool_state["failure_count"]) + 1
            pool_state["total_retry_count"] = _coerce_int(pool_state["total_retry_count"]) + (
                retry_count if isinstance(retry_count, int) else 0
            )
            pool_state["total_cost_estimate"] = _coerce_float(pool_state["total_cost_estimate"]) + (
                float(cost_estimate)
                if isinstance(cost_estimate, (int, float))
                else 0.0
            )
            pool_state["last_health_status"] = (
                str(health_status) if isinstance(health_status, str) else None
            )
            pool_state["last_seen_at"] = event_ts

            members = pool_state["members"]
            if not isinstance(members, dict):
                continue
            member_state = members.setdefault(
                member_name,
                {
                    "success_count": 0,
                    "failure_count": 0,
                    "retry_count": 0,
                    "total_cost_estimate": 0.0,
                    "last_health_status": None,
                    "last_seen_at": None,
                },
            )
            if success:
                member_state["success_count"] = _coerce_int(member_state["success_count"]) + 1
            else:
                member_state["failure_count"] = _coerce_int(member_state["failure_count"]) + 1
            member_state["retry_count"] = _coerce_int(member_state["retry_count"]) + (
                retry_count if isinstance(retry_count, int) else 0
            )
            member_state["total_cost_estimate"] = _coerce_float(
                member_state["total_cost_estimate"]
            ) + (
                float(cost_estimate)
                if isinstance(cost_estimate, (int, float))
                else 0.0
            )
            member_state["last_health_status"] = (
                str(health_status) if isinstance(health_status, str) else None
            )
            member_state["last_seen_at"] = event_ts

        summaries: list[ProviderPoolRuntimeSummary] = []
        for pool_name in sorted(pool_totals):
            pool_state = pool_totals[pool_name]
            members = pool_state["members"]
            if not isinstance(members, dict):
                continue
            member_summaries = [
                ProviderMemberRuntimeSummary(
                    member_name=member_name,
                    success_count=_coerce_int(member_state["success_count"]),
                    failure_count=_coerce_int(member_state["failure_count"]),
                    retry_count=_coerce_int(member_state["retry_count"]),
                    total_cost_estimate=round(
                        _coerce_float(member_state["total_cost_estimate"]),
                        6,
                    ),
                    last_health_status=(
                        str(member_state["last_health_status"])
                        if member_state["last_health_status"] is not None
                        else None
                    ),
                    last_seen_at=_coerce_datetime(member_state["last_seen_at"]),
                )
                for member_name, member_state in sorted(members.items())
            ]
            summaries.append(
                ProviderPoolRuntimeSummary(
                    pool_name=pool_name,
                    success_count=_coerce_int(pool_state["success_count"]),
                    failure_count=_coerce_int(pool_state["failure_count"]),
                    total_retry_count=_coerce_int(pool_state["total_retry_count"]),
                    total_cost_estimate=round(
                        _coerce_float(pool_state["total_cost_estimate"]),
                        6,
                    ),
                    last_health_status=(
                        str(pool_state["last_health_status"])
                        if pool_state["last_health_status"] is not None
                        else None
                    ),
                    last_seen_at=_coerce_datetime(pool_state["last_seen_at"]),
                    members=member_summaries,
                )
            )
        return summaries

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
            owner_worker_id=row["owner_worker_id"],
            owner_job_id=row["owner_job_id"],
            owner_claimed_at=datetime.fromisoformat(row["owner_claimed_at"])
            if row["owner_claimed_at"]
            else None,
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
