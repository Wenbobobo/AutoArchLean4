from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from .models import EventRecord, RunStatus, RunSummary, WorkflowMode


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
            """
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
