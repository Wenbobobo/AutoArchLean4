from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from .adapter import ArchonAdapter
from .events import EventStore
from .models import AppConfig, EventRecord, RunResult, RunStatus, RunSummary


class RunService:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.adapter = ArchonAdapter(config.project)
        self.event_store = EventStore(config.run.artifact_root / "archonlab.db")

    def start(self, *, dry_run: bool | None = None) -> RunResult:
        self.adapter.ensure_valid()
        progress = self.adapter.read_progress()
        run_id = self._new_run_id()
        actual_dry_run = self.config.run.dry_run if dry_run is None else dry_run

        run_dir = self.config.run.artifact_root / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        events_jsonl = run_dir / "events.jsonl"
        prompt_path = run_dir / "next-prompt.txt"

        summary = RunSummary(
            run_id=run_id,
            project_id=self.config.project.name,
            workflow=self.config.run.workflow,
            status=RunStatus.STARTED,
            stage=progress.stage,
            dry_run=actual_dry_run,
            started_at=datetime.now(UTC),
            artifact_dir=run_dir,
        )
        self.event_store.register_run(summary)
        self.event_store.append(
            EventRecord(
                run_id=run_id,
                kind="run.started",
                project_id=self.config.project.name,
                payload={
                    "stage": progress.stage,
                    "dry_run": actual_dry_run,
                    "workflow": self.config.run.workflow.value,
                    "objectives": progress.objectives,
                },
            ),
            jsonl_path=events_jsonl,
        )

        action = self.adapter.choose_next_action(self.config.run.workflow, progress)
        prompt_text = action.prompt_preview or ""
        prompt_path.write_text(prompt_text, encoding="utf-8")

        self.event_store.append(
            EventRecord(
                run_id=run_id,
                kind="workflow.next_action",
                project_id=self.config.project.name,
                payload={
                    "phase": action.phase,
                    "reason": action.reason,
                    "stage": action.stage,
                    "prompt_path": str(prompt_path),
                },
            ),
            jsonl_path=events_jsonl,
        )

        run_snapshot_path = run_dir / "run-summary.json"
        run_snapshot_path.write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "project": self.config.project.model_dump(mode="json"),
                    "run": self.config.run.model_dump(mode="json"),
                    "progress": progress.model_dump(mode="json"),
                    "next_action": action.model_dump(mode="json"),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        if not actual_dry_run:
            self.event_store.append(
                EventRecord(
                    run_id=run_id,
                    kind="run.failed",
                    project_id=self.config.project.name,
                    payload={
                        "reason": "non_dry_run_not_implemented",
                        "message": (
                            "Execution path is intentionally deferred until "
                            "Phase 2 completion."
                        ),
                    },
                ),
                jsonl_path=events_jsonl,
            )
            self.event_store.complete_run(run_id, RunStatus.FAILED)
            raise NotImplementedError(
                "Non-dry-run execution is intentionally deferred; "
                "use --dry-run for the current baseline."
            )

        self.event_store.append(
            EventRecord(
                run_id=run_id,
                kind="run.completed",
                project_id=self.config.project.name,
                payload={"mode": "dry_run"},
            ),
            jsonl_path=events_jsonl,
        )
        self.event_store.complete_run(run_id, RunStatus.COMPLETED)

        return RunResult(
            run_id=run_id,
            status=RunStatus.COMPLETED,
            action=action,
            artifact_dir=run_dir,
            prompt_path=prompt_path,
        )

    @staticmethod
    def _new_run_id() -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"run-{timestamp}-{uuid.uuid4().hex[:8]}"
