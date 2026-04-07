from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from .adapter import ArchonAdapter
from .events import EventStore
from .models import (
    AppConfig,
    EventRecord,
    RunResult,
    RunStatus,
    RunSummary,
    SupervisorAction,
    SupervisorDecision,
    SupervisorReason,
)
from .planner import select_next_action
from .project_state import collect_project_snapshot
from .supervisor import decide_supervisor_action
from .task_graph import build_task_graph


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
        task_graph_path = run_dir / "task-graph.json"
        supervisor_path = run_dir / "supervisor.json"

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

        snapshot = collect_project_snapshot(
            project_path=self.config.project.project_path,
            archon_path=self.config.project.archon_path,
        )
        task_graph = build_task_graph(
            project_path=self.config.project.project_path,
            archon_path=self.config.project.archon_path,
        )
        task_graph_path.write_text(
            json.dumps(task_graph.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.event_store.append(
            EventRecord(
                run_id=run_id,
                kind="task_graph.generated",
                project_id=self.config.project.name,
                payload={
                    "task_count": len(task_graph.nodes),
                    "edge_count": len(task_graph.edges),
                    "task_graph_path": str(task_graph_path),
                },
            ),
            jsonl_path=events_jsonl,
        )

        recent_events = self.event_store.list_recent_project_events(
            self.config.project.name,
            limit=20,
        )
        provisional_supervisor = SupervisorDecision(
            project_id=self.config.project.name,
            action=SupervisorAction.CONTINUE,
            reason=SupervisorReason.HEALTHY,
            summary="Provisional supervisor state before historical evaluation.",
        )
        predicted_action = select_next_action(
            adapter=self.adapter,
            workflow=self.config.run.workflow,
            snapshot=snapshot,
            task_graph=task_graph,
            supervisor=provisional_supervisor,
        )
        recent_events = [
            *recent_events,
            EventRecord(
                run_id=run_id,
                kind="workflow.next_action",
                project_id=self.config.project.name,
                payload={
                    "phase": predicted_action.phase,
                    "reason": predicted_action.reason,
                },
            ),
        ]

        supervisor = decide_supervisor_action(
            snapshot=snapshot,
            task_graph=task_graph,
            recent_events=recent_events,
        )
        supervisor_path.write_text(
            json.dumps(supervisor.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.event_store.append(
            EventRecord(
                run_id=run_id,
                kind="supervisor.decision",
                project_id=self.config.project.name,
                payload={
                    "action": supervisor.action.value,
                    "reason": supervisor.reason.value,
                    "summary": supervisor.summary,
                    "supervisor_path": str(supervisor_path),
                },
            ),
            jsonl_path=events_jsonl,
        )

        action = select_next_action(
            adapter=self.adapter,
            workflow=self.config.run.workflow,
            snapshot=snapshot,
            task_graph=task_graph,
            supervisor=supervisor,
        )
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
                    "task_id": action.task_id,
                    "task_title": action.task_title,
                    "file_path": str(action.file_path) if action.file_path is not None else None,
                    "supervisor_action": (
                        action.supervisor_action.value
                        if action.supervisor_action is not None
                        else None
                    ),
                    "supervisor_reason": (
                        action.supervisor_reason.value
                        if action.supervisor_reason is not None
                        else None
                    ),
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
                    "snapshot": snapshot.model_dump(mode="json"),
                    "task_graph": task_graph.model_dump(mode="json"),
                    "supervisor": supervisor.model_dump(mode="json"),
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
            task_graph_path=task_graph_path,
            supervisor_path=supervisor_path,
        )

    @staticmethod
    def _new_run_id() -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"run-{timestamp}-{uuid.uuid4().hex[:8]}"
