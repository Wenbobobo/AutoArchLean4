from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from .adapter import ArchonAdapter
from .control import ControlService
from .events import EventStore
from .execution_policy import resolve_app_phase_configs
from .executors import create_executor
from .models import (
    AppConfig,
    EventRecord,
    ExecutionRequest,
    ExecutionStatus,
    RunPreview,
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
from .workflow import load_workflow_spec


class RunService:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.adapter = ArchonAdapter(config.project)
        self.event_store = EventStore(config.run.artifact_root / "archonlab.db")
        self.control = ControlService(config.run.artifact_root)

    def start(self, *, dry_run: bool | None = None) -> RunResult:
        preview = self.preview()
        progress = preview.progress
        run_id = self._new_run_id()
        actual_dry_run = self.config.run.dry_run if dry_run is None else dry_run

        run_dir = self.config.run.artifact_root / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        events_jsonl = run_dir / "events.jsonl"
        prompt_path = run_dir / "next-prompt.txt"
        task_graph_path = run_dir / "task-graph.json"
        supervisor_path = run_dir / "supervisor.json"
        control_path = run_dir / "control.json"
        execution_path = run_dir / "execution.json"

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

        snapshot = preview.snapshot
        control_state = preview.control
        control_path.write_text(
            json.dumps(control_state.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        task_graph = preview.task_graph
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

        workflow_spec = preview.workflow_spec
        supervisor = preview.supervisor
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

        action = preview.action
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
                    "theorem_name": action.theorem_name,
                    "file_path": str(action.file_path) if action.file_path is not None else None,
                    "task_status": (
                        action.task_status.value if action.task_status is not None else None
                    ),
                    "task_sources": [source.value for source in action.task_sources],
                    "task_priority": action.task_priority,
                    "task_blockers": action.task_blockers,
                    "objective_relevant": action.objective_relevant,
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
                    "paused": control_state.paused,
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
                    "executor": self.config.executor.model_dump(mode="json"),
                    "provider": self.config.provider.model_dump(mode="json"),
                    "execution_policy": self.config.execution_policy.model_dump(mode="json"),
                    "progress": progress.model_dump(mode="json"),
                    "snapshot": snapshot.model_dump(mode="json"),
                    "control": control_state.model_dump(mode="json"),
                    "workflow_spec": (
                        workflow_spec.model_dump(mode="json")
                        if workflow_spec is not None
                        else None
                    ),
                    "task_graph": task_graph.model_dump(mode="json"),
                    "supervisor": supervisor.model_dump(mode="json"),
                    "next_action": action.model_dump(mode="json"),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        execution_result = None
        if not actual_dry_run and str(action.phase) != "stop":
            resolved_executor = preview.resolved_executor
            resolved_provider = preview.resolved_provider
            if resolved_executor is None or resolved_provider is None:
                raise RuntimeError("Missing resolved execution config for non-stop action.")
            executor = create_executor(
                executor_config=resolved_executor,
                provider_config=resolved_provider,
            )
            execution_result = executor.execute(
                ExecutionRequest(
                    run_id=run_id,
                    project_id=self.config.project.name,
                    phase=str(action.phase),
                    prompt=prompt_text,
                    cwd=self.config.project.project_path,
                    artifact_dir=run_dir,
                    task_id=action.task_id,
                    task_title=action.task_title,
                )
            )
            execution_path.write_text(
                json.dumps(
                    execution_result.model_dump(mode="json"),
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            self.event_store.append(
                EventRecord(
                    run_id=run_id,
                    kind=(
                        "executor.completed"
                        if execution_result.status is ExecutionStatus.COMPLETED
                        else "executor.failed"
                    ),
                    project_id=self.config.project.name,
                    task_id=action.task_id,
                    payload={
                        "executor": execution_result.executor.value,
                        "resolved_executor": resolved_executor.model_dump(mode="json"),
                        "resolved_provider": resolved_provider.model_dump(mode="json"),
                        "status": execution_result.status.value,
                        "command": execution_result.command,
                        "output_path": (
                            str(execution_result.output_path)
                            if execution_result.output_path is not None
                            else None
                        ),
                        "request_path": (
                            str(execution_result.request_path)
                            if execution_result.request_path is not None
                            else None
                        ),
                        "response_path": (
                            str(execution_result.response_path)
                            if execution_result.response_path is not None
                            else None
                        ),
                        "error_message": execution_result.error_message,
                        "metadata": execution_result.metadata,
                    },
                ),
                jsonl_path=events_jsonl,
            )
            if execution_result.status is ExecutionStatus.FAILED:
                self.event_store.append(
                    EventRecord(
                        run_id=run_id,
                        kind="run.failed",
                        project_id=self.config.project.name,
                        task_id=action.task_id,
                        payload={
                            "reason": "executor_failed",
                            "executor": execution_result.executor.value,
                            "resolved_executor": resolved_executor.model_dump(mode="json"),
                            "resolved_provider": resolved_provider.model_dump(mode="json"),
                            "error_message": execution_result.error_message,
                        },
                    ),
                    jsonl_path=events_jsonl,
                )
                self.event_store.complete_run(run_id, RunStatus.FAILED)
                raise RuntimeError(
                    execution_result.error_message or "Executor failed without an error message."
                )

        self.event_store.append(
            EventRecord(
                run_id=run_id,
                kind="run.completed",
                project_id=self.config.project.name,
                payload={
                    "mode": "dry_run" if actual_dry_run else "execute",
                    "executor": (
                        execution_result.executor.value
                        if execution_result is not None
                        else None
                    ),
                },
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
            execution=execution_result,
        )

    def preview(self) -> RunPreview:
        self.adapter.ensure_valid()
        progress = self.adapter.read_progress()
        snapshot = collect_project_snapshot(
            project_path=self.config.project.project_path,
            archon_path=self.config.project.archon_path,
        )
        control_state = self.control.read(self.config.project)
        task_graph = build_task_graph(
            project_path=self.config.project.project_path,
            archon_path=self.config.project.archon_path,
        )
        workflow_spec = (
            load_workflow_spec(self.config.run.workflow_spec)
            if self.config.run.workflow_spec is not None
            else None
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
            workflow_spec=workflow_spec,
            control_state=control_state,
        )
        recent_events = [
            *recent_events,
            EventRecord(
                run_id="preview",
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
        action = select_next_action(
            adapter=self.adapter,
            workflow=self.config.run.workflow,
            snapshot=snapshot,
            task_graph=task_graph,
            supervisor=supervisor,
            workflow_spec=workflow_spec,
            control_state=control_state,
        )
        resolved_executor = None
        resolved_provider = None
        if str(action.phase) != "stop":
            resolved_executor, resolved_provider = resolve_app_phase_configs(
                self.config,
                phase=action.phase,
                action=action,
            )
        return RunPreview(
            progress=progress,
            snapshot=snapshot,
            control=control_state,
            workflow_spec=workflow_spec,
            task_graph=task_graph,
            supervisor=supervisor,
            action=action,
            resolved_executor=resolved_executor,
            resolved_provider=resolved_provider,
        )

    @staticmethod
    def _new_run_id() -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"run-{timestamp}-{uuid.uuid4().hex[:8]}"
