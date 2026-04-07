from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

from .adapter import ArchonAdapter
from .control import ControlService
from .events import EventStore
from .execution_policy import resolve_app_phase_configs
from .executors import create_executor
from .models import (
    ActionPhase,
    AppConfig,
    ControlState,
    EventRecord,
    ExecutionCapability,
    ExecutionRequest,
    ExecutionStatus,
    ProjectSession,
    RunLoopResult,
    RunPreview,
    RunResult,
    RunStatus,
    RunSummary,
    SessionIteration,
    SessionQuantumResult,
    SessionStatus,
    SupervisorAction,
    SupervisorDecision,
    SupervisorReason,
    WorkflowMode,
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
            workflow=preview.workflow,
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
                    "workflow": preview.workflow.value,
                    "workflow_spec_path": (
                        str(preview.workflow_spec_path)
                        if preview.workflow_spec_path is not None
                        else None
                    ),
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
                    "effective_workflow": preview.workflow.value,
                    "effective_workflow_spec_path": (
                        str(preview.workflow_spec_path)
                        if preview.workflow_spec_path is not None
                        else None
                    ),
                    "executor": self.config.executor.model_dump(mode="json"),
                    "provider": self.config.provider.model_dump(mode="json"),
                    "execution_policy": self.config.execution_policy.model_dump(mode="json"),
                    "progress": progress.model_dump(mode="json"),
                    "snapshot": snapshot.model_dump(mode="json"),
                    "control": control_state.model_dump(mode="json"),
                    "resolved_capability": (
                        preview.resolved_capability.model_dump(mode="json")
                        if preview.resolved_capability is not None
                        else None
                    ),
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
                provider_pools=self.config.provider_pools,
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
                        "resolved_capability": (
                            preview.resolved_capability.model_dump(mode="json")
                            if preview.resolved_capability is not None
                            else None
                        ),
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
                        "telemetry": (
                            execution_result.telemetry.model_dump(mode="json")
                            if execution_result.telemetry is not None
                            else None
                        ),
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
                            "resolved_capability": (
                                preview.resolved_capability.model_dump(mode="json")
                                if preview.resolved_capability is not None
                                else None
                            ),
                            "resolved_executor": resolved_executor.model_dump(mode="json"),
                            "resolved_provider": resolved_provider.model_dump(mode="json"),
                            "error_message": execution_result.error_message,
                            "metadata": execution_result.metadata,
                            "telemetry": (
                                execution_result.telemetry.model_dump(mode="json")
                                if execution_result.telemetry is not None
                                else None
                            ),
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

    def run_loop(
        self,
        *,
        dry_run: bool | None = None,
        max_iterations: int | None = None,
        workspace_id: str = "standalone",
        session_id: str | None = None,
        note: str | None = None,
    ) -> RunLoopResult:
        actual_dry_run = self.config.run.dry_run if dry_run is None else dry_run
        session = self.event_store.get_session(session_id) if session_id is not None else None
        if session is None:
            session = ProjectSession(
                session_id=session_id or self._new_session_id(),
                workspace_id=workspace_id,
                project_id=self.config.project.name,
                workflow=self.config.run.workflow,
                dry_run=actual_dry_run,
                max_iterations=max_iterations or self.config.run.max_iterations,
                note=note,
            )
            self.event_store.register_session(session)
        limit = max_iterations or session.max_iterations
        session = self.event_store.update_session(
            session.session_id,
            status=SessionStatus.RUNNING,
            resume_reason="run_loop",
            clear_stop_reason=True,
            note=note,
        )
        run_ids: list[str] = []
        stop_reason = "max_iterations_reached"
        iteration_index = session.completed_iterations
        try:
            while iteration_index < limit:
                result = self.start(dry_run=actual_dry_run)
                iteration_index += 1
                run_ids.append(result.run_id)
                self.event_store.append_session_iteration(
                    SessionIteration(
                        session_id=session.session_id,
                        iteration_index=iteration_index,
                        project_id=self.config.project.name,
                        run_id=result.run_id,
                        status=result.status,
                        action_phase=(
                            result.action.phase
                            if isinstance(result.action.phase, ActionPhase)
                            else ActionPhase(str(result.action.phase))
                        ),
                        action_reason=result.action.reason,
                        finished_at=datetime.now(UTC),
                    )
                )
                terminal_status = (
                    SessionStatus.COMPLETED
                    if str(result.action.phase) == ActionPhase.STOP.value
                    else (
                        SessionStatus.PAUSED
                        if iteration_index >= limit
                        else SessionStatus.RUNNING
                    )
                )
                if terminal_status is SessionStatus.COMPLETED:
                    stop_reason = f"stop:{result.action.reason}"
                elif terminal_status is SessionStatus.PAUSED:
                    stop_reason = "max_iterations_reached"
                session = self.event_store.update_session(
                    session.session_id,
                    status=terminal_status,
                    completed_iterations=iteration_index,
                    last_run_id=result.run_id,
                    stop_reason=stop_reason,
                    note=note,
                )
                if terminal_status is not SessionStatus.RUNNING:
                    break
        except Exception as exc:
            session = self.event_store.update_session(
                session.session_id,
                status=SessionStatus.FAILED,
                completed_iterations=iteration_index,
                error_message=str(exc),
                stop_reason="run_failed",
                note=note,
            )
            stop_reason = "run_failed"
            raise
        return RunLoopResult(
            session_id=session.session_id,
            workspace_id=session.workspace_id,
            project_id=session.project_id,
            status=session.status,
            dry_run=session.dry_run,
            max_iterations=limit,
            completed_iterations=session.completed_iterations,
            run_ids=run_ids,
            stop_reason=stop_reason,
        )

    def run_session_quantum(
        self,
        session_id: str,
        *,
        owner_worker_id: str | None = None,
        owner_job_id: str | None = None,
        note: str | None = None,
    ) -> SessionQuantumResult:
        session = self.event_store.get_session(session_id)
        if session is None:
            raise KeyError(f"Unknown project session: {session_id}")
        if session.status in {SessionStatus.COMPLETED, SessionStatus.CANCELED}:
            raise ValueError(f"Session {session_id} is already terminal: {session.status.value}")
        expected_owner_worker_id: str | None = None
        expected_owner_job_id: str | None = None
        if owner_worker_id is not None and owner_job_id is not None:
            session = self.event_store.claim_session(
                session_id,
                owner_worker_id=owner_worker_id,
                owner_job_id=owner_job_id,
                note=note,
            )
            expected_owner_worker_id = owner_worker_id
            expected_owner_job_id = owner_job_id
        elif owner_worker_id is not None or owner_job_id is not None:
            raise ValueError("owner_worker_id and owner_job_id must be provided together")
        try:
            if session.completed_iterations >= session.max_iterations:
                paused = self.event_store.update_session(
                    session_id,
                    status=SessionStatus.PAUSED,
                    stop_reason="max_iterations_reached",
                    expected_owner_worker_id=expected_owner_worker_id,
                    expected_owner_job_id=expected_owner_job_id,
                    note=note,
                )
                return SessionQuantumResult(
                    session_id=paused.session_id,
                    workspace_id=paused.workspace_id,
                    project_id=paused.project_id,
                    status=paused.status,
                    dry_run=paused.dry_run,
                    max_iterations=paused.max_iterations,
                    completed_iterations=paused.completed_iterations,
                    stop_reason="max_iterations_reached",
                )

            self.event_store.update_session(
                session_id,
                status=SessionStatus.RUNNING,
                clear_error_message=True,
                clear_stop_reason=True,
                resume_reason=(
                    f"claimed_by:{owner_worker_id}"
                    if owner_worker_id is not None
                    else session.last_resume_reason
                ),
                expected_owner_worker_id=expected_owner_worker_id,
                expected_owner_job_id=expected_owner_job_id,
                note=note,
            )
            result = self.start(dry_run=session.dry_run)
            iteration_index = session.completed_iterations + 1
            action_phase = (
                result.action.phase
                if isinstance(result.action.phase, ActionPhase)
                else ActionPhase(str(result.action.phase))
            )
            self.event_store.append_session_iteration(
                SessionIteration(
                    session_id=session.session_id,
                    iteration_index=iteration_index,
                    project_id=self.config.project.name,
                    run_id=result.run_id,
                    status=result.status,
                    action_phase=action_phase,
                    action_reason=result.action.reason,
                    finished_at=datetime.now(UTC),
                )
            )
            if str(result.action.phase) == ActionPhase.STOP.value:
                status = SessionStatus.COMPLETED
                stop_reason = f"stop:{result.action.reason}"
            elif iteration_index >= session.max_iterations:
                status = SessionStatus.PAUSED
                stop_reason = "max_iterations_reached"
            else:
                status = SessionStatus.PENDING
                stop_reason = "quantum_complete"
            updated = self.event_store.update_session(
                session.session_id,
                status=status,
                completed_iterations=iteration_index,
                last_run_id=result.run_id,
                clear_error_message=True,
                stop_reason=stop_reason,
                expected_owner_worker_id=expected_owner_worker_id,
                expected_owner_job_id=expected_owner_job_id,
                note=note,
            )
            return SessionQuantumResult(
                session_id=updated.session_id,
                workspace_id=updated.workspace_id,
                project_id=updated.project_id,
                status=updated.status,
                dry_run=updated.dry_run,
                max_iterations=updated.max_iterations,
                completed_iterations=updated.completed_iterations,
                run_id=result.run_id,
                action_phase=action_phase,
                action_reason=result.action.reason,
                stop_reason=stop_reason,
            )
        except Exception as exc:
            self.event_store.update_session(
                session.session_id,
                status=SessionStatus.FAILED,
                completed_iterations=session.completed_iterations,
                error_message=str(exc),
                stop_reason="run_failed",
                expected_owner_worker_id=expected_owner_worker_id,
                expected_owner_job_id=expected_owner_job_id,
                note=note,
            )
            raise
        finally:
            if owner_worker_id is not None and owner_job_id is not None:
                self.event_store.release_session_claim(
                    session_id,
                    owner_worker_id=owner_worker_id,
                    owner_job_id=owner_job_id,
                    note=note,
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
        workflow, workflow_spec_path = self._resolve_effective_workflow(control_state)
        workflow_spec = (
            load_workflow_spec(workflow_spec_path)
            if workflow_spec_path is not None
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
            workflow=workflow,
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
            workflow=workflow,
            snapshot=snapshot,
            task_graph=task_graph,
            supervisor=supervisor,
            workflow_spec=workflow_spec,
            control_state=control_state,
        )
        resolved_executor = None
        resolved_provider = None
        resolved_capability = None
        if str(action.phase) != "stop":
            resolved_executor, resolved_provider = resolve_app_phase_configs(
                self.config,
                phase=action.phase,
                action=action,
            )
            resolved_capability = ExecutionCapability.from_configs(
                executor=resolved_executor,
                provider=resolved_provider,
            )
        return RunPreview(
            workflow=workflow,
            workflow_spec_path=workflow_spec_path,
            progress=progress,
            snapshot=snapshot,
            control=control_state,
            workflow_spec=workflow_spec,
            task_graph=task_graph,
            supervisor=supervisor,
            action=action,
            resolved_capability=resolved_capability,
            resolved_executor=resolved_executor,
            resolved_provider=resolved_provider,
        )

    def _resolve_effective_workflow(
        self,
        control_state: ControlState,
    ) -> tuple[WorkflowMode, Path | None]:
        workflow = control_state.workflow_override or self.config.run.workflow
        if control_state.clear_workflow_spec:
            return workflow, None
        workflow_spec_path = (
            control_state.workflow_spec_override or self.config.run.workflow_spec
        )
        return workflow, workflow_spec_path

    @staticmethod
    def _new_run_id() -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"run-{timestamp}-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _new_session_id() -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"session-{timestamp}-{uuid.uuid4().hex[:8]}"
