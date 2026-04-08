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
from .lean_analyzer import build_lean_analyzer, collect_lean_analysis
from .models import (
    ActionPhase,
    AdapterAction,
    AppConfig,
    ControlState,
    EventRecord,
    ExecutionCapability,
    ExecutionIngestionResult,
    ExecutionRequest,
    ExecutionResult,
    ExecutionStatus,
    ExecutorKind,
    ProjectSession,
    RunLoopResult,
    RunPreview,
    RunResult,
    RunStatus,
    RunSummary,
    SessionIteration,
    SessionQuantumResult,
    SessionStatus,
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
        analysis_path = run_dir / "lean-analysis.json"
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
        analysis = preview.analysis
        control_path.write_text(
            json.dumps(control_state.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        analysis_path.write_text(
            json.dumps(analysis.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.event_store.append(
            EventRecord(
                run_id=run_id,
                kind="lean_analysis.generated",
                project_id=self.config.project.name,
                payload={
                    "backend": analysis.backend,
                    "fallback_used": analysis.fallback_used,
                    "fallback_reason": analysis.fallback_reason,
                    "theorem_count": analysis.theorem_count,
                    "proof_gap_count": len(analysis.proof_gaps),
                    "diagnostic_count": len(analysis.diagnostics),
                    "analysis_path": str(analysis_path),
                },
            ),
            jsonl_path=events_jsonl,
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
                    "analysis": analysis.model_dump(mode="json"),
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
        ingestion_result = None
        if not actual_dry_run and str(action.phase) != "stop":
            resolved_executor = preview.resolved_executor
            resolved_provider = preview.resolved_provider
            if resolved_executor is None or resolved_provider is None:
                raise RuntimeError("Missing resolved execution config for non-stop action.")
            executor = create_executor(
                executor_config=resolved_executor,
                provider_config=resolved_provider,
                provider_pools=self.config.provider_pools,
                provider_health_db_path=self.config.run.artifact_root / "archonlab.db",
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
            ingestion_result = self._maybe_ingest_execution_output(
                run_id=run_id,
                run_dir=run_dir,
                action=action,
                execution_result=execution_result,
                jsonl_path=events_jsonl,
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
            ingestion=ingestion_result,
        )

    def run_loop(
        self,
        *,
        dry_run: bool | None = None,
        max_iterations: int | None = None,
        workspace_id: str = "standalone",
        session_id: str | None = None,
        note: str | None = None,
        config_path: Path | None = None,
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
        loop_run_id = self._new_run_loop_id()
        resolved_config_path = config_path.resolve() if config_path is not None else None
        artifact_dir = self.config.run.artifact_root / "run-loops" / loop_run_id
        artifact_dir.mkdir(parents=True, exist_ok=False)
        self._write_json(
            artifact_dir / "request.json",
            {
                "loop_run_id": loop_run_id,
                "workspace_id": workspace_id,
                "session_id": session.session_id,
                "project_id": self.config.project.name,
                "config_path": (
                    str(resolved_config_path) if resolved_config_path is not None else None
                ),
                "dry_run": actual_dry_run,
                "max_iterations": limit,
                "note": note,
            },
        )
        if resolved_config_path is not None and resolved_config_path.exists():
            (artifact_dir / resolved_config_path.name).write_text(
                resolved_config_path.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
        session = self.event_store.update_session(
            session.session_id,
            status=SessionStatus.RUNNING,
            resume_reason="run_loop",
            clear_stop_reason=True,
            clear_error_message=True,
            reset_consecutive_failures=True,
            clear_last_failure_at=True,
            clear_cooldown_until=True,
            max_iterations=limit,
            note=note,
        )
        run_ids: list[str] = []
        stop_reason = "max_iterations_reached"
        iteration_index = session.completed_iterations
        loop_result = RunLoopResult(
            loop_run_id=loop_run_id,
            session_id=session.session_id,
            workspace_id=session.workspace_id,
            project_id=session.project_id,
            status=session.status,
            dry_run=session.dry_run,
            max_iterations=limit,
            completed_iterations=session.completed_iterations,
            config_path=resolved_config_path,
            artifact_dir=artifact_dir,
            note=note,
            started_at=datetime.now(UTC),
            stop_reason="running",
        )
        self._persist_run_loop_result(loop_result)
        try:
            while iteration_index < limit:
                result = self.start(dry_run=actual_dry_run)
                run_ids.append(result.run_id)
                action_phase = (
                    result.action.phase
                    if isinstance(result.action.phase, ActionPhase)
                    else ActionPhase(str(result.action.phase))
                )
                if self._consumes_iteration_budget(action_phase):
                    iteration = SessionIteration(
                        session_id=session.session_id,
                        iteration_index=iteration_index + 1,
                        project_id=self.config.project.name,
                        run_id=result.run_id,
                        status=result.status,
                        action_phase=action_phase,
                        action_reason=result.action.reason,
                        finished_at=datetime.now(UTC),
                    )
                    iteration_index += 1
                    self.event_store.append_session_iteration(iteration)
                    self._write_json(
                        artifact_dir / f"iteration-{iteration_index:03d}.json",
                        iteration.model_dump(mode="json"),
                    )
                terminal_status = self._session_status_after_run(
                    action_phase=action_phase,
                    action_reason=result.action.reason,
                    completed_iterations=iteration_index,
                    max_iterations=limit,
                    continue_status=SessionStatus.RUNNING,
                )
                if action_phase is ActionPhase.STOP:
                    stop_reason = f"stop:{result.action.reason}"
                elif terminal_status is SessionStatus.PAUSED:
                    stop_reason = "max_iterations_reached"
                session = self.event_store.update_session(
                    session.session_id,
                    status=terminal_status,
                    completed_iterations=iteration_index,
                    last_run_id=result.run_id,
                    stop_reason=stop_reason,
                    clear_error_message=True,
                    reset_consecutive_failures=True,
                    clear_last_failure_at=True,
                    clear_cooldown_until=True,
                    note=note,
                )
                loop_result.status = session.status
                loop_result.completed_iterations = session.completed_iterations
                loop_result.run_ids = list(run_ids)
                loop_result.stop_reason = stop_reason
                loop_result.error_message = None
                self._persist_run_loop_result(loop_result)
                if terminal_status is not SessionStatus.RUNNING:
                    break
        except Exception as exc:
            session = self.event_store.record_session_failure(
                session.session_id,
                error_message=str(exc),
                stop_reason="run_failed",
                note=note,
            )
            if iteration_index != session.completed_iterations:
                session = self.event_store.update_session(
                    session.session_id,
                    completed_iterations=iteration_index,
                    note=note,
                )
            stop_reason = "run_failed"
            loop_result.status = session.status
            loop_result.completed_iterations = session.completed_iterations
            loop_result.run_ids = list(run_ids)
            loop_result.stop_reason = stop_reason
            loop_result.error_message = str(exc)
            loop_result.finished_at = datetime.now(UTC)
            self._write_json(
                artifact_dir / "error.json",
                {"loop_run_id": loop_run_id, "error": str(exc)},
            )
            self._persist_run_loop_result(loop_result)
            raise
        loop_result.status = session.status
        loop_result.completed_iterations = session.completed_iterations
        loop_result.run_ids = list(run_ids)
        loop_result.stop_reason = stop_reason
        loop_result.finished_at = datetime.now(UTC)
        self._persist_run_loop_result(loop_result)
        return loop_result

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
            action_phase = (
                result.action.phase
                if isinstance(result.action.phase, ActionPhase)
                else ActionPhase(str(result.action.phase))
            )
            iteration_index = session.completed_iterations
            if self._consumes_iteration_budget(action_phase):
                iteration_index += 1
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
            status = self._session_status_after_run(
                action_phase=action_phase,
                action_reason=result.action.reason,
                completed_iterations=iteration_index,
                max_iterations=session.max_iterations,
                continue_status=SessionStatus.PENDING,
            )
            if action_phase is ActionPhase.STOP:
                stop_reason = f"stop:{result.action.reason}"
            elif status is SessionStatus.PAUSED:
                stop_reason = "max_iterations_reached"
            else:
                stop_reason = "quantum_complete"
            updated = self.event_store.update_session(
                session.session_id,
                status=status,
                completed_iterations=iteration_index,
                last_run_id=result.run_id,
                clear_error_message=True,
                stop_reason=stop_reason,
                reset_consecutive_failures=True,
                clear_last_failure_at=True,
                clear_cooldown_until=True,
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
            self.event_store.record_session_failure(
                session.session_id,
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
        analyzer = build_lean_analyzer(self.config.lean_analyzer)
        analysis = collect_lean_analysis(
            project_path=self.config.project.project_path,
            archon_path=self.config.project.archon_path,
            analyzer=analyzer,
        )
        snapshot = collect_project_snapshot(
            project_path=self.config.project.project_path,
            archon_path=self.config.project.archon_path,
            analyzer=analyzer,
            analysis=analysis,
        )
        control_state = self.control.read(self.config.project)
        task_graph = build_task_graph(
            project_path=self.config.project.project_path,
            archon_path=self.config.project.archon_path,
            analyzer=analyzer,
            analysis=analysis,
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
            analysis=analysis,
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

    @staticmethod
    def _consumes_iteration_budget(action_phase: ActionPhase) -> bool:
        return action_phase is not ActionPhase.STOP

    @staticmethod
    def _session_status_after_run(
        *,
        action_phase: ActionPhase,
        action_reason: str,
        completed_iterations: int,
        max_iterations: int,
        continue_status: SessionStatus,
    ) -> SessionStatus:
        if action_phase is ActionPhase.STOP:
            if action_reason == "control_paused":
                return SessionStatus.PAUSED
            return SessionStatus.COMPLETED
        if completed_iterations >= max_iterations:
            return SessionStatus.PAUSED
        return continue_status

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
    def _new_run_loop_id() -> str:
        return f"run-loop-{uuid.uuid4().hex[:10]}"

    @staticmethod
    def _new_session_id() -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"session-{timestamp}-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _write_json(path: Path, payload: object) -> None:
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _maybe_ingest_execution_output(
        self,
        *,
        run_id: str,
        run_dir: Path,
        action: AdapterAction,
        execution_result: ExecutionResult,
        jsonl_path: Path,
    ) -> ExecutionIngestionResult | None:
        if execution_result.executor is not ExecutorKind.OPENAI_COMPATIBLE:
            return None
        ingestion = self.adapter.ingest_execution_output(
            run_id=run_id,
            phase=str(action.phase),
            response_text=execution_result.text,
            task_id=action.task_id,
            task_title=action.task_title,
        )
        if ingestion is None:
            return None
        ingestion_path = run_dir / "ingestion.json"
        self._write_json(ingestion_path, ingestion.model_dump(mode="json"))
        self.event_store.append(
            EventRecord(
                run_id=run_id,
                kind="execution.ingested",
                project_id=self.config.project.name,
                task_id=action.task_id,
                payload={
                    "phase": ingestion.phase,
                    "task_result_path": (
                        str(ingestion.task_result_path)
                        if ingestion.task_result_path is not None
                        else None
                    ),
                    "proof_journal_session_path": (
                        str(ingestion.proof_journal_session_path)
                        if ingestion.proof_journal_session_path is not None
                        else None
                    ),
                    "archived_task_results": [
                        str(path) for path in ingestion.archived_task_results
                    ],
                    "ingestion_path": str(ingestion_path),
                },
            ),
            jsonl_path=jsonl_path,
        )
        return ingestion

    def _persist_run_loop_result(self, result: RunLoopResult) -> None:
        if result.artifact_dir is None:
            return
        self._write_json(
            result.artifact_dir / "summary.json",
            result.model_dump(mode="json"),
        )
        self.event_store.upsert_run_loop_run(result)
