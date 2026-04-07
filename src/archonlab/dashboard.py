from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .batch import BatchRunner
from .benchmark import load_benchmark_manifest
from .config import build_workspace_project_app_config, load_config, load_workspace_config
from .control import ControlService
from .events import EventStore
from .executors import snapshot_provider_pool_health
from .experiment_ledger import (
    build_experiment_ledger_comparison,
    build_experiment_replay,
    load_experiment_ledger,
)
from .fleet import persist_batch_fleet_run
from .models import (
    ActionPhase,
    AppConfig,
    ExecutorKind,
    ExperimentLedger,
    LeanAnalysisSnapshot,
    ProjectSession,
    ProviderKind,
    QueueJob,
    RunPreview,
    SessionStatus,
    TaskGraph,
    TaskSource,
    TaskStatus,
    WorkflowMode,
    WorkflowRule,
    WorkflowSpec,
    WorkspaceConfig,
    WorkspaceProjectConfig,
)
from .queue import QueueStore, session_block_reason
from .services import RunService
from .workspace_daemon import load_workspace_daemon_state, workspace_daemon_state_path


class PauseRequest(BaseModel):
    reason: str | None = None


class HintRequest(BaseModel):
    text: str
    author: str = "user"


class QueueEnqueueRequest(BaseModel):
    manifest_path: Path
    dry_run: bool = True
    use_worktrees: bool = False


class QueueCancelRequest(BaseModel):
    reason: str | None = None


class QueueFleetRequest(BaseModel):
    workers: int | None = None
    plan_driven: bool = False
    target_jobs_per_worker: int = 2
    max_jobs_per_worker: int | None = None
    poll_seconds: float = 2.0
    idle_timeout_seconds: float = 30.0
    stale_after_seconds: float | None = 120.0
    executor_kinds: list[ExecutorKind] | None = None
    provider_kinds: list[ProviderKind] | None = None
    models: list[str] | None = None
    cost_tiers: list[str] | None = None
    endpoint_classes: list[str] | None = None


class QueueSweepWorkersRequest(BaseModel):
    stale_after_seconds: float = 120.0
    requeue_running_jobs: bool = True


class WorkspaceEnqueueRequest(BaseModel):
    project_id: str | None = None
    tags: list[str] = Field(default_factory=list)
    max_iterations: int | None = None
    dry_run: bool | None = None
    priority: int = 0
    note: str | None = None


class WorkspaceResumeRequest(BaseModel):
    project_id: str | None = None
    tags: list[str] = Field(default_factory=list)
    max_iterations: int | None = None
    priority: int = 0
    resume_reason: str | None = None
    note: str | None = None


class WorkflowOverrideRequest(BaseModel):
    workflow: WorkflowMode | None = None
    workflow_spec_path: Path | None = None
    clear_workflow_spec: bool = False


class BenchmarkLedgerRequest(BaseModel):
    summary_path: Path | None = None
    ledger_path: Path | None = None


class BenchmarkRunSourceRequest(BaseModel):
    manifest_path: Path | None = None
    artifact_root: Path | None = None


class BenchmarkRunsRequest(BenchmarkRunSourceRequest):
    limit: int = Field(default=20, ge=1, le=200)


class BenchmarkRunDetailRequest(BenchmarkRunSourceRequest):
    run_id: str = Field(min_length=1)


class BenchmarkCompareRequest(BaseModel):
    baseline_summary_path: Path | None = None
    baseline_ledger_path: Path | None = None
    candidate_summary_path: Path | None = None
    candidate_ledger_path: Path | None = None


class BenchmarkReplayRequest(BaseModel):
    summary_path: Path | None = None
    ledger_path: Path | None = None
    project_id: str = Field(min_length=1)
    theorem_name: str | None = None


def create_dashboard_app(config_path: Path) -> FastAPI:
    resolved_config_path = config_path.resolve()
    try:
        workspace_config = load_workspace_config(resolved_config_path)
        default_project_id = workspace_config.projects[0].id
        config = build_workspace_project_app_config(
            workspace_config,
            project_id=default_project_id,
        )
        dashboard_title = workspace_config.name
        available_projects = {project.id for project in workspace_config.projects}
    except (KeyError, ValueError):
        workspace_config = None
        config = load_config(resolved_config_path)
        default_project_id = config.project.name
        dashboard_title = config.project.name
        available_projects = {config.project.name}

    store = EventStore(config.run.artifact_root / "archonlab.db")
    control = ControlService(config.run.artifact_root)
    queue = QueueStore(config.run.artifact_root / "archonlab.db")
    app = FastAPI(title="ArchonLab Dashboard")

    def resolve_project_app_config(project_id: str) -> AppConfig:
        _ensure_project(available_projects, project_id)
        if workspace_config is None:
            return config
        return build_workspace_project_app_config(
            workspace_config,
            project_id=project_id,
        )

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return render_dashboard_html(
            dashboard_title,
            default_project_id=default_project_id,
            available_project_ids=sorted(available_projects),
        )

    @app.get("/api/runs")
    def list_runs(limit: int = 20) -> list[dict[str, Any]]:
        runs = store.list_runs(limit=limit)
        return [run.model_dump(mode="json") for run in runs]

    @app.get("/api/runs/{run_id}")
    def run_detail(run_id: str) -> dict[str, Any]:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        events = store.get_run_events(run_id)
        summary_path = run.artifact_dir / "run-summary.json"
        summary = None
        if summary_path.exists():
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        return {
            "run": run.model_dump(mode="json"),
            "events": [event.model_dump(mode="json") for event in events],
            "summary": summary,
        }

    @app.get("/api/projects/{project_id}/control")
    def get_control(project_id: str) -> dict[str, Any]:
        project_config = resolve_project_app_config(project_id)
        return control.read(project_config.project).model_dump(mode="json")

    @app.get("/api/projects/{project_id}/preview")
    def get_project_preview(project_id: str) -> dict[str, Any]:
        project_config = resolve_project_app_config(project_id)
        preview = RunService(project_config).preview()
        workflow_spec = preview.workflow_spec
        return {
            "project_id": project_config.project.name,
            "workflow": preview.workflow.value,
            "configured_workflow": project_config.run.workflow.value,
            "workflow_spec_path": (
                str(preview.workflow_spec_path)
                if preview.workflow_spec_path is not None
                else None
            ),
            "configured_workflow_spec_path": (
                str(project_config.run.workflow_spec)
                if project_config.run.workflow_spec is not None
                else None
            ),
            "workflow_spec": (
                {
                    "name": workflow_spec.name,
                    "description": workflow_spec.description,
                    "rule_count": len(workflow_spec.rules),
                }
                if workflow_spec is not None
                else None
            ),
            "analysis_summary": _summarize_analysis(preview.analysis),
            "task_graph_summary": _summarize_task_graph(preview.task_graph),
            "focus_task": _summarize_focus_task(preview),
            "workflow_rules": _summarize_workflow_rules(workflow_spec),
            "supervisor_evidence": preview.supervisor.evidence,
            "preview": preview.model_dump(mode="json"),
        }

    @app.get("/api/projects/{project_id}/run-loops")
    def get_project_run_loops(project_id: str, limit: int = 20) -> dict[str, Any]:
        project_config = resolve_project_app_config(project_id)
        workspace_name = workspace_config.name if workspace_config is not None else "standalone"
        loops = store.list_run_loop_runs(
            workspace_id=workspace_name,
            project_id=project_config.project.name,
            limit=limit,
        )
        return {
            "project": project_config.project.name,
            "workspace": workspace_name,
            "loops": [loop.model_dump(mode="json") for loop in loops],
        }

    @app.post("/api/projects/{project_id}/workflow")
    def set_project_workflow(project_id: str, body: WorkflowOverrideRequest) -> dict[str, Any]:
        project_config = resolve_project_app_config(project_id)
        if (
            body.workflow is None
            and body.workflow_spec_path is None
            and not body.clear_workflow_spec
        ):
            raise HTTPException(
                status_code=400,
                detail="Specify workflow, workflow_spec_path, or clear_workflow_spec.",
            )
        if body.workflow_spec_path is not None and body.clear_workflow_spec:
            raise HTTPException(
                status_code=400,
                detail="workflow_spec_path cannot be combined with clear_workflow_spec.",
            )
        workflow_spec_path = (
            _resolve_dashboard_path(resolved_config_path.parent, body.workflow_spec_path)
            if body.workflow_spec_path is not None
            else None
        )
        if workflow_spec_path is not None and not workflow_spec_path.exists():
            raise HTTPException(status_code=404, detail="Workflow spec not found")
        state = control.set_workflow(
            project_config.project,
            workflow=body.workflow,
            workflow_spec_override=workflow_spec_path,
            clear_workflow_spec=body.clear_workflow_spec,
        )
        return state.model_dump(mode="json")

    @app.post("/api/projects/{project_id}/workflow/reset")
    def reset_project_workflow(project_id: str) -> dict[str, Any]:
        project_config = resolve_project_app_config(project_id)
        return control.reset_workflow(project_config.project).model_dump(mode="json")

    @app.post("/api/projects/{project_id}/pause")
    def pause_project(project_id: str, body: PauseRequest) -> dict[str, Any]:
        project_config = resolve_project_app_config(project_id)
        return control.pause(project_config.project, reason=body.reason).model_dump(mode="json")

    @app.post("/api/projects/{project_id}/resume")
    def resume_project(project_id: str) -> dict[str, Any]:
        project_config = resolve_project_app_config(project_id)
        return control.resume(project_config.project).model_dump(mode="json")

    @app.post("/api/projects/{project_id}/hint")
    def add_hint(project_id: str, body: HintRequest) -> dict[str, Any]:
        project_config = resolve_project_app_config(project_id)
        return control.add_hint(
            project_config.project,
            text=body.text,
            author=body.author,
        ).model_dump(mode="json")

    @app.get("/api/workspace/overview")
    def get_workspace_overview(limit_sessions: int = 100) -> dict[str, Any]:
        return _build_workspace_overview(
            config=config,
            workspace_config=workspace_config,
            store=store,
            queue=queue,
            limit_sessions=limit_sessions,
        )

    @app.get("/api/workspace/loops")
    def get_workspace_loops(limit: int = 20) -> dict[str, Any]:
        workspace_name = workspace_config.name if workspace_config is not None else "standalone"
        loops = store.list_workspace_loop_runs(
            workspace_id=workspace_name,
            limit=limit,
        )
        return {
            "workspace": workspace_name,
            "loops": [item.model_dump(mode="json") for item in loops],
        }

    @app.get("/api/workspace/fleet-runs")
    def get_workspace_fleet_runs(limit: int = 20) -> dict[str, Any]:
        workspace_name = workspace_config.name if workspace_config is not None else "standalone"
        runs = store.list_fleet_runs(
            workspace_id=workspace_name,
            limit=limit,
        )
        return {
            "workspace": workspace_name,
            "runs": [item.model_dump(mode="json") for item in runs],
        }

    @app.post("/api/workspace/enqueue")
    def enqueue_workspace(body: WorkspaceEnqueueRequest) -> list[dict[str, Any]]:
        resolved_workspace = _require_workspace_mode(workspace_config)
        if body.project_id is not None and body.project_id not in {
            project.id for project in resolved_workspace.projects
        }:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown workspace project: {body.project_id}",
            )
        jobs = queue.enqueue_workspace_sessions(
            resolved_config_path,
            project_ids=[body.project_id] if body.project_id is not None else None,
            project_tags=body.tags or None,
            max_iterations=body.max_iterations,
            dry_run=body.dry_run,
            priority=body.priority,
            note=body.note,
        )
        return [job.model_dump(mode="json") for job in jobs]

    @app.post("/api/workspace/resume")
    def resume_workspace(body: WorkspaceResumeRequest) -> dict[str, Any]:
        resolved_workspace = _require_workspace_mode(workspace_config)
        if body.project_id is not None and body.project_id not in {
            project.id for project in resolved_workspace.projects
        }:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown workspace project: {body.project_id}",
            )
        result = queue.resume_workspace_sessions(
            resolved_config_path,
            project_ids=[body.project_id] if body.project_id is not None else None,
            project_tags=body.tags or None,
            max_iterations=body.max_iterations,
            priority=body.priority,
            resume_reason=body.resume_reason,
            note=body.note,
        )
        return {
            "resumed": [
                {
                    "session": session.model_dump(mode="json"),
                    "job": job.model_dump(mode="json"),
                }
                for session, job in result.resumed
            ],
            "skipped": [
                {
                    "project_id": item.project_id,
                    "session_id": item.session_id,
                    "reason": item.reason,
                }
                for item in result.skipped
            ],
        }

    @app.get("/api/queue/jobs")
    def list_queue_jobs(limit: int = 50) -> list[dict[str, Any]]:
        return [job.model_dump(mode="json") for job in queue.list_jobs(limit=limit)]

    @app.get("/api/queue/jobs/{job_id}")
    def queue_job_detail(job_id: str) -> dict[str, Any]:
        return _queue_job_or_404(queue, job_id).model_dump(mode="json")

    @app.get("/api/queue/workers")
    def list_queue_workers(stale_after_seconds: float | None = 120.0) -> list[dict[str, Any]]:
        return [
            worker.model_dump(mode="json")
            for worker in queue.list_workers(stale_after_seconds=stale_after_seconds)
        ]

    @app.get("/api/queue/fleet-plan")
    def get_queue_fleet_plan(
        target_jobs_per_worker: int = 2,
        stale_after_seconds: float = 120.0,
    ) -> dict[str, Any]:
        try:
            plan = queue.plan_fleet(
                target_jobs_per_worker=target_jobs_per_worker,
                stale_after_seconds=stale_after_seconds,
                provider_pools=(
                    config.provider_pools if config.provider.pool is not None else None
                ),
                provider_health_db_path=config.run.artifact_root / "archonlab.db",
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return plan.model_dump(mode="json")

    @app.post("/api/queue/enqueue")
    def enqueue_queue_job(body: QueueEnqueueRequest) -> list[dict[str, Any]]:
        jobs = queue.enqueue_benchmark_manifest(
            body.manifest_path,
            dry_run=body.dry_run,
            use_worktrees=body.use_worktrees,
        )
        return [job.model_dump(mode="json") for job in jobs]

    @app.post("/api/queue/run")
    def run_queue(max_jobs: int | None = None) -> dict[str, Any]:
        runner = BatchRunner(
            queue_store=queue,
            control_service=control,
            artifact_root=config.run.artifact_root,
            slot_limit=config.run.max_parallel,
            provider_pools=(
                config.provider_pools if config.provider.pool is not None else None
            ),
        )
        return runner.run_pending(max_jobs=max_jobs).model_dump(mode="json")

    @app.post("/api/queue/fleet")
    def run_queue_fleet(body: QueueFleetRequest) -> dict[str, Any]:
        initial_plan = queue.plan_fleet(
            target_jobs_per_worker=body.target_jobs_per_worker,
            stale_after_seconds=body.stale_after_seconds,
            provider_pools=(
                config.provider_pools if config.provider.pool is not None else None
            ),
            provider_health_db_path=config.run.artifact_root / "archonlab.db",
        )
        started_at = datetime.now(UTC)
        runner = BatchRunner(
            queue_store=queue,
            control_service=control,
            artifact_root=config.run.artifact_root,
            slot_limit=body.workers or config.run.max_parallel,
            provider_pools=(
                config.provider_pools if config.provider.pool is not None else None
            ),
        )
        report = runner.run_fleet(
            worker_count=(
                body.workers
                if body.plan_driven
                else (body.workers or config.run.max_parallel)
            ),
            plan_driven=body.plan_driven,
            target_jobs_per_worker=body.target_jobs_per_worker,
            max_jobs_per_worker=body.max_jobs_per_worker,
            poll_seconds=body.poll_seconds,
            idle_timeout_seconds=body.idle_timeout_seconds,
            stale_after_seconds=body.stale_after_seconds,
            executor_kinds=body.executor_kinds,
            provider_kinds=body.provider_kinds,
            models=body.models,
            cost_tiers=body.cost_tiers,
            endpoint_classes=body.endpoint_classes,
        )
        result = persist_batch_fleet_run(
            queue_store=queue,
            artifact_root=config.run.artifact_root,
            initial_plan=initial_plan,
            report=report,
            started_at=started_at,
            target_jobs_per_worker=body.target_jobs_per_worker,
            stale_after_seconds=body.stale_after_seconds,
            workspace_id=workspace_config.name if workspace_config is not None else "standalone",
            config_path=resolved_config_path,
            launcher="dashboard_batch_runner",
            provider_pools=(
                config.provider_pools if config.provider.pool is not None else None
            ),
            provider_health_db_path=config.run.artifact_root / "archonlab.db",
            request_payload=body.model_dump(mode="json"),
        )
        return {
            **report.model_dump(mode="json"),
            "fleet_run_id": result.fleet_run_id,
            "artifact_dir": str(result.artifact_dir) if result.artifact_dir is not None else None,
            "stop_reason": result.stop_reason,
            "cycles_completed": result.cycles_completed,
            "total_processed_jobs": result.total_processed_jobs,
            "total_paused_jobs": result.total_paused_jobs,
            "total_failed_jobs": result.total_failed_jobs,
            "total_workers_launched": result.total_workers_launched,
        }

    @app.post("/api/queue/jobs/{job_id}/cancel")
    def cancel_queue_job(job_id: str, body: QueueCancelRequest) -> dict[str, Any]:
        _queue_job_or_404(queue, job_id)
        job = queue.cancel(job_id, reason=body.reason)
        return job.model_dump(mode="json")

    @app.post("/api/queue/jobs/{job_id}/requeue")
    def requeue_queue_job(job_id: str) -> dict[str, Any]:
        _queue_job_or_404(queue, job_id)
        try:
            job = queue.requeue(job_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return job.model_dump(mode="json")

    @app.post("/api/queue/workers/sweep")
    def sweep_queue_workers(body: QueueSweepWorkersRequest) -> list[dict[str, Any]]:
        workers = queue.reap_stale_workers(
            stale_after_seconds=body.stale_after_seconds,
            requeue_running_jobs=body.requeue_running_jobs,
        )
        return [worker.model_dump(mode="json") for worker in workers]

    @app.post("/api/benchmark/runs")
    def benchmark_runs(body: BenchmarkRunsRequest) -> list[dict[str, Any]]:
        try:
            store = _resolve_dashboard_benchmark_store(
                base_dir=resolved_config_path.parent,
                manifest_path=body.manifest_path,
                artifact_root=body.artifact_root,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return [
            result.model_dump(mode="json")
            for result in store.list_benchmark_runs(limit=body.limit)
        ]

    @app.post("/api/benchmark/run-detail")
    def benchmark_run_detail(body: BenchmarkRunDetailRequest) -> dict[str, Any]:
        try:
            store = _resolve_dashboard_benchmark_store(
                base_dir=resolved_config_path.parent,
                manifest_path=body.manifest_path,
                artifact_root=body.artifact_root,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        result = store.get_benchmark_run(body.run_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"Benchmark run not found: {body.run_id}")
        return result.model_dump(mode="json")

    @app.post("/api/benchmark/experiment-ledger")
    def benchmark_experiment_ledger(body: BenchmarkLedgerRequest) -> dict[str, Any]:
        try:
            ledger = _load_dashboard_experiment_ledger(
                base_dir=resolved_config_path.parent,
                summary_path=body.summary_path,
                ledger_path=body.ledger_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=404,
                detail=f"Experiment ledger not found: {exc}",
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return ledger.model_dump(mode="json")

    @app.post("/api/benchmark/compare")
    def benchmark_compare(body: BenchmarkCompareRequest) -> dict[str, Any]:
        try:
            baseline = _load_dashboard_experiment_ledger(
                base_dir=resolved_config_path.parent,
                summary_path=body.baseline_summary_path,
                ledger_path=body.baseline_ledger_path,
            )
            candidate = _load_dashboard_experiment_ledger(
                base_dir=resolved_config_path.parent,
                summary_path=body.candidate_summary_path,
                ledger_path=body.candidate_ledger_path,
            )
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=404,
                detail=f"Experiment ledger not found: {exc}",
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        comparison = build_experiment_ledger_comparison(
            baseline_ledger=baseline,
            candidate_ledger=candidate,
        )
        return comparison.model_dump(mode="json")

    @app.post("/api/benchmark/replay")
    def benchmark_replay(body: BenchmarkReplayRequest) -> dict[str, Any]:
        try:
            ledger = _load_dashboard_experiment_ledger(
                base_dir=resolved_config_path.parent,
                summary_path=body.summary_path,
                ledger_path=body.ledger_path,
            )
            replay = build_experiment_replay(
                experiment_ledger=ledger,
                project_id=body.project_id,
                theorem_name=body.theorem_name,
            )
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=404,
                detail=f"Experiment ledger not found: {exc}",
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return replay.model_dump(mode="json")

    return app


def _ensure_project(available_project_ids: set[str], actual_project_id: str) -> None:
    if actual_project_id not in available_project_ids:
        raise HTTPException(status_code=404, detail="Project not found")


def _queue_job_or_404(queue: QueueStore, job_id: str) -> QueueJob:
    job = queue.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Queue job not found")
    return job


def _resolve_dashboard_path(base_dir: Path, raw_path: Path) -> Path:
    if raw_path.is_absolute():
        return raw_path.resolve()
    return (base_dir / raw_path).resolve()


def _load_dashboard_experiment_ledger(
    *,
    base_dir: Path,
    summary_path: Path | None,
    ledger_path: Path | None,
) -> ExperimentLedger:
    if summary_path is not None and ledger_path is not None:
        raise ValueError("Specify either summary_path or ledger_path, not both.")
    if summary_path is None and ledger_path is None:
        raise ValueError("Specify summary_path or ledger_path.")
    raw_path = summary_path if summary_path is not None else ledger_path
    if raw_path is None:
        raise ValueError("Specify summary_path or ledger_path.")
    resolved_path = _resolve_dashboard_path(base_dir, raw_path)
    if not resolved_path.exists():
        raise FileNotFoundError(resolved_path)
    return load_experiment_ledger(resolved_path)


def _resolve_dashboard_benchmark_store(
    *,
    base_dir: Path,
    manifest_path: Path | None,
    artifact_root: Path | None,
) -> EventStore:
    if manifest_path is not None and artifact_root is not None:
        raise ValueError("Specify either manifest_path or artifact_root, not both.")
    if manifest_path is None and artifact_root is None:
        raise ValueError("Specify manifest_path or artifact_root.")
    if manifest_path is not None:
        resolved_manifest_path = _resolve_dashboard_path(base_dir, manifest_path)
        if not resolved_manifest_path.exists():
            raise FileNotFoundError(f"Benchmark manifest not found: {resolved_manifest_path}")
        resolved_artifact_root = load_benchmark_manifest(
            resolved_manifest_path
        ).benchmark.artifact_root
    else:
        if artifact_root is None:
            raise ValueError("Specify manifest_path or artifact_root.")
        resolved_artifact_root = _resolve_dashboard_path(base_dir, artifact_root)
    return EventStore(resolved_artifact_root / "archonlab.db")


def _require_workspace_mode(workspace_config: WorkspaceConfig | None) -> WorkspaceConfig:
    if workspace_config is None:
        raise HTTPException(status_code=400, detail="Workspace mode is required for this action.")
    return workspace_config


def _build_workspace_overview(
    *,
    config: AppConfig,
    workspace_config: WorkspaceConfig | None,
    store: EventStore,
    queue: QueueStore,
    limit_sessions: int,
) -> dict[str, Any]:
    if workspace_config is None:
        workspace_name = "standalone"
        project_rows = [
            WorkspaceProjectConfig(
                id=config.project.name,
                project_path=config.project.project_path,
                archon_path=config.project.archon_path,
                workflow=config.run.workflow,
                max_iterations=config.run.max_iterations,
                dry_run=config.run.dry_run,
            )
        ]
        sessions = store.list_sessions(project_id=config.project.name, limit=limit_sessions)
    else:
        workspace_name = workspace_config.name
        project_rows = workspace_config.projects
        sessions = store.list_sessions(workspace_id=workspace_name, limit=limit_sessions)
    latest_loop = next(
        iter(store.list_workspace_loop_runs(workspace_id=workspace_name, limit=1)),
        None,
    )
    latest_fleet = next(
        iter(store.list_fleet_runs(workspace_id=workspace_name, limit=1)),
        None,
    )
    daemon_state = None
    if workspace_daemon_state_path(config.run.artifact_root).exists():
        daemon_state = load_workspace_daemon_state(
            config.run.artifact_root,
            workspace_id=workspace_name,
        )
    loop_history = [
        item.model_dump(mode="json")
        for item in store.list_workspace_loop_runs(workspace_id=workspace_name, limit=20)
    ]
    fleet_history = [
        item.model_dump(mode="json")
        for item in store.list_fleet_runs(workspace_id=workspace_name, limit=20)
    ]

    project_ids = {project.id for project in project_rows}
    project_tags = {project.id: list(project.tags) for project in project_rows}
    jobs = [
        job
        for job in queue.list_jobs(limit=200)
        if (
            (job.workspace_id == workspace_name)
            or (job.workspace_id is None and job.project_id in project_ids)
        )
    ]
    workers = queue.list_workers(stale_after_seconds=120.0)
    now = datetime.now(UTC)
    session_payloads = [
        _workspace_session_payload(
            session,
            tags=project_tags.get(session.project_id, []),
            now=now,
        )
        for session in sessions
    ]
    blocked_session_counts = _count_session_block_reasons(session_payloads)
    sessions_by_project: dict[str, list[dict[str, Any]]] = {}
    for session_payload in session_payloads:
        sessions_by_project.setdefault(session_payload["project_id"], []).append(
            session_payload
        )

    project_summaries = []
    for project in project_rows:
        project_sessions = sessions_by_project.get(project.id, [])
        project_jobs = [job for job in jobs if job.project_id == project.id]
        project_summaries.append(
            {
                "project_id": project.id,
                "enabled": project.enabled,
                "workflow": (project.workflow or config.run.workflow).value,
                "tags": list(project.tags),
                "dry_run": (
                    project.dry_run
                    if project.dry_run is not None
                    else config.run.dry_run
                ),
                "max_iterations": (
                    project.max_iterations
                    if project.max_iterations is not None
                    else config.run.max_iterations
                ),
                "session_count": len(project_sessions),
                "running_sessions": sum(
                    1
                    for session in project_sessions
                    if session["status"] == SessionStatus.RUNNING.value
                ),
                "queued_jobs": sum(
                    1 for job in project_jobs if job.status.value in {"queued", "pending"}
                ),
                "blocked_sessions": sum(
                    1 for session in project_sessions if session["blocked_reason"] is not None
                ),
                "blocked_session_counts": _count_session_block_reasons(project_sessions),
            }
        )

    return {
        "workspace": workspace_name,
        "mode": "workspace" if workspace_config is not None else "project",
        "default_project_id": project_rows[0].id,
        "project_count": len(project_rows),
        "session_count": len(sessions),
        "running_sessions": sum(
            1 for session in sessions if session.status is SessionStatus.RUNNING
        ),
        "queued_jobs": sum(1 for job in jobs if job.status.value in {"queued", "pending"}),
        "running_jobs": sum(1 for job in jobs if job.status.value == "running"),
        "active_workers": sum(1 for worker in workers if worker.status.value != "stopped"),
        "blocked_sessions": sum(blocked_session_counts.values()),
        "blocked_session_counts": blocked_session_counts,
        "budget": {
            "max_iterations": sum(session.max_iterations for session in sessions),
            "completed_iterations": sum(session.completed_iterations for session in sessions),
            "remaining_iterations": sum(
                max(session.max_iterations - session.completed_iterations, 0)
                for session in sessions
            ),
        },
        "latest_loop": (
            latest_loop.model_dump(mode="json") if latest_loop is not None else None
        ),
        "latest_fleet": (
            latest_fleet.model_dump(mode="json") if latest_fleet is not None else None
        ),
        "daemon": (
            daemon_state.model_dump(mode="json") if daemon_state is not None else None
        ),
        "loop_history": loop_history,
        "fleet_history": fleet_history,
        "projects": project_summaries,
        "provider_runtime": [
            item.model_dump(mode="json")
            for item in store.summarize_provider_runtime(limit=200)
        ],
        "provider_health": [
            item.model_dump(mode="json")
            for item in snapshot_provider_pool_health(
                config.provider_pools,
                db_path=config.run.artifact_root / "archonlab.db",
            )
        ],
        "sessions": session_payloads,
        "workers": [worker.model_dump(mode="json") for worker in workers],
    }


def _workspace_session_payload(
    session: ProjectSession,
    *,
    tags: list[str],
    now: datetime,
) -> dict[str, Any]:
    blocked_reason = session_block_reason(session, now=now)
    cooldown_seconds_remaining = 0
    if session.cooldown_until is not None and session.cooldown_until > now:
        cooldown_seconds_remaining = int((session.cooldown_until - now).total_seconds())
    return {
        **session.model_dump(mode="json"),
        "remaining_iterations": max(
            session.max_iterations - session.completed_iterations,
            0,
        ),
        "failure_budget_remaining": max(
            session.max_consecutive_failures - session.consecutive_failures,
            0,
        ),
        "tags": tags,
        "blocked_reason": blocked_reason,
        "cooldown_seconds_remaining": max(cooldown_seconds_remaining, 0),
    }


def _count_session_block_reasons(sessions: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for session in sessions:
        blocked_reason = session.get("blocked_reason")
        if not isinstance(blocked_reason, str) or not blocked_reason:
            continue
        counts[blocked_reason] = counts.get(blocked_reason, 0) + 1
    return counts


def _summarize_task_graph(task_graph: TaskGraph) -> dict[str, int]:
    return {
        "total_nodes": len(task_graph.nodes),
        "objective_nodes": sum(
            1 for node in task_graph.nodes if TaskSource.OBJECTIVE in node.sources
        ),
        "declaration_nodes": sum(
            1 for node in task_graph.nodes if TaskSource.LEAN_DECLARATION in node.sources
        ),
        "blocked_nodes": sum(
            1 for node in task_graph.nodes if node.status is TaskStatus.BLOCKED
        ),
        "pending_nodes": sum(
            1 for node in task_graph.nodes if node.status is TaskStatus.PENDING
        ),
        "completed_nodes": sum(
            1 for node in task_graph.nodes if node.status is TaskStatus.COMPLETED
        ),
    }


def _summarize_analysis(analysis: LeanAnalysisSnapshot) -> dict[str, Any]:
    proof_gaps = [
        gap.kind
        for gap in analysis.proof_gaps[:3]
    ]
    diagnostics = [
        diagnostic.code or diagnostic.severity
        for diagnostic in analysis.diagnostics[:3]
    ]
    return {
        "backend": analysis.backend,
        "fallback_used": analysis.fallback_used,
        "fallback_reason": analysis.fallback_reason,
        "theorem_count": analysis.theorem_count,
        "declaration_count": len(analysis.declarations),
        "proof_gap_count": len(analysis.proof_gaps),
        "diagnostic_count": len(analysis.diagnostics),
        "top_proof_gaps": proof_gaps,
        "top_diagnostics": diagnostics,
    }


def _summarize_focus_task(preview: RunPreview) -> dict[str, Any] | None:
    action = preview.action
    if action.task_id is None and action.task_title is None and action.theorem_name is None:
        return None
    return {
        "task_id": action.task_id,
        "title": action.task_title,
        "theorem_name": action.theorem_name,
        "file_path": str(action.file_path) if action.file_path is not None else None,
        "task_status": (
            action.task_status.value if action.task_status is not None else None
        ),
        "task_priority": action.task_priority,
        "objective_relevant": action.objective_relevant,
        "task_sources": [source.value for source in action.task_sources],
        "task_blockers": action.task_blockers,
    }


def _summarize_workflow_rules(spec: WorkflowSpec | None) -> list[dict[str, Any]]:
    if spec is None:
        return []
    return [_summarize_workflow_rule(rule) for rule in spec.rules]


def _summarize_workflow_rule(rule: WorkflowRule) -> dict[str, Any]:
    conditions: list[str] = []
    if rule.when_supervisor_reason is not None:
        conditions.append(f"supervisor={rule.when_supervisor_reason.value}")
    if rule.when_task_status is not None:
        conditions.append(f"task_status={rule.when_task_status.value}")
    if rule.when_phase is not None:
        conditions.append(f"phase={rule.when_phase.value}")
    if rule.when_has_task_results is not None:
        conditions.append(
            f"task_results={'yes' if rule.when_has_task_results else 'no'}"
        )
    if rule.when_has_review_sessions is not None:
        conditions.append(
            f"review_sessions={'yes' if rule.when_has_review_sessions else 'no'}"
        )
    return {
        "name": rule.name,
        "phase": (
            rule.phase.value if isinstance(rule.phase, ActionPhase) else str(rule.phase)
        ),
        "reason": rule.reason,
        "conditions": conditions,
    }


def render_dashboard_html(
    title: str,
    *,
    default_project_id: str,
    available_project_ids: list[str],
) -> str:
    project_selector_options = "".join(
        (
            f'<option value="{project_id}"'
            f'{" selected" if project_id == default_project_id else ""}>'
            f"{project_id}</option>"
        )
        for project_id in available_project_ids
    )
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>ArchonLab Control Deck</title>
    <style>
      :root {{
        --bg: #f5efe1;
        --panel: rgba(255, 252, 245, 0.84);
        --ink: #1f2833;
        --muted: #5d6a72;
        --accent: #c4472d;
        --accent-soft: #f0c4b7;
        --line: rgba(31, 40, 51, 0.14);
        --shadow: 0 22px 50px rgba(31, 40, 51, 0.12);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        min-height: 100vh;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(196, 71, 45, 0.16), transparent 32%),
          radial-gradient(circle at right 20%, rgba(67, 119, 140, 0.14), transparent 28%),
          linear-gradient(180deg, #f6f1e5 0%, #efe6d2 100%);
        font-family: "Iosevka Aile", "IBM Plex Sans", "Avenir Next", sans-serif;
      }}
      .shell {{
        max-width: 1280px;
        margin: 0 auto;
        padding: 24px;
      }}
      .hero {{
        display: grid;
        gap: 18px;
        padding: 28px;
        border: 1px solid var(--line);
        border-radius: 28px;
        background: linear-gradient(135deg, rgba(255,255,255,0.72), rgba(255,248,238,0.84));
        box-shadow: var(--shadow);
      }}
      .eyebrow {{
        letter-spacing: 0.18em;
        text-transform: uppercase;
        font-size: 12px;
        color: var(--accent);
      }}
      h1 {{
        margin: 0;
        font-family: "Iowan Old Style", "Palatino Linotype", serif;
        font-size: clamp(32px, 5vw, 56px);
        line-height: 0.95;
      }}
      .subtitle {{
        max-width: 760px;
        color: var(--muted);
        font-size: 16px;
        line-height: 1.55;
      }}
      .playbook {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 14px;
        margin-top: 18px;
      }}
      .playbook-card {{
        display: grid;
        gap: 10px;
        padding: 18px;
        border-radius: 22px;
        border: 1px solid var(--line);
        background: linear-gradient(135deg, rgba(255,255,255,0.82), rgba(247,239,227,0.9));
        box-shadow: var(--shadow);
      }}
      .playbook-card h2 {{
        margin: 0;
        font-size: 14px;
        text-transform: uppercase;
        letter-spacing: 0.14em;
      }}
      .playbook-card p {{
        margin: 0;
        color: var(--muted);
        font-size: 14px;
        line-height: 1.55;
      }}
      .playbook-card ol {{
        margin: 0;
        padding-left: 18px;
        color: var(--ink);
        display: grid;
        gap: 6px;
        font-size: 13px;
      }}
      .guide-links {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }}
      .guide-link {{
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 8px 10px;
        border-radius: 999px;
        border: 1px solid var(--line);
        background: rgba(255,255,255,0.72);
        color: var(--ink);
        font-size: 12px;
        text-decoration: none;
      }}
      .command-strip {{
        font-family: "Iosevka", "SFMono-Regular", monospace;
        font-size: 12px;
        padding: 10px 12px;
        border-radius: 14px;
        border: 1px solid var(--line);
        background: rgba(34, 39, 46, 0.92);
        color: #f7f3e8;
        overflow: auto;
      }}
      .grid {{
        display: grid;
        grid-template-columns: 340px minmax(0, 1fr) 320px;
        gap: 18px;
        margin-top: 18px;
      }}
      .panel {{
        border: 1px solid var(--line);
        border-radius: 24px;
        background: var(--panel);
        box-shadow: var(--shadow);
        padding: 18px;
        backdrop-filter: blur(16px);
      }}
      .panel h2 {{
        margin: 0 0 12px;
        font-size: 15px;
        text-transform: uppercase;
        letter-spacing: 0.12em;
      }}
      .status {{
        display: grid;
        gap: 10px;
      }}
      .pill {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 8px 12px;
        border-radius: 999px;
        background: rgba(255,255,255,0.72);
        border: 1px solid var(--line);
        font-size: 14px;
      }}
      .controls {{
        display: grid;
        gap: 10px;
        margin-top: 16px;
      }}
      button {{
        border: none;
        border-radius: 14px;
        padding: 12px 14px;
        font: inherit;
        cursor: pointer;
        color: white;
        background: linear-gradient(135deg, #c4472d, #8f2f1f);
        transition: transform 140ms ease, box-shadow 140ms ease;
        box-shadow: 0 10px 24px rgba(196, 71, 45, 0.28);
      }}
      button.secondary {{
        color: var(--ink);
        background: linear-gradient(135deg, #f4d8c9, #f6f1e6);
      }}
      button:hover {{ transform: translateY(-1px); }}
      button:disabled {{
        opacity: 0.55;
        cursor: not-allowed;
        transform: none;
        box-shadow: none;
      }}
      textarea {{
        width: 100%;
        min-height: 110px;
        resize: vertical;
        border-radius: 18px;
        border: 1px solid var(--line);
        padding: 14px;
        font: inherit;
        background: rgba(255,255,255,0.72);
      }}
      input, select {{
        width: 100%;
        border-radius: 14px;
        border: 1px solid var(--line);
        padding: 11px 12px;
        font: inherit;
        background: rgba(255,255,255,0.72);
        color: var(--ink);
      }}
      label {{
        display: grid;
        gap: 6px;
        font-size: 13px;
        color: var(--muted);
      }}
      .toggle {{
        display: flex;
        align-items: center;
        gap: 10px;
        font-size: 13px;
        color: var(--muted);
      }}
      .toggle input {{
        width: auto;
        margin: 0;
      }}
      .list {{
        display: grid;
        gap: 10px;
      }}
      .run {{
        padding: 14px;
        border-radius: 18px;
        border: 1px solid var(--line);
        background: rgba(255,255,255,0.64);
        cursor: pointer;
      }}
      .run:hover {{
        border-color: rgba(196, 71, 45, 0.42);
      }}
      .run strong {{
        display: block;
        margin-bottom: 6px;
      }}
      pre {{
        margin: 0;
        padding: 16px;
        border-radius: 18px;
        overflow: auto;
        background: rgba(25, 31, 38, 0.94);
        color: #f7f3e8;
        font-family: "Iosevka", "SFMono-Regular", monospace;
        font-size: 12px;
        line-height: 1.5;
      }}
      .meta {{
        color: var(--muted);
        font-size: 13px;
      }}
      .summary-grid {{
        display: grid;
        gap: 10px;
      }}
      .board-grid {{
        display: grid;
        grid-template-columns: minmax(0, 1.45fr) minmax(320px, 0.75fr);
        gap: 18px;
        margin-top: 18px;
      }}
      .section-head {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        margin-bottom: 14px;
      }}
      .section-head h2 {{
        margin: 0;
      }}
      .chip-row {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }}
      .chip {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 6px 10px;
        border-radius: 999px;
        border: 1px solid var(--line);
        background: rgba(255,255,255,0.64);
        font-size: 12px;
        color: var(--muted);
      }}
      .board {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 12px;
      }}
      .column {{
        display: grid;
        gap: 10px;
        align-content: start;
        padding: 12px;
        min-height: 260px;
        border-radius: 20px;
        border: 1px solid var(--line);
        background: rgba(255,255,255,0.42);
      }}
      .column-head {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
        font-size: 12px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--muted);
      }}
      .stack {{
        display: grid;
        gap: 10px;
      }}
      .card {{
        width: 100%;
        text-align: left;
        padding: 12px;
        border-radius: 16px;
        border: 1px solid transparent;
        background: linear-gradient(135deg, rgba(255,255,255,0.88), rgba(245,236,223,0.92));
        color: var(--ink);
        box-shadow: 0 8px 18px rgba(31, 40, 51, 0.08);
      }}
      .card.active {{
        border-color: rgba(196, 71, 45, 0.55);
        box-shadow: 0 14px 28px rgba(196, 71, 45, 0.18);
      }}
      .card strong {{
        display: block;
        margin-bottom: 6px;
      }}
      .compact-controls {{
        display: grid;
        gap: 10px;
        margin: 12px 0;
      }}
      .workflow-box {{
        display: grid;
        gap: 10px;
        margin-top: 16px;
        padding-top: 16px;
        border-top: 1px solid var(--line);
      }}
      .preview-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 12px;
      }}
      .preview-card {{
        display: grid;
        gap: 10px;
        padding: 14px;
        border-radius: 18px;
        border: 1px solid var(--line);
        background: rgba(255,255,255,0.56);
      }}
      .preview-card h3 {{
        margin: 0;
        font-size: 13px;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--muted);
      }}
      .fact-grid {{
        display: grid;
        gap: 8px;
      }}
      .fact {{
        display: flex;
        justify-content: space-between;
        gap: 10px;
        padding-bottom: 8px;
        border-bottom: 1px solid rgba(31, 40, 51, 0.08);
      }}
      .fact:last-child {{
        border-bottom: none;
        padding-bottom: 0;
      }}
      .fact span {{
        font-size: 12px;
        color: var(--muted);
      }}
      .fact strong {{
        text-align: right;
        font-size: 13px;
      }}
      .rule-list {{
        display: grid;
        gap: 10px;
      }}
      .rule {{
        display: grid;
        gap: 5px;
        padding: 10px 12px;
        border-radius: 14px;
        border: 1px solid var(--line);
        background: rgba(255,255,255,0.7);
      }}
      .rule strong {{
        font-size: 13px;
      }}
      .drawer {{
        margin-top: 14px;
        border-top: 1px solid var(--line);
        padding-top: 14px;
      }}
      .drawer summary {{
        cursor: pointer;
        color: var(--muted);
        font-size: 13px;
      }}
      @media (max-width: 920px) {{
        .grid {{ grid-template-columns: 1fr; }}
        .board-grid {{ grid-template-columns: 1fr; }}
      }}
    </style>
  </head>
  <body>
    <div class="shell">
      <section class="hero">
        <div class="eyebrow">ArchonLab Control Deck</div>
        <h1>{title}</h1>
        <div class="subtitle">
          Watch runs, inspect structured artifacts, pause the project, resume it, or inject a hint
          without dropping back to raw files. This dashboard sits directly on top of the existing
          control plane and uses the same event store.
        </div>
      </section>

      <section class="playbook" id="mission-control-guide">
        <article class="playbook-card">
          <h2>Mission Control Guide</h2>
          <p>
            第一次进入时先看 workspace 的 blocked sessions、provider health、daemon 状态，
            再决定是 enqueue/resume、调整 workflow，还是转去 benchmark 回放。
          </p>
          <div class="guide-links">
            <a class="guide-link" href="#workspace-operations-section">Workspace</a>
            <a class="guide-link" href="#queue-operations-section">Queue</a>
            <a class="guide-link" href="#project-preview-section">Preview</a>
            <a class="guide-link" href="#benchmark-lab-section">Benchmark Lab</a>
          </div>
        </article>
        <article class="playbook-card">
          <h2>Bring Up A Workspace</h2>
          <ol>
            <li>先用 doctor 和 workspace status 检查 Lean、provider、配置文件。</li>
            <li>需要持续自治时优先跑 workspace daemon，而不是手工反复触发。</li>
            <li>打开 Workspace Overview，确认 blocked reason、budget、daemon ticks。</li>
          </ol>
          <div class="command-strip">uv run archonlab doctor
uv run archonlab workspace status --config workspace.toml
uv run archonlab workspace daemon run --config workspace.toml</div>
        </article>
        <article class="playbook-card">
          <h2>Operate The Loop</h2>
          <ol>
            <li>Queue Board 看 backlog、worker health、job reason 和 capability 匹配。</li>
            <li>Project Preview 看下一个 focus theorem、workflow rule 和 supervisor reason。</li>
            <li>遇到 blocked session，先看 cooldown / failure budget / control pause。</li>
          </ol>
          <div class="command-strip">uv run archonlab workspace enqueue --config workspace.toml
uv run archonlab workspace resume --config workspace.toml
uv run archonlab queue session-status --config workspace.toml</div>
        </article>
        <article class="playbook-card">
          <h2>Benchmark And Replay</h2>
          <ol>
            <li>Run Index 先选一轮 benchmark，再自动填入 ledger / summary。</li>
            <li>Compare 优先看 theorem-level improved / regressed，而不是只看总分。</li>
            <li>Replay 用来回放单 theorem 的上下文、artifact 和失败模式。</li>
          </ol>
          <div class="command-strip">uv run archonlab benchmark runs
--manifest benchmarks/smoke.example.toml
uv run archonlab benchmark run-detail
--manifest benchmarks/smoke.example.toml --run-id &lt;id&gt;</div>
        </article>
      </section>

      <div class="grid" id="queue-operations-section">
        <aside class="panel">
          <h2>Project Control</h2>
          <div class="status" id="control-status"></div>
          <div class="controls">
            <label>
              Active Project
              <select id="project-selector">
                {project_selector_options}
              </select>
            </label>
            <button id="pause-button">Pause Project</button>
            <button class="secondary" id="resume-button">Resume Project</button>
            <textarea
              id="hint-input"
              placeholder="Write a hint for the next planning/proving cycle."
            ></textarea>
            <button id="hint-button">Inject Hint</button>
          </div>
          <div class="workflow-box">
            <h2>Workflow Override</h2>
            <label>
              Mode Override
              <select id="workflow-mode-select">
                <option value="">Default Config</option>
                <option value="adaptive_loop">adaptive_loop</option>
                <option value="fixed_loop">fixed_loop</option>
              </select>
            </label>
            <label>
              Workflow Spec Override
              <input
                id="workflow-spec-input"
                type="text"
                placeholder="./workflows/review-on-stuck.example.toml"
              />
            </label>
            <label class="toggle">
              <input id="clear-workflow-spec-checkbox" type="checkbox" />
              Ignore configured workflow spec
            </label>
            <div class="controls">
              <button class="secondary" id="workflow-apply-button">Apply Workflow Override</button>
              <button class="secondary" id="workflow-reset-button">Reset Workflow Override</button>
            </div>
          </div>
        </aside>

        <section class="panel">
          <h2>Runs</h2>
          <div class="list" id="runs-list"></div>
          <div style="height: 16px"></div>
          <h2>Run Detail</h2>
          <div class="meta" id="detail-meta">Select a run to inspect its summary and events.</div>
          <div style="height: 10px"></div>
          <pre id="detail-json">{{}}</pre>
        </section>

        <aside class="panel">
          <h2>Queue Ops</h2>
          <div class="controls">
            <button class="secondary" id="queue-run-button">Run Pending Queue</button>
            <button class="secondary" id="queue-fleet-button">Run Auto-Slot Fleet</button>
            <button class="secondary" id="worker-sweep-button">Sweep Stale Workers</button>
          </div>
          <div style="height: 12px"></div>
          <div class="summary-grid" id="queue-summary"></div>
          <div style="height: 16px"></div>
          <div class="section-head">
            <h2>Fleet Plan</h2>
            <div class="meta" id="fleet-plan-meta">
              Recommended dedicated worker pools for the active queue.
            </div>
          </div>
          <div class="summary-grid" id="fleet-plan-summary"></div>
          <div style="height: 12px"></div>
          <div class="rule-list" id="fleet-plan-list"></div>
          <div style="height: 16px"></div>
          <h2>Workers</h2>
          <div class="list" id="workers-list"></div>
        </aside>
      </div>

      <div class="board-grid">
        <section class="panel">
          <div class="section-head">
            <h2>Queue Board</h2>
            <div class="chip-row" id="queue-counts"></div>
          </div>
          <div class="board" id="queue-board"></div>
        </section>

        <aside class="panel">
          <h2>Job Detail</h2>
          <div class="meta" id="queue-detail-meta">
            Select a queue card to inspect and operate on it.
          </div>
          <div class="compact-controls">
            <button class="secondary" id="job-requeue-button" disabled>Requeue Selected Job</button>
            <button class="secondary" id="job-cancel-button" disabled>Cancel Selected Job</button>
          </div>
          <pre id="queue-detail-json">{{}}</pre>
        </aside>
      </div>

      <section class="panel" id="workspace-operations-section" style="margin-top: 18px;">
        <div class="section-head">
          <div>
            <h2>Workspace Overview</h2>
            <div class="meta" id="workspace-overview-meta">
              Aggregate sessions, queue pressure, and worker health across the workspace.
            </div>
            <div class="meta">Tag filter: comma-separated AND match for enqueue/resume.</div>
          </div>
          <div class="compact-controls">
            <input
              id="workspace-tag-input"
              type="text"
              placeholder="geometry,batch"
              aria-label="Workspace tag filter"
            />
            <button class="secondary" id="workspace-enqueue-button">Enqueue Workspace</button>
            <button class="secondary" id="workspace-resume-button">Resume Sessions</button>
          </div>
        </div>
        <div class="preview-grid">
          <section class="preview-card">
            <h3>Overview</h3>
            <div class="fact-grid" id="workspace-overview-summary"></div>
          </section>
          <section class="preview-card">
            <h3>Runtime Budget</h3>
            <div class="fact-grid" id="workspace-runtime-summary"></div>
          </section>
          <section class="preview-card">
            <h3>Latest Loop</h3>
            <div class="fact-grid" id="workspace-latest-loop"></div>
          </section>
          <section class="preview-card">
            <h3>Loop History</h3>
            <div class="rule-list" id="workspace-loop-history"></div>
          </section>
          <section class="preview-card">
            <h3>Fleet History</h3>
            <div class="rule-list" id="workspace-fleet-history"></div>
          </section>
          <section class="preview-card">
            <h3>Daemon</h3>
            <div class="fact-grid" id="workspace-daemon-state"></div>
          </section>
          <section class="preview-card">
            <h3>Sessions</h3>
            <div class="rule-list" id="workspace-session-table"></div>
          </section>
          <section class="preview-card">
            <h3>Provider Runtime</h3>
            <div class="rule-list" id="workspace-provider-runtime"></div>
          </section>
          <section class="preview-card">
            <h3>Provider Health</h3>
            <div class="rule-list" id="workspace-provider-health"></div>
          </section>
          <section class="preview-card">
            <h3>Worker Pool</h3>
            <div class="rule-list" id="workspace-worker-pool"></div>
          </section>
        </div>
      </section>

      <section class="panel" id="project-preview-section" style="margin-top: 18px;">
        <div class="section-head">
          <h2>Current Preview</h2>
          <div class="meta" id="project-preview-meta">
            Inspect the live supervisor/workflow prediction before launching the next run.
          </div>
        </div>
        <div class="chip-row" id="project-preview-chips"></div>
        <div style="height: 12px"></div>
        <div class="preview-grid">
          <section class="preview-card">
            <h3>Overview</h3>
            <div class="fact-grid" id="project-preview-overview"></div>
          </section>
          <section class="preview-card">
            <h3>Focus Task</h3>
            <div class="fact-grid" id="project-preview-focus"></div>
          </section>
          <section class="preview-card">
            <h3>Supervisor</h3>
            <div class="fact-grid" id="project-preview-supervisor"></div>
          </section>
          <section class="preview-card">
            <h3>Lean Analysis</h3>
            <div class="fact-grid" id="project-preview-analysis"></div>
          </section>
          <section class="preview-card">
            <h3>Latest Run Loop</h3>
            <div class="fact-grid" id="project-latest-run-loop"></div>
          </section>
          <section class="preview-card">
            <h3>Run Loop History</h3>
            <div class="rule-list" id="project-run-loop-history"></div>
          </section>
          <section class="preview-card">
            <h3>Workflow Rules</h3>
            <div class="rule-list" id="project-preview-rules"></div>
          </section>
          <section class="preview-card">
            <h3>Task Graph</h3>
            <div class="rule-list" id="project-preview-graph"></div>
          </section>
        </div>
        <details class="drawer">
          <summary>Raw Preview JSON</summary>
          <div style="height: 10px"></div>
          <pre id="project-preview-json">{{}}</pre>
        </details>
      </section>

      <section class="panel" id="benchmark-lab-section" style="margin-top: 18px;">
        <div class="section-head">
          <div>
            <h2>Benchmark Lab</h2>
            <div class="meta">
              Browse recorded benchmark runs, then load ledgers, compare theorem-level changes,
              and replay specific outcomes.
            </div>
          </div>
        </div>
        <div class="preview-grid">
          <section class="preview-card">
            <h3>Run Index</h3>
            <label>
              Manifest Path
              <input
                id="benchmark-manifest-input"
                type="text"
                placeholder="dashboard-benchmark.toml"
              />
            </label>
            <label>
              Artifact Root
              <input
                id="benchmark-artifact-root-input"
                type="text"
                placeholder="./benchmark-artifacts"
              />
            </label>
            <div class="compact-controls">
              <button class="secondary" id="benchmark-runs-button">Load Runs</button>
            </div>
            <div class="rule-list" id="benchmark-runs-list"></div>
          </section>
          <section class="preview-card">
            <h3>Run Detail</h3>
            <div class="rule-list" id="benchmark-run-detail"></div>
          </section>
          <section class="preview-card">
            <h3>Ledger</h3>
            <label>
              Summary Path
              <input
                id="benchmark-summary-input"
                type="text"
                placeholder="benchmark-summary.json"
              />
            </label>
            <label>
              Ledger Path
              <input
                id="benchmark-ledger-input"
                type="text"
                placeholder="experiment-ledger.json"
              />
            </label>
            <div class="compact-controls">
              <button class="secondary" id="benchmark-ledger-button">Load Ledger</button>
            </div>
            <div class="fact-grid" id="benchmark-ledger-summary"></div>
          </section>
          <section class="preview-card">
            <h3>Compare</h3>
            <label>
              Baseline Ledger
              <input
                id="benchmark-baseline-ledger-input"
                type="text"
                placeholder="baseline-ledger.json"
              />
            </label>
            <label>
              Candidate Ledger
              <input
                id="benchmark-candidate-ledger-input"
                type="text"
                placeholder="candidate-ledger.json"
              />
            </label>
            <div class="compact-controls">
              <button class="secondary" id="benchmark-compare-button">Compare</button>
            </div>
            <div class="rule-list" id="benchmark-compare-summary"></div>
          </section>
          <section class="preview-card">
            <h3>Replay</h3>
            <label>
              Ledger Path
              <input
                id="benchmark-replay-ledger-input"
                type="text"
                placeholder="candidate-ledger.json"
              />
            </label>
            <label>
              Project ID
              <input id="benchmark-project-input" type="text" placeholder="demo" />
            </label>
            <label>
              Theorem Name
              <input id="benchmark-theorem-input" type="text" placeholder="foo" />
            </label>
            <div class="compact-controls">
              <button class="secondary" id="benchmark-replay-button">Replay</button>
            </div>
            <div class="rule-list" id="benchmark-replay-detail"></div>
          </section>
        </div>
      </section>
    </div>

    <script>
      const defaultProjectId = {json.dumps(default_project_id)};
      let currentProjectId = defaultProjectId;
      const projectSelector = document.getElementById("project-selector");
      const controlStatus = document.getElementById("control-status");
      const runsList = document.getElementById("runs-list");
      const detailMeta = document.getElementById("detail-meta");
      const detailJson = document.getElementById("detail-json");
      const hintInput = document.getElementById("hint-input");
      const workflowModeSelect = document.getElementById("workflow-mode-select");
      const workflowSpecInput = document.getElementById("workflow-spec-input");
      const clearWorkflowSpecCheckbox = document.getElementById("clear-workflow-spec-checkbox");
      const workflowApplyButton = document.getElementById("workflow-apply-button");
      const workflowResetButton = document.getElementById("workflow-reset-button");
      const queueSummary = document.getElementById("queue-summary");
      const queueCounts = document.getElementById("queue-counts");
      const queueBoard = document.getElementById("queue-board");
      const queueDetailMeta = document.getElementById("queue-detail-meta");
      const queueDetailJson = document.getElementById("queue-detail-json");
      const projectPreviewMeta = document.getElementById("project-preview-meta");
      const projectPreviewChips = document.getElementById("project-preview-chips");
      const projectPreviewOverview = document.getElementById("project-preview-overview");
      const projectPreviewFocus = document.getElementById("project-preview-focus");
      const projectPreviewSupervisor = document.getElementById("project-preview-supervisor");
      const projectPreviewAnalysis = document.getElementById("project-preview-analysis");
      const projectLatestRunLoop = document.getElementById("project-latest-run-loop");
      const projectRunLoopHistory = document.getElementById("project-run-loop-history");
      const projectPreviewRules = document.getElementById("project-preview-rules");
      const projectPreviewGraph = document.getElementById("project-preview-graph");
      const projectPreviewJson = document.getElementById("project-preview-json");
      const jobRequeueButton = document.getElementById("job-requeue-button");
      const jobCancelButton = document.getElementById("job-cancel-button");
      const fleetPlanMeta = document.getElementById("fleet-plan-meta");
      const fleetPlanSummary = document.getElementById("fleet-plan-summary");
      const fleetPlanList = document.getElementById("fleet-plan-list");
      const workersList = document.getElementById("workers-list");
      const workspaceOverviewMeta = document.getElementById("workspace-overview-meta");
      const workspaceOverviewSummary = document.getElementById("workspace-overview-summary");
      const workspaceRuntimeSummary = document.getElementById("workspace-runtime-summary");
      const workspaceLatestLoop = document.getElementById("workspace-latest-loop");
      const workspaceLoopHistory = document.getElementById("workspace-loop-history");
      const workspaceFleetHistory = document.getElementById("workspace-fleet-history");
      const workspaceDaemonState = document.getElementById("workspace-daemon-state");
      const workspaceSessionTable = document.getElementById("workspace-session-table");
      const workspaceProviderRuntime = document.getElementById("workspace-provider-runtime");
      const workspaceProviderHealth = document.getElementById("workspace-provider-health");
      const workspaceWorkerPool = document.getElementById("workspace-worker-pool");
      const workspaceTagInput = document.getElementById("workspace-tag-input");
      const workspaceEnqueueButton = document.getElementById("workspace-enqueue-button");
      const workspaceResumeButton = document.getElementById("workspace-resume-button");
      const benchmarkManifestInput = document.getElementById("benchmark-manifest-input");
      const benchmarkArtifactRootInput = document.getElementById("benchmark-artifact-root-input");
      const benchmarkRunsButton = document.getElementById("benchmark-runs-button");
      const benchmarkRunsList = document.getElementById("benchmark-runs-list");
      const benchmarkRunDetail = document.getElementById("benchmark-run-detail");
      const benchmarkSummaryInput = document.getElementById("benchmark-summary-input");
      const benchmarkLedgerInput = document.getElementById("benchmark-ledger-input");
      const benchmarkLedgerButton = document.getElementById("benchmark-ledger-button");
      const benchmarkLedgerSummary = document.getElementById("benchmark-ledger-summary");
      const benchmarkBaselineLedgerInput = document.getElementById(
        "benchmark-baseline-ledger-input",
      );
      const benchmarkCandidateLedgerInput = document.getElementById(
        "benchmark-candidate-ledger-input",
      );
      const benchmarkCompareButton = document.getElementById("benchmark-compare-button");
      const benchmarkCompareSummary = document.getElementById("benchmark-compare-summary");
      const benchmarkReplayLedgerInput = document.getElementById("benchmark-replay-ledger-input");
      const benchmarkProjectInput = document.getElementById("benchmark-project-input");
      const benchmarkTheoremInput = document.getElementById("benchmark-theorem-input");
      const benchmarkReplayButton = document.getElementById("benchmark-replay-button");
      const benchmarkReplayDetail = document.getElementById("benchmark-replay-detail");
      let latestJobs = [];
      let selectedQueueJobId = null;

      async function fetchJson(url, options) {{
        const response = await fetch(url, options);
        if (!response.ok) {{
          const detail = await response.text();
          throw new Error(detail || response.statusText);
        }}
        return response.json();
      }}

      function renderControl(state) {{
        const hints = state.hints || [];
        const latestHint = hints.length ? hints[hints.length - 1].text : "No hints yet.";
        const workflowMode = state.workflow_override || "default";
        const workflowSpec = state.workflow_spec_override || "-";
        const clearSpec = state.clear_workflow_spec ? "yes" : "no";
        controlStatus.innerHTML = `
          <div class="pill">Paused: <strong>${{state.paused ? "yes" : "no"}}</strong></div>
          <div class="pill">Hints: <strong>${{hints.length}}</strong></div>
          <div class="pill">Reason: <strong>${{state.pause_reason || "none"}}</strong></div>
          <div class="pill">Workflow: <strong>${{workflowMode}}</strong></div>
          <div class="pill">
            Spec: <strong>${{clearSpec === "yes" ? "disabled" : workflowSpec}}</strong>
          </div>
          <div class="meta">Latest hint: ${{latestHint}}</div>
        `;
        workflowModeSelect.value = state.workflow_override || "";
        workflowSpecInput.value = state.workflow_spec_override || "";
        clearWorkflowSpecCheckbox.checked = Boolean(state.clear_workflow_spec);
      }}

      function renderRuns(runs) {{
        if (!runs.length) {{
          runsList.innerHTML = '<div class="meta">No runs recorded yet.</div>';
          return;
        }}
        runsList.innerHTML = "";
        for (const run of runs) {{
          const item = document.createElement("button");
          item.className = "run";
          item.innerHTML = `
            <strong>${{run.run_id}}</strong>
            <div class="meta">${{run.status}} · ${{run.workflow}} · stage=${{run.stage}}</div>
          `;
          item.addEventListener("click", async () => {{
            const detail = await fetchJson(`/api/runs/${{run.run_id}}`);
            detailMeta.textContent = `${{detail.run.run_id}} · ${{detail.events.length}} events`;
            detailJson.textContent = JSON.stringify(detail, null, 2);
          }});
          runsList.appendChild(item);
        }}
      }}

      function queueBuckets() {{
        return [
          {{ key: "queued", label: "Queued", statuses: ["queued", "pending"] }},
          {{ key: "running", label: "Running", statuses: ["running"] }},
          {{ key: "paused", label: "Paused", statuses: ["paused"] }},
          {{ key: "failed", label: "Failed", statuses: ["failed"] }},
          {{ key: "done", label: "Done", statuses: ["completed", "canceled"] }},
        ];
      }}

      function queueCountsByStatus(jobs) {{
        const counts = {{}};
        for (const job of jobs) {{
          counts[job.status] = (counts[job.status] || 0) + 1;
        }}
        return counts;
      }}

      function summarizeQueueJob(job) {{
        const preview = job.preview || {{}};
        const focus = preview.theorem_name || preview.task_title || preview.task_id || "-";
        const phase = preview.phase || "-";
        const stage = preview.stage || "-";
        const reason = preview.reason || "-";
        const priority = preview.final_priority ?? job.priority;
        const workerId = job.worker_id || "-";
        return {{
          focus,
          phase,
          stage,
          reason,
          priority,
          workerId,
          executors: (job.required_executor_kinds || []).join(",") || "-",
          providers: (job.required_provider_kinds || []).join(",") || "-",
          models: (job.required_models || []).join(",") || "-",
          costTiers: (job.required_cost_tiers || []).join(",") || "-",
          endpointClasses: (job.required_endpoint_classes || []).join(",") || "-",
        }};
      }}

      function renderQueueSummary(jobs) {{
        const counts = queueCountsByStatus(jobs);
        queueSummary.innerHTML = `
          <div class="pill">Total jobs: <strong>${{jobs.length}}</strong></div>
          <div class="pill">
            Queued: <strong>${{(counts.queued || 0) + (counts.pending || 0)}}</strong>
          </div>
          <div class="pill">Running: <strong>${{counts.running || 0}}</strong></div>
          <div class="pill">
            Blocked: <strong>${{(counts.paused || 0) + (counts.failed || 0)}}</strong>
          </div>
        `;
        const bucketCounts = queueBuckets().map((bucket) => {{
          const total = bucket.statuses.reduce((sum, status) => sum + (counts[status] || 0), 0);
          return `<div class="chip">${{bucket.label}} <strong>${{total}}</strong></div>`;
        }});
        queueCounts.innerHTML = bucketCounts.join("");
      }}

      function renderQueueBoard(jobs) {{
        if (!jobs.length) {{
          queueBoard.innerHTML = '<div class="meta">No queue jobs.</div>';
          return;
        }}
        queueBoard.innerHTML = "";
        for (const bucket of queueBuckets()) {{
          const column = document.createElement("section");
          column.className = "column";
          const cards = jobs.filter((job) => bucket.statuses.includes(job.status));
          column.innerHTML = `
            <div class="column-head">
              <span>${{bucket.label}}</span>
              <strong>${{cards.length}}</strong>
            </div>
            <div class="stack"></div>
          `;
          const stack = column.querySelector(".stack");
          if (!cards.length) {{
            stack.innerHTML = '<div class="meta">No jobs.</div>';
          }} else {{
            for (const job of cards) {{
              const summary = summarizeQueueJob(job);
              const item = document.createElement("button");
              item.className = selectedQueueJobId === job.job_id ? "card active" : "card";
              item.innerHTML = `
                <strong>${{job.project_id}}</strong>
                <div class="meta">${{job.job_id}}</div>
                <div class="meta">
                  ${{summary.phase}} · ${{summary.stage}} · p=${{summary.priority}}
                </div>
                <div class="meta">${{summary.focus}}</div>
                <div class="meta">worker=${{summary.workerId}}</div>
              `;
              item.addEventListener("click", async () => {{
                await selectQueueJob(job.job_id);
              }});
              stack.appendChild(item);
            }}
          }}
          queueBoard.appendChild(column);
        }}
      }}

      function renderQueueDetail(job) {{
        if (!job) {{
          queueDetailMeta.textContent = "Select a queue card to inspect and operate on it.";
          queueDetailJson.textContent = JSON.stringify({{}}, null, 2);
          jobRequeueButton.disabled = true;
          jobCancelButton.disabled = true;
          return;
        }}
        const summary = summarizeQueueJob(job);
        queueDetailMeta.textContent =
          `${{job.job_id}} · ${{job.status}} · phase=${{summary.phase}} · focus=${{summary.focus}}`;
        queueDetailJson.textContent = JSON.stringify(job, null, 2);
        jobRequeueButton.disabled = ["queued", "pending", "running"].includes(job.status);
        jobCancelButton.disabled = ["completed", "canceled"].includes(job.status);
      }}

      async function selectQueueJob(jobId) {{
        selectedQueueJobId = jobId;
        const detail = await fetchJson(`/api/queue/jobs/${{jobId}}`);
        latestJobs = latestJobs.map((job) => (job.job_id === jobId ? detail : job));
        renderQueue(latestJobs);
      }}

      function renderQueue(jobs) {{
        latestJobs = jobs;
        if (!selectedQueueJobId || !jobs.some((job) => job.job_id === selectedQueueJobId)) {{
          selectedQueueJobId = jobs.length ? jobs[0].job_id : null;
        }}
        renderQueueSummary(jobs);
        renderQueueBoard(jobs);
        renderQueueDetail(
          jobs.find((job) => job.job_id === selectedQueueJobId) || null,
        );
      }}

      function renderFleetPlan(plan) {{
        const profiles = plan.profiles || [];
        fleetPlanMeta.textContent =
          `Target ${{plan.target_jobs_per_worker}} jobs/worker · ${{plan.total_profiles}} profiles`;
        fleetPlanSummary.innerHTML = `
          <div class="pill">Active jobs: <strong>${{plan.active_jobs || 0}}</strong></div>
          <div class="pill">Active workers: <strong>${{plan.active_workers || 0}}</strong></div>
          <div class="pill">Dedicated: <strong>${{plan.dedicated_workers || 0}}</strong></div>
          <div class="pill">Generic: <strong>${{plan.generic_workers || 0}}</strong></div>
          <div class="pill">
            Recommended: <strong>${{plan.recommended_total_workers || 0}}</strong>
          </div>
          <div class="pill">
            Add: <strong>${{plan.recommended_additional_workers || 0}}</strong>
          </div>
        `;
        if (!profiles.length) {{
          fleetPlanList.innerHTML = '<div class="meta">No active queue demand.</div>';
          return;
        }}
        fleetPlanList.innerHTML = profiles.map((profile) => {{
          const dominantPhase = profile.dominant_phase || "-";
          const executors = (profile.required_executor_kinds || []).join(",") || "any";
          const providers = (profile.required_provider_kinds || []).join(",") || "any";
          const models = (profile.required_models || []).join(",") || "any";
          const costTiers = (profile.required_cost_tiers || []).join(",") || "any";
          const endpoints = (profile.required_endpoint_classes || []).join(",") || "any";
          const phases = Object.entries(profile.phase_counts || {{}})
            .map(([phase, count]) => `${{phase}}:${{count}}`)
            .join(" · ") || "-";
          const stages = Object.entries(profile.stage_counts || {{}})
            .map(([stage, count]) => `${{stage}}:${{count}}`)
            .join(" · ") || "-";
          const projects = (profile.project_ids || []).join(", ") || "-";
          const focus = (profile.focus_examples || []).join(", ") || "-";
          const providerCapacityStatus = profile.provider_capacity_status || "unknown";
          const availableProviderMembers = profile.available_provider_members ?? "-";
          return `
            <div class="rule">
              <strong>${{dominantPhase}} · model=${{models}} · cost=${{costTiers}}</strong>
              <div class="meta">
                jobs=${{profile.active_jobs}} · queued=${{profile.queued_jobs}}
                · running=${{profile.running_jobs}}
              </div>
              <div class="meta">
                dedicated=${{profile.dedicated_workers}}
                · matching=${{profile.matching_workers || 0}}
                · recommend=${{profile.recommended_total_workers}} total
                / +${{profile.recommended_additional_workers}}
              </div>
              <div class="meta">
                provider_capacity=${{providerCapacityStatus}}
                · available_members=${{availableProviderMembers}}
              </div>
              <div class="meta">
                executor=${{executors}} · provider=${{providers}} · endpoint=${{endpoints}}
              </div>
              <div class="meta">projects=${{projects}}</div>
              <div class="meta">focus=${{focus}}</div>
              <div class="meta">phases=${{phases}} · stages=${{stages}}</div>
            </div>
          `;
        }}).join("");
      }}

      function renderFleetPlanError(message) {{
        fleetPlanMeta.textContent = "Fleet plan unavailable.";
        fleetPlanSummary.innerHTML =
          '<div class="pill">error <strong>fleet_plan_failed</strong></div>';
        fleetPlanList.innerHTML = `<div class="meta">${{message}}</div>`;
      }}

      function renderProjectPreview(payload) {{
        const preview = payload.preview || {{}};
        const action = preview.action || {{}};
        const supervisor = preview.supervisor || {{}};
        const executor = preview.resolved_executor || {{}};
        const provider = preview.resolved_provider || {{}};
        const analysis = payload.analysis_summary || {{}};
        const graph = payload.task_graph_summary || {{}};
        const workflowSpec = payload.workflow_spec;
        const workflowRules = payload.workflow_rules || [];
        const focusTask = payload.focus_task || null;
        const evidence = payload.supervisor_evidence || {{}};
        const taskGraph = preview.task_graph || {{}};
        const taskNodes = taskGraph.nodes || [];
        const blockedNodes = taskNodes.filter((node) => node.status === "blocked").slice(0, 4);
        const pendingNodes = taskNodes.filter((node) => node.status === "pending").slice(0, 4);
        const blockedTitles = blockedNodes.map((node) => node.title || node.id).join(", ");
        const pendingTitles = pendingNodes.map((node) => node.title || node.id).join(", ");
        const chips = [
          ["workflow", payload.workflow || "-"],
          ["configured", payload.configured_workflow || "-"],
          ["phase", action.phase || "-"],
          ["reason", action.reason || "-"],
          ["stage", action.stage || preview.progress?.stage || "-"],
          ["supervisor", `${{supervisor.action || "-"}}/${{supervisor.reason || "-"}}`],
          ["executor", executor.kind || "-"],
          ["model", provider.model || "-"],
          ["cost_tier", provider.cost_tier || "-"],
          [
            "task_graph",
            `${{graph.total_nodes || 0}} nodes / ${{graph.blocked_nodes || 0}} blocked`,
          ],
        ];
        if (workflowSpec) {{
          chips.push([
            "workflow_spec",
            `${{workflowSpec.name}} (${{workflowSpec.rule_count}} rules)`,
          ]);
        }}
        if (payload.workflow_spec_path) {{
          chips.push(["spec_path", payload.workflow_spec_path]);
        }}
        projectPreviewMeta.textContent =
          `${{payload.project_id}} · ${{action.phase || "-"}} · ${{action.reason || "-"}}`;
        projectPreviewChips.innerHTML = chips
          .map(([label, value]) => `<div class="chip">${{label}} <strong>${{value}}</strong></div>`)
          .join("");
        projectPreviewOverview.innerHTML = renderFacts([
          ["phase", action.phase || "-"],
          ["reason", action.reason || "-"],
          ["stage", action.stage || "-"],
          ["workflow", payload.workflow || "-"],
          ["spec", payload.workflow_spec_path || "-"],
          ["spec_name", workflowSpec?.name || "-"],
          ["executor", executor.kind || "-"],
          ["provider", provider.kind || "-"],
          ["model", provider.model || "-"],
          ["endpoint", provider.endpoint_class || "-"],
          [
            "task_graph",
            `${{graph.total_nodes || 0}} total / ${{graph.blocked_nodes || 0}} blocked`,
          ],
        ]);
        projectPreviewFocus.innerHTML = focusTask
          ? renderFacts([
              ["task_id", focusTask.task_id || "-"],
              ["title", focusTask.title || "-"],
              ["theorem", focusTask.theorem_name || "-"],
              ["file", focusTask.file_path || "-"],
              ["status", focusTask.task_status || "-"],
              ["priority", `${{focusTask.task_priority ?? "-"}}`],
              ["objective", focusTask.objective_relevant ? "yes" : "no"],
              ["sources", (focusTask.task_sources || []).join(", ") || "-"],
              ["blockers", (focusTask.task_blockers || []).join(", ") || "-"],
            ])
          : '<div class="meta">No focused task for the current prediction.</div>';
        const evidenceEntries = Object.entries(evidence);
        projectPreviewSupervisor.innerHTML = [
          renderFacts([
            ["action", supervisor.action || "-"],
            ["reason", supervisor.reason || "-"],
            ["summary", supervisor.summary || "-"],
          ]),
          evidenceEntries.length
            ? evidenceEntries
                .map(
                  ([key, value]) => `
                    <div class="rule">
                      <strong>${{key}}</strong>
                      <div class="meta">${{value}}</div>
                    </div>
                  `,
                )
                .join("")
            : '<div class="meta">No supervisor evidence.</div>',
        ].join("");
        projectPreviewAnalysis.innerHTML = renderFacts([
          ["backend", analysis.backend || "-"],
          ["fallback", analysis.fallback_used ? "yes" : "no"],
          ["theorems", `${{analysis.theorem_count || 0}}`],
          ["declarations", `${{analysis.declaration_count || 0}}`],
          ["proof_gaps", `${{analysis.proof_gap_count || 0}}`],
          ["diagnostics", `${{analysis.diagnostic_count || 0}}`],
          ["top_gap", (analysis.top_proof_gaps || []).join(", ") || "-"],
          ["top_diag", (analysis.top_diagnostics || []).join(", ") || "-"],
        ]);
        projectPreviewRules.innerHTML = workflowRules.length
          ? [
              workflowSpec?.description
                ? `
                    <div class="rule">
                      <strong>${{workflowSpec.name}}</strong>
                      <div class="meta">${{workflowSpec.description}}</div>
                    </div>
                  `
                : "",
              ...workflowRules.map((rule) => `
                <div class="rule">
                  <strong>${{rule.name}}</strong>
                  <div class="meta">phase=${{rule.phase}} · reason=${{rule.reason}}</div>
                  <div class="meta">${{(rule.conditions || []).join(" · ") || "always"}}</div>
                </div>
              `),
            ].join("")
          : (
              workflowSpec
                ? '<div class="meta">Workflow spec loaded, but no rules are defined.</div>'
                : '<div class="meta">No workflow spec override is active.</div>'
            );
        projectPreviewGraph.innerHTML = [
          renderFacts([
            ["total", `${{graph.total_nodes || 0}}`],
            ["blocked", `${{graph.blocked_nodes || 0}}`],
            ["pending", `${{graph.pending_nodes || 0}}`],
            ["completed", `${{graph.completed_nodes || 0}}`],
          ]),
          blockedNodes.length
            ? `
                <div class="rule">
                  <strong>Blocked Nodes</strong>
                  <div class="meta">${{blockedTitles}}</div>
                </div>
              `
            : '<div class="meta">No blocked nodes.</div>',
          pendingNodes.length
            ? `
                <div class="rule">
                  <strong>Pending Nodes</strong>
                  <div class="meta">${{pendingTitles}}</div>
                </div>
              `
            : '<div class="meta">No pending nodes.</div>',
        ].join("");
        projectPreviewJson.textContent = JSON.stringify(payload, null, 2);
      }}

      function renderProjectPreviewError(message) {{
        projectPreviewMeta.textContent = "Current preview is unavailable.";
        projectPreviewChips.innerHTML =
          '<div class="chip">error <strong>preview_failed</strong></div>';
        projectPreviewOverview.innerHTML = '<div class="meta">Preview unavailable.</div>';
        projectPreviewFocus.innerHTML = '<div class="meta">Preview unavailable.</div>';
        projectPreviewSupervisor.innerHTML = '<div class="meta">Preview unavailable.</div>';
        projectPreviewAnalysis.innerHTML = '<div class="meta">Preview unavailable.</div>';
        projectPreviewRules.innerHTML = '<div class="meta">Preview unavailable.</div>';
        projectPreviewGraph.innerHTML = '<div class="meta">Preview unavailable.</div>';
        projectPreviewJson.textContent = message;
      }}

      function renderProjectRunLoops(payload) {{
        const loops = payload.loops || [];
        const latestLoop = loops.length ? loops[0] : null;
        projectLatestRunLoop.innerHTML = latestLoop
          ? renderFacts([
              ["id", latestLoop.loop_run_id || "-"],
              ["stop", latestLoop.stop_reason || "-"],
              ["status", latestLoop.status || "-"],
              [
                "iterations",
                `${{latestLoop.completed_iterations || 0}} / ${{latestLoop.max_iterations || 0}}`,
              ],
              ["runs", `${{(latestLoop.run_ids || []).length}}`],
              ["note", latestLoop.note || "-"],
            ])
          : '<div class="meta">No project run loops yet.</div>';
        projectRunLoopHistory.innerHTML = loops.length
          ? loops.slice(0, 8).map((loop) => `
              <div class="rule">
                <strong>${{loop.loop_run_id || "-"}}</strong>
                <div class="meta">
                  stop=${{loop.stop_reason || "-"}} · status=${{loop.status || "-"}}
                </div>
                <div class="meta">
                  iterations=${{loop.completed_iterations || 0}}/${{loop.max_iterations || 0}}
                  · runs=${{(loop.run_ids || []).length}}
                </div>
                <div class="meta">note=${{loop.note || "-"}}</div>
              </div>
            `).join("")
          : '<div class="meta">No project run loop history yet.</div>';
      }}

      function renderProjectRunLoopsError(message) {{
        projectLatestRunLoop.innerHTML = '<div class="meta">Project run loops unavailable.</div>';
        projectRunLoopHistory.innerHTML = `<div class="meta">${{message}}</div>`;
      }}

      function benchmarkSourcePayload() {{
        return {{
          manifest_path: trimOrNull(benchmarkManifestInput.value),
          artifact_root: trimOrNull(benchmarkArtifactRootInput.value),
        }};
      }}

      async function loadBenchmarkRunDetail(runId) {{
        const payload = await fetchJson(`/api/benchmark/run-detail`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            ...benchmarkSourcePayload(),
            run_id: runId,
          }}),
        }});
        renderBenchmarkRunDetail(payload);
        benchmarkSummaryInput.value = payload.summary_path || "";
        benchmarkLedgerInput.value = payload.ledger_path || "";
        benchmarkReplayLedgerInput.value = payload.ledger_path || "";
        if (payload.projects && payload.projects.length) {{
          benchmarkProjectInput.value = payload.projects[0].id || benchmarkProjectInput.value;
        }}
        return payload;
      }}

      function renderBenchmarkRuns(runs) {{
        if (!runs.length) {{
          benchmarkRunsList.innerHTML = '<div class="meta">No benchmark runs recorded.</div>';
          benchmarkRunDetail.innerHTML = '<div class="meta">No benchmark run selected.</div>';
          return;
        }}
        benchmarkRunsList.innerHTML = "";
        for (const run of runs) {{
          const item = document.createElement("button");
          item.className = "run";
          item.innerHTML = `
            <strong>${{run.run_id}}</strong>
            <div class="meta">
              ${{run.benchmark?.name || "-"}} · ${{run.status || "-"}}
            </div>
            <div class="meta">${{run.started_at || "-"}}</div>
          `;
          item.addEventListener("click", async () => {{
            try {{
              await loadBenchmarkRunDetail(run.run_id);
            }} catch (error) {{
              renderBenchmarkError(benchmarkRunDetail, error.message);
            }}
          }});
          benchmarkRunsList.appendChild(item);
        }}
      }}

      function renderBenchmarkRunDetail(payload) {{
        const projects = payload.projects || [];
        benchmarkRunDetail.innerHTML = [
          renderFacts([
            ["benchmark", payload.benchmark?.name || "-"],
            ["run", payload.run_id || "-"],
            ["status", payload.status || "-"],
            ["projects", `${{projects.length}}`],
            ["summary", payload.summary_path || "-"],
            ["ledger", payload.ledger_path || "-"],
          ]),
          projects.length
            ? projects.slice(0, 6).map((project) => `
                <div class="rule">
                  <strong>${{project.id}}</strong>
                  <div class="meta">
                    ${{project.run_status || "-"}} · workflow=${{project.workflow || "-"}}
                  </div>
                  <div class="meta">${{project.artifact_dir || "-"}}</div>
                </div>
              `).join("")
            : '<div class="meta">No benchmark projects recorded.</div>',
        ].join("");
      }}

      function renderBenchmarkLedger(payload) {{
        const summary = payload.summary || {{}};
        benchmarkLedgerSummary.innerHTML = renderFacts([
          ["benchmark", payload.benchmark_name || "-"],
          ["run", payload.benchmark_run_id || "-"],
          ["projects", `${{summary.total_projects || 0}}`],
          ["theorems", `${{summary.total_theorems || 0}}`],
          ["improved", `${{summary.improved || 0}}`],
          ["regressed", `${{summary.regressed || 0}}`],
          ["new", `${{summary.new || 0}}`],
          ["removed", `${{summary.removed || 0}}`],
        ]);
      }}

      function renderBenchmarkCompare(payload) {{
        const summary = payload.summary || {{}};
        const changes = payload.changes || [];
        benchmarkCompareSummary.innerHTML = [
          renderFacts([
            ["baseline", payload.baseline_benchmark || "-"],
            ["candidate", payload.candidate_benchmark || "-"],
            ["theorems", `${{summary.total_theorems || 0}}`],
            ["improved", `${{summary.improved || 0}}`],
            ["regressed", `${{summary.regressed || 0}}`],
            ["new", `${{summary.new || 0}}`],
            ["removed", `${{summary.removed || 0}}`],
          ]),
          changes.length
            ? changes.slice(0, 6).map((change) => `
                <div class="rule">
                  <strong>${{change.project_id}} · ${{change.theorem_name}}</strong>
                  <div class="meta">
                    ${{change.baseline_state}} -> ${{change.candidate_state}}
                    · ${{change.change}}
                  </div>
                  <div class="meta">${{change.file_path || "-"}}</div>
                </div>
              `).join("")
            : '<div class="meta">No theorem-level changes.</div>',
        ].join("");
      }}

      function renderBenchmarkReplay(payload) {{
        const theoremOutcomes = payload.theorem_outcomes || [];
        const taxonomy = payload.failure_taxonomy || [];
        benchmarkReplayDetail.innerHTML = [
          renderFacts([
            ["benchmark", payload.benchmark_name || "-"],
            ["project", payload.project_id || "-"],
            ["run", payload.run_id || "-"],
            ["status", payload.run_status || "-"],
            ["outcomes", `${{theoremOutcomes.length}}`],
            ["artifact", payload.artifact_dir || "-"],
          ]),
          theoremOutcomes.length
            ? theoremOutcomes.slice(0, 6).map((outcome) => `
                <div class="rule">
                  <strong>${{outcome.theorem_name}}</strong>
                  <div class="meta">
                    ${{outcome.before_state}} -> ${{outcome.after_state}}
                    · ${{outcome.outcome}}
                  </div>
                  <div class="meta">${{outcome.file_path || "-"}}</div>
                </div>
              `).join("")
            : '<div class="meta">No theorem outcomes matched.</div>',
          taxonomy.length
            ? `
                <div class="rule">
                  <strong>Failure Taxonomy</strong>
                  <div class="meta">
                    ${{
                      taxonomy
                        .map((entry) => `${{entry.category}}:${{entry.count}}`)
                        .join(" · ")
                    }}
                  </div>
                </div>
              `
            : '<div class="meta">No failure taxonomy entries.</div>',
        ].join("");
      }}

      function renderBenchmarkError(target, message) {{
        target.innerHTML = `<div class="meta">${{message}}</div>`;
      }}

      function renderFacts(entries) {{
        return entries.map(([label, value]) => `
          <div class="fact">
            <span>${{label}}</span>
            <strong>${{value}}</strong>
          </div>
        `).join("");
      }}

      function formatCost(value) {{
        if (value == null || Number.isNaN(Number(value))) {{
          return "-";
        }}
        return Number(value).toFixed(3);
      }}

      function formatBlockReason(reason) {{
        if (!reason) {{
          return "-";
        }}
        return String(reason).split("_").join(" ");
      }}

      function formatBlockReasonCounts(counts) {{
        const entries = Object.entries(counts || {{}})
          .filter(([, count]) => Number(count) > 0);
        if (!entries.length) {{
          return "-";
        }}
        return entries
          .map(([reason, count]) => `${{formatBlockReason(reason)}}=${{count}}`)
          .join(" · ");
      }}

      function formatCooldown(seconds) {{
        const resolvedSeconds = Number(seconds || 0);
        if (!resolvedSeconds) {{
          return "-";
        }}
        return `${{resolvedSeconds}}s`;
      }}

      function trimOrNull(value) {{
        const trimmed = (value || "").trim();
        return trimmed || null;
      }}

      function parseWorkspaceTags() {{
        return (workspaceTagInput.value || "")
          .split(",")
          .map((value) => value.trim())
          .filter(Boolean);
      }}

      function renderWorkers(workers) {{
        if (!workers.length) {{
          workersList.innerHTML = '<div class="meta">No worker telemetry yet.</div>';
          return;
        }}
        workersList.innerHTML = "";
        for (const worker of workers) {{
          const item = document.createElement("div");
          item.className = "run";
          const currentJob = worker.current_job_id || "-";
          const heartbeatAge = worker.heartbeat_age_seconds == null
            ? "-"
            : `${{worker.heartbeat_age_seconds.toFixed(1)}}s`;
          const stale = worker.stale ? " · stale" : "";
          const executors = (worker.executor_kinds || []).join(",") || "-";
          const models = (worker.models || []).join(",") || "-";
          const costTiers = (worker.cost_tiers || []).join(",") || "-";
          item.innerHTML = `
            <strong>${{worker.worker_id}}</strong>
            <div class="meta">slot=${{worker.slot_index}} · ${{worker.status}}${{stale}}</div>
            <div class="meta">current=${{currentJob}}</div>
            <div class="meta">processed=${{worker.processed_jobs}}</div>
            <div class="meta">failed=${{worker.failed_jobs}}</div>
            <div class="meta">executors=${{executors}}</div>
            <div class="meta">models=${{models}} · cost_tiers=${{costTiers}}</div>
            <div class="meta">heartbeat_age=${{heartbeatAge}}</div>
          `;
          workersList.appendChild(item);
        }}
      }}

      function renderWorkspaceOverview(payload) {{
        const budget = payload.budget || {{}};
        const latestLoop = payload.latest_loop || null;
        const latestFleet = payload.latest_fleet || null;
        const daemon = payload.daemon || null;
        const sessions = payload.sessions || [];
        const workers = payload.workers || [];
        const projects = payload.projects || [];
        const providerRuntime = payload.provider_runtime || [];
        const providerHealth = payload.provider_health || [];
        const runtimeTotals = providerRuntime.reduce((accumulator, pool) => {{
          accumulator.success += Number(pool.success_count || 0);
          accumulator.failure += Number(pool.failure_count || 0);
          accumulator.retries += Number(pool.total_retry_count || 0);
          accumulator.cost += Number(pool.total_cost_estimate || 0);
          return accumulator;
        }}, {{ success: 0, failure: 0, retries: 0, cost: 0 }});
        const degradedPools = providerHealth.filter((pool) => pool.status !== "healthy").length;
        workspaceOverviewMeta.textContent =
          `${{payload.workspace}} · ${{payload.project_count}} projects · ` +
          `${{payload.session_count}} sessions`;
        const workspaceMode = payload.mode === "workspace";
        workspaceEnqueueButton.disabled = !workspaceMode;
        workspaceResumeButton.disabled = !workspaceMode;
        workspaceOverviewSummary.innerHTML = renderFacts([
          ["mode", payload.mode || "-"],
          ["projects", `${{payload.project_count || 0}}`],
          ["sessions", `${{payload.session_count || 0}}`],
          ["running_sessions", `${{payload.running_sessions || 0}}`],
          ["blocked_sessions", `${{payload.blocked_sessions || 0}}`],
          ["queued_jobs", `${{payload.queued_jobs || 0}}`],
          ["running_jobs", `${{payload.running_jobs || 0}}`],
          ["active_workers", `${{payload.active_workers || 0}}`],
          [
            "budget",
            `${{budget.completed_iterations || 0}} / ` +
            `${{budget.max_iterations || 0}} iter`,
          ],
        ]) + `
          <div class="meta">
            blocked_detail=${{formatBlockReasonCounts(payload.blocked_session_counts)}}
          </div>
        `;
        workspaceRuntimeSummary.innerHTML = renderFacts([
          ["remaining_iter", `${{budget.remaining_iterations || 0}}`],
          ["runtime_cost", formatCost(runtimeTotals.cost)],
          ["executor_calls", `${{runtimeTotals.success + runtimeTotals.failure}}`],
          ["executor_failures", `${{runtimeTotals.failure}}`],
          ["retries", `${{runtimeTotals.retries}}`],
          ["degraded_pools", `${{degradedPools}} / ${{providerHealth.length}}`],
        ]);
        workspaceLatestLoop.innerHTML = latestLoop
          ? renderFacts([
              ["id", latestLoop.loop_id || latestLoop.loop_run_id || "-"],
              ["stop", latestLoop.stop_reason || "-"],
              ["cycles", `${{latestLoop.cycles_completed || 0}}`],
              ["processed", `${{latestLoop.total_processed_jobs || 0}}`],
              [
                "fleet",
                latestFleet
                  ? `${{latestFleet.fleet_run_id || "-"}} · ${{latestFleet.stop_reason || "-"}}`
                  : "-",
              ],
            ])
          : '<div class="meta">No workspace loop history yet.</div>';
        const loopHistory = payload.loop_history || [];
        workspaceLoopHistory.innerHTML = loopHistory.length
          ? loopHistory.slice(0, 8).map((loop) => `
              <div class="rule">
                <strong>${{loop.loop_id || loop.loop_run_id || "-"}}</strong>
                <div class="meta">
                  project=${{loop.project_id || "-"}} · stop=${{loop.stop_reason || "-"}}
                </div>
                <div class="meta">
                  cycles=${{loop.cycles_completed || 0}}
                  · processed=${{loop.total_processed_jobs || 0}}
                </div>
              </div>
            `).join("")
          : '<div class="meta">No workspace loop history yet.</div>';
        const fleetHistory = payload.fleet_history || [];
        workspaceFleetHistory.innerHTML = fleetHistory.length
          ? fleetHistory.slice(0, 8).map((run) => `
              <div class="rule">
                <strong>${{run.fleet_run_id || "-"}}</strong>
                <div class="meta">
                  launcher=${{run.launcher || "-"}} · stop=${{run.stop_reason || "-"}}
                </div>
                <div class="meta">
                  cycles=${{run.cycles_completed || 0}}
                  · processed=${{run.total_processed_jobs || 0}}
                  · workers=${{run.total_workers_launched || 0}}
                </div>
              </div>
            `).join("")
          : '<div class="meta">No fleet history yet.</div>';
        workspaceDaemonState.innerHTML = daemon
          ? renderFacts([
              ["status", daemon.status || "-"],
              ["ticks", `${{daemon.tick_count || 0}}`],
              ["last_loop", daemon.last_loop_run_id || "-"],
              [
                "reason",
                daemon.exit_reason || daemon.request_reason || "-",
              ],
              ["stop_requested", daemon.stop_requested ? "yes" : "no"],
            ])
          : '<div class="meta">No workspace daemon state yet.</div>';
        workspaceSessionTable.innerHTML = sessions.length
          ? sessions.slice(0, 8).map((session) => {{
              const failures =
                `${{session.consecutive_failures || 0}}/` +
                `${{session.max_consecutive_failures || 0}}`;
              return `
                <div class="rule">
                  <strong>${{session.project_id}} · ${{session.status}}</strong>
                  <div class="meta">${{session.session_id}}</div>
                  <div class="meta">
                    iter=${{session.completed_iterations}}/${{session.max_iterations}}
                    · remaining=${{session.remaining_iterations}}
                  </div>
                  <div class="meta">
                    blocked=${{formatBlockReason(session.blocked_reason)}}
                    · failure=${{failures}}
                    · retry_budget=${{session.failure_budget_remaining ?? 0}}
                  </div>
                  <div class="meta">
                    cooldown=${{formatCooldown(session.cooldown_seconds_remaining)}}
                    · error=${{session.error_message || "-"}}
                  </div>
                  <div class="meta">tags=${{(session.tags || []).join(",") || "-"}}</div>
                  <div class="meta">
                    stop=${{session.last_stop_reason || "-"}}
                    · resume=${{session.last_resume_reason || "-"}}
                  </div>
                </div>
              `;
            }}).join("")
          : '<div class="meta">No workspace sessions yet.</div>';
        workspaceProviderRuntime.innerHTML = providerRuntime.length
          ? providerRuntime.slice(0, 6).map((pool) => {{
              const members = (pool.members || []).slice(0, 3).map((member) => {{
                const cost = formatCost(member.total_cost_estimate);
                return (
                  `${{member.member_name}}:${{member.success_count}}/${{member.failure_count}} ` +
                  `r=${{member.retry_count}} c=${{cost}}`
                );
              }}).join(" · ");
              return `
                <div class="rule">
                  <strong>${{pool.pool_name}} · health=${{pool.last_health_status || "-"}}</strong>
                  <div class="meta">
                    success=${{pool.success_count}} · failed=${{pool.failure_count}}
                    · retries=${{pool.total_retry_count}}
                  </div>
                  <div class="meta">
                    cost=${{formatCost(pool.total_cost_estimate)}}
                    · last=${{pool.last_seen_at || "-"}}
                  </div>
                  <div class="meta">members=${{members || "-"}}</div>
                </div>
              `;
            }}).join("")
          : '<div class="meta">No persisted provider runtime telemetry yet.</div>';
        workspaceProviderHealth.innerHTML = providerHealth.length
          ? providerHealth.slice(0, 6).map((pool) => {{
              const members = (pool.members || []).map((member) => {{
                const failures = Number(member.consecutive_failures || 0);
                return `${{member.member_name}}:${{member.status}}(f=${{failures}})`;
              }}).join(" · ");
              return `
                <div class="rule">
                  <strong>${{pool.pool_name}} · ${{pool.status}}</strong>
                  <div class="meta">
                    available=${{pool.available_members}}/${{pool.total_members}}
                    · quarantined=${{pool.quarantined_members}}
                  </div>
                  <div class="meta">strategy=${{pool.strategy || "-"}}</div>
                  <div class="meta">members=${{members || "-"}}</div>
                </div>
              `;
            }}).join("")
          : '<div class="meta">No provider pools configured.</div>';
        workspaceWorkerPool.innerHTML = [
          projects.length
            ? projects.slice(0, 8).map((project) => `
                <div class="rule">
                  <strong>${{project.project_id}}</strong>
                  <div class="meta">
                    workflow=${{project.workflow}} · dry_run=${{project.dry_run}}
                  </div>
                  <div class="meta">
                    sessions=${{project.session_count}} · running=${{project.running_sessions}}
                    · queued_jobs=${{project.queued_jobs}}
                    · blocked=${{project.blocked_sessions || 0}}
                  </div>
                  <div class="meta">
                    blocked_detail=${{formatBlockReasonCounts(project.blocked_session_counts)}}
                  </div>
                  <div class="meta">tags=${{(project.tags || []).join(",") || "-"}}</div>
                </div>
              `).join("")
            : '<div class="meta">No workspace projects.</div>',
          workers.length
            ? `
                <div class="rule">
                  <strong>Workers</strong>
                  <div class="meta">
                    ${{
                      workers
                        .map((worker) => `${{worker.worker_id}}:${{worker.status}}`)
                        .join(" · ")
                    }}
                  </div>
                </div>
              `
            : '<div class="meta">No worker pool activity yet.</div>',
        ].join("");
      }}

      function renderWorkspaceOverviewError(message) {{
        workspaceOverviewMeta.textContent = "Workspace overview unavailable.";
        workspaceEnqueueButton.disabled = true;
        workspaceResumeButton.disabled = true;
        workspaceOverviewSummary.innerHTML =
          '<div class="meta">Workspace overview unavailable.</div>';
        workspaceRuntimeSummary.innerHTML =
          '<div class="meta">Workspace overview unavailable.</div>';
        workspaceDaemonState.innerHTML =
          '<div class="meta">Workspace overview unavailable.</div>';
        workspaceSessionTable.innerHTML = `<div class="meta">${{message}}</div>`;
        workspaceProviderRuntime.innerHTML =
          '<div class="meta">Workspace overview unavailable.</div>';
        workspaceProviderHealth.innerHTML =
          '<div class="meta">Workspace overview unavailable.</div>';
        workspaceWorkerPool.innerHTML = '<div class="meta">Workspace overview unavailable.</div>';
      }}

      async function refresh() {{
        const previewPromise = fetchJson(`/api/projects/${{currentProjectId}}/preview`)
          .catch((error) => ({{ error: error.message }}));
        const projectRunLoopsPromise = fetchJson(`/api/projects/${{currentProjectId}}/run-loops`)
          .catch((error) => ({{ error: error.message }}));
        const fleetPlanPromise = fetchJson(`/api/queue/fleet-plan`)
          .catch((error) => ({{ error: error.message }}));
        const workspaceOverviewPromise = fetchJson(`/api/workspace/overview`)
          .catch((error) => ({{ error: error.message }}));
        const [
          control,
          runs,
          jobs,
          workers,
          projectPreview,
          projectRunLoops,
          fleetPlan,
          workspaceOverview,
        ] =
          await Promise.all([
          fetchJson(`/api/projects/${{currentProjectId}}/control`),
          fetchJson(`/api/runs?limit=20`),
          fetchJson(`/api/queue/jobs?limit=20`),
          fetchJson(`/api/queue/workers`),
          previewPromise,
          projectRunLoopsPromise,
          fleetPlanPromise,
          workspaceOverviewPromise,
        ]);
        renderControl(control);
        renderRuns(runs);
        renderQueue(jobs);
        renderWorkers(workers);
        if (fleetPlan.error) {{
          renderFleetPlanError(fleetPlan.error);
        }} else {{
          renderFleetPlan(fleetPlan);
        }}
        if (projectPreview.error) {{
          renderProjectPreviewError(projectPreview.error);
        }} else {{
          renderProjectPreview(projectPreview);
        }}
        if (projectRunLoops.error) {{
          renderProjectRunLoopsError(projectRunLoops.error);
        }} else {{
          renderProjectRunLoops(projectRunLoops);
        }}
        if (workspaceOverview.error) {{
          renderWorkspaceOverviewError(workspaceOverview.error);
        }} else {{
          renderWorkspaceOverview(workspaceOverview);
        }}
      }}

      projectSelector.addEventListener("change", async () => {{
        currentProjectId = projectSelector.value || defaultProjectId;
        await refresh();
      }});

      document.getElementById("pause-button").addEventListener("click", async () => {{
        await fetchJson(`/api/projects/${{currentProjectId}}/pause`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ reason: "Paused from dashboard" }}),
        }});
        await refresh();
      }});

      document.getElementById("resume-button").addEventListener("click", async () => {{
        await fetchJson(`/api/projects/${{currentProjectId}}/resume`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{}}),
        }});
        await refresh();
      }});

      document.getElementById("hint-button").addEventListener("click", async () => {{
        const text = hintInput.value.trim();
        if (!text) {{
          return;
        }}
        await fetchJson(`/api/projects/${{currentProjectId}}/hint`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ text, author: "dashboard" }}),
        }});
        hintInput.value = "";
        await refresh();
      }});

      workflowApplyButton.addEventListener("click", async () => {{
        const workflow = workflowModeSelect.value || null;
        const workflowSpecPath = workflowSpecInput.value.trim() || null;
        const clearWorkflowSpec = clearWorkflowSpecCheckbox.checked;
        await fetchJson(`/api/projects/${{currentProjectId}}/workflow`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            workflow,
            workflow_spec_path: workflowSpecPath,
            clear_workflow_spec: clearWorkflowSpec,
          }}),
        }});
        await refresh();
      }});

      workflowResetButton.addEventListener("click", async () => {{
        await fetchJson(`/api/projects/${{currentProjectId}}/workflow/reset`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{}}),
        }});
        await refresh();
      }});

      document.getElementById("queue-run-button").addEventListener("click", async () => {{
        await fetchJson(`/api/queue/run`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{}}),
        }});
        await refresh();
      }});

      document.getElementById("queue-fleet-button").addEventListener("click", async () => {{
        await fetchJson(`/api/queue/fleet`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ plan_driven: true }}),
        }});
        await refresh();
      }});

      jobRequeueButton.addEventListener("click", async () => {{
        if (!selectedQueueJobId) {{
          return;
        }}
        await fetchJson(`/api/queue/jobs/${{selectedQueueJobId}}/requeue`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{}}),
        }});
        await refresh();
      }});

      jobCancelButton.addEventListener("click", async () => {{
        if (!selectedQueueJobId) {{
          return;
        }}
        await fetchJson(`/api/queue/jobs/${{selectedQueueJobId}}/cancel`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ reason: "Canceled from dashboard" }}),
        }});
        await refresh();
      }});

      document.getElementById("worker-sweep-button").addEventListener("click", async () => {{
        await fetchJson(`/api/queue/workers/sweep`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ stale_after_seconds: 120, requeue_running_jobs: true }}),
        }});
        await refresh();
      }});

      workspaceEnqueueButton.addEventListener("click", async () => {{
        await fetchJson(`/api/workspace/enqueue`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            note: "dashboard_enqueue_workspace",
            tags: parseWorkspaceTags(),
          }}),
        }});
        await refresh();
      }});

      workspaceResumeButton.addEventListener("click", async () => {{
        await fetchJson(`/api/workspace/resume`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{
            resume_reason: "dashboard_resume_workspace",
            tags: parseWorkspaceTags(),
          }}),
        }});
        await refresh();
      }});

      benchmarkRunsButton.addEventListener("click", async () => {{
        try {{
          const payload = await fetchJson(`/api/benchmark/runs`, {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify({{
              ...benchmarkSourcePayload(),
              limit: 20,
            }}),
          }});
          renderBenchmarkRuns(payload);
          if (payload.length) {{
            await loadBenchmarkRunDetail(payload[0].run_id);
          }} else {{
            renderBenchmarkRunDetail({{}});
          }}
        }} catch (error) {{
          renderBenchmarkError(benchmarkRunsList, error.message);
          renderBenchmarkError(benchmarkRunDetail, error.message);
        }}
      }});

      benchmarkLedgerButton.addEventListener("click", async () => {{
        try {{
          const payload = await fetchJson(`/api/benchmark/experiment-ledger`, {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify({{
              summary_path: trimOrNull(benchmarkSummaryInput.value),
              ledger_path: trimOrNull(benchmarkLedgerInput.value),
            }}),
          }});
          renderBenchmarkLedger(payload);
        }} catch (error) {{
          renderBenchmarkError(benchmarkLedgerSummary, error.message);
        }}
      }});

      benchmarkCompareButton.addEventListener("click", async () => {{
        try {{
          const payload = await fetchJson(`/api/benchmark/compare`, {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify({{
              baseline_ledger_path: trimOrNull(benchmarkBaselineLedgerInput.value),
              candidate_ledger_path: trimOrNull(benchmarkCandidateLedgerInput.value),
            }}),
          }});
          renderBenchmarkCompare(payload);
        }} catch (error) {{
          renderBenchmarkError(benchmarkCompareSummary, error.message);
        }}
      }});

      benchmarkReplayButton.addEventListener("click", async () => {{
        try {{
          const payload = await fetchJson(`/api/benchmark/replay`, {{
            method: "POST",
            headers: {{ "Content-Type": "application/json" }},
            body: JSON.stringify({{
              ledger_path: trimOrNull(benchmarkReplayLedgerInput.value),
              project_id: (benchmarkProjectInput.value || "").trim(),
              theorem_name: trimOrNull(benchmarkTheoremInput.value),
            }}),
          }});
          renderBenchmarkReplay(payload);
        }} catch (error) {{
          renderBenchmarkError(benchmarkReplayDetail, error.message);
        }}
      }});

      refresh().catch((error) => {{
        detailMeta.textContent = "Dashboard failed to load.";
        detailJson.textContent = error.message;
      }});
    </script>
  </body>
</html>"""
