from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

from .batch import BatchRunner
from .benchmark import BenchmarkRunService
from .checks import gather_doctor_report
from .config import (
    build_workspace_project_app_config,
    init_config,
    init_workspace_config,
    load_config,
    load_workspace_config,
)
from .control import ControlService
from .dashboard import create_dashboard_app
from .events import EventStore
from .experiment_ledger import compare_experiment_ledgers, load_experiment_ledger
from .fleet import FleetController
from .ledger import load_benchmark_ledger
from .models import (
    ExecutorConfig,
    ExecutorKind,
    ExperimentLedger,
    ProjectSession,
    ProviderConfig,
    ProviderKind,
    QueueJobStatus,
    SessionStatus,
    WorkflowMode,
    WorktreeLease,
)
from .queue import QueueStore
from .services import RunService
from .worktree import WorktreeManager

app = typer.Typer(no_args_is_help=True, help="archonlab external orchestrator control plane")
project_app = typer.Typer(no_args_is_help=True)
run_app = typer.Typer(no_args_is_help=True)
benchmark_app = typer.Typer(no_args_is_help=True)
worktree_app = typer.Typer(no_args_is_help=True)
control_app = typer.Typer(no_args_is_help=True)
dashboard_app = typer.Typer(no_args_is_help=True)
queue_app = typer.Typer(no_args_is_help=True)
workspace_app = typer.Typer(no_args_is_help=True)
app.add_typer(project_app, name="project")
app.add_typer(run_app, name="run")
app.add_typer(benchmark_app, name="benchmark")
app.add_typer(worktree_app, name="worktree")
app.add_typer(control_app, name="control")
app.add_typer(dashboard_app, name="dashboard")
app.add_typer(queue_app, name="queue")
app.add_typer(workspace_app, name="workspace")


def _parse_executor_kinds(raw: str | None) -> list[ExecutorKind]:
    if raw is None:
        return list(ExecutorKind)
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if not values:
        return list(ExecutorKind)
    return [ExecutorKind(value) for value in values]


def _parse_provider_kinds(raw: str | None) -> list[ProviderKind]:
    if raw is None:
        return list(ProviderKind)
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if not values:
        return list(ProviderKind)
    return [ProviderKind(value) for value in values]


def _parse_csv_strings(raw: str | None) -> list[str]:
    if raw is None:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


@app.command()
def doctor(
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Optional archonlab.toml to enrich checks with project-specific paths.",
        ),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
) -> None:
    project = load_config(config).project if config else None
    report = gather_doctor_report(project)
    if json_output:
        typer.echo(json.dumps(report.model_dump(mode="json"), ensure_ascii=False, indent=2))
        return

    typer.echo(f"Python 3.12: {report.python_version}")
    typer.echo("Tools:")
    for tool in report.tools:
        marker = "OK" if tool.available else "MISSING"
        version = f" ({tool.version})" if tool.version else ""
        typer.echo(f"  - [{marker}] {tool.name}{version}")
    if report.paths:
        typer.echo("Paths:")
        for status in report.paths:
            marker = "OK" if status.ok else "FAIL"
            typer.echo(f"  - [{marker}] {status.name}: {status.detail}")


@project_app.command("init")
def project_init(
    project_path: Annotated[
        Path,
        typer.Option("--project-path", exists=False, help="Lean project path."),
    ],
    archon_path: Annotated[
        Path,
        typer.Option("--archon-path", exists=False, help="Archon checkout path."),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="Output config file."),
    ] = Path("archonlab.toml"),
    artifact_root: Annotated[
        Path,
        typer.Option("--artifact-root", help="Artifact directory."),
    ] = Path("artifacts"),
    workflow: Annotated[
        WorkflowMode,
        typer.Option("--workflow", case_sensitive=False, help="Baseline workflow mode."),
    ] = WorkflowMode.ADAPTIVE_LOOP,
    workflow_spec: Annotated[
        Path | None,
        typer.Option("--workflow-spec", exists=False, help="Optional workflow DSL TOML."),
    ] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run/--execute", help="Default run mode.")
    ] = True,
    executor_kind: Annotated[
        ExecutorKind,
        typer.Option("--executor-kind", case_sensitive=False, help="Executor backend."),
    ] = ExecutorKind.DRY_RUN,
    model: Annotated[
        str | None,
        typer.Option("--model", help="Provider model or Codex model override."),
    ] = None,
    base_url: Annotated[
        str | None,
        typer.Option("--base-url", help="OpenAI-compatible base URL."),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option("--api-key-env", help="Environment variable holding the API key."),
    ] = "OPENAI_API_KEY",
    codex_command: Annotated[
        str,
        typer.Option("--codex-command", help="Codex executable path."),
    ] = "codex",
    codex_profile: Annotated[
        str | None,
        typer.Option("--codex-profile", help="Optional Codex profile."),
    ] = None,
    codex_auto_approve: Annotated[
        bool,
        typer.Option("--codex-auto-approve", help="Allow unattended codex exec runs."),
    ] = False,
    force: Annotated[
        bool, typer.Option("--force", help="Overwrite an existing config.")
    ] = False,
) -> None:
    written = init_config(
        config_path=config_path,
        project_path=project_path.resolve(),
        archon_path=archon_path.resolve(),
        artifact_root=artifact_root,
        workflow=workflow,
        workflow_spec=workflow_spec.resolve() if workflow_spec is not None else None,
        dry_run=dry_run,
        executor=ExecutorConfig(
            kind=executor_kind,
            command=codex_command,
            profile=codex_profile,
            auto_approve=codex_auto_approve,
        ),
        provider=ProviderConfig(
            model=model,
            base_url=base_url,
            api_key_env=api_key_env,
        ),
        force=force,
    )
    typer.echo(f"Wrote config: {written}")
    typer.echo("Next steps:")
    typer.echo(f"  1. archonlab doctor --config {written}")
    typer.echo(f"  2. archonlab run start --config {written} --dry-run")


@workspace_app.command("init")
def workspace_init(
    project_path: Annotated[
        Path,
        typer.Option("--project-path", exists=False, help="Initial Lean project path."),
    ],
    archon_path: Annotated[
        Path,
        typer.Option("--archon-path", exists=False, help="Archon checkout path."),
    ],
    config_path: Annotated[
        Path,
        typer.Option("--config-path", help="Output workspace config file."),
    ] = Path("workspace.toml"),
    workspace_name: Annotated[
        str | None,
        typer.Option("--name", help="Workspace name. Defaults to the initial project name."),
    ] = None,
    project_id: Annotated[
        str | None,
        typer.Option("--project-id", help="Initial workspace project identifier."),
    ] = None,
    artifact_root: Annotated[
        Path,
        typer.Option("--artifact-root", help="Workspace artifact directory."),
    ] = Path("artifacts"),
    workflow: Annotated[
        WorkflowMode,
        typer.Option("--workflow", case_sensitive=False, help="Default workflow mode."),
    ] = WorkflowMode.ADAPTIVE_LOOP,
    workflow_spec: Annotated[
        Path | None,
        typer.Option("--workflow-spec", exists=False, help="Optional workflow DSL TOML."),
    ] = None,
    dry_run: Annotated[
        bool, typer.Option("--dry-run/--execute", help="Default workspace run mode.")
    ] = True,
    executor_kind: Annotated[
        ExecutorKind,
        typer.Option("--executor-kind", case_sensitive=False, help="Executor backend."),
    ] = ExecutorKind.DRY_RUN,
    model: Annotated[
        str | None,
        typer.Option("--model", help="Provider model or Codex model override."),
    ] = None,
    base_url: Annotated[
        str | None,
        typer.Option("--base-url", help="OpenAI-compatible base URL."),
    ] = None,
    api_key_env: Annotated[
        str,
        typer.Option("--api-key-env", help="Environment variable holding the API key."),
    ] = "OPENAI_API_KEY",
    codex_command: Annotated[
        str,
        typer.Option("--codex-command", help="Codex executable path."),
    ] = "codex",
    codex_profile: Annotated[
        str | None,
        typer.Option("--codex-profile", help="Optional Codex profile."),
    ] = None,
    codex_auto_approve: Annotated[
        bool,
        typer.Option("--codex-auto-approve", help="Allow unattended codex exec runs."),
    ] = False,
    force: Annotated[
        bool, typer.Option("--force", help="Overwrite an existing config.")
    ] = False,
) -> None:
    resolved_project_id = project_id or project_path.name
    written = init_workspace_config(
        config_path=config_path,
        workspace_name=workspace_name or resolved_project_id,
        project_id=resolved_project_id,
        project_path=project_path.resolve(),
        archon_path=archon_path.resolve(),
        artifact_root=artifact_root,
        workflow=workflow,
        workflow_spec=workflow_spec.resolve() if workflow_spec is not None else None,
        dry_run=dry_run,
        executor=ExecutorConfig(
            kind=executor_kind,
            command=codex_command,
            profile=codex_profile,
            auto_approve=codex_auto_approve,
        ),
        provider=ProviderConfig(
            model=model,
            base_url=base_url,
            api_key_env=api_key_env,
        ),
        force=force,
    )
    typer.echo(f"Wrote workspace config: {written}")
    typer.echo("Next steps:")
    typer.echo(f"  1. archonlab workspace status --config {written}")
    typer.echo(
        "  2. archonlab workspace start-session "
        f"--config {written} --project-id {resolved_project_id}"
    )


@workspace_app.command("status")
def workspace_status(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
) -> None:
    workspace_config = load_workspace_config(config)
    store = EventStore(workspace_config.run.artifact_root / "archonlab.db")
    sessions = store.list_sessions(workspace_id=workspace_config.name, limit=500)
    project_rows = []
    for project in workspace_config.projects:
        project_sessions = [item for item in sessions if item.project_id == project.id]
        running_sessions = sum(
            1 for item in project_sessions if item.status is SessionStatus.RUNNING
        )
        project_rows.append(
            {
                "project_id": project.id,
                "enabled": project.enabled,
                "workflow": (
                    project.workflow or workspace_config.run.workflow
                ).value,
                "dry_run": (
                    project.dry_run
                    if project.dry_run is not None
                    else workspace_config.run.dry_run
                ),
                "session_count": len(project_sessions),
                "running_sessions": running_sessions,
            }
        )
    payload = {
        "workspace": workspace_config.name,
        "artifact_root": str(workspace_config.run.artifact_root),
        "project_count": len(workspace_config.projects),
        "session_count": len(sessions),
        "projects": project_rows,
    }
    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    typer.echo(f"Workspace: {workspace_config.name}")
    typer.echo(f"Artifact root: {workspace_config.run.artifact_root}")
    typer.echo(f"Projects: {len(workspace_config.projects)}")
    typer.echo(f"Sessions: {len(sessions)}")
    for row in project_rows:
        typer.echo(
            f"{row['project_id']} | enabled={row['enabled']} | workflow={row['workflow']} | "
            f"dry_run={row['dry_run']} | sessions={row['session_count']} | "
            f"running={row['running_sessions']}"
        )


@workspace_app.command("start-session")
def workspace_start_session(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    project_id: Annotated[
        str,
        typer.Option("--project-id", help="Workspace project identifier."),
    ] = "",
    max_iterations: Annotated[
        int | None,
        typer.Option("--max-iterations", min=1, help="Optional session iteration cap."),
    ] = None,
    dry_run: Annotated[
        bool | None,
        typer.Option("--dry-run/--execute", help="Override the workspace project run mode."),
    ] = None,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional session note."),
    ] = None,
) -> None:
    if not project_id:
        raise typer.BadParameter("--project-id is required")
    workspace_config = load_workspace_config(config)
    project = next(
        (candidate for candidate in workspace_config.projects if candidate.id == project_id),
        None,
    )
    if project is None:
        raise typer.BadParameter(f"Unknown workspace project: {project_id}")
    if not project.enabled:
        raise typer.BadParameter(f"Workspace project is disabled: {project_id}")
    session = ProjectSession(
        session_id=f"session-{project_id}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}",
        workspace_id=workspace_config.name,
        project_id=project.id,
        workflow=project.workflow or workspace_config.run.workflow,
        dry_run=workspace_config.run.dry_run if dry_run is None else dry_run,
        max_iterations=(
            max_iterations
            if max_iterations is not None
            else (
                project.max_iterations
                if project.max_iterations is not None
                else workspace_config.run.max_iterations
            )
        ),
        note=note,
    )
    store = EventStore(workspace_config.run.artifact_root / "archonlab.db")
    store.register_session(session)
    typer.echo(f"Session: {session.session_id}")
    typer.echo(f"Project: {session.project_id}")
    typer.echo(f"Workflow: {session.workflow.value}")
    typer.echo(f"Dry run: {session.dry_run}")
    typer.echo(f"Max iterations: {session.max_iterations}")


@workspace_app.command("run-project")
def workspace_run_project(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    project_id: Annotated[
        str,
        typer.Option("--project-id", help="Workspace project identifier."),
    ] = "",
    max_iterations: Annotated[
        int | None,
        typer.Option("--max-iterations", min=1, help="Optional session iteration cap."),
    ] = None,
    dry_run: Annotated[
        bool | None,
        typer.Option("--dry-run/--execute", help="Override the project run mode."),
    ] = None,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional session note."),
    ] = None,
) -> None:
    if not project_id:
        raise typer.BadParameter("--project-id is required")
    workspace_config = load_workspace_config(config)
    app_config = build_workspace_project_app_config(
        workspace_config,
        project_id=project_id,
    )
    result = RunService(app_config).run_loop(
        dry_run=dry_run,
        max_iterations=max_iterations,
        workspace_id=workspace_config.name,
        note=note,
    )
    typer.echo(f"Session: {result.session_id}")
    typer.echo(f"Project: {result.project_id}")
    typer.echo(f"Status: {result.status.value}")
    typer.echo(f"Completed iterations: {result.completed_iterations}")
    typer.echo(f"Stop reason: {result.stop_reason}")
    typer.echo(f"Runs: {len(result.run_ids)}")


@run_app.command("start")
def run_start(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    dry_run: Annotated[
        bool, typer.Option("--dry-run/--execute", help="Use the dry-run baseline.")
    ] = True,
) -> None:
    app_config = load_config(config)
    service = RunService(app_config)
    result = service.start(dry_run=dry_run)
    typer.echo(f"Run: {result.run_id}")
    typer.echo(f"Status: {result.status.value}")
    typer.echo(f"Next action: {result.action.phase} ({result.action.reason})")
    if result.action.task_title is not None:
        target = result.action.task_title
        if result.action.file_path is not None:
            target = f"{target} @ {result.action.file_path}"
        typer.echo(f"Focus task: {target}")
    typer.echo(f"Artifacts: {result.artifact_dir}")
    typer.echo(f"Prompt preview: {result.prompt_path}")
    typer.echo(f"Executor: {app_config.executor.kind.value}")
    if app_config.provider.model is not None:
        typer.echo(f"Model: {app_config.provider.model}")
    if result.task_graph_path is not None:
        typer.echo(f"Task graph: {result.task_graph_path}")
    if result.supervisor_path is not None:
        typer.echo(f"Supervisor: {result.supervisor_path}")
    if result.execution is not None and result.execution.output_path is not None:
        typer.echo(f"Execution output: {result.execution.output_path}")


@run_app.command("status")
def run_status(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    run_id: Annotated[
        str | None, typer.Option("--run-id", help="Inspect a single run.")
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, max=100, help="Number of runs to show."),
    ] = 10,
) -> None:
    app_config = load_config(config)
    store = EventStore(app_config.run.artifact_root / "archonlab.db")
    if run_id is not None:
        events = store.get_run_events(run_id)
        if not events:
            raise typer.Exit(code=1)
        for event in events:
            typer.echo(
                json.dumps(event.model_dump(mode="json"), ensure_ascii=False, indent=2)
            )
        return

    runs = store.list_runs(limit=limit)
    if not runs:
        typer.echo("No runs recorded yet.")
        return
    for run in runs:
        typer.echo(
            f"{run.run_id} | {run.status.value} | {run.workflow.value} | "
            f"stage={run.stage} | dry_run={run.dry_run}"
        )


@run_app.command("loop")
def run_loop(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    max_iterations: Annotated[
        int | None,
        typer.Option("--max-iterations", min=1, help="Optional iteration cap."),
    ] = None,
    dry_run: Annotated[
        bool | None,
        typer.Option("--dry-run/--execute", help="Override the configured run mode."),
    ] = None,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional session note."),
    ] = None,
) -> None:
    app_config = load_config(config)
    result = RunService(app_config).run_loop(
        dry_run=dry_run,
        max_iterations=max_iterations,
        note=note,
    )
    typer.echo(f"Session: {result.session_id}")
    typer.echo(f"Status: {result.status.value}")
    typer.echo(f"Completed iterations: {result.completed_iterations}")
    typer.echo(f"Stop reason: {result.stop_reason}")
    typer.echo(f"Runs: {len(result.run_ids)}")


@benchmark_app.command("run")
def benchmark_run(
    manifest: Annotated[
        Path,
        typer.Option("--manifest", exists=True, help="Benchmark manifest TOML."),
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run/--execute",
            help="Use the current dry-run execution path for each project.",
        ),
    ] = True,
    use_worktrees: Annotated[
        bool,
        typer.Option(
            "--use-worktrees/--direct-checkout",
            help="Run benchmark projects from isolated git worktrees.",
        ),
    ] = False,
    keep_worktrees: Annotated[
        bool,
        typer.Option(
            "--keep-worktrees/--cleanup-worktrees",
            help="Keep created worktrees after the benchmark run finishes.",
        ),
    ] = False,
    worker_slots: Annotated[
        int | None,
        typer.Option("--worker-slots", min=1, help="Override benchmark worker slots."),
    ] = None,
) -> None:
    service = BenchmarkRunService(manifest)
    result = service.run(
        dry_run=dry_run,
        use_worktrees=use_worktrees,
        cleanup_worktrees=not keep_worktrees,
        worker_slots=worker_slots,
    )
    typer.echo(f"Benchmark: {result.benchmark.name}")
    typer.echo(f"Run: {result.run_id}")
    typer.echo(f"Status: {result.status.value}")
    typer.echo(f"Artifacts: {result.artifact_dir}")
    for project in result.projects:
        typer.echo(
            f"{project.id} | {project.run_status.value} | "
            f"score={project.score.score} | "
            f"sorry={project.snapshot.sorry_count} | "
            f"reviews={len(project.snapshot.review_sessions)}"
        )
        if project.worktree_path is not None:
            typer.echo(f"  worktree={project.worktree_path}")


@benchmark_app.command("ledger")
def benchmark_ledger(
    summary: Annotated[
        Path,
        typer.Option("--summary", exists=True, help="Benchmark summary JSON."),
    ],
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
) -> None:
    ledger = load_benchmark_ledger(summary)
    if json_output:
        typer.echo(json.dumps(ledger.model_dump(mode="json"), ensure_ascii=False, indent=2))
        return

    typer.echo(f"Benchmark: {ledger.benchmark_name}")
    typer.echo(f"Run: {ledger.benchmark_run_id}")
    typer.echo(f"Projects: {ledger.summary.total_projects}")
    typer.echo(f"Targeted theorems: {ledger.summary.targeted_projects}")
    typer.echo(
        "Outcomes: "
        + ", ".join(
            f"{name}={count}" for name, count in sorted(ledger.summary.outcome_counts.items())
        )
    )
    if ledger.summary.failure_counts:
        typer.echo(
            "Failures: "
            + ", ".join(
                f"{name}={count}"
                for name, count in sorted(ledger.summary.failure_counts.items())
            )
        )
    for outcome in ledger.outcomes:
        typer.echo(
            f"{outcome.project_id} | theorem={outcome.theorem_name or '-'} | "
            f"outcome={outcome.outcome.value} | failure={outcome.failure_kind.value}"
        )


def _load_experiment_ledger_from_options(
    *,
    summary: Path | None,
    ledger: Path | None,
) -> ExperimentLedger:
    if summary is not None and ledger is not None:
        raise typer.BadParameter("Specify either --summary or --ledger, not both.")
    if summary is None and ledger is None:
        raise typer.BadParameter("Specify --summary or --ledger.")
    resolved_path = summary if summary is not None else ledger
    if resolved_path is None:
        raise typer.BadParameter("Specify --summary or --ledger.")
    return load_experiment_ledger(resolved_path)


@benchmark_app.command("experiment-ledger")
def benchmark_experiment_ledger(
    summary: Annotated[
        Path | None,
        typer.Option("--summary", exists=True, help="Benchmark summary JSON."),
    ] = None,
    ledger: Annotated[
        Path | None,
        typer.Option("--ledger", exists=True, help="Experiment ledger JSON."),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
) -> None:
    experiment_ledger = _load_experiment_ledger_from_options(
        summary=summary,
        ledger=ledger,
    )
    if json_output:
        typer.echo(
            json.dumps(
                experiment_ledger.model_dump(mode="json"),
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    typer.echo(f"Benchmark: {experiment_ledger.benchmark_name}")
    typer.echo(f"Run: {experiment_ledger.benchmark_run_id}")
    typer.echo(f"Projects: {experiment_ledger.summary.total_projects}")
    typer.echo(f"Theorems: {experiment_ledger.summary.total_theorems}")
    typer.echo(
        "Changes: "
        f"improved={experiment_ledger.summary.improved}, "
        f"regressed={experiment_ledger.summary.regressed}, "
        f"new={experiment_ledger.summary.new}, "
        f"removed={experiment_ledger.summary.removed}"
    )


@benchmark_app.command("compare")
def benchmark_compare(
    baseline_summary: Annotated[
        Path | None,
        typer.Option("--baseline-summary", exists=True, help="Baseline benchmark summary JSON."),
    ] = None,
    baseline_ledger: Annotated[
        Path | None,
        typer.Option(
            "--baseline-ledger",
            exists=True,
            help="Baseline experiment ledger JSON.",
        ),
    ] = None,
    candidate_summary: Annotated[
        Path | None,
        typer.Option("--candidate-summary", exists=True, help="Candidate benchmark summary JSON."),
    ] = None,
    candidate_ledger: Annotated[
        Path | None,
        typer.Option(
            "--candidate-ledger",
            exists=True,
            help="Candidate experiment ledger JSON.",
        ),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
) -> None:
    baseline = _load_experiment_ledger_from_options(
        summary=baseline_summary,
        ledger=baseline_ledger,
    )
    candidate = _load_experiment_ledger_from_options(
        summary=candidate_summary,
        ledger=candidate_ledger,
    )
    comparison = compare_experiment_ledgers(baseline, candidate)
    if json_output:
        typer.echo(json.dumps(comparison.model_dump(mode="json"), ensure_ascii=False, indent=2))
        return

    typer.echo(f"Baseline: {comparison.baseline_benchmark}")
    typer.echo(f"Candidate: {comparison.candidate_benchmark}")
    typer.echo(f"Theorems compared: {comparison.summary.total_theorems}")
    typer.echo(
        "Diffs: "
        f"improved={comparison.summary.improved}, "
        f"regressed={comparison.summary.regressed}, "
        f"new={comparison.summary.new}, "
        f"removed={comparison.summary.removed}"
    )
    for change in comparison.changes[:20]:
        typer.echo(
            f"{change.project_id} | {change.theorem_name} | "
            f"{change.baseline_state.value} -> {change.candidate_state.value} | "
            f"{change.change.value}"
        )


@worktree_app.command("create")
def worktree_create(
    repo_path: Annotated[
        Path,
        typer.Option("--repo-path", exists=True, file_okay=False, help="Git repository path."),
    ],
    name: Annotated[
        str | None,
        typer.Option("--name", help="Optional stable worktree lease name."),
    ] = None,
    root: Annotated[
        Path | None,
        typer.Option("--root", file_okay=False, help="Optional worktree root directory."),
    ] = None,
) -> None:
    manager = WorktreeManager(root=root)
    lease = manager.create(repo_path, name=name)
    lease_dir = lease.worktree_path.parent / ".leases"
    lease_dir.mkdir(parents=True, exist_ok=True)
    lease_path = lease_dir / f"{lease.lease_id}.json"
    lease_path.write_text(
        json.dumps(lease.model_dump(mode="json"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    typer.echo(f"Lease: {lease.lease_id}")
    typer.echo(f"Worktree: {lease.worktree_path}")
    typer.echo(f"Metadata: {lease_path}")


@worktree_app.command("remove")
def worktree_remove(
    lease: Annotated[
        Path,
        typer.Option("--lease", exists=True, dir_okay=False, help="Lease metadata JSON."),
    ],
    force: Annotated[
        bool,
        typer.Option("--force", help="Force removal if the worktree is dirty."),
    ] = False,
) -> None:
    lease_data = WorktreeLease.model_validate_json(lease.read_text(encoding="utf-8"))
    manager = WorktreeManager()
    manager.release(lease_data, force=force)
    lease.unlink(missing_ok=True)
    typer.echo(f"Removed: {lease_data.worktree_path}")


@control_app.command("status")
def control_status(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
) -> None:
    app_config = load_config(config)
    state = ControlService(app_config.run.artifact_root).read(app_config.project)
    typer.echo(json.dumps(state.model_dump(mode="json"), ensure_ascii=False, indent=2))


@control_app.command("pause")
def control_pause(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    reason: Annotated[
        str | None,
        typer.Option("--reason", help="Optional pause reason."),
    ] = None,
) -> None:
    app_config = load_config(config)
    state = ControlService(app_config.run.artifact_root).pause(
        app_config.project,
        reason=reason,
    )
    typer.echo(f"Paused: {state.project_id}")


@control_app.command("resume")
def control_resume(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
) -> None:
    app_config = load_config(config)
    state = ControlService(app_config.run.artifact_root).resume(app_config.project)
    typer.echo(f"Resumed: {state.project_id}")


@control_app.command("hint")
def control_hint(
    text: Annotated[
        str,
        typer.Option("--text", help="Hint text for the next cycle."),
    ],
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    author: Annotated[
        str,
        typer.Option("--author", help="Hint author label."),
    ] = "user",
) -> None:
    app_config = load_config(config)
    state = ControlService(app_config.run.artifact_root).add_hint(
        app_config.project,
        text=text,
        author=author,
    )
    typer.echo(f"Hints: {len(state.hints)}")


@control_app.command("workflow")
def control_workflow(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    workflow: Annotated[
        WorkflowMode | None,
        typer.Option("--workflow", help="Override workflow mode for future runs."),
    ] = None,
    workflow_spec: Annotated[
        Path | None,
        typer.Option("--workflow-spec", help="Optional override workflow spec path."),
    ] = None,
    clear_workflow_spec: Annotated[
        bool,
        typer.Option("--clear-workflow-spec", help="Disable the configured workflow spec."),
    ] = False,
) -> None:
    if workflow is None and workflow_spec is None and not clear_workflow_spec:
        raise typer.BadParameter(
            "Specify --workflow, --workflow-spec, or --clear-workflow-spec."
        )
    if workflow_spec is not None and clear_workflow_spec:
        raise typer.BadParameter(
            "--workflow-spec cannot be combined with --clear-workflow-spec."
        )
    app_config = load_config(config)
    resolved_spec = None
    if workflow_spec is not None:
        resolved_spec = workflow_spec
        if not resolved_spec.is_absolute():
            resolved_spec = (config.resolve().parent / resolved_spec).resolve()
        else:
            resolved_spec = resolved_spec.resolve()
        if not resolved_spec.exists():
            raise typer.BadParameter(f"Workflow spec does not exist: {resolved_spec}")
    state = ControlService(app_config.run.artifact_root).set_workflow(
        app_config.project,
        workflow=workflow,
        workflow_spec_override=resolved_spec,
        clear_workflow_spec=clear_workflow_spec,
    )
    typer.echo(
        "Workflow override updated: "
        f"workflow={state.workflow_override or '-'} "
        f"workflow_spec={state.workflow_spec_override or '-'} "
        f"clear_workflow_spec={'yes' if state.clear_workflow_spec else 'no'}"
    )


@control_app.command("clear-workflow")
def control_clear_workflow(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
) -> None:
    app_config = load_config(config)
    state = ControlService(app_config.run.artifact_root).reset_workflow(app_config.project)
    typer.echo(f"Workflow override cleared: {state.project_id}")


@control_app.command("reset-workflow")
def control_reset_workflow(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
) -> None:
    control_clear_workflow(config=config)


@dashboard_app.command("serve")
def dashboard_serve(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    host: Annotated[
        str,
        typer.Option("--host", help="Host interface."),
    ] = "127.0.0.1",
    port: Annotated[
        int,
        typer.Option("--port", min=1, max=65535, help="Port."),
    ] = 8000,
) -> None:
    import uvicorn

    uvicorn.run(create_dashboard_app(config), host=host, port=port)


@queue_app.command("enqueue-benchmark")
def queue_enqueue_benchmark(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    manifest: Annotated[
        Path,
        typer.Option("--manifest", exists=True, help="Benchmark manifest TOML."),
    ] = Path("benchmarks/smoke.example.toml"),
    dry_run: Annotated[
        bool, typer.Option("--dry-run/--execute", help="Queue dry-run benchmark jobs.")
    ] = True,
    use_worktrees: Annotated[
        bool,
        typer.Option("--use-worktrees/--direct-checkout", help="Use isolated worktrees."),
    ] = False,
) -> None:
    app_config = load_config(config)
    runner = BatchRunner(
        queue_store=QueueStore(app_config.run.artifact_root / "archonlab.db"),
        control_service=ControlService(app_config.run.artifact_root),
        artifact_root=app_config.run.artifact_root,
        slot_limit=app_config.run.max_parallel,
    )
    jobs = runner.queue_store.enqueue_benchmark_manifest(
        manifest,
        dry_run=dry_run,
        use_worktrees=use_worktrees,
    )
    typer.echo(f"Enqueued: {len(jobs)} jobs")
    for job in jobs:
        typer.echo(f"{job.id} | {job.project_id} | {job.status.value}")


@queue_app.command("enqueue-workspace")
def queue_enqueue_workspace(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    project_id: Annotated[
        str | None,
        typer.Option("--project-id", help="Optional project filter."),
    ] = None,
    max_iterations: Annotated[
        int | None,
        typer.Option("--max-iterations", min=1, help="Optional session iteration cap."),
    ] = None,
    priority: Annotated[
        int,
        typer.Option("--priority", help="Base queue priority for created session jobs."),
    ] = 0,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional session note."),
    ] = None,
) -> None:
    workspace_config = load_workspace_config(config)
    queue_store = QueueStore(workspace_config.run.artifact_root / "archonlab.db")
    jobs = queue_store.enqueue_workspace_sessions(
        config,
        project_ids=[project_id] if project_id is not None else None,
        max_iterations=max_iterations,
        priority=priority,
        note=note,
    )
    typer.echo(f"Enqueued sessions: {len(jobs)}")
    for job in jobs:
        typer.echo(
            f"{job.id} | project={job.project_id} | session={job.session_id or '-'} | "
            f"status={job.status.value}"
        )


@queue_app.command("run")
def queue_run(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    max_jobs: Annotated[
        int | None,
        typer.Option("--max-jobs", min=1, help="Optional maximum number of jobs to process."),
    ] = None,
    slots: Annotated[
        int | None,
        typer.Option("--slots", min=1, help="Override queue worker slots."),
    ] = None,
) -> None:
    app_config = load_config(config)
    runner = BatchRunner(
        queue_store=QueueStore(app_config.run.artifact_root / "archonlab.db"),
        control_service=ControlService(app_config.run.artifact_root),
        artifact_root=app_config.run.artifact_root,
        slot_limit=slots or app_config.run.max_parallel,
    )
    report = runner.run_pending(max_jobs=max_jobs)
    typer.echo(f"Processed: {len(report.processed_job_ids)}")
    typer.echo(f"Paused: {len(report.paused_job_ids)}")
    typer.echo(f"Failed: {len(report.failed_job_ids)}")
    typer.echo(f"Workers: {len(report.worker_ids)}")


@queue_app.command("fleet")
def queue_fleet(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    workers: Annotated[
        int | None,
        typer.Option(
            "--workers",
            min=1,
            help="Number of workers to launch, or a max cap when --plan-driven is enabled.",
        ),
    ] = None,
    plan_driven: Annotated[
        bool,
        typer.Option(
            "--plan-driven/--fixed-fleet",
            help=(
                "Launch heterogeneous workers from queue fleet planning "
                "instead of a uniform fleet."
            ),
        ),
    ] = False,
    target_jobs_per_worker: Annotated[
        int,
        typer.Option(
            "--target-jobs-per-worker",
            min=1,
            help="Heuristic queue load assigned to each planned worker profile.",
        ),
    ] = 2,
    max_jobs_per_worker: Annotated[
        int | None,
        typer.Option(
            "--max-jobs-per-worker",
            min=1,
            help="Optional maximum jobs processed by each worker.",
        ),
    ] = None,
    poll_seconds: Annotated[
        float,
        typer.Option("--poll-seconds", min=0.1, help="Polling interval when the queue is empty."),
    ] = 2.0,
    idle_timeout_seconds: Annotated[
        float,
        typer.Option(
            "--idle-timeout-seconds",
            min=0.1,
            help="Stop each worker after this many idle seconds.",
        ),
    ] = 30.0,
    stale_after_seconds: Annotated[
        float | None,
        typer.Option(
            "--stale-after-seconds",
            min=0.1,
            help="Reap stale active workers before each fleet worker claims a slot.",
        ),
    ] = 120.0,
    executor_kinds: Annotated[
        str | None,
        typer.Option(
            "--executor-kinds",
            help="Comma-separated executor kinds this fleet can run.",
        ),
    ] = None,
    provider_kinds: Annotated[
        str | None,
        typer.Option(
            "--provider-kinds",
            help="Comma-separated provider kinds this fleet can run.",
        ),
    ] = None,
    models: Annotated[
        str | None,
        typer.Option(
            "--models",
            help="Comma-separated provider models this fleet can run.",
        ),
    ] = None,
    cost_tiers: Annotated[
        str | None,
        typer.Option(
            "--cost-tiers",
            help="Comma-separated provider cost tiers this fleet can run.",
        ),
    ] = None,
    endpoint_classes: Annotated[
        str | None,
        typer.Option(
            "--endpoint-classes",
            help="Comma-separated endpoint classes this fleet can run.",
        ),
    ] = None,
) -> None:
    app_config = load_config(config)
    runner = BatchRunner(
        queue_store=QueueStore(app_config.run.artifact_root / "archonlab.db"),
        control_service=ControlService(app_config.run.artifact_root),
        artifact_root=app_config.run.artifact_root,
        slot_limit=workers or app_config.run.max_parallel,
    )
    report = runner.run_fleet(
        worker_count=workers if plan_driven else (workers or app_config.run.max_parallel),
        plan_driven=plan_driven,
        target_jobs_per_worker=target_jobs_per_worker,
        max_jobs_per_worker=max_jobs_per_worker,
        poll_seconds=poll_seconds,
        idle_timeout_seconds=idle_timeout_seconds,
        stale_after_seconds=stale_after_seconds,
        executor_kinds=_parse_executor_kinds(executor_kinds),
        provider_kinds=_parse_provider_kinds(provider_kinds),
        models=_parse_csv_strings(models),
        cost_tiers=_parse_csv_strings(cost_tiers),
        endpoint_classes=_parse_csv_strings(endpoint_classes),
    )
    typer.echo(f"Processed: {len(report.processed_job_ids)}")
    typer.echo(f"Paused: {len(report.paused_job_ids)}")
    typer.echo(f"Failed: {len(report.failed_job_ids)}")
    typer.echo(f"Workers: {len(report.worker_ids)}")


@queue_app.command("plan-fleet")
def queue_plan_fleet(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    target_jobs_per_worker: Annotated[
        int,
        typer.Option(
            "--target-jobs-per-worker",
            min=1,
            help="Heuristic queue load to assign per dedicated worker.",
        ),
    ] = 2,
    stale_after_seconds: Annotated[
        float | None,
        typer.Option(
            "--stale-after-seconds",
            min=0.1,
            help="Ignore active workers older than this heartbeat age.",
        ),
    ] = 120.0,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable fleet plan JSON."),
    ] = False,
) -> None:
    app_config = load_config(config)
    plan = QueueStore(app_config.run.artifact_root / "archonlab.db").plan_fleet(
        target_jobs_per_worker=target_jobs_per_worker,
        stale_after_seconds=stale_after_seconds,
    )
    if json_output:
        typer.echo(json.dumps(plan.model_dump(mode="json"), ensure_ascii=False, indent=2))
        return

    typer.echo(f"Target jobs/worker: {plan.target_jobs_per_worker}")
    typer.echo(f"Profiles: {plan.total_profiles}")
    typer.echo(
        f"Active jobs: {plan.active_jobs} | queued={plan.queued_jobs} | "
        f"pending={plan.pending_jobs} | running={plan.running_jobs}"
    )
    typer.echo(f"Active workers: {plan.active_workers}")
    typer.echo(f"Dedicated workers: {plan.dedicated_workers}")
    typer.echo(f"Generic workers: {plan.generic_workers}")
    typer.echo(f"Recommended workers: {plan.recommended_total_workers}")
    typer.echo(f"Additional workers: {plan.recommended_additional_workers}")
    if not plan.profiles:
        typer.echo("No active queue demand.")
        return

    for profile in plan.profiles:
        dominant_phase = profile.dominant_phase.value if profile.dominant_phase else "-"
        executor_kinds = ",".join(kind.value for kind in profile.required_executor_kinds) or "-"
        provider_kinds = ",".join(kind.value for kind in profile.required_provider_kinds) or "-"
        models = ",".join(profile.required_models) or "-"
        cost_tiers = ",".join(profile.required_cost_tiers) or "-"
        endpoints = ",".join(profile.required_endpoint_classes) or "-"
        typer.echo(
            f"{profile.profile_id} | phase={dominant_phase} | jobs={profile.active_jobs} | "
            f"dedicated={profile.dedicated_workers} | total={profile.recommended_total_workers} | "
            f"add={profile.recommended_additional_workers} | executor={executor_kinds} | "
            f"provider={provider_kinds} | model={models} | cost={cost_tiers} | endpoint={endpoints}"
        )


@queue_app.command("autoscale")
def queue_autoscale(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    max_cycles: Annotated[
        int,
        typer.Option("--max-cycles", min=1, help="Maximum autoscaling control cycles."),
    ] = 10,
    idle_cycles: Annotated[
        int,
        typer.Option(
            "--idle-cycles",
            min=1,
            help="Stop after this many consecutive no-progress control cycles.",
        ),
    ] = 1,
    workers: Annotated[
        int | None,
        typer.Option(
            "--workers",
            min=1,
            help="Optional cap on workers launched per autoscaling cycle.",
        ),
    ] = None,
    target_jobs_per_worker: Annotated[
        int,
        typer.Option(
            "--target-jobs-per-worker",
            min=1,
            help="Heuristic queue load assigned to each planned worker profile.",
        ),
    ] = 2,
    max_jobs_per_worker: Annotated[
        int | None,
        typer.Option(
            "--max-jobs-per-worker",
            min=1,
            help="Optional maximum jobs processed by each worker in a cycle.",
        ),
    ] = None,
    poll_seconds: Annotated[
        float,
        typer.Option("--poll-seconds", min=0.1, help="Polling interval when the queue is empty."),
    ] = 2.0,
    idle_timeout_seconds: Annotated[
        float,
        typer.Option(
            "--idle-timeout-seconds",
            min=0.1,
            help="Stop each launched worker after this many idle seconds.",
        ),
    ] = 30.0,
    stale_after_seconds: Annotated[
        float | None,
        typer.Option(
            "--stale-after-seconds",
            min=0.1,
            help="Ignore or reap active workers older than this heartbeat age.",
        ),
    ] = 120.0,
) -> None:
    app_config = load_config(config)
    queue_store = QueueStore(app_config.run.artifact_root / "archonlab.db")
    batch_runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(app_config.run.artifact_root),
        artifact_root=app_config.run.artifact_root,
        slot_limit=workers or app_config.run.max_parallel,
    )
    result = FleetController(
        queue_store=queue_store,
        batch_runner=batch_runner,
    ).run(
        max_cycles=max_cycles,
        idle_cycles=idle_cycles,
        worker_count=workers,
        target_jobs_per_worker=target_jobs_per_worker,
        max_jobs_per_worker=max_jobs_per_worker,
        poll_seconds=poll_seconds,
        idle_timeout_seconds=idle_timeout_seconds,
        stale_after_seconds=stale_after_seconds,
    )
    typer.echo(f"Cycles: {result.cycles_completed}")
    typer.echo(f"Stop reason: {result.stop_reason}")
    typer.echo(f"Processed: {result.total_processed_jobs}")
    typer.echo(f"Paused: {result.total_paused_jobs}")
    typer.echo(f"Failed: {result.total_failed_jobs}")
    typer.echo(f"Workers launched: {result.total_workers_launched}")


@queue_app.command("status")
def queue_status(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    limit: Annotated[
        int, typer.Option("--limit", min=1, max=200, help="Number of jobs to show.")
    ] = 20,
    status: Annotated[
        QueueJobStatus | None,
        typer.Option("--status", case_sensitive=False, help="Optional status filter."),
    ] = None,
) -> None:
    app_config = load_config(config)
    jobs = QueueStore(app_config.run.artifact_root / "archonlab.db").list_jobs(
        limit=limit,
        status=status,
    )
    if not jobs:
        typer.echo("No queue jobs.")
        return
    for job in jobs:
        worker = job.worker_id or "-"
        requirements = ",".join(kind.value for kind in job.required_executor_kinds) or "-"
        models = ",".join(job.required_models) or "-"
        cost_tiers = ",".join(job.required_cost_tiers) or "-"
        typer.echo(
            f"{job.id} | {job.status.value} | {job.project_id} | worker={worker} | "
            f"requires={requirements} | models={models} | cost_tiers={cost_tiers}"
        )


@queue_app.command("session-status")
def queue_session_status(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    session_id: Annotated[
        str | None,
        typer.Option("--session-id", help="Optional session filter."),
    ] = None,
    project_id: Annotated[
        str | None,
        typer.Option("--project-id", help="Optional project filter."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, max=200, help="Number of sessions to show."),
    ] = 50,
) -> None:
    workspace_config = load_workspace_config(config)
    event_store = EventStore(workspace_config.run.artifact_root / "archonlab.db")
    queue_store = QueueStore(workspace_config.run.artifact_root / "archonlab.db")
    sessions = event_store.list_sessions(
        workspace_id=workspace_config.name,
        project_id=project_id,
        limit=limit,
    )
    if session_id is not None:
        sessions = [session for session in sessions if session.session_id == session_id]
    if not sessions:
        typer.echo("No project sessions.")
        return
    for session in sessions:
        active_job = queue_store.get_active_session_job(session.session_id)
        typer.echo(
            f"{session.session_id} | {session.project_id} | {session.status.value} | "
            f"iterations={session.completed_iterations}/{session.max_iterations} | "
            f"job={(active_job.id if active_job is not None else '-')}"
        )


@queue_app.command("workers")
def queue_workers(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    stale_after_seconds: Annotated[
        float | None,
        typer.Option(
            "--stale-after-seconds",
            min=0.1,
            help="Flag active workers as stale when heartbeat age exceeds this threshold.",
        ),
    ] = 120.0,
) -> None:
    app_config = load_config(config)
    workers = QueueStore(app_config.run.artifact_root / "archonlab.db").list_workers(
        stale_after_seconds=stale_after_seconds
    )
    if not workers:
        typer.echo("No queue workers.")
        return
    for worker in workers:
        current_job = worker.current_job_id or "-"
        stale = " stale" if worker.stale else ""
        capabilities = ",".join(kind.value for kind in worker.executor_kinds) or "-"
        models = ",".join(worker.models) or "-"
        cost_tiers = ",".join(worker.cost_tiers) or "-"
        typer.echo(
            f"{worker.worker_id} | slot={worker.slot_index} | {worker.status.value} | "
            f"current={current_job} | processed={worker.processed_jobs}{stale} | "
            f"executors={capabilities} | models={models} | cost_tiers={cost_tiers}"
        )


@queue_app.command("worker")
def queue_worker(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    slot_index: Annotated[
        int,
        typer.Option("--slot-index", min=1, help="Stable worker slot index."),
    ] = 1,
    auto_slot: Annotated[
        bool,
        typer.Option(
            "--auto-slot/--manual-slot",
            help="Automatically claim the next free worker slot.",
        ),
    ] = False,
    max_jobs: Annotated[
        int | None,
        typer.Option("--max-jobs", min=1, help="Optional maximum jobs for this worker."),
    ] = None,
    poll_seconds: Annotated[
        float,
        typer.Option("--poll-seconds", min=0.1, help="Polling interval when the queue is empty."),
    ] = 2.0,
    idle_timeout_seconds: Annotated[
        float,
        typer.Option(
            "--idle-timeout-seconds",
            min=0.1,
            help="Stop the worker after this many idle seconds.",
        ),
    ] = 30.0,
    worker_id: Annotated[
        str | None,
        typer.Option("--worker-id", help="Optional explicit worker identifier."),
    ] = None,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional worker note."),
    ] = "external_worker",
    stale_after_seconds: Annotated[
        float | None,
        typer.Option(
            "--stale-after-seconds",
            min=0.1,
            help="Reap stale active workers before claiming a slot.",
        ),
    ] = 120.0,
    executor_kinds: Annotated[
        str | None,
        typer.Option(
            "--executor-kinds",
            help="Comma-separated executor kinds this worker can run.",
        ),
    ] = None,
    provider_kinds: Annotated[
        str | None,
        typer.Option(
            "--provider-kinds",
            help="Comma-separated provider kinds this worker can run.",
        ),
    ] = None,
    models: Annotated[
        str | None,
        typer.Option(
            "--models",
            help="Comma-separated provider models this worker can run.",
        ),
    ] = None,
    cost_tiers: Annotated[
        str | None,
        typer.Option(
            "--cost-tiers",
            help="Comma-separated provider cost tiers this worker can run.",
        ),
    ] = None,
    endpoint_classes: Annotated[
        str | None,
        typer.Option(
            "--endpoint-classes",
            help="Comma-separated endpoint classes this worker can run.",
        ),
    ] = None,
) -> None:
    app_config = load_config(config)
    runner = BatchRunner(
        queue_store=QueueStore(app_config.run.artifact_root / "archonlab.db"),
        control_service=ControlService(app_config.run.artifact_root),
        artifact_root=app_config.run.artifact_root,
        slot_limit=1,
    )
    report = runner.run_worker(
        slot_index=None if auto_slot else slot_index,
        max_jobs=max_jobs,
        poll_seconds=poll_seconds,
        idle_timeout_seconds=idle_timeout_seconds,
        worker_id=worker_id,
        note=note,
        stale_after_seconds=stale_after_seconds,
        executor_kinds=_parse_executor_kinds(executor_kinds),
        provider_kinds=_parse_provider_kinds(provider_kinds),
        models=_parse_csv_strings(models),
        cost_tiers=_parse_csv_strings(cost_tiers),
        endpoint_classes=_parse_csv_strings(endpoint_classes),
    )
    if report.worker_ids:
        lease = runner.queue_store.get_worker(report.worker_ids[0])
        if lease is not None:
            typer.echo(f"Worker id: {lease.worker_id}")
            typer.echo(f"Worker slot: {lease.slot_index}")
    typer.echo(f"Processed: {len(report.processed_job_ids)}")
    typer.echo(f"Paused: {len(report.paused_job_ids)}")
    typer.echo(f"Failed: {len(report.failed_job_ids)}")
    typer.echo(f"Workers: {len(report.worker_ids)}")


@queue_app.command("sweep-workers")
def queue_sweep_workers(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    stale_after_seconds: Annotated[
        float,
        typer.Option(
            "--stale-after-seconds",
            min=0.1,
            help="Treat active workers older than this heartbeat age as stale.",
        ),
    ] = 120.0,
    requeue_running_jobs: Annotated[
        bool,
        typer.Option(
            "--requeue-running-jobs/--leave-running-jobs",
            help="Requeue running jobs owned by stale workers.",
        ),
    ] = True,
) -> None:
    app_config = load_config(config)
    workers = QueueStore(app_config.run.artifact_root / "archonlab.db").reap_stale_workers(
        stale_after_seconds=stale_after_seconds,
        requeue_running_jobs=requeue_running_jobs,
    )
    typer.echo(f"Reaped: {len(workers)}")
    for worker in workers:
        typer.echo(f"{worker.worker_id} | slot={worker.slot_index} | {worker.status.value}")


@queue_app.command("cancel")
def queue_cancel(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    job_id: Annotated[
        str, typer.Option("--job-id", help="Queue job identifier.")
    ] = "",
    reason: Annotated[
        str | None, typer.Option("--reason", help="Optional cancellation reason.")
    ] = None,
) -> None:
    if not job_id:
        raise typer.BadParameter("--job-id is required")
    app_config = load_config(config)
    job = QueueStore(app_config.run.artifact_root / "archonlab.db").cancel(job_id, reason=reason)
    typer.echo(f"Canceled: {job.id}")


@queue_app.command("requeue")
def queue_requeue(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    job_id: Annotated[
        str, typer.Option("--job-id", help="Queue job identifier.")
    ] = "",
) -> None:
    if not job_id:
        raise typer.BadParameter("--job-id is required")
    app_config = load_config(config)
    job = QueueStore(app_config.run.artifact_root / "archonlab.db").requeue(job_id)
    typer.echo(f"Requeued: {job.id} | priority={job.priority}")


@queue_app.command("resume-session")
def queue_resume_session(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    session_id: Annotated[
        str,
        typer.Option("--session-id", help="Project session identifier."),
    ] = "",
    max_iterations: Annotated[
        int | None,
        typer.Option(
            "--max-iterations",
            min=1,
            help="Optional new absolute session iteration cap before resuming.",
        ),
    ] = None,
    priority: Annotated[
        int,
        typer.Option("--priority", help="Base queue priority for the resumed session job."),
    ] = 0,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional updated session note."),
    ] = None,
) -> None:
    if not session_id:
        raise typer.BadParameter("--session-id is required")
    workspace_config = load_workspace_config(config)
    event_store = EventStore(workspace_config.run.artifact_root / "archonlab.db")
    session = event_store.get_session(session_id)
    if session is None:
        raise typer.BadParameter(f"Unknown project session: {session_id}")
    if session.workspace_id != workspace_config.name:
        raise typer.BadParameter(
            f"Session {session_id} does not belong to workspace {workspace_config.name}"
        )
    if session.status in {SessionStatus.COMPLETED, SessionStatus.CANCELED}:
        raise typer.BadParameter(f"Cannot resume terminal session: {session.status.value}")
    resolved_max_iterations = max_iterations
    if (
        resolved_max_iterations is not None
        and resolved_max_iterations < session.completed_iterations
    ):
        raise typer.BadParameter(
            "The new --max-iterations must be >= the current completed iteration count."
        )
    if resolved_max_iterations is None and session.completed_iterations >= session.max_iterations:
        raise typer.BadParameter(
            "Session budget is exhausted; provide a larger --max-iterations to resume."
        )
    updated = event_store.update_session(
        session_id,
        status=SessionStatus.PENDING,
        max_iterations=resolved_max_iterations,
        clear_error_message=True,
        note=note,
    )
    job = QueueStore(workspace_config.run.artifact_root / "archonlab.db").enqueue_session_quantum(
        config,
        session_id=updated.session_id,
        priority=priority,
    )
    typer.echo(f"Session: {updated.session_id}")
    typer.echo(f"Status: {updated.status.value}")
    typer.echo(f"Max iterations: {updated.max_iterations}")
    typer.echo(f"Enqueued job: {job.id}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
