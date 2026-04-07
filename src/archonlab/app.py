from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import typer

from .batch import BatchRunner
from .benchmark import BenchmarkRunService, load_benchmark_manifest
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
from .executors import reset_provider_pool_health, snapshot_provider_pool_health
from .experiment_ledger import (
    build_experiment_replay,
    compare_experiment_ledgers,
    load_experiment_ledger,
)
from .fleet import FleetController, create_worker_launcher, persist_batch_fleet_run
from .ledger import load_benchmark_ledger
from .models import (
    ExecutorConfig,
    ExecutorKind,
    ExperimentLedger,
    ProjectSession,
    ProviderConfig,
    ProviderKind,
    ProviderPoolConfig,
    QueueJobStatus,
    SessionStatus,
    WorkflowMode,
    WorktreeLease,
)
from .queue import QueueStore
from .services import RunService
from .workspace_daemon import (
    WorkspaceDaemonRunner,
    load_workspace_daemon_state,
    workspace_daemon_state_path,
)
from .workspace_loop import WorkspaceLoopController, request_workspace_loop_stop
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
workspace_daemon_app = typer.Typer(no_args_is_help=True)
app.add_typer(project_app, name="project")
app.add_typer(run_app, name="run")
app.add_typer(benchmark_app, name="benchmark")
app.add_typer(worktree_app, name="worktree")
app.add_typer(control_app, name="control")
app.add_typer(dashboard_app, name="dashboard")
app.add_typer(queue_app, name="queue")
app.add_typer(workspace_app, name="workspace")
workspace_app.add_typer(workspace_daemon_app, name="daemon")


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


def _exception_message(error: Exception) -> str:
    return str(error.args[0]) if error.args else str(error)


def _resolve_queue_runtime(config_path: Path) -> tuple[Path, int]:
    resolved_config_path = config_path.resolve()
    try:
        workspace_config = load_workspace_config(resolved_config_path)
        return workspace_config.run.artifact_root, workspace_config.run.max_parallel
    except (KeyError, ValueError):
        app_config = load_config(resolved_config_path)
        return app_config.run.artifact_root, app_config.run.max_parallel


def _resolve_workspace_name(config_path: Path) -> str:
    resolved_config_path = config_path.resolve()
    try:
        return load_workspace_config(resolved_config_path).name
    except (KeyError, ValueError):
        return "standalone"


def _resolve_batch_provider_pools(
    config_path: Path,
) -> dict[str, ProviderPoolConfig]:
    resolved_config_path = config_path.resolve()
    try:
        workspace_config = load_workspace_config(resolved_config_path)
        if workspace_config.provider.pool is None:
            return {}
        return workspace_config.provider_pools
    except (KeyError, ValueError):
        app_config = load_config(resolved_config_path)
        if app_config.provider.pool is None:
            return {}
        return app_config.provider_pools


def _resolve_provider_runtime(
    config_path: Path,
) -> tuple[str | None, dict[str, ProviderPoolConfig]]:
    resolved_config_path = config_path.resolve()
    try:
        workspace_config = load_workspace_config(resolved_config_path)
        return workspace_config.provider.pool, workspace_config.provider_pools
    except (KeyError, ValueError):
        app_config = load_config(resolved_config_path)
        return app_config.provider.pool, app_config.provider_pools


def _resolve_benchmark_artifact_root(
    *,
    manifest_path: Path | None,
    artifact_root: Path | None,
) -> Path:
    if manifest_path is not None and artifact_root is not None:
        raise typer.BadParameter("Specify either --manifest or --artifact-root, not both.")
    if manifest_path is None and artifact_root is None:
        raise typer.BadParameter("Specify --manifest or --artifact-root.")
    if manifest_path is not None:
        return load_benchmark_manifest(manifest_path.resolve()).benchmark.artifact_root
    if artifact_root is None:
        raise typer.BadParameter("Specify --manifest or --artifact-root.")
    return artifact_root.resolve()


@app.command()
def doctor(
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Optional project or workspace config to enrich checks with local paths.",
        ),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
) -> None:
    project = None
    lean_analyzer = None
    if config is not None:
        resolved_config = config.resolve()
        try:
            workspace_config = load_workspace_config(resolved_config)
            project = workspace_config.projects[0].as_project_config()
            lean_analyzer = workspace_config.lean_analyzer
        except (KeyError, ValueError):
            app_config = load_config(resolved_config)
            project = app_config.project
            lean_analyzer = app_config.lean_analyzer
    report = gather_doctor_report(project, lean_analyzer=lean_analyzer)
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
    latest_loop = next(
        iter(store.list_workspace_loop_runs(workspace_id=workspace_config.name, limit=1)),
        None,
    )
    latest_fleet = next(
        iter(store.list_fleet_runs(workspace_id=workspace_config.name, limit=1)),
        None,
    )
    daemon_state = None
    if workspace_daemon_state_path(workspace_config.run.artifact_root).exists():
        daemon_state = load_workspace_daemon_state(
            workspace_config.run.artifact_root,
            workspace_id=workspace_config.name,
            config_path=config,
        )
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
        "latest_loop": (
            latest_loop.model_dump(mode="json") if latest_loop is not None else None
        ),
        "latest_fleet": (
            latest_fleet.model_dump(mode="json") if latest_fleet is not None else None
        ),
        "daemon": (
            daemon_state.model_dump(mode="json") if daemon_state is not None else None
        ),
    }
    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    typer.echo(f"Workspace: {workspace_config.name}")
    typer.echo(f"Artifact root: {workspace_config.run.artifact_root}")
    typer.echo(f"Projects: {len(workspace_config.projects)}")
    typer.echo(f"Sessions: {len(sessions)}")
    if latest_loop is not None:
        loop_id = latest_loop.loop_run_id or latest_loop.loop_id
        typer.echo(
            f"Latest loop: {loop_id} | stop={latest_loop.stop_reason} | "
            f"cycles={latest_loop.cycles_completed} | "
            f"processed={latest_loop.total_processed_jobs}"
        )
    if latest_fleet is not None:
        typer.echo(
            f"Latest fleet: {latest_fleet.fleet_run_id} | stop={latest_fleet.stop_reason} | "
            f"cycles={latest_fleet.cycles_completed} | "
            f"processed={latest_fleet.total_processed_jobs}"
        )
    if daemon_state is not None:
        typer.echo(
            f"Daemon: {daemon_state.status} | ticks={daemon_state.tick_count} | "
            f"last_loop={daemon_state.last_loop_run_id or '-'} | "
            f"reason={daemon_state.exit_reason or daemon_state.request_reason or '-'}"
        )
    for row in project_rows:
        typer.echo(
            f"{row['project_id']} | enabled={row['enabled']} | workflow={row['workflow']} | "
            f"dry_run={row['dry_run']} | sessions={row['session_count']} | "
            f"running={row['running_sessions']}"
        )


@workspace_app.command("loops")
def workspace_loops(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, max=200, help="Number of loop runs to show."),
    ] = 20,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable JSON."),
    ] = False,
) -> None:
    workspace_config = load_workspace_config(config)
    store = EventStore(workspace_config.run.artifact_root / "archonlab.db")
    loops = store.list_workspace_loop_runs(workspace_id=workspace_config.name, limit=limit)
    payload = {
        "workspace": workspace_config.name,
        "loops": [loop.model_dump(mode="json") for loop in loops],
    }
    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if not loops:
        typer.echo("No workspace loops.")
        return
    for loop in loops:
        loop_id = loop.loop_run_id or loop.loop_id
        typer.echo(
            f"{loop_id} | project={loop.project_id or '-'} | stop={loop.stop_reason} | "
            f"cycles={loop.cycles_completed} | processed={loop.total_processed_jobs}"
        )


@workspace_app.command("fleets")
def workspace_fleets(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, max=200, help="Number of fleet runs to show."),
    ] = 20,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable JSON."),
    ] = False,
) -> None:
    workspace_config = load_workspace_config(config)
    store = EventStore(workspace_config.run.artifact_root / "archonlab.db")
    fleets = store.list_fleet_runs(workspace_id=workspace_config.name, limit=limit)
    payload = {
        "workspace": workspace_config.name,
        "fleets": [fleet.model_dump(mode="json") for fleet in fleets],
    }
    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if not fleets:
        typer.echo("No workspace fleets.")
        return
    for fleet in fleets:
        typer.echo(
            f"{fleet.fleet_run_id} | launcher={fleet.launcher or '-'} | "
            f"stop={fleet.stop_reason} | cycles={fleet.cycles_completed} | "
            f"processed={fleet.total_processed_jobs}"
        )


@workspace_app.command("stop-loop")
def workspace_stop_loop(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    loop_run_id: Annotated[
        str,
        typer.Option("--loop-run-id", help="Workspace loop run identifier."),
    ] = "",
    reason: Annotated[
        str,
        typer.Option("--reason", help="Operator reason recorded in the stop request."),
    ] = "operator_stop_requested",
) -> None:
    workspace_config = load_workspace_config(config)
    store = EventStore(workspace_config.run.artifact_root / "archonlab.db")
    loop = store.get_workspace_loop_run(loop_run_id)
    if loop is None:
        raise typer.BadParameter(f"Unknown workspace loop run: {loop_run_id}")
    if loop.finished_at is not None:
        raise typer.BadParameter(f"Workspace loop is already finished: {loop_run_id}")
    if loop.artifact_dir is None:
        raise typer.BadParameter(f"Workspace loop has no artifact directory: {loop_run_id}")
    state = request_workspace_loop_stop(
        loop.artifact_dir,
        reason=reason,
        requested_by="workspace.stop-loop",
    )
    typer.echo(f"Stop requested: {loop_run_id}")
    typer.echo(f"Reason: {state.reason or '-'}")


@workspace_daemon_app.command("run")
def workspace_daemon_run(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    project_id: Annotated[
        str | None,
        typer.Option("--project-id", help="Optional workspace project filter."),
    ] = None,
    tags: Annotated[
        list[str] | None,
        typer.Option(
            "--tag",
            help="Repeat to target projects containing all specified workspace tags.",
        ),
    ] = None,
    max_iterations: Annotated[
        int | None,
        typer.Option("--max-iterations", min=1, help="Optional session iteration cap."),
    ] = None,
    dry_run: Annotated[
        bool | None,
        typer.Option("--dry-run/--execute", help="Override the workspace project run mode."),
    ] = None,
    priority: Annotated[
        int,
        typer.Option("--priority", help="Base queue priority for created session jobs."),
    ] = 0,
    max_ticks: Annotated[
        int | None,
        typer.Option("--max-ticks", min=1, help="Optional maximum daemon ticks before exit."),
    ] = None,
    poll_seconds: Annotated[
        float,
        typer.Option("--poll-seconds", min=0.0, help="Delay between daemon ticks."),
    ] = 5.0,
    fleet_max_cycles: Annotated[
        int,
        typer.Option("--fleet-max-cycles", min=1, help="Maximum autoscaling cycles per tick."),
    ] = 10,
    fleet_idle_cycles: Annotated[
        int,
        typer.Option(
            "--fleet-idle-cycles",
            min=1,
            help="Stop each fleet controller after this many idle cycles.",
        ),
    ] = 1,
    workers: Annotated[
        int | None,
        typer.Option("--workers", min=1, help="Optional cap on workers launched per tick."),
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
            help="Optional maximum jobs processed by each worker in a tick.",
        ),
    ] = None,
    queue_poll_seconds: Annotated[
        float,
        typer.Option("--queue-poll-seconds", min=0.1, help="Queue polling interval per tick."),
    ] = 2.0,
    queue_idle_timeout_seconds: Annotated[
        float,
        typer.Option(
            "--queue-idle-timeout-seconds",
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
    state = WorkspaceDaemonRunner(config).run(
        project_id=project_id,
        project_tags=tags,
        max_iterations=max_iterations,
        dry_run=dry_run,
        priority=priority,
        max_ticks=max_ticks,
        poll_seconds=poll_seconds,
        fleet_max_cycles=fleet_max_cycles,
        fleet_idle_cycles=fleet_idle_cycles,
        workers=workers,
        target_jobs_per_worker=target_jobs_per_worker,
        max_jobs_per_worker=max_jobs_per_worker,
        queue_poll_seconds=queue_poll_seconds,
        queue_idle_timeout_seconds=queue_idle_timeout_seconds,
        stale_after_seconds=stale_after_seconds,
    )
    typer.echo(f"Daemon: {state.daemon_run_id or '-'}")
    typer.echo(f"Status: {state.status}")
    typer.echo(f"Ticks: {state.tick_count}")
    typer.echo(f"Last loop: {state.last_loop_run_id or '-'}")
    typer.echo(f"Reason: {state.exit_reason or '-'}")


@workspace_daemon_app.command("status")
def workspace_daemon_status(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable JSON."),
    ] = False,
) -> None:
    state = WorkspaceDaemonRunner(config).status()
    if json_output:
        typer.echo(json.dumps(state.model_dump(mode="json"), ensure_ascii=False, indent=2))
        return
    typer.echo(f"Workspace: {state.workspace_id or '-'}")
    typer.echo(f"Daemon: {state.daemon_run_id or '-'}")
    typer.echo(f"Status: {state.status}")
    typer.echo(f"Ticks: {state.tick_count}")
    typer.echo(f"Last loop: {state.last_loop_run_id or '-'}")
    typer.echo(f"Stop requested: {'yes' if state.stop_requested else 'no'}")
    typer.echo(f"Reason: {state.exit_reason or state.request_reason or '-'}")


@workspace_daemon_app.command("stop")
def workspace_daemon_stop(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    reason: Annotated[
        str,
        typer.Option("--reason", help="Operator reason recorded in the stop request."),
    ] = "operator_stop_requested",
) -> None:
    state = WorkspaceDaemonRunner(config).request_stop(
        reason=reason,
        requested_by="workspace.daemon.stop",
    )
    typer.echo(f"Stop requested: {state.workspace_id or '-'}")
    typer.echo(f"Reason: {state.request_reason or '-'}")


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
        config_path=config,
    )
    if result.loop_run_id:
        typer.echo(f"Loop: {result.loop_run_id}")
    if result.artifact_dir is not None:
        typer.echo(f"Artifacts: {result.artifact_dir}")
    typer.echo(f"Session: {result.session_id}")
    typer.echo(f"Project: {result.project_id}")
    typer.echo(f"Status: {result.status.value}")
    typer.echo(f"Completed iterations: {result.completed_iterations}")
    typer.echo(f"Stop reason: {result.stop_reason}")
    typer.echo(f"Runs: {len(result.run_ids)}")


@workspace_app.command("run")
def workspace_run(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    project_id: Annotated[
        str | None,
        typer.Option("--project-id", help="Optional workspace project filter."),
    ] = None,
    tags: Annotated[
        list[str] | None,
        typer.Option(
            "--tag",
            help="Repeat to target projects containing all specified workspace tags.",
        ),
    ] = None,
    max_iterations: Annotated[
        int | None,
        typer.Option("--max-iterations", min=1, help="Optional session iteration cap."),
    ] = None,
    dry_run: Annotated[
        bool | None,
        typer.Option("--dry-run/--execute", help="Override the workspace project run mode."),
    ] = None,
    priority: Annotated[
        int,
        typer.Option("--priority", help="Base queue priority for created session jobs."),
    ] = 0,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional session note."),
    ] = None,
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
    launcher: Annotated[
        str,
        typer.Option(
            "--launcher",
            help="Worker launcher kind used by autoscaling control cycles.",
        ),
    ] = "in_process",
) -> None:
    workspace_config = load_workspace_config(config)
    queue_store = QueueStore(workspace_config.run.artifact_root / "archonlab.db")
    jobs = queue_store.enqueue_workspace_sessions(
        config,
        project_ids=[project_id] if project_id is not None else None,
        project_tags=tags,
        max_iterations=max_iterations,
        dry_run=dry_run,
        priority=priority,
        note=note,
    )
    typer.echo(f"Enqueued sessions: {len(jobs)}")
    if not jobs:
        typer.echo("Stop reason: no_sessions_enqueued")
        typer.echo("Processed: 0")
        typer.echo("Paused: 0")
        typer.echo("Failed: 0")
        typer.echo("Workers launched: 0")
        return

    batch_runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(workspace_config.run.artifact_root),
        artifact_root=workspace_config.run.artifact_root,
        slot_limit=workers or workspace_config.run.max_parallel,
        provider_pools=(
            workspace_config.provider_pools
            if workspace_config.provider.pool is not None
            else None
        ),
    )
    try:
        worker_launcher = create_worker_launcher(launcher, config)
    except ValueError as error:
        raise typer.BadParameter(_exception_message(error), param_hint="--launcher") from error
    result = FleetController(
        queue_store=queue_store,
        batch_runner=batch_runner,
        worker_launcher=worker_launcher,
        config_path=config,
        workspace_id=workspace_config.name,
        note=note,
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
    typer.echo(f"Fleet: {result.fleet_run_id}")
    if result.artifact_dir is not None:
        typer.echo(f"Artifacts: {result.artifact_dir}")
    typer.echo(f"Cycles: {result.cycles_completed}")
    typer.echo(f"Stop reason: {result.stop_reason}")
    typer.echo(f"Processed: {result.total_processed_jobs}")
    typer.echo(f"Paused: {result.total_paused_jobs}")
    typer.echo(f"Failed: {result.total_failed_jobs}")
    typer.echo(f"Workers launched: {result.total_workers_launched}")


@workspace_app.command("loop")
def workspace_loop(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    project_id: Annotated[
        str | None,
        typer.Option("--project-id", help="Optional workspace project filter."),
    ] = None,
    tags: Annotated[
        list[str] | None,
        typer.Option(
            "--tag",
            help="Repeat to target projects containing all specified workspace tags.",
        ),
    ] = None,
    max_iterations: Annotated[
        int | None,
        typer.Option("--max-iterations", min=1, help="Optional session iteration cap."),
    ] = None,
    dry_run: Annotated[
        bool | None,
        typer.Option("--dry-run/--execute", help="Override the workspace project run mode."),
    ] = None,
    priority: Annotated[
        int,
        typer.Option("--priority", help="Base queue priority for created session jobs."),
    ] = 0,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional session note."),
    ] = None,
    max_cycles: Annotated[
        int,
        typer.Option("--max-cycles", min=1, help="Maximum workspace control cycles."),
    ] = 10,
    idle_cycles: Annotated[
        int,
        typer.Option(
            "--idle-cycles",
            min=1,
            help="Stop after this many consecutive workspace idle cycles.",
        ),
    ] = 1,
    sleep_seconds: Annotated[
        float,
        typer.Option(
            "--sleep-seconds",
            min=0.0,
            help="Optional delay between workspace control cycles.",
        ),
    ] = 0.0,
    fleet_max_cycles: Annotated[
        int,
        typer.Option("--fleet-max-cycles", min=1, help="Maximum autoscaling cycles per tick."),
    ] = 10,
    fleet_idle_cycles: Annotated[
        int,
        typer.Option(
            "--fleet-idle-cycles",
            min=1,
            help="Stop a fleet tick after this many consecutive no-progress cycles.",
        ),
    ] = 1,
    workers: Annotated[
        int | None,
        typer.Option(
            "--workers",
            min=1,
            help="Optional cap on workers launched per fleet tick.",
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
            help="Optional maximum jobs processed by each worker in a tick.",
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
    launcher: Annotated[
        str,
        typer.Option(
            "--launcher",
            help="Worker launcher kind used by autoscaling control cycles.",
        ),
    ] = "in_process",
) -> None:
    try:
        worker_launcher = create_worker_launcher(launcher, config)
    except ValueError as error:
        raise typer.BadParameter(_exception_message(error), param_hint="--launcher") from error

    result = WorkspaceLoopController(
        config,
        worker_launcher=worker_launcher,
    ).run(
        project_id=project_id,
        project_tags=tags,
        max_iterations=max_iterations,
        dry_run=dry_run,
        priority=priority,
        note=note,
        max_cycles=max_cycles,
        idle_cycles=idle_cycles,
        sleep_seconds=sleep_seconds,
        fleet_max_cycles=fleet_max_cycles,
        fleet_idle_cycles=fleet_idle_cycles,
        workers=workers,
        target_jobs_per_worker=target_jobs_per_worker,
        max_jobs_per_worker=max_jobs_per_worker,
        queue_poll_seconds=poll_seconds,
        queue_idle_timeout_seconds=idle_timeout_seconds,
        stale_after_seconds=stale_after_seconds,
    )
    loop_id = result.loop_id or result.loop_run_id
    if loop_id:
        typer.echo(f"Loop: {loop_id}")
    if result.artifact_dir is not None:
        typer.echo(f"Artifacts: {result.artifact_dir}")
    typer.echo(f"Cycles: {result.cycles_completed}")
    typer.echo(f"Stop reason: {result.stop_reason}")
    typer.echo(f"Scheduled jobs: {result.total_scheduled_jobs}")
    typer.echo(f"Processed: {result.total_processed_jobs}")
    typer.echo(f"Paused: {result.total_paused_jobs}")
    typer.echo(f"Failed: {result.total_failed_jobs}")
    typer.echo(f"Workers launched: {result.total_workers_launched}")


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


@run_app.command("loops")
def run_loops(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, max=200, help="Number of loop runs to show."),
    ] = 20,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable JSON."),
    ] = False,
) -> None:
    app_config = load_config(config)
    store = EventStore(app_config.run.artifact_root / "archonlab.db")
    loops = store.list_run_loop_runs(
        workspace_id="standalone",
        project_id=app_config.project.name,
        limit=limit,
    )
    payload = {
        "project": app_config.project.name,
        "workspace": "standalone",
        "loops": [loop.model_dump(mode="json") for loop in loops],
    }
    if json_output:
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if not loops:
        typer.echo("No run loops.")
        return
    for loop in loops:
        typer.echo(
            f"{loop.loop_run_id} | stop={loop.stop_reason} | "
            f"iterations={loop.completed_iterations}/{loop.max_iterations} | "
            f"runs={len(loop.run_ids)}"
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
        config_path=config,
    )
    if result.loop_run_id:
        typer.echo(f"Loop: {result.loop_run_id}")
    if result.artifact_dir is not None:
        typer.echo(f"Artifacts: {result.artifact_dir}")
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


@benchmark_app.command("runs")
def benchmark_runs(
    manifest: Annotated[
        Path | None,
        typer.Option("--manifest", exists=True, help="Benchmark manifest TOML."),
    ] = None,
    artifact_root: Annotated[
        Path | None,
        typer.Option("--artifact-root", help="Benchmark artifact root."),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, help="Maximum number of benchmark runs to list."),
    ] = 20,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
) -> None:
    resolved_artifact_root = _resolve_benchmark_artifact_root(
        manifest_path=manifest,
        artifact_root=artifact_root,
    )
    runs = EventStore(resolved_artifact_root / "archonlab.db").list_benchmark_runs(limit=limit)
    if json_output:
        typer.echo(
            json.dumps(
                [result.model_dump(mode="json") for result in runs],
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    typer.echo(f"Artifact Root: {resolved_artifact_root}")
    typer.echo(f"Runs: {len(runs)}")
    if not runs:
        typer.echo("No benchmark runs recorded.")
        return
    for result in runs:
        typer.echo(
            f"{result.run_id} | {result.status.value} | "
            f"benchmark={result.benchmark.name} | projects={len(result.projects)} | "
            f"started={result.started_at.isoformat()}"
        )


@benchmark_app.command("run-detail")
def benchmark_run_detail(
    run_id: Annotated[
        str,
        typer.Option("--run-id", help="Benchmark run identifier."),
    ],
    manifest: Annotated[
        Path | None,
        typer.Option("--manifest", exists=True, help="Benchmark manifest TOML."),
    ] = None,
    artifact_root: Annotated[
        Path | None,
        typer.Option("--artifact-root", help="Benchmark artifact root."),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
) -> None:
    resolved_artifact_root = _resolve_benchmark_artifact_root(
        manifest_path=manifest,
        artifact_root=artifact_root,
    )
    result = EventStore(resolved_artifact_root / "archonlab.db").get_benchmark_run(run_id)
    if result is None:
        raise typer.BadParameter(f"Unknown benchmark run: {run_id}")
    if json_output:
        typer.echo(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2))
        return

    typer.echo(f"Benchmark: {result.benchmark.name}")
    typer.echo(f"Run: {result.run_id}")
    typer.echo(f"Status: {result.status.value}")
    typer.echo(f"Artifacts: {result.artifact_dir}")
    if result.ledger_path is not None:
        typer.echo(f"Ledger: {result.ledger_path}")
    typer.echo(f"Projects: {len(result.projects)}")


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


@benchmark_app.command("replay")
def benchmark_replay(
    summary: Annotated[
        Path | None,
        typer.Option("--summary", exists=True, help="Benchmark summary JSON."),
    ] = None,
    ledger: Annotated[
        Path | None,
        typer.Option("--ledger", exists=True, help="Experiment ledger JSON."),
    ] = None,
    project_id: Annotated[
        str,
        typer.Option("--project-id", help="Project identifier inside the experiment ledger."),
    ] = "",
    theorem_name: Annotated[
        str | None,
        typer.Option("--theorem-name", help="Optional theorem filter."),
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
) -> None:
    experiment_ledger = _load_experiment_ledger_from_options(
        summary=summary,
        ledger=ledger,
    )
    try:
        replay = build_experiment_replay(
            experiment_ledger=experiment_ledger,
            project_id=project_id,
            theorem_name=theorem_name,
        )
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc

    if json_output:
        typer.echo(json.dumps(replay.model_dump(mode="json"), ensure_ascii=False, indent=2))
        return

    typer.echo(f"Benchmark: {replay.benchmark_name}")
    typer.echo(f"Run: {replay.benchmark_run_id}")
    typer.echo(f"Project: {replay.project_id}")
    typer.echo(f"Status: {replay.run_status.value}")
    if replay.artifact_dir is not None:
        typer.echo(f"Artifacts: {replay.artifact_dir}")
    if replay.run_summary_path is not None:
        typer.echo(f"Run Summary: {replay.run_summary_path}")
    if replay.execution_path is not None:
        typer.echo(f"Execution: {replay.execution_path}")
    typer.echo(f"Theorems: {len(replay.theorem_outcomes)}")


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
    tags: Annotated[
        list[str] | None,
        typer.Option(
            "--tag",
            help="Repeat to target projects containing all specified workspace tags.",
        ),
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
        project_tags=tags,
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
    artifact_root, max_parallel = _resolve_queue_runtime(config)
    runner = BatchRunner(
        queue_store=QueueStore(artifact_root / "archonlab.db"),
        control_service=ControlService(artifact_root),
        artifact_root=artifact_root,
        slot_limit=slots or max_parallel,
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
    artifact_root, max_parallel = _resolve_queue_runtime(config)
    queue_store = QueueStore(artifact_root / "archonlab.db")
    provider_pools = _resolve_batch_provider_pools(config)
    initial_plan = queue_store.plan_fleet(
        target_jobs_per_worker=target_jobs_per_worker,
        stale_after_seconds=stale_after_seconds,
        provider_pools=provider_pools or None,
        provider_health_db_path=artifact_root / "archonlab.db",
    )
    started_at = datetime.now(UTC)
    runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(artifact_root),
        artifact_root=artifact_root,
        slot_limit=workers or max_parallel,
        provider_pools=provider_pools,
    )
    report = runner.run_fleet(
        worker_count=workers if plan_driven else (workers or max_parallel),
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
    fleet_run = persist_batch_fleet_run(
        queue_store=queue_store,
        artifact_root=artifact_root,
        initial_plan=initial_plan,
        report=report,
        started_at=started_at,
        target_jobs_per_worker=target_jobs_per_worker,
        stale_after_seconds=stale_after_seconds,
        workspace_id=_resolve_workspace_name(config),
        config_path=config,
        launcher="batch_runner",
        provider_pools=provider_pools or None,
        provider_health_db_path=artifact_root / "archonlab.db",
        request_payload={
            "worker_count": workers,
            "plan_driven": plan_driven,
            "target_jobs_per_worker": target_jobs_per_worker,
            "max_jobs_per_worker": max_jobs_per_worker,
            "poll_seconds": poll_seconds,
            "idle_timeout_seconds": idle_timeout_seconds,
            "stale_after_seconds": stale_after_seconds,
            "executor_kinds": _parse_executor_kinds(executor_kinds),
            "provider_kinds": _parse_provider_kinds(provider_kinds),
            "models": _parse_csv_strings(models),
            "cost_tiers": _parse_csv_strings(cost_tiers),
            "endpoint_classes": _parse_csv_strings(endpoint_classes),
        },
    )
    typer.echo(f"Fleet: {fleet_run.fleet_run_id}")
    if fleet_run.artifact_dir is not None:
        typer.echo(f"Artifacts: {fleet_run.artifact_dir}")
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
    artifact_root, _ = _resolve_queue_runtime(config)
    provider_pools = _resolve_batch_provider_pools(config)
    plan = QueueStore(artifact_root / "archonlab.db").plan_fleet(
        target_jobs_per_worker=target_jobs_per_worker,
        stale_after_seconds=stale_after_seconds,
        provider_pools=provider_pools or None,
        provider_health_db_path=artifact_root / "archonlab.db",
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
        provider_capacity_status = profile.provider_capacity_status or "unknown"
        available_provider_members = (
            str(profile.available_provider_members)
            if profile.available_provider_members is not None
            else "-"
        )
        typer.echo(
            f"{profile.profile_id} | phase={dominant_phase} | jobs={profile.active_jobs} | "
            f"dedicated={profile.dedicated_workers} | matching={profile.matching_workers} | "
            f"total={profile.recommended_total_workers} | "
            f"add={profile.recommended_additional_workers} | executor={executor_kinds} | "
            f"provider_kind={provider_kinds} | provider={provider_capacity_status}/"
            f"{available_provider_members} | model={models} | cost={cost_tiers} | "
            f"endpoint={endpoints}"
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
    launcher: Annotated[
        str,
        typer.Option(
            "--launcher",
            help="Worker launcher kind used by autoscaling control cycles.",
        ),
    ] = "in_process",
) -> None:
    artifact_root, max_parallel = _resolve_queue_runtime(config)
    queue_store = QueueStore(artifact_root / "archonlab.db")
    batch_runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(artifact_root),
        artifact_root=artifact_root,
        slot_limit=workers or max_parallel,
        provider_pools=_resolve_batch_provider_pools(config),
    )
    try:
        worker_launcher = create_worker_launcher(launcher, config)
    except ValueError as error:
        raise typer.BadParameter(_exception_message(error), param_hint="--launcher") from error
    result = FleetController(
        queue_store=queue_store,
        batch_runner=batch_runner,
        worker_launcher=worker_launcher,
        config_path=config,
        workspace_id=_resolve_workspace_name(config),
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
    typer.echo(f"Fleet: {result.fleet_run_id}")
    if result.artifact_dir is not None:
        typer.echo(f"Artifacts: {result.artifact_dir}")
    typer.echo(f"Cycles: {result.cycles_completed}")
    typer.echo(f"Stop reason: {result.stop_reason}")
    typer.echo(f"Processed: {result.total_processed_jobs}")
    typer.echo(f"Paused: {result.total_paused_jobs}")
    typer.echo(f"Failed: {result.total_failed_jobs}")
    typer.echo(f"Workers launched: {result.total_workers_launched}")


@queue_app.command("fleet-runs")
def queue_fleet_runs(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    limit: Annotated[
        int, typer.Option("--limit", min=1, max=200, help="Number of fleet runs to show.")
    ] = 20,
    json_output: Annotated[
        bool, typer.Option("--json", help="Print machine-readable JSON.")
    ] = False,
) -> None:
    artifact_root, _ = _resolve_queue_runtime(config)
    workspace_id = _resolve_workspace_name(config)
    runs = EventStore(artifact_root / "archonlab.db").list_fleet_runs(
        workspace_id=workspace_id,
        limit=limit,
    )
    if json_output:
        typer.echo(
            json.dumps(
                [item.model_dump(mode="json") for item in runs],
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if not runs:
        typer.echo("No fleet runs.")
        return
    for run in runs:
        fleet_id = run.fleet_run_id or "-"
        typer.echo(
            f"{fleet_id} | stop={run.stop_reason} | cycles={run.cycles_completed} | "
            f"processed={run.total_processed_jobs} | workers={run.total_workers_launched}"
        )


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
    artifact_root, _ = _resolve_queue_runtime(config)
    jobs = QueueStore(artifact_root / "archonlab.db").list_jobs(
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


@queue_app.command("runtime-summary")
def queue_runtime_summary(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    limit: Annotated[
        int,
        typer.Option("--limit", min=1, max=1000, help="Number of executor events to scan."),
    ] = 200,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable JSON."),
    ] = False,
) -> None:
    artifact_root, _ = _resolve_queue_runtime(config)
    summary = EventStore(artifact_root / "archonlab.db").summarize_provider_runtime(limit=limit)
    if json_output:
        typer.echo(
            json.dumps(
                [item.model_dump(mode="json") for item in summary],
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if not summary:
        typer.echo("No provider runtime telemetry.")
        return
    for pool in summary:
        typer.echo(
            f"{pool.pool_name} | success={pool.success_count} | failed={pool.failure_count} | "
            f"retries={pool.total_retry_count} | cost={pool.total_cost_estimate:.6f} | "
            f"health={pool.last_health_status or '-'}"
        )
        for member in pool.members:
            typer.echo(
                f"  {member.member_name} | success={member.success_count} | "
                f"failed={member.failure_count} | retries={member.retry_count} | "
                f"cost={member.total_cost_estimate:.6f} | health={member.last_health_status or '-'}"
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
            f"job={(active_job.id if active_job is not None else '-')} | "
            f"stop={session.last_stop_reason or '-'} | "
            f"resume={session.last_resume_reason or '-'} | "
            f"owner={session.owner_worker_id or '-'}:{session.owner_job_id or '-'}"
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
    artifact_root, _ = _resolve_queue_runtime(config)
    workers = QueueStore(artifact_root / "archonlab.db").list_workers(
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


@queue_app.command("provider-health")
def queue_provider_health(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable provider pool health JSON."),
    ] = False,
) -> None:
    artifact_root, _ = _resolve_queue_runtime(config)
    default_pool, provider_pools = _resolve_provider_runtime(config)
    reports = snapshot_provider_pool_health(
        provider_pools,
        db_path=artifact_root / "archonlab.db",
    )
    if json_output:
        typer.echo(json.dumps([report.model_dump(mode="json") for report in reports], indent=2))
        return
    if not reports:
        typer.echo("No provider pools configured.")
        return
    for report in reports:
        default_marker = " default" if report.pool_name == default_pool else ""
        typer.echo(
            f"{report.pool_name}{default_marker} | {report.status.value} | "
            f"available={report.available_members}/{report.total_members} | "
            f"quarantined={report.quarantined_members}"
        )
        for member in report.members:
            typer.echo(
                f"{member.member_name} | {member.status.value} | "
                f"failures={member.consecutive_failures} | model={member.model or '-'}"
            )


@queue_app.command("reset-provider-health")
def queue_reset_provider_health(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Config file."),
    ] = Path("archonlab.toml"),
    pool: Annotated[
        str | None,
        typer.Option("--pool", help="Optional provider pool name."),
    ] = None,
    member: Annotated[
        str | None,
        typer.Option("--member", help="Optional provider pool member name."),
    ] = None,
) -> None:
    artifact_root, _ = _resolve_queue_runtime(config)
    _, provider_pools = _resolve_provider_runtime(config)
    if member is not None and pool is None:
        raise typer.BadParameter("--pool is required when --member is set")
    if pool is not None and pool not in provider_pools:
        raise typer.BadParameter(f"Unknown provider pool: {pool}")
    if pool is not None and member is not None:
        known_members = {pool_member.name for pool_member in provider_pools[pool].members}
        if member not in known_members:
            raise typer.BadParameter(f"Unknown provider pool member: {member}")
    removed = reset_provider_pool_health(
        pool_name=pool,
        member_name=member,
        db_path=artifact_root / "archonlab.db",
    )
    typer.echo(f"Reset health entries: {removed}")


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
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Print machine-readable worker report JSON."),
    ] = False,
) -> None:
    artifact_root, _ = _resolve_queue_runtime(config)
    runner = BatchRunner(
        queue_store=QueueStore(artifact_root / "archonlab.db"),
        control_service=ControlService(artifact_root),
        artifact_root=artifact_root,
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
    if json_output:
        typer.echo(report.model_dump_json(indent=2))
        return
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
    artifact_root, _ = _resolve_queue_runtime(config)
    workers = QueueStore(artifact_root / "archonlab.db").reap_stale_workers(
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
    artifact_root, _ = _resolve_queue_runtime(config)
    job = QueueStore(artifact_root / "archonlab.db").cancel(job_id, reason=reason)
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
    artifact_root, _ = _resolve_queue_runtime(config)
    job = QueueStore(artifact_root / "archonlab.db").requeue(job_id)
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
    resume_reason: Annotated[
        str | None,
        typer.Option("--resume-reason", help="Optional operator reason for resuming."),
    ] = None,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional updated session note."),
    ] = None,
) -> None:
    if not session_id:
        raise typer.BadParameter("--session-id is required")
    workspace_config = load_workspace_config(config)
    queue_store = QueueStore(workspace_config.run.artifact_root / "archonlab.db")
    try:
        updated, job = queue_store.resume_session(
            config,
            session_id=session_id,
            max_iterations=max_iterations,
            priority=priority,
            resume_reason=resume_reason,
            note=note,
        )
    except (KeyError, ValueError) as err:
        raise typer.BadParameter(_exception_message(err)) from err
    typer.echo(f"Session: {updated.session_id}")
    typer.echo(f"Status: {updated.status.value}")
    typer.echo(f"Max iterations: {updated.max_iterations}")
    typer.echo(f"Enqueued job: {job.id}")


@queue_app.command("resume-workspace")
def queue_resume_workspace(
    config: Annotated[
        Path,
        typer.Option("--config", exists=True, help="Workspace config file."),
    ] = Path("workspace.toml"),
    project_id: Annotated[
        str | None,
        typer.Option("--project-id", help="Optional project filter."),
    ] = None,
    tags: Annotated[
        list[str] | None,
        typer.Option(
            "--tag",
            help="Repeat to target projects containing all specified workspace tags.",
        ),
    ] = None,
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
        typer.Option("--priority", help="Base queue priority for resumed session jobs."),
    ] = 0,
    resume_reason: Annotated[
        str | None,
        typer.Option("--resume-reason", help="Optional operator reason for resuming."),
    ] = None,
    note: Annotated[
        str | None,
        typer.Option("--note", help="Optional updated session note."),
    ] = None,
) -> None:
    workspace_config = load_workspace_config(config)
    queue_store = QueueStore(workspace_config.run.artifact_root / "archonlab.db")
    result = queue_store.resume_workspace_sessions(
        config,
        project_ids=[project_id] if project_id is not None else None,
        project_tags=tags,
        max_iterations=max_iterations,
        priority=priority,
        resume_reason=resume_reason,
        note=note,
    )
    typer.echo(f"Resumed sessions: {len(result.resumed)}")
    for session, job in result.resumed:
        typer.echo(
            f"{session.session_id} | project={session.project_id} | "
            f"status={session.status.value} | max_iterations={session.max_iterations} | "
            f"job={job.id}"
        )
    if result.skipped:
        typer.echo(f"Skipped sessions: {len(result.skipped)}")
        for skipped in result.skipped:
            typer.echo(
                f"project={skipped.project_id} | session={skipped.session_id or '-'} | "
                f"reason={skipped.reason}"
            )


def main() -> None:
    app()


if __name__ == "__main__":
    main()
