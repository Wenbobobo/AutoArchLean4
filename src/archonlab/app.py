from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from .benchmark import BenchmarkRunService
from .checks import gather_doctor_report
from .config import init_config, load_config
from .control import ControlService
from .dashboard import create_dashboard_app
from .events import EventStore
from .models import WorkflowMode, WorktreeLease
from .services import RunService
from .worktree import WorktreeManager

app = typer.Typer(no_args_is_help=True, help="archonlab external orchestrator control plane")
project_app = typer.Typer(no_args_is_help=True)
run_app = typer.Typer(no_args_is_help=True)
benchmark_app = typer.Typer(no_args_is_help=True)
worktree_app = typer.Typer(no_args_is_help=True)
control_app = typer.Typer(no_args_is_help=True)
dashboard_app = typer.Typer(no_args_is_help=True)
app.add_typer(project_app, name="project")
app.add_typer(run_app, name="run")
app.add_typer(benchmark_app, name="benchmark")
app.add_typer(worktree_app, name="worktree")
app.add_typer(control_app, name="control")
app.add_typer(dashboard_app, name="dashboard")


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
        force=force,
    )
    typer.echo(f"Wrote config: {written}")
    typer.echo("Next steps:")
    typer.echo(f"  1. archonlab doctor --config {written}")
    typer.echo(f"  2. archonlab run start --config {written} --dry-run")


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
    if result.task_graph_path is not None:
        typer.echo(f"Task graph: {result.task_graph_path}")
    if result.supervisor_path is not None:
        typer.echo(f"Supervisor: {result.supervisor_path}")


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
) -> None:
    service = BenchmarkRunService(manifest)
    result = service.run(
        dry_run=dry_run,
        use_worktrees=use_worktrees,
        cleanup_worktrees=not keep_worktrees,
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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
