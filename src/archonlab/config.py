from __future__ import annotations

import tomllib
from pathlib import Path

from .models import AppConfig, ProjectConfig, RunConfig, WorkflowMode


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


def load_config(config_path: Path) -> AppConfig:
    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    base_dir = config_path.parent.resolve()
    project_raw = raw["project"]
    run_raw = raw.get("run", {})

    project_path = _resolve_path(base_dir, project_raw["project_path"])
    archon_path = _resolve_path(base_dir, project_raw["archon_path"])
    artifact_root = _resolve_path(base_dir, run_raw.get("artifact_root", "artifacts"))
    workflow_spec = (
        _resolve_path(base_dir, run_raw["workflow_spec"])
        if "workflow_spec" in run_raw
        else None
    )

    return AppConfig(
        project=ProjectConfig(
            name=project_raw.get("name", project_path.name),
            project_path=project_path,
            archon_path=archon_path,
            backend=project_raw.get("backend", "archon"),
        ),
        run=RunConfig(
            workflow=WorkflowMode(run_raw.get("workflow", WorkflowMode.ADAPTIVE_LOOP)),
            workflow_spec=workflow_spec,
            stage_policy=run_raw.get("stage_policy", "auto"),
            max_iterations=run_raw.get("max_iterations", 10),
            max_parallel=run_raw.get("max_parallel", 8),
            review=run_raw.get("review", True),
            dry_run=run_raw.get("dry_run", True),
            artifact_root=artifact_root,
        ),
    )


def render_config(
    *,
    project_name: str,
    project_path: Path,
    archon_path: Path,
    artifact_root: Path,
    workflow: WorkflowMode,
    workflow_spec: Path | None,
    dry_run: bool,
) -> str:
    content = (
        "[project]\n"
        f'name = "{project_name}"\n'
        'backend = "archon"\n'
        f'project_path = "{project_path}"\n'
        f'archon_path = "{archon_path}"\n\n'
        "[run]\n"
        f'workflow = "{workflow.value}"\n'
        'stage_policy = "auto"\n'
        "max_iterations = 10\n"
        "max_parallel = 8\n"
        "review = true\n"
        f"dry_run = {'true' if dry_run else 'false'}\n"
        f'artifact_root = "{artifact_root}"\n'
    )
    if workflow_spec is not None:
        content += f'workflow_spec = "{workflow_spec}"\n'
    return content


def init_config(
    *,
    config_path: Path,
    project_path: Path,
    archon_path: Path,
    artifact_root: Path,
    workflow: WorkflowMode = WorkflowMode.ADAPTIVE_LOOP,
    workflow_spec: Path | None = None,
    dry_run: bool = True,
    force: bool = False,
) -> Path:
    if config_path.exists() and not force:
        raise FileExistsError(f"Config already exists at {config_path}")

    content = render_config(
        project_name=project_path.name,
        project_path=project_path,
        archon_path=archon_path,
        artifact_root=artifact_root,
        workflow=workflow,
        workflow_spec=workflow_spec,
        dry_run=dry_run,
    )
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(content, encoding="utf-8")
    return config_path
