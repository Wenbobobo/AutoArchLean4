from __future__ import annotations

import tomllib
from pathlib import Path

from .models import (
    AppConfig,
    ExecutorConfig,
    ExecutorKind,
    ProjectConfig,
    ProviderConfig,
    ProviderKind,
    RunConfig,
    WorkflowMode,
)


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
    executor_raw = raw.get("executor", {})
    provider_raw = raw.get("provider", {})

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
        executor=ExecutorConfig(
            kind=ExecutorKind(executor_raw.get("kind", ExecutorKind.DRY_RUN)),
            command=executor_raw.get("command", "codex"),
            profile=executor_raw.get("profile"),
            auto_approve=executor_raw.get("auto_approve", False),
            skip_git_repo_check=executor_raw.get("skip_git_repo_check", True),
            sandbox=executor_raw.get("sandbox"),
            color=executor_raw.get("color", "never"),
            extra_args=executor_raw.get("extra_args", []),
            timeout_seconds=executor_raw.get("timeout_seconds", 600),
        ),
        provider=ProviderConfig(
            kind=ProviderKind(
                provider_raw.get("kind", ProviderKind.OPENAI_COMPATIBLE)
            ),
            model=provider_raw.get("model"),
            base_url=provider_raw.get("base_url"),
            api_key_env=provider_raw.get("api_key_env", "OPENAI_API_KEY"),
            endpoint_path=provider_raw.get(
                "endpoint_path",
                provider_raw.get("chat_completions_path", "/v1/responses"),
            ),
            headers=provider_raw.get("headers", {}),
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
    executor: ExecutorConfig,
    provider: ProviderConfig,
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
    content += (
        "\n[executor]\n"
        f'kind = "{executor.kind.value}"\n'
        f'command = "{executor.command}"\n'
        f"auto_approve = {'true' if executor.auto_approve else 'false'}\n"
        f"skip_git_repo_check = {'true' if executor.skip_git_repo_check else 'false'}\n"
        f'timeout_seconds = {executor.timeout_seconds}\n'
    )
    if executor.profile is not None:
        content += f'profile = "{executor.profile}"\n'
    if executor.sandbox is not None:
        content += f'sandbox = "{executor.sandbox}"\n'
    if executor.color != "never":
        content += f'color = "{executor.color}"\n'
    if executor.extra_args:
        serialized_args = ", ".join(f'"{arg}"' for arg in executor.extra_args)
        content += f"extra_args = [{serialized_args}]\n"
    content += (
        "\n[provider]\n"
        f'kind = "{provider.kind.value}"\n'
        f'api_key_env = "{provider.api_key_env}"\n'
        f'endpoint_path = "{provider.endpoint_path}"\n'
    )
    if provider.model is not None:
        content += f'model = "{provider.model}"\n'
    if provider.base_url is not None:
        content += f'base_url = "{provider.base_url}"\n'
    if provider.headers:
        serialized_headers = ", ".join(
            f'"{key}" = "{value}"' for key, value in provider.headers.items()
        )
        content += f"headers = {{ {serialized_headers} }}\n"
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
    executor: ExecutorConfig | None = None,
    provider: ProviderConfig | None = None,
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
        executor=executor or ExecutorConfig(),
        provider=provider or ProviderConfig(),
    )
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(content, encoding="utf-8")
    return config_path
