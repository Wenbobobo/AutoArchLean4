from __future__ import annotations

import tomllib
from pathlib import Path

from .execution_policy import build_execution_policy
from .models import (
    AppConfig,
    ExecutionPolicy,
    ExecutorConfig,
    ExecutorKind,
    LeanAnalyzerConfig,
    LeanAnalyzerKind,
    ProjectConfig,
    ProviderConfig,
    ProviderKind,
    ProviderPoolConfig,
    ProviderPoolMemberConfig,
    RunConfig,
    WorkflowMode,
    WorkspaceConfig,
    WorkspaceProjectConfig,
)


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


def _get_str(raw: dict[str, object], key: str, default: str) -> str:
    value = raw.get(key, default)
    return str(value)


def _get_optional_str(raw: dict[str, object], key: str) -> str | None:
    value = raw.get(key)
    return None if value is None else str(value)


def _get_bool(raw: dict[str, object], key: str, default: bool) -> bool:
    value = raw.get(key, default)
    return bool(value)


def _get_int(raw: dict[str, object], key: str, default: int) -> int:
    value = raw.get(key, default)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    return default


def _get_optional_float(raw: dict[str, object], key: str) -> float | None:
    value = raw.get(key)
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value)
    return None


def _get_str_list(raw: dict[str, object], key: str) -> list[str]:
    value = raw.get(key, [])
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _get_command_list(raw: dict[str, object], key: str) -> list[str]:
    value = raw.get(key, [])
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _get_str_dict(raw: dict[str, object], key: str) -> dict[str, str]:
    value = raw.get(key, {})
    if not isinstance(value, dict):
        return {}
    return {str(item_key): str(item_value) for item_key, item_value in value.items()}


def _load_executor_config(raw: dict[str, object]) -> ExecutorConfig:
    return ExecutorConfig(
        kind=ExecutorKind(_get_str(raw, "kind", ExecutorKind.DRY_RUN.value)),
        command=_get_str(raw, "command", "codex"),
        profile=_get_optional_str(raw, "profile"),
        auto_approve=_get_bool(raw, "auto_approve", False),
        skip_git_repo_check=_get_bool(raw, "skip_git_repo_check", True),
        sandbox=_get_optional_str(raw, "sandbox"),
        color=_get_str(raw, "color", "never"),
        extra_args=_get_str_list(raw, "extra_args"),
        timeout_seconds=_get_int(raw, "timeout_seconds", 600),
    )


def _load_provider_config(raw: dict[str, object]) -> ProviderConfig:
    return ProviderConfig(
        kind=ProviderKind(_get_str(raw, "kind", ProviderKind.OPENAI_COMPATIBLE.value)),
        pool=_get_optional_str(raw, "pool"),
        member_name=_get_optional_str(raw, "member_name"),
        model=_get_optional_str(raw, "model"),
        cost_tier=_get_optional_str(raw, "cost_tier"),
        endpoint_class=_get_optional_str(raw, "endpoint_class"),
        base_url=_get_optional_str(raw, "base_url"),
        api_key_env=_get_str(raw, "api_key_env", "OPENAI_API_KEY"),
        endpoint_path=_get_str(
            raw,
            "endpoint_path",
            _get_str(raw, "chat_completions_path", "/v1/responses"),
        ),
        headers=_get_str_dict(raw, "headers"),
        input_cost_per_1k_tokens=_get_optional_float(raw, "input_cost_per_1k_tokens"),
        output_cost_per_1k_tokens=_get_optional_float(raw, "output_cost_per_1k_tokens"),
    )


def _load_provider_pools(raw: object) -> dict[str, ProviderPoolConfig]:
    if not isinstance(raw, dict):
        return {}
    pools: dict[str, ProviderPoolConfig] = {}
    for pool_name, pool_raw in raw.items():
        if not isinstance(pool_raw, dict):
            continue
        members_raw = pool_raw.get("members", [])
        members: list[ProviderPoolMemberConfig] = []
        if isinstance(members_raw, list):
            for member_raw in members_raw:
                if not isinstance(member_raw, dict):
                    continue
                members.append(
                    ProviderPoolMemberConfig(
                        name=str(member_raw["name"]),
                        enabled=_get_bool(member_raw, "enabled", True),
                        priority=_get_int(member_raw, "priority", 0),
                        kind=(
                            ProviderKind(_get_str(member_raw, "kind", ""))
                            if "kind" in member_raw
                            else None
                        ),
                        model=_get_optional_str(member_raw, "model"),
                        cost_tier=_get_optional_str(member_raw, "cost_tier"),
                        endpoint_class=_get_optional_str(member_raw, "endpoint_class"),
                        base_url=_get_optional_str(member_raw, "base_url"),
                        api_key_env=_get_optional_str(member_raw, "api_key_env"),
                        endpoint_path=_get_optional_str(member_raw, "endpoint_path"),
                        headers=_get_str_dict(member_raw, "headers"),
                        input_cost_per_1k_tokens=_get_optional_float(
                            member_raw,
                            "input_cost_per_1k_tokens",
                        ),
                        output_cost_per_1k_tokens=_get_optional_float(
                            member_raw,
                            "output_cost_per_1k_tokens",
                        ),
                    )
                )
        pools[str(pool_name)] = ProviderPoolConfig(
            name=str(pool_name),
            strategy=_get_str(pool_raw, "strategy", "ordered_failover"),
            max_consecutive_failures=_get_int(pool_raw, "max_consecutive_failures", 2),
            quarantine_seconds=_get_int(pool_raw, "quarantine_seconds", 300),
            members=members,
        )
    return pools


def _load_lean_analyzer_config(raw: object) -> LeanAnalyzerConfig:
    if not isinstance(raw, dict):
        return LeanAnalyzerConfig()
    return LeanAnalyzerConfig(
        kind=LeanAnalyzerKind(_get_str(raw, "kind", LeanAnalyzerKind.REGEX.value)),
        command=_get_command_list(raw, "command"),
        timeout_seconds=_get_int(raw, "timeout_seconds", 60),
    )


def _load_run_config(base_dir: Path, raw: dict[str, object]) -> RunConfig:
    workflow_spec = (
        _resolve_path(base_dir, str(raw["workflow_spec"]))
        if "workflow_spec" in raw
        else None
    )
    return RunConfig(
        workflow=WorkflowMode(_get_str(raw, "workflow", WorkflowMode.ADAPTIVE_LOOP.value)),
        workflow_spec=workflow_spec,
        stage_policy=_get_str(raw, "stage_policy", "auto"),
        max_iterations=_get_int(raw, "max_iterations", 10),
        max_parallel=_get_int(raw, "max_parallel", 8),
        review=_get_bool(raw, "review", True),
        dry_run=_get_bool(raw, "dry_run", True),
        artifact_root=_resolve_path(base_dir, _get_str(raw, "artifact_root", "artifacts")),
    )


def _load_execution_policy(
    *,
    executor: ExecutorConfig,
    provider: ProviderConfig,
    phase_executor_raw: dict[str, object],
    phase_provider_raw: dict[str, object],
    task_matcher_raw: dict[str, object],
    task_executor_raw: dict[str, object],
    task_provider_raw: dict[str, object],
) -> ExecutionPolicy:
    return build_execution_policy(
        base_executor=executor,
        base_provider=provider,
        phase_executor_raw=phase_executor_raw if isinstance(phase_executor_raw, dict) else {},
        phase_provider_raw=phase_provider_raw if isinstance(phase_provider_raw, dict) else {},
        task_matcher_raw=task_matcher_raw if isinstance(task_matcher_raw, dict) else {},
        task_executor_raw=task_executor_raw if isinstance(task_executor_raw, dict) else {},
        task_provider_raw=task_provider_raw if isinstance(task_provider_raw, dict) else {},
    )


def load_config(config_path: Path) -> AppConfig:
    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    base_dir = config_path.parent.resolve()
    project_raw = raw["project"]
    run_raw = raw.get("run", {})
    executor_raw = raw.get("executor", {})
    provider_raw = raw.get("provider", {})
    phase_executor_raw = raw.get("phase_executor", {})
    phase_provider_raw = raw.get("phase_provider", {})
    task_matcher_raw = raw.get("task_matcher", {})
    task_executor_raw = raw.get("task_executor", {})
    task_provider_raw = raw.get("task_provider", {})
    provider_pool_raw = raw.get("provider_pool", {})
    lean_analyzer_raw = raw.get("lean_analyzer", {})

    project_path = _resolve_path(base_dir, project_raw["project_path"])
    archon_path = _resolve_path(base_dir, project_raw["archon_path"])
    executor = _load_executor_config(executor_raw)
    provider = _load_provider_config(provider_raw)

    return AppConfig(
        project=ProjectConfig(
            name=project_raw.get("name", project_path.name),
            project_path=project_path,
            archon_path=archon_path,
            backend=project_raw.get("backend", "archon"),
        ),
        run=_load_run_config(base_dir, run_raw),
        lean_analyzer=_load_lean_analyzer_config(lean_analyzer_raw),
        executor=executor,
        provider=provider,
        provider_pools=_load_provider_pools(provider_pool_raw),
        execution_policy=_load_execution_policy(
            executor=executor,
            provider=provider,
            phase_executor_raw=phase_executor_raw,
            phase_provider_raw=phase_provider_raw,
            task_matcher_raw=task_matcher_raw,
            task_executor_raw=task_executor_raw,
            task_provider_raw=task_provider_raw,
        ),
    )


def load_workspace_config(config_path: Path) -> WorkspaceConfig:
    raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
    base_dir = config_path.parent.resolve()
    workspace_raw = raw["workspace"]
    run_raw = raw.get("run", {})
    executor_raw = raw.get("executor", {})
    provider_raw = raw.get("provider", {})
    phase_executor_raw = raw.get("phase_executor", {})
    phase_provider_raw = raw.get("phase_provider", {})
    task_matcher_raw = raw.get("task_matcher", {})
    task_executor_raw = raw.get("task_executor", {})
    task_provider_raw = raw.get("task_provider", {})
    provider_pool_raw = raw.get("provider_pool", {})
    lean_analyzer_raw = raw.get("lean_analyzer", {})
    projects_raw = raw.get("projects", [])
    if not isinstance(projects_raw, list) or not projects_raw:
        raise ValueError("Workspace config must define at least one [[projects]] entry.")

    executor = _load_executor_config(executor_raw)
    provider = _load_provider_config(provider_raw)
    projects: list[WorkspaceProjectConfig] = []
    for project_raw in projects_raw:
        if not isinstance(project_raw, dict):
            raise ValueError("Each [[projects]] entry must be a table.")
        projects.append(
            WorkspaceProjectConfig(
                id=str(project_raw["id"]),
                project_path=_resolve_path(base_dir, str(project_raw["project_path"])),
                archon_path=_resolve_path(base_dir, str(project_raw["archon_path"])),
                workflow=(
                    WorkflowMode(str(project_raw["workflow"]))
                    if "workflow" in project_raw
                    else None
                ),
                workflow_spec=(
                    _resolve_path(base_dir, str(project_raw["workflow_spec"]))
                    if "workflow_spec" in project_raw
                    else None
                ),
                max_iterations=(
                    int(project_raw["max_iterations"])
                    if "max_iterations" in project_raw
                    else None
                ),
                dry_run=(
                    bool(project_raw["dry_run"])
                    if "dry_run" in project_raw
                    else None
                ),
                backend=str(project_raw.get("backend", "archon")),
                enabled=bool(project_raw.get("enabled", True)),
                tags=[str(tag) for tag in project_raw.get("tags", [])]
                if isinstance(project_raw.get("tags", []), list)
                else [],
            )
        )
    return WorkspaceConfig(
        name=str(workspace_raw.get("name", config_path.parent.name)),
        run=_load_run_config(base_dir, run_raw),
        lean_analyzer=_load_lean_analyzer_config(lean_analyzer_raw),
        executor=executor,
        provider=provider,
        provider_pools=_load_provider_pools(provider_pool_raw),
        execution_policy=_load_execution_policy(
            executor=executor,
            provider=provider,
            phase_executor_raw=phase_executor_raw,
            phase_provider_raw=phase_provider_raw,
            task_matcher_raw=task_matcher_raw,
            task_executor_raw=task_executor_raw,
            task_provider_raw=task_provider_raw,
        ),
        projects=projects,
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
    if provider.pool is not None:
        content += f'pool = "{provider.pool}"\n'
    if provider.cost_tier is not None:
        content += f'cost_tier = "{provider.cost_tier}"\n'
    if provider.endpoint_class is not None:
        content += f'endpoint_class = "{provider.endpoint_class}"\n'
    if provider.base_url is not None:
        content += f'base_url = "{provider.base_url}"\n'
    if provider.input_cost_per_1k_tokens is not None:
        content += f"input_cost_per_1k_tokens = {provider.input_cost_per_1k_tokens}\n"
    if provider.output_cost_per_1k_tokens is not None:
        content += f"output_cost_per_1k_tokens = {provider.output_cost_per_1k_tokens}\n"
    if provider.headers:
        serialized_headers = ", ".join(
            f'"{key}" = "{value}"' for key, value in provider.headers.items()
        )
        content += f"headers = {{ {serialized_headers} }}\n"
    return content


def render_workspace_config(
    *,
    workspace_name: str,
    projects: list[WorkspaceProjectConfig],
    artifact_root: Path,
    workflow: WorkflowMode,
    workflow_spec: Path | None,
    dry_run: bool,
    executor: ExecutorConfig,
    provider: ProviderConfig,
) -> str:
    content = (
        "[workspace]\n"
        f'name = "{workspace_name}"\n\n'
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
    if provider.pool is not None:
        content += f'pool = "{provider.pool}"\n'
    if provider.cost_tier is not None:
        content += f'cost_tier = "{provider.cost_tier}"\n'
    if provider.endpoint_class is not None:
        content += f'endpoint_class = "{provider.endpoint_class}"\n'
    if provider.base_url is not None:
        content += f'base_url = "{provider.base_url}"\n'
    if provider.input_cost_per_1k_tokens is not None:
        content += f"input_cost_per_1k_tokens = {provider.input_cost_per_1k_tokens}\n"
    if provider.output_cost_per_1k_tokens is not None:
        content += f"output_cost_per_1k_tokens = {provider.output_cost_per_1k_tokens}\n"
    if provider.headers:
        serialized_headers = ", ".join(
            f'"{key}" = "{value}"' for key, value in provider.headers.items()
        )
        content += f"headers = {{ {serialized_headers} }}\n"
    for project in projects:
        content += (
            "\n[[projects]]\n"
            f'id = "{project.id}"\n'
            f'project_path = "{project.project_path}"\n'
            f'archon_path = "{project.archon_path}"\n'
            f"enabled = {'true' if project.enabled else 'false'}\n"
        )
        if project.workflow is not None:
            content += f'workflow = "{project.workflow.value}"\n'
        if project.workflow_spec is not None:
            content += f'workflow_spec = "{project.workflow_spec}"\n'
        if project.max_iterations is not None:
            content += f"max_iterations = {project.max_iterations}\n"
        if project.dry_run is not None:
            content += f"dry_run = {'true' if project.dry_run else 'false'}\n"
        if project.backend != "archon":
            content += f'backend = "{project.backend}"\n'
        if project.tags:
            serialized_tags = ", ".join(f'"{tag}"' for tag in project.tags)
            content += f"tags = [{serialized_tags}]\n"
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


def init_workspace_config(
    *,
    config_path: Path,
    workspace_name: str,
    project_id: str,
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

    content = render_workspace_config(
        workspace_name=workspace_name,
        projects=[
            WorkspaceProjectConfig(
                id=project_id,
                project_path=project_path,
                archon_path=archon_path,
            )
        ],
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


def build_workspace_project_app_config(
    workspace_config: WorkspaceConfig,
    *,
    project_id: str,
) -> AppConfig:
    project = next(
        (candidate for candidate in workspace_config.projects if candidate.id == project_id),
        None,
    )
    if project is None:
        raise KeyError(f"Unknown workspace project: {project_id}")
    return AppConfig(
        project=project.as_project_config(),
        run=workspace_config.run.model_copy(
            update={
                "workflow": project.workflow or workspace_config.run.workflow,
                "workflow_spec": (
                    project.workflow_spec or workspace_config.run.workflow_spec
                ),
                "max_iterations": (
                    project.max_iterations
                    if project.max_iterations is not None
                    else workspace_config.run.max_iterations
                ),
                "dry_run": (
                    project.dry_run
                    if project.dry_run is not None
                    else workspace_config.run.dry_run
                ),
            }
        ),
        lean_analyzer=workspace_config.lean_analyzer,
        executor=workspace_config.executor,
        provider=workspace_config.provider,
        provider_pools=workspace_config.provider_pools,
        execution_policy=workspace_config.execution_policy,
    )
