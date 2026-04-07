from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from urllib import error, request
from urllib.parse import urlsplit, urlunsplit

from .models import (
    ExecutionCapability,
    ExecutionRequest,
    ExecutionResult,
    ExecutionStatus,
    ExecutionTelemetry,
    ExecutionUsage,
    ExecutorConfig,
    ExecutorKind,
    ProviderConfig,
    ProviderPoolConfig,
    ProviderPoolMemberConfig,
)


class Executor(Protocol):
    def execute(
        self,
        request_or_prompt: ExecutionRequest | str,
        system_prompt: str | None = None,
    ) -> ExecutionResult: ...


class ExecutorProvider:
    def resolve(self, config: dict[str, object]) -> Executor:
        kind = str(config.get("kind", "dry_run"))
        resolved_kind = "openai_compatible" if kind == "openai_compatible_http" else kind
        executor_config = ExecutorConfig(kind=ExecutorKind(resolved_kind))
        provider_config = ProviderConfig(
            model=str(config["model"]) if "model" in config else None,
            base_url=str(config["base_url"]) if "base_url" in config else None,
        )
        executor = create_executor(
            executor_config=executor_config,
            provider_config=provider_config,
        )
        if isinstance(executor, OpenAICompatibleHttpExecutor):
            executor.api_key = str(config.get("api_key", ""))
            return executor
        if isinstance(executor, CodexExecExecutor):
            command = config.get("command", ["codex", "exec"])
            if isinstance(command, str):
                resolved_command = [command]
            elif isinstance(command, (list, tuple)):
                resolved_command = [str(part) for part in command]
            else:
                raise ValueError("codex_exec command must be a string or list of strings")
            cwd_value = config.get("cwd")
            cwd = Path(str(cwd_value)).resolve() if cwd_value is not None else None
            return CodexExecExecutor(command=resolved_command, cwd=cwd)
        if isinstance(executor, DryRunExecutor):
            return executor
        raise ValueError(f"Unsupported executor kind: {kind}")

ExecutorFactory = Callable[[ExecutorConfig, ProviderConfig], Executor]


@dataclass
class _ProviderPoolHealthState:
    consecutive_failures: int = 0
    quarantined_until: datetime | None = None


_PROVIDER_POOL_HEALTH_LOCK = threading.Lock()
_PROVIDER_POOL_HEALTH: dict[tuple[str, str], _ProviderPoolHealthState] = {}


def create_executor(
    *,
    executor_config: ExecutorConfig,
    provider_config: ProviderConfig,
    provider_pools: dict[str, ProviderPoolConfig] | None = None,
) -> Executor:
    if provider_config.pool is not None:
        return ProviderPoolExecutor(
            executor_config=executor_config,
            provider_config=provider_config,
            provider_pools=provider_pools or {},
        )
    return _create_direct_executor(
        executor_config=executor_config,
        provider_config=provider_config,
    )


def _create_direct_executor(
    *,
    executor_config: ExecutorConfig,
    provider_config: ProviderConfig,
) -> Executor:
    capability = ExecutionCapability.from_configs(
        executor=executor_config,
        provider=provider_config,
    )
    factory = _EXECUTOR_FACTORIES.get(capability.executor_kind)
    if factory is None:
        raise ValueError(f"Unsupported executor kind: {capability.executor_kind.value}")
    return factory(executor_config, provider_config)


def _build_dry_run_executor(
    executor_config: ExecutorConfig,
    provider_config: ProviderConfig,
) -> Executor:
    return DryRunExecutor(
        executor_config=executor_config,
        provider_config=provider_config,
    )


def _build_openai_executor(
    executor_config: ExecutorConfig,
    provider_config: ProviderConfig,
) -> Executor:
    return OpenAICompatibleHttpExecutor(
        executor_config=executor_config,
        provider_config=provider_config,
        endpoint_path=provider_config.endpoint_path,
    )


def _build_codex_executor(
    executor_config: ExecutorConfig,
    provider_config: ProviderConfig,
) -> Executor:
    return CodexExecExecutor(
        executor_config=executor_config,
        provider_config=provider_config,
    )


_EXECUTOR_FACTORIES: dict[ExecutorKind, ExecutorFactory] = {
    ExecutorKind.DRY_RUN: _build_dry_run_executor,
    ExecutorKind.OPENAI_COMPATIBLE: _build_openai_executor,
    ExecutorKind.CODEX_EXEC: _build_codex_executor,
}


class DryRunExecutor:
    def __init__(
        self,
        *,
        executor_config: ExecutorConfig | None = None,
        provider_config: ProviderConfig | None = None,
    ) -> None:
        self.executor_config = executor_config or ExecutorConfig(kind=ExecutorKind.DRY_RUN)
        self.provider_config = provider_config or ProviderConfig()

    def execute(
        self,
        request_or_prompt: ExecutionRequest | str,
        system_prompt: str | None = None,
    ) -> ExecutionResult:
        started_at, started_perf = _telemetry_clock()
        request_data = (
            request_or_prompt
            if isinstance(request_or_prompt, ExecutionRequest)
            else None
        )
        prompt = (
            request_data.prompt
            if request_data is not None
            else str(request_or_prompt)
        )
        system = system_prompt or (request_data.phase if request_data is not None else "dry_run")
        output_text = (
            "Execution skipped because the configured executor kind is dry_run.\n\n"
            f"system: {system}\n"
            f"prompt: {prompt}\n"
        )
        output_path = None
        if request_data is not None:
            executor_dir = request_data.artifact_dir / "executor"
            executor_dir.mkdir(parents=True, exist_ok=True)
            output_path = executor_dir / "dry-run-output.txt"
            output_path.write_text(output_text, encoding="utf-8")
        result = ExecutionResult(
            executor=ExecutorKind.DRY_RUN,
            status=ExecutionStatus.COMPLETED,
            response_text=output_text,
            output_path=output_path,
            metadata={"provider": "dry_run"},
        )
        return _with_execution_telemetry(
            result,
            started_at=started_at,
            started_perf=started_perf,
            provider_config=self.provider_config,
        )


class OpenAICompatibleHttpExecutor:
    def __init__(
        self,
        *,
        executor_config: ExecutorConfig | None = None,
        provider_config: ProviderConfig | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        model: str | None = None,
        endpoint_path: str = "/v1/responses",
    ) -> None:
        self.executor_config = executor_config or ExecutorConfig(
            kind=ExecutorKind.OPENAI_COMPATIBLE
        )
        self.provider_config = provider_config or ProviderConfig(
            model=model,
            base_url=base_url,
        )
        self.api_key = api_key
        self.endpoint_path = endpoint_path

    def execute(
        self,
        request_or_prompt: ExecutionRequest | str,
        system_prompt: str | None = None,
    ) -> ExecutionResult:
        started_at, started_perf = _telemetry_clock()
        request_data = (
            request_or_prompt
            if isinstance(request_or_prompt, ExecutionRequest)
            else None
        )
        prompt = request_data.prompt if request_data is not None else str(request_or_prompt)
        if self.provider_config.base_url is None:
            raise ValueError("provider.base_url is required for openai-compatible execution")
        if self.provider_config.model is None:
            raise ValueError("provider.model is required for openai-compatible execution")

        payload = {
            "model": self.provider_config.model,
            "input": _build_openai_input(prompt=prompt, system_prompt=system_prompt),
        }
        request_path = None
        response_path = None
        output_path = None
        if request_data is not None:
            executor_dir = request_data.artifact_dir / "executor"
            executor_dir.mkdir(parents=True, exist_ok=True)
            request_path = executor_dir / "openai-request.json"
            response_path = executor_dir / "openai-response.json"
            output_path = executor_dir / "openai-output.txt"
            request_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        headers = {
            "Content-Type": "application/json",
            **self.provider_config.headers,
        }
        api_key = self.api_key or os.environ.get(self.provider_config.api_key_env)
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        http_request = request.Request(
            self._endpoint_url(),
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with request.urlopen(
                http_request,
                timeout=self.executor_config.timeout_seconds,
            ) as response:
                raw_response = response.read().decode("utf-8")
                status_code = getattr(response, "status", 200)
        except error.HTTPError as exc:
            raw_response = exc.read().decode("utf-8", errors="replace")
            if response_path is not None:
                response_path.write_text(raw_response, encoding="utf-8")
            result = ExecutionResult(
                executor=ExecutorKind.OPENAI_COMPATIBLE,
                status=ExecutionStatus.FAILED,
                request_path=request_path,
                response_path=response_path,
                error_message=f"HTTP {exc.code}: {raw_response}",
                metadata={
                    "provider": "openai_compatible_http",
                    "status_code": exc.code,
                },
            )
            return _with_execution_telemetry(
                result,
                started_at=started_at,
                started_perf=started_perf,
                provider_config=self.provider_config,
                status_code=exc.code,
            )
        except (error.URLError, OSError, TimeoutError) as exc:
            result = ExecutionResult(
                executor=ExecutorKind.OPENAI_COMPATIBLE,
                status=ExecutionStatus.FAILED,
                request_path=request_path,
                response_path=response_path,
                error_message=str(exc),
                metadata={"provider": "openai_compatible_http"},
            )
            return _with_execution_telemetry(
                result,
                started_at=started_at,
                started_perf=started_perf,
                provider_config=self.provider_config,
            )

        if response_path is not None:
            response_path.write_text(raw_response, encoding="utf-8")
        parsed = json.loads(raw_response)
        response_text = _extract_openai_text(parsed)
        usage = _extract_openai_usage(parsed)
        cost_estimate = _estimate_cost(
            provider_config=self.provider_config,
            usage=usage,
        )
        if output_path is not None:
            output_path.write_text(response_text, encoding="utf-8")
        result = ExecutionResult(
            executor=ExecutorKind.OPENAI_COMPATIBLE,
            status=ExecutionStatus.COMPLETED,
            response_text=response_text,
            output_path=output_path,
            request_path=request_path,
            response_path=response_path,
            metadata={
                "provider": "openai_compatible_http",
                "status_code": status_code,
            },
        )
        return _with_execution_telemetry(
            result,
            started_at=started_at,
            started_perf=started_perf,
            provider_config=self.provider_config,
            usage=usage,
            cost_estimate=cost_estimate,
            status_code=status_code,
        )

    def _endpoint_url(self) -> str:
        base_url_value = self.provider_config.base_url
        if base_url_value is None:
            raise ValueError("provider.base_url is required for openai-compatible execution")
        base_url = base_url_value.rstrip("/")
        path = self.endpoint_path
        if base_url.endswith(path):
            return base_url
        if not path.startswith("/"):
            path = f"/{path}"
        parsed = urlsplit(base_url)
        base_path = parsed.path.rstrip("/")
        if base_path and path.startswith(f"{base_path}/"):
            path = path[len(base_path) :]
        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                f"{base_path}{path}",
                parsed.query,
                parsed.fragment,
            )
        )


class CodexExecExecutor:
    def __init__(
        self,
        *,
        executor_config: ExecutorConfig | None = None,
        provider_config: ProviderConfig | None = None,
        command: list[str] | None = None,
        cwd: Path | None = None,
    ) -> None:
        self.executor_config = executor_config or ExecutorConfig(kind=ExecutorKind.CODEX_EXEC)
        self.provider_config = provider_config or ProviderConfig()
        self.command = command
        self.cwd = cwd

    def execute(
        self,
        request_or_prompt: ExecutionRequest | str,
        system_prompt: str | None = None,
    ) -> ExecutionResult:
        started_at, started_perf = _telemetry_clock()
        request_data = (
            request_or_prompt
            if isinstance(request_or_prompt, ExecutionRequest)
            else None
        )
        prompt = request_data.prompt if request_data is not None else str(request_or_prompt)
        cwd = (
            request_data.cwd
            if request_data is not None
            else self.cwd or Path.cwd()
        )
        stdout_path = None
        stderr_path = None
        output_path = None
        if request_data is not None:
            executor_dir = request_data.artifact_dir / "executor"
            executor_dir.mkdir(parents=True, exist_ok=True)
            output_path = executor_dir / "codex-last-message.txt"
            stdout_path = executor_dir / "codex-stdout.log"
            stderr_path = executor_dir / "codex-stderr.log"

        command = (
            self.command
            if self.command is not None
            else self._codex_exec_command(cwd=cwd, output_path=output_path)
        )
        try:
            proc = subprocess.run(
                command,
                cwd=cwd,
                input=prompt,
                text=True,
                capture_output=True,
                timeout=self.executor_config.timeout_seconds,
                check=False,
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            result = ExecutionResult(
                executor=ExecutorKind.CODEX_EXEC,
                status=ExecutionStatus.FAILED,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                command=command,
                error_message=str(exc),
                metadata={
                    "provider": "codex_exec",
                    "system_prompt": system_prompt,
                },
            )
            return _with_execution_telemetry(
                result,
                started_at=started_at,
                started_perf=started_perf,
                provider_config=self.provider_config,
            )
        if stdout_path is not None:
            stdout_path.write_text(proc.stdout, encoding="utf-8")
        if stderr_path is not None:
            stderr_path.write_text(proc.stderr, encoding="utf-8")
        response_text = (
            output_path.read_text(encoding="utf-8")
            if output_path is not None and output_path.exists()
            else proc.stdout
        )
        result = ExecutionResult(
            executor=ExecutorKind.CODEX_EXEC,
            status=(
                ExecutionStatus.COMPLETED
                if proc.returncode == 0
                else ExecutionStatus.FAILED
            ),
            response_text=response_text,
            output_path=output_path if output_path is not None and output_path.exists() else None,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            command=command,
            error_message=proc.stderr.strip() or None if proc.returncode != 0 else None,
            metadata={
                "provider": "codex_exec",
                "returncode": proc.returncode,
                "system_prompt": system_prompt,
            },
        )
        return _with_execution_telemetry(
            result,
            started_at=started_at,
            started_perf=started_perf,
            provider_config=self.provider_config,
        )

    def _codex_exec_command(self, *, cwd: Path, output_path: Path | None) -> list[str]:
        command = [
            self.executor_config.command,
            "exec",
            "--cd",
            str(cwd),
            "--color",
            self.executor_config.color,
        ]
        if output_path is not None:
            command.extend(["--output-last-message", str(output_path)])
        if self.provider_config.model is not None:
            command.extend(["--model", self.provider_config.model])
        if self.executor_config.profile is not None:
            command.extend(["--profile", self.executor_config.profile])
        if self.executor_config.skip_git_repo_check:
            command.append("--skip-git-repo-check")
        if self.executor_config.auto_approve:
            command.append("--dangerously-bypass-approvals-and-sandbox")
        if self.executor_config.sandbox is not None:
            command.extend(["--sandbox", self.executor_config.sandbox])
        command.extend(self.executor_config.extra_args)
        command.append("-")
        return command


class ProviderPoolExecutor:
    def __init__(
        self,
        *,
        executor_config: ExecutorConfig,
        provider_config: ProviderConfig,
        provider_pools: dict[str, ProviderPoolConfig],
    ) -> None:
        self.executor_config = executor_config
        self.provider_config = provider_config
        self.provider_pools = provider_pools

    def execute(
        self,
        request_or_prompt: ExecutionRequest | str,
        system_prompt: str | None = None,
    ) -> ExecutionResult:
        pool_name = self.provider_config.pool
        if pool_name is None:
            raise ValueError("provider.pool is required for provider pool routing")
        pool = self.provider_pools.get(pool_name)
        if pool is None:
            raise ValueError(f"Unknown provider pool: {pool_name}")
        members, health_status = _eligible_pool_members(pool)
        if not members:
            raise ValueError(f"Provider pool {pool_name} has no enabled members")

        attempted_members: list[str] = []
        last_result: ExecutionResult | None = None
        for index, member in enumerate(members):
            attempted_members.append(member.name)
            resolved_provider = member.as_provider_config(self.provider_config)
            try:
                executor = _create_direct_executor(
                    executor_config=self.executor_config,
                    provider_config=resolved_provider,
                )
                result = executor.execute(
                    request_or_prompt,
                    system_prompt=system_prompt,
                )
            except Exception as exc:  # noqa: BLE001
                started_at, started_perf = _telemetry_clock()
                result = _with_execution_telemetry(
                    ExecutionResult(
                        executor=self.executor_config.kind,
                        status=ExecutionStatus.FAILED,
                        error_message=str(exc),
                        metadata={"provider": self.executor_config.kind.value},
                    ),
                    started_at=started_at,
                    started_perf=started_perf,
                    provider_config=resolved_provider,
                )
            result = _augment_pool_result(
                result,
                provider_pool=pool_name,
                provider_member=member.name,
                attempted_members=list(attempted_members),
                retry_count=index,
                health_status=health_status,
            )
            if result.status is ExecutionStatus.COMPLETED:
                _mark_pool_success(pool_name, member.name)
                return result
            _mark_pool_failure(pool, member.name)
            last_result = result

        if last_result is None:
            raise RuntimeError(f"Provider pool {pool_name} did not produce any result")
        last_member = (
            last_result.telemetry.provider_member
            if last_result.telemetry is not None
            else None
        )
        return _augment_pool_result(
            last_result,
            provider_pool=pool_name,
            provider_member=last_member,
            attempted_members=attempted_members,
            retry_count=max(len(attempted_members) - 1, 0),
            health_status="all_members_failed",
        )


def _build_openai_input(*, prompt: str, system_prompt: str | None) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    return messages


def _extract_openai_text(payload: dict[str, object]) -> str:
    output_text = payload.get("output_text")
    if isinstance(output_text, str):
        return output_text
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        return ""
    message = first_choice.get("message")
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str):
            parts.append(text)
    return "\n".join(parts)


def _extract_openai_usage(payload: dict[str, object]) -> ExecutionUsage | None:
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None
    input_tokens = usage.get("input_tokens", usage.get("prompt_tokens"))
    output_tokens = usage.get("output_tokens", usage.get("completion_tokens"))
    total_tokens = usage.get("total_tokens")
    return ExecutionUsage(
        input_tokens=int(input_tokens) if isinstance(input_tokens, int) else None,
        output_tokens=int(output_tokens) if isinstance(output_tokens, int) else None,
        total_tokens=int(total_tokens) if isinstance(total_tokens, int) else None,
    )


def _estimate_cost(
    *,
    provider_config: ProviderConfig,
    usage: ExecutionUsage | None,
) -> float | None:
    if usage is None:
        return None
    if (
        provider_config.input_cost_per_1k_tokens is None
        and provider_config.output_cost_per_1k_tokens is None
    ):
        return None
    input_cost = (
        (usage.input_tokens or 0) / 1000 * (provider_config.input_cost_per_1k_tokens or 0.0)
    )
    output_cost = (
        (usage.output_tokens or 0)
        / 1000
        * (provider_config.output_cost_per_1k_tokens or 0.0)
    )
    return round(input_cost + output_cost, 6)


def _telemetry_clock() -> tuple[datetime, float]:
    return datetime.now(UTC), time.perf_counter()


def _with_execution_telemetry(
    result: ExecutionResult,
    *,
    started_at: datetime,
    started_perf: float,
    provider_config: ProviderConfig,
    usage: ExecutionUsage | None = None,
    cost_estimate: float | None = None,
    status_code: int | None = None,
) -> ExecutionResult:
    finished_at = datetime.now(UTC)
    telemetry = ExecutionTelemetry(
        started_at=started_at,
        finished_at=finished_at,
        latency_ms=max(0, int((time.perf_counter() - started_perf) * 1000)),
        provider_pool=provider_config.pool,
        provider_member=provider_config.member_name,
        usage=usage,
        cost_estimate=cost_estimate,
        status_code=status_code,
    )
    metadata = dict(result.metadata)
    if telemetry.provider_pool is not None:
        metadata["provider_pool"] = telemetry.provider_pool
    if telemetry.provider_member is not None:
        metadata["provider_member"] = telemetry.provider_member
    if telemetry.status_code is not None:
        metadata["status_code"] = telemetry.status_code
    if telemetry.usage is not None:
        metadata["usage"] = telemetry.usage.model_dump(mode="json")
    if telemetry.cost_estimate is not None:
        metadata["cost_estimate"] = telemetry.cost_estimate
    return result.model_copy(update={"telemetry": telemetry, "metadata": metadata})


def _augment_pool_result(
    result: ExecutionResult,
    *,
    provider_pool: str,
    provider_member: str | None,
    attempted_members: list[str],
    retry_count: int,
    health_status: str | None,
) -> ExecutionResult:
    telemetry = (
        result.telemetry
        if result.telemetry is not None
        else ExecutionTelemetry()
    ).model_copy(
        update={
            "provider_pool": provider_pool,
            "provider_member": provider_member,
            "attempted_members": attempted_members,
            "retry_count": retry_count,
            "health_status": health_status,
        }
    )
    metadata = dict(result.metadata)
    metadata["provider_pool"] = provider_pool
    if provider_member is not None:
        metadata["provider_member"] = provider_member
    metadata["attempted_members"] = attempted_members
    metadata["retry_count"] = retry_count
    if health_status is not None:
        metadata["health_status"] = health_status
    return result.model_copy(update={"telemetry": telemetry, "metadata": metadata})


def _eligible_pool_members(
    pool: ProviderPoolConfig,
) -> tuple[list[ProviderPoolMemberConfig], str]:
    enabled_members = [member for member in pool.members if member.enabled]
    sorted_members = sorted(
        enabled_members,
        key=lambda member: -member.priority,
    )
    now = datetime.now(UTC)
    healthy_members: list[ProviderPoolMemberConfig] = []
    quarantined_members: list[ProviderPoolMemberConfig] = []
    with _PROVIDER_POOL_HEALTH_LOCK:
        for member in sorted_members:
            state = _PROVIDER_POOL_HEALTH.get((pool.name, member.name))
            if (
                state is not None
                and state.quarantined_until is not None
                and state.quarantined_until > now
            ):
                quarantined_members.append(member)
            else:
                healthy_members.append(member)
    if healthy_members:
        return healthy_members, ("degraded" if quarantined_members else "healthy")
    return sorted_members, "all_quarantined"


def _mark_pool_success(pool_name: str, member_name: str) -> None:
    with _PROVIDER_POOL_HEALTH_LOCK:
        _PROVIDER_POOL_HEALTH[(pool_name, member_name)] = _ProviderPoolHealthState()


def _mark_pool_failure(pool: ProviderPoolConfig, member_name: str) -> None:
    key = (pool.name, member_name)
    with _PROVIDER_POOL_HEALTH_LOCK:
        state = _PROVIDER_POOL_HEALTH.get(key, _ProviderPoolHealthState())
        state.consecutive_failures += 1
        if state.consecutive_failures >= pool.max_consecutive_failures:
            state.quarantined_until = datetime.now(UTC) + timedelta(
                seconds=pool.quarantine_seconds
            )
        _PROVIDER_POOL_HEALTH[key] = state


def reset_provider_pool_health() -> None:
    with _PROVIDER_POOL_HEALTH_LOCK:
        _PROVIDER_POOL_HEALTH.clear()
