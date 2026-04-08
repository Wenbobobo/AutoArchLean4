from __future__ import annotations

import json
import os
import re
import sqlite3
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
    ProviderPoolHealthReport,
    ProviderPoolHealthStatus,
    ProviderPoolMemberConfig,
    ProviderPoolMemberHealth,
    ProviderPoolMemberHealthStatus,
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
_PROVIDER_POOL_CURSOR_LOCK = threading.Lock()
_PROVIDER_POOL_CURSORS: dict[str, int] = {}
_PROVIDER_POOL_HEALTH_DB_LOCK = threading.Lock()
_PROVIDER_POOL_HEALTH_DB_READY: set[Path] = set()


def create_executor(
    *,
    executor_config: ExecutorConfig,
    provider_config: ProviderConfig,
    provider_pools: dict[str, ProviderPoolConfig] | None = None,
    provider_health_db_path: Path | None = None,
) -> Executor:
    if provider_config.pool is not None:
        return ProviderPoolExecutor(
            executor_config=executor_config,
            provider_config=provider_config,
            provider_pools=provider_pools or {},
            provider_health_db_path=provider_health_db_path,
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

        payload = _build_openai_payload(
            endpoint_path=self.endpoint_path,
            model=self.provider_config.model,
            prompt=prompt,
            system_prompt=system_prompt,
        )
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
        api_key = self.api_key or _resolve_provider_api_key(self.provider_config)
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
        provider_health_db_path: Path | None = None,
    ) -> None:
        self.executor_config = executor_config
        self.provider_config = provider_config
        self.provider_pools = provider_pools
        self.provider_health_db_path = (
            provider_health_db_path.resolve()
            if provider_health_db_path is not None
            else None
        )

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
        pinned_member_name = self.provider_config.member_name
        if pinned_member_name is not None:
            pinned_member, health_status, preflight_result = _resolve_pinned_pool_member(
                executor_kind=self.executor_config.kind,
                provider_config=self.provider_config,
                pool=pool,
                db_path=self.provider_health_db_path,
            )
            if preflight_result is not None:
                return preflight_result
            assert pinned_member is not None
            members = [pinned_member]
        else:
            members, health_status = _eligible_pool_members(
                pool,
                db_path=self.provider_health_db_path,
            )
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
                _mark_pool_success(
                    pool_name,
                    member.name,
                    db_path=self.provider_health_db_path,
                )
                return result
            _mark_pool_failure(
                pool,
                member.name,
                db_path=self.provider_health_db_path,
            )
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


def _build_openai_payload(
    *,
    endpoint_path: str,
    model: str,
    prompt: str,
    system_prompt: str | None,
) -> dict[str, object]:
    messages = _build_openai_input(prompt=prompt, system_prompt=system_prompt)
    if _uses_chat_completions_api(endpoint_path):
        return {
            "model": model,
            "messages": messages,
        }
    return {
        "model": model,
        "input": messages,
    }


def _uses_chat_completions_api(endpoint_path: str) -> bool:
    normalized = endpoint_path.strip().rstrip("/").lower()
    return normalized.endswith("/chat/completions")


def _resolve_provider_api_key(provider_config: ProviderConfig) -> str | None:
    env_name_or_literal = provider_config.api_key_env
    env_value = os.environ.get(env_name_or_literal)
    if env_value:
        return env_value
    if _looks_like_env_var_name(env_name_or_literal):
        return None
    return env_name_or_literal


def _looks_like_env_var_name(value: str) -> bool:
    return re.fullmatch(r"[A-Z_][A-Z0-9_]*", value) is not None


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
    *,
    db_path: Path | None = None,
) -> tuple[list[ProviderPoolMemberConfig], str]:
    enabled_members = [member for member in pool.members if member.enabled]
    sorted_members = sorted(
        enabled_members,
        key=lambda member: -member.priority,
    )
    now = datetime.now(UTC)
    healthy_members: list[ProviderPoolMemberConfig] = []
    quarantined_members: list[ProviderPoolMemberConfig] = []
    health_state = _load_provider_pool_health_state(db_path=db_path)
    for member in sorted_members:
        state = health_state.get((pool.name, member.name))
        if (
            state is not None
            and state.quarantined_until is not None
            and state.quarantined_until > now
        ):
            quarantined_members.append(member)
        else:
            healthy_members.append(member)
    if healthy_members:
        return (
            _order_pool_members(
                pool,
                healthy_members,
                db_path=db_path,
            ),
            ("degraded" if quarantined_members else "healthy"),
        )
    return _order_pool_members(pool, sorted_members, db_path=db_path), "all_quarantined"


def _order_pool_members(
    pool: ProviderPoolConfig,
    members: list[ProviderPoolMemberConfig],
    *,
    db_path: Path | None = None,
) -> list[ProviderPoolMemberConfig]:
    if len(members) <= 1:
        return members
    if pool.strategy != "round_robin":
        return members
    start_index = _advance_provider_pool_cursor(
        pool_name=pool.name,
        member_count=len(members),
        db_path=db_path,
    )
    return members[start_index:] + members[:start_index]


def _advance_provider_pool_cursor(
    *,
    pool_name: str,
    member_count: int,
    db_path: Path | None = None,
) -> int:
    if member_count <= 0:
        return 0
    if db_path is None:
        with _PROVIDER_POOL_CURSOR_LOCK:
            current = _PROVIDER_POOL_CURSORS.get(pool_name, 0) % member_count
            _PROVIDER_POOL_CURSORS[pool_name] = (current + 1) % member_count
            return current

    resolved_path = db_path.resolve()
    _ensure_provider_pool_health_table(resolved_path)
    conn = sqlite3.connect(resolved_path, timeout=30)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT next_index
            FROM provider_pool_strategy_state
            WHERE pool_name = ?
            """,
            (pool_name,),
        ).fetchone()
        current = (int(row[0]) if row is not None else 0) % member_count
        conn.execute(
            """
            INSERT INTO provider_pool_strategy_state (pool_name, next_index, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(pool_name) DO UPDATE SET
                next_index = excluded.next_index,
                updated_at = excluded.updated_at
            """,
            (
                pool_name,
                (current + 1) % member_count,
                datetime.now(UTC).isoformat(),
            ),
        )
        conn.commit()
        return current
    finally:
        conn.close()


def _resolve_pinned_pool_member(
    *,
    executor_kind: ExecutorKind,
    provider_config: ProviderConfig,
    pool: ProviderPoolConfig,
    db_path: Path | None,
) -> tuple[ProviderPoolMemberConfig | None, str, ExecutionResult | None]:
    member_name = provider_config.member_name
    if member_name is None:
        raise ValueError("provider.member_name is required for pinned pool routing")

    requested_member = next((member for member in pool.members if member.name == member_name), None)
    if requested_member is None:
        return None, "member_missing", _pool_preflight_failure_result(
            executor_kind=executor_kind,
            provider_config=provider_config,
            pool_name=pool.name,
            provider_member=member_name,
            health_status="member_missing",
            error_message=f"Unknown provider pool member: {pool.name}/{member_name}",
        )
    if not requested_member.enabled:
        return None, "disabled", _pool_preflight_failure_result(
            executor_kind=executor_kind,
            provider_config=provider_config,
            pool_name=pool.name,
            provider_member=member_name,
            health_status="disabled",
            error_message=f"Provider pool member is disabled: {pool.name}/{member_name}",
        )

    state = _load_provider_pool_health_state(db_path=db_path).get((pool.name, member_name))
    now = datetime.now(UTC)
    if state is not None and state.quarantined_until is not None and state.quarantined_until > now:
        return None, "quarantined", _pool_preflight_failure_result(
            executor_kind=executor_kind,
            provider_config=provider_config,
            pool_name=pool.name,
            provider_member=member_name,
            health_status="quarantined",
            error_message=f"Provider pool member is quarantined: {pool.name}/{member_name}",
        )
    if state is not None and state.consecutive_failures > 0:
        return requested_member, "degraded", None
    return requested_member, "healthy", None


def _pool_preflight_failure_result(
    *,
    executor_kind: ExecutorKind,
    provider_config: ProviderConfig,
    pool_name: str,
    provider_member: str,
    health_status: str,
    error_message: str,
) -> ExecutionResult:
    started_at, started_perf = _telemetry_clock()
    result = _with_execution_telemetry(
        ExecutionResult(
            executor=executor_kind,
            status=ExecutionStatus.FAILED,
            error_message=error_message,
            metadata={"provider": provider_config.kind.value},
        ),
        started_at=started_at,
        started_perf=started_perf,
        provider_config=provider_config,
    )
    return _augment_pool_result(
        result,
        provider_pool=pool_name,
        provider_member=provider_member,
        attempted_members=[provider_member],
        retry_count=0,
        health_status=health_status,
    )


def _mark_pool_success(
    pool_name: str,
    member_name: str,
    *,
    db_path: Path | None = None,
) -> None:
    if db_path is not None:
        _write_provider_pool_health_state(
            db_path=db_path,
            pool_name=pool_name,
            member_name=member_name,
            state=_ProviderPoolHealthState(),
        )
        return
    with _PROVIDER_POOL_HEALTH_LOCK:
        _PROVIDER_POOL_HEALTH[(pool_name, member_name)] = _ProviderPoolHealthState()


def _mark_pool_failure(
    pool: ProviderPoolConfig,
    member_name: str,
    *,
    db_path: Path | None = None,
) -> None:
    key = (pool.name, member_name)
    if db_path is not None:
        state = _load_provider_pool_health_state(db_path=db_path).get(
            key,
            _ProviderPoolHealthState(),
        )
        state.consecutive_failures += 1
        if state.consecutive_failures >= pool.max_consecutive_failures:
            state.quarantined_until = datetime.now(UTC) + timedelta(
                seconds=pool.quarantine_seconds
            )
        _write_provider_pool_health_state(
            db_path=db_path,
            pool_name=pool.name,
            member_name=member_name,
            state=state,
        )
        return
    with _PROVIDER_POOL_HEALTH_LOCK:
        state = _PROVIDER_POOL_HEALTH.get(key, _ProviderPoolHealthState())
        state.consecutive_failures += 1
        if state.consecutive_failures >= pool.max_consecutive_failures:
            state.quarantined_until = datetime.now(UTC) + timedelta(
                seconds=pool.quarantine_seconds
            )
        _PROVIDER_POOL_HEALTH[key] = state


def snapshot_provider_pool_health(
    provider_pools: dict[str, ProviderPoolConfig],
    *,
    db_path: Path | None = None,
) -> list[ProviderPoolHealthReport]:
    now = datetime.now(UTC)
    health_state = _load_provider_pool_health_state(db_path=db_path)

    reports: list[ProviderPoolHealthReport] = []
    for pool_name in sorted(provider_pools):
        pool = provider_pools[pool_name]
        members: list[ProviderPoolMemberHealth] = []
        available_members = 0
        quarantined_members = 0
        degraded_members = 0
        ordered_members = sorted(pool.members, key=lambda member: -member.priority)
        for member in ordered_members:
            state = health_state.get((pool_name, member.name), _ProviderPoolHealthState())
            if not member.enabled:
                status = ProviderPoolMemberHealthStatus.DISABLED
            elif state.quarantined_until is not None and state.quarantined_until > now:
                status = ProviderPoolMemberHealthStatus.QUARANTINED
                quarantined_members += 1
            elif state.consecutive_failures > 0:
                status = ProviderPoolMemberHealthStatus.DEGRADED
                degraded_members += 1
                available_members += 1
            else:
                status = ProviderPoolMemberHealthStatus.HEALTHY
                available_members += 1
            members.append(
                ProviderPoolMemberHealth(
                    pool_name=pool_name,
                    member_name=member.name,
                    status=status,
                    enabled=member.enabled,
                    priority=member.priority,
                    model=member.model,
                    base_url=member.base_url,
                    cost_tier=member.cost_tier,
                    endpoint_class=member.endpoint_class,
                    consecutive_failures=state.consecutive_failures,
                    quarantined_until=state.quarantined_until,
                )
            )

        enabled_members = sum(1 for member in ordered_members if member.enabled)
        if enabled_members == 0:
            overall_status = ProviderPoolHealthStatus.EMPTY
        elif available_members == 0 and quarantined_members > 0:
            overall_status = ProviderPoolHealthStatus.ALL_QUARANTINED
        elif quarantined_members > 0 or degraded_members > 0:
            overall_status = ProviderPoolHealthStatus.DEGRADED
        else:
            overall_status = ProviderPoolHealthStatus.HEALTHY
        reports.append(
            ProviderPoolHealthReport(
                pool_name=pool_name,
                status=overall_status,
                strategy=pool.strategy,
                total_members=len(ordered_members),
                available_members=available_members,
                quarantined_members=quarantined_members,
                members=members,
            )
        )
    return reports


def reset_provider_pool_health(
    *,
    pool_name: str | None = None,
    member_name: str | None = None,
    db_path: Path | None = None,
) -> int:
    if db_path is not None:
        removed = _reset_provider_pool_health_db(
            db_path=db_path,
            pool_name=pool_name,
            member_name=member_name,
        )
        if pool_name is not None:
            _reset_provider_pool_cursor_db(db_path=db_path, pool_name=pool_name)
        elif member_name is None:
            _reset_provider_pool_cursor_db(db_path=db_path, pool_name=None)
        return removed
    with _PROVIDER_POOL_HEALTH_LOCK:
        if pool_name is None and member_name is None:
            with _PROVIDER_POOL_CURSOR_LOCK:
                _PROVIDER_POOL_CURSORS.clear()
        elif pool_name is not None:
            with _PROVIDER_POOL_CURSOR_LOCK:
                _PROVIDER_POOL_CURSORS.pop(pool_name, None)
        if pool_name is None and member_name is None:
            removed = len(_PROVIDER_POOL_HEALTH)
            _PROVIDER_POOL_HEALTH.clear()
            return removed
        keys = [
            key
            for key in _PROVIDER_POOL_HEALTH
            if (pool_name is None or key[0] == pool_name)
            and (member_name is None or key[1] == member_name)
        ]
        for key in keys:
            _PROVIDER_POOL_HEALTH.pop(key, None)
        return len(keys)


def _load_provider_pool_health_state(
    *,
    db_path: Path | None,
) -> dict[tuple[str, str], _ProviderPoolHealthState]:
    if db_path is None:
        with _PROVIDER_POOL_HEALTH_LOCK:
            return {
                key: _ProviderPoolHealthState(
                    consecutive_failures=value.consecutive_failures,
                    quarantined_until=value.quarantined_until,
                )
                for key, value in _PROVIDER_POOL_HEALTH.items()
            }

    resolved_path = db_path.resolve()
    _ensure_provider_pool_health_table(resolved_path)
    conn = sqlite3.connect(resolved_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT pool_name, member_name, consecutive_failures, quarantined_until
            FROM provider_pool_health
            """
        ).fetchall()
    finally:
        conn.close()

    return {
        (str(row["pool_name"]), str(row["member_name"])): _ProviderPoolHealthState(
            consecutive_failures=int(row["consecutive_failures"] or 0),
            quarantined_until=(
                datetime.fromisoformat(str(row["quarantined_until"]))
                if row["quarantined_until"]
                else None
            ),
        )
        for row in rows
    }


def _write_provider_pool_health_state(
    *,
    db_path: Path,
    pool_name: str,
    member_name: str,
    state: _ProviderPoolHealthState,
) -> None:
    resolved_path = db_path.resolve()
    _ensure_provider_pool_health_table(resolved_path)
    conn = sqlite3.connect(resolved_path, timeout=30)
    try:
        if state.consecutive_failures <= 0 and state.quarantined_until is None:
            conn.execute(
                """
                DELETE FROM provider_pool_health
                WHERE pool_name = ? AND member_name = ?
                """,
                (pool_name, member_name),
            )
        else:
            conn.execute(
                """
                INSERT INTO provider_pool_health (
                    pool_name, member_name, consecutive_failures, quarantined_until, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(pool_name, member_name) DO UPDATE SET
                    consecutive_failures = excluded.consecutive_failures,
                    quarantined_until = excluded.quarantined_until,
                    updated_at = excluded.updated_at
                """,
                (
                    pool_name,
                    member_name,
                    state.consecutive_failures,
                    (
                        state.quarantined_until.isoformat()
                        if state.quarantined_until is not None
                        else None
                    ),
                    datetime.now(UTC).isoformat(),
                ),
            )
        conn.commit()
    finally:
        conn.close()


def _reset_provider_pool_health_db(
    *,
    db_path: Path,
    pool_name: str | None = None,
    member_name: str | None = None,
) -> int:
    resolved_path = db_path.resolve()
    _ensure_provider_pool_health_table(resolved_path)
    conn = sqlite3.connect(resolved_path, timeout=30)
    try:
        where_clauses: list[str] = []
        params: list[str] = []
        if pool_name is not None:
            where_clauses.append("pool_name = ?")
            params.append(pool_name)
        if member_name is not None:
            where_clauses.append("member_name = ?")
            params.append(member_name)
        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        removed = int(
            conn.execute(
                f"SELECT COUNT(*) FROM provider_pool_health{where_sql}",
                params,
            ).fetchone()[0]
        )
        conn.execute(f"DELETE FROM provider_pool_health{where_sql}", params)
        conn.commit()
        return removed
    finally:
        conn.close()


def _ensure_provider_pool_health_table(db_path: Path) -> None:
    with _PROVIDER_POOL_HEALTH_DB_LOCK:
        if db_path in _PROVIDER_POOL_HEALTH_DB_READY:
            return
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS provider_pool_health (
                    pool_name TEXT NOT NULL,
                    member_name TEXT NOT NULL,
                    consecutive_failures INTEGER NOT NULL,
                    quarantined_until TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(pool_name, member_name)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS provider_pool_strategy_state (
                    pool_name TEXT NOT NULL PRIMARY KEY,
                    next_index INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()
        _PROVIDER_POOL_HEALTH_DB_READY.add(db_path)


def _reset_provider_pool_cursor_db(
    *,
    db_path: Path,
    pool_name: str | None,
) -> None:
    resolved_path = db_path.resolve()
    _ensure_provider_pool_health_table(resolved_path)
    conn = sqlite3.connect(resolved_path, timeout=30)
    try:
        if pool_name is None:
            conn.execute("DELETE FROM provider_pool_strategy_state")
        else:
            conn.execute(
                """
                DELETE FROM provider_pool_strategy_state
                WHERE pool_name = ?
                """,
                (pool_name,),
            )
        conn.commit()
    finally:
        conn.close()
