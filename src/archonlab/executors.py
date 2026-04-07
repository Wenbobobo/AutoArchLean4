from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Protocol
from urllib import error, request
from urllib.parse import urlsplit, urlunsplit

from .models import (
    ExecutionRequest,
    ExecutionResult,
    ExecutionStatus,
    ExecutorConfig,
    ExecutorKind,
    ProviderConfig,
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
        if kind == "dry_run":
            return DryRunExecutor()
        if kind in {"openai_compatible", "openai_compatible_http"}:
            return OpenAICompatibleHttpExecutor(
                base_url=str(config["base_url"]),
                api_key=str(config.get("api_key", "")),
                model=str(config["model"]),
            )
        if kind == "codex_exec":
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
        raise ValueError(f"Unsupported executor kind: {kind}")


def create_executor(
    *,
    executor_config: ExecutorConfig,
    provider_config: ProviderConfig,
) -> Executor:
    if executor_config.kind is ExecutorKind.DRY_RUN:
        return DryRunExecutor()
    if executor_config.kind is ExecutorKind.CODEX_EXEC:
        return CodexExecExecutor(
            executor_config=executor_config,
            provider_config=provider_config,
        )
    return OpenAICompatibleHttpExecutor(
        executor_config=executor_config,
        provider_config=provider_config,
        endpoint_path=provider_config.endpoint_path,
    )


class DryRunExecutor:
    def execute(
        self,
        request_or_prompt: ExecutionRequest | str,
        system_prompt: str | None = None,
    ) -> ExecutionResult:
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
        return ExecutionResult(
            executor=ExecutorKind.DRY_RUN,
            status=ExecutionStatus.COMPLETED,
            response_text=output_text,
            output_path=output_path,
            metadata={"provider": "dry_run"},
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
            return ExecutionResult(
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

        if response_path is not None:
            response_path.write_text(raw_response, encoding="utf-8")
        parsed = json.loads(raw_response)
        response_text = _extract_openai_text(parsed)
        if output_path is not None:
            output_path.write_text(response_text, encoding="utf-8")
        return ExecutionResult(
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
        proc = subprocess.run(
            command,
            cwd=cwd,
            input=prompt,
            text=True,
            capture_output=True,
            timeout=self.executor_config.timeout_seconds,
            check=False,
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
        return ExecutionResult(
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
