from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from archonlab.executors import (
    CodexExecExecutor,
    DryRunExecutor,
    ExecutorProvider,
    OpenAICompatibleHttpExecutor,
    create_executor,
    reset_provider_pool_health,
    snapshot_provider_pool_health,
)
from archonlab.models import (
    ExecutionCapability,
    ExecutionStatus,
    ExecutorConfig,
    ExecutorKind,
    ProviderConfig,
    ProviderPoolConfig,
    ProviderPoolHealthStatus,
    ProviderPoolMemberConfig,
    ProviderPoolMemberHealthStatus,
)


def test_executor_provider_resolves_dry_run_http_and_codex_executors(
    tmp_path: Path,
) -> None:
    provider = ExecutorProvider()

    dry = provider.resolve({"kind": "dry_run"})
    http = provider.resolve(
        {
            "kind": "openai_compatible_http",
            "base_url": "https://api.example.test/v1",
            "api_key": "test-key",
            "model": "gpt-4.1-mini",
        }
    )
    codex = provider.resolve(
        {
            "kind": "codex_exec",
            "command": ["python", "-c", "print('ok')"],
            "cwd": str(tmp_path),
        }
    )

    assert isinstance(dry, DryRunExecutor)
    assert isinstance(http, OpenAICompatibleHttpExecutor)
    assert isinstance(codex, CodexExecExecutor)


def test_dry_run_executor_is_deterministic_and_side_effect_free() -> None:
    executor = DryRunExecutor()

    first = executor.execute("prove foo", system_prompt="plan")
    second = executor.execute("prove foo", system_prompt="plan")

    assert first.text == second.text
    assert first.text
    assert first.provider == "dry_run"


def test_openai_compatible_http_executor_posts_to_responses_endpoint_and_parses_text(
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["path"] = self.path
            captured["auth"] = self.headers.get("Authorization")
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode("utf-8")
            captured["body"] = json.loads(body)

            payload = {
                "output_text": "hello from http executor",
                "id": "resp-1",
                "status": "completed",
            }
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}/v1"
        executor = OpenAICompatibleHttpExecutor(
            base_url=base_url,
            api_key="test-key",
            model="gpt-4.1-mini",
        )

        result = executor.execute("prove foo", system_prompt="plan")

        assert captured["path"] == "/v1/responses"
        assert captured["auth"] == "Bearer test-key"
        assert captured["body"]["model"] == "gpt-4.1-mini"
        assert "prove foo" in json.dumps(captured["body"])
        assert result.text == "hello from http executor"
        assert result.provider == "openai_compatible_http"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_openai_compatible_http_executor_accepts_literal_api_key_in_api_key_env_field() -> None:
    captured: dict[str, object] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["auth"] = self.headers.get("Authorization")
            encoded = json.dumps({"output_text": "ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        executor = OpenAICompatibleHttpExecutor(
            provider_config=ProviderConfig(
                model="deepseek-reasoner",
                base_url=f"http://127.0.0.1:{server.server_port}",
                api_key_env="sk-literal-deepseek-key",
            ),
        )

        result = executor.execute("prove foo", system_prompt="plan")

        assert result.status is ExecutionStatus.COMPLETED
        assert captured["auth"] == "Bearer sk-literal-deepseek-key"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_openai_compatible_http_executor_posts_chat_completions_payload_when_configured() -> None:
    captured: dict[str, object] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["path"] = self.path
            captured["auth"] = self.headers.get("Authorization")
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode("utf-8")
            captured["body"] = json.loads(body)

            payload = {
                "id": "chatcmpl-1",
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": "hello from chat completions",
                        },
                    }
                ],
                "usage": {
                    "prompt_tokens": 120,
                    "completion_tokens": 30,
                    "total_tokens": 150,
                },
            }
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        executor = OpenAICompatibleHttpExecutor(
            api_key="test-key",
            endpoint_path="/v1/chat/completions",
            provider_config=ProviderConfig(
                model="deepseek-chat",
                base_url=base_url,
                endpoint_path="/v1/chat/completions",
                api_key_env="DEEPSEEK_API_KEY",
            ),
        )

        result = executor.execute("prove foo", system_prompt="plan")

        assert captured["path"] == "/v1/chat/completions"
        assert captured["auth"] == "Bearer test-key"
        assert captured["body"] == {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": "plan"},
                {"role": "user", "content": "prove foo"},
            ],
        }
        assert result.text == "hello from chat completions"
        assert result.status is ExecutionStatus.COMPLETED
        assert result.telemetry is not None
        assert result.telemetry.usage is not None
        assert result.telemetry.usage.input_tokens == 120
        assert result.telemetry.usage.output_tokens == 30
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_codex_exec_executor_streams_prompt_to_stdin_and_returns_stdout(
    tmp_path: Path,
) -> None:
    prompt_file = tmp_path / "prompt.txt"
    prompt_file.write_text("", encoding="utf-8")
    script = tmp_path / "codex.sh"
    script.write_text(
        "#!/usr/bin/env bash\n"
        "cat > \"$1\"\n"
        "printf 'codex:%s' \"$(cat \"$1\")\"\n",
        encoding="utf-8",
    )
    script.chmod(0o755)

    executor = CodexExecExecutor(
        command=[str(script), str(prompt_file)],
        cwd=tmp_path,
    )

    result = executor.execute("prove foo", system_prompt="plan")

    assert prompt_file.read_text(encoding="utf-8") == "prove foo"
    assert result.text == "codex:prove foo"
    assert result.provider == "codex_exec"


def test_create_executor_uses_capability_normalization_for_dispatch() -> None:
    dry_executor = create_executor(
        executor_config=ExecutorConfig(kind=ExecutorKind.DRY_RUN),
        provider_config=ProviderConfig(model="gpt-5.4-mini"),
    )
    http_executor = create_executor(
        executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
        provider_config=ProviderConfig(
            model="gpt-5.4-mini",
            base_url="https://api.example.test/v1",
        ),
    )
    codex_executor = create_executor(
        executor_config=ExecutorConfig(kind=ExecutorKind.CODEX_EXEC),
        provider_config=ProviderConfig(model="gpt-5.4-mini"),
    )

    assert isinstance(dry_executor, DryRunExecutor)
    assert isinstance(http_executor, OpenAICompatibleHttpExecutor)
    assert isinstance(codex_executor, CodexExecExecutor)
    capability = ExecutionCapability.from_configs(
        executor=ExecutorConfig(kind=ExecutorKind.CODEX_EXEC),
        provider=ProviderConfig(model="gpt-5.4-mini"),
    )
    assert capability.capability_id == (
        "executor=codex_exec__provider=openai_compatible__"
        "model=gpt-5.4-mini__cost=any__endpoint=any"
    )


def test_openai_compatible_http_executor_records_telemetry_usage_and_cost(tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            captured["body"] = json.loads(self.rfile.read(length).decode("utf-8"))
            payload = {
                "output_text": "telemetry-ok",
                "usage": {
                    "input_tokens": 200,
                    "output_tokens": 50,
                    "total_tokens": 250,
                },
            }
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        executor = OpenAICompatibleHttpExecutor(
            base_url=f"http://127.0.0.1:{server.server_port}/v1",
            api_key="test-key",
            model="gpt-4.1-mini",
            provider_config=ProviderConfig(
                model="gpt-4.1-mini",
                base_url=f"http://127.0.0.1:{server.server_port}/v1",
                input_cost_per_1k_tokens=0.5,
                output_cost_per_1k_tokens=1.5,
            ),
        )

        result = executor.execute("prove foo", system_prompt="plan")

        assert result.status is ExecutionStatus.COMPLETED
        assert result.telemetry is not None
        assert result.telemetry.latency_ms is not None
        assert result.telemetry.latency_ms >= 0
        assert result.telemetry.usage is not None
        assert result.telemetry.usage.input_tokens == 200
        assert result.telemetry.usage.output_tokens == 50
        assert result.telemetry.cost_estimate == 0.175
        assert result.telemetry.status_code == 200
        assert result.text == "telemetry-ok"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_provider_pool_executor_fails_over_and_quarantines_unhealthy_member() -> None:
    reset_provider_pool_health()
    captured = {"primary": 0, "backup": 0}

    class PrimaryHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["primary"] += 1
            payload = {"error": "primary unavailable"}
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    class BackupHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["backup"] += 1
            payload = {"output_text": "backup-ok"}
            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    primary_server = ThreadingHTTPServer(("127.0.0.1", 0), PrimaryHandler)
    backup_server = ThreadingHTTPServer(("127.0.0.1", 0), BackupHandler)
    primary_thread = threading.Thread(target=primary_server.serve_forever, daemon=True)
    backup_thread = threading.Thread(target=backup_server.serve_forever, daemon=True)
    primary_thread.start()
    backup_thread.start()
    try:
        pool_name = "research"
        provider_pools = {
            pool_name: ProviderPoolConfig(
                name=pool_name,
                max_consecutive_failures=1,
                quarantine_seconds=600,
                members=[
                    ProviderPoolMemberConfig(
                        name="primary",
                        base_url=f"http://127.0.0.1:{primary_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                    ProviderPoolMemberConfig(
                        name="backup",
                        base_url=f"http://127.0.0.1:{backup_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                ],
            )
        }

        executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
        )
        first = executor.execute("prove foo", system_prompt="plan")
        second = executor.execute("prove foo again", system_prompt="plan")

        assert first.status is ExecutionStatus.COMPLETED
        assert first.text == "backup-ok"
        assert first.telemetry is not None
        assert first.telemetry.provider_pool == pool_name
        assert first.telemetry.provider_member == "backup"
        assert first.telemetry.retry_count == 1
        assert first.telemetry.attempted_members == ["primary", "backup"]

        assert second.status is ExecutionStatus.COMPLETED
        assert second.telemetry is not None
        assert second.telemetry.provider_member == "backup"
        assert second.telemetry.retry_count == 0
        assert second.telemetry.attempted_members == ["backup"]

        assert captured["primary"] == 1
        assert captured["backup"] == 2
    finally:
        primary_server.shutdown()
        backup_server.shutdown()
        primary_server.server_close()
        backup_server.server_close()
        primary_thread.join(timeout=2)
        backup_thread.join(timeout=2)
        reset_provider_pool_health()


def test_provider_pool_health_snapshot_reports_quarantine_and_manual_reset() -> None:
    reset_provider_pool_health()
    captured = {"primary": 0, "backup": 0}

    class PrimaryHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["primary"] += 1
            encoded = json.dumps({"error": "primary unavailable"}).encode("utf-8")
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    class BackupHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["backup"] += 1
            encoded = json.dumps({"output_text": "backup-ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    primary_server = ThreadingHTTPServer(("127.0.0.1", 0), PrimaryHandler)
    backup_server = ThreadingHTTPServer(("127.0.0.1", 0), BackupHandler)
    primary_thread = threading.Thread(target=primary_server.serve_forever, daemon=True)
    backup_thread = threading.Thread(target=backup_server.serve_forever, daemon=True)
    primary_thread.start()
    backup_thread.start()
    try:
        pool_name = "research"
        provider_pools = {
            pool_name: ProviderPoolConfig(
                name=pool_name,
                max_consecutive_failures=1,
                quarantine_seconds=600,
                members=[
                    ProviderPoolMemberConfig(
                        name="primary",
                        base_url=f"http://127.0.0.1:{primary_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                    ProviderPoolMemberConfig(
                        name="backup",
                        base_url=f"http://127.0.0.1:{backup_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                ],
            )
        }

        executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
        )
        result = executor.execute("prove foo", system_prompt="plan")

        assert result.status is ExecutionStatus.COMPLETED
        report = snapshot_provider_pool_health(provider_pools)[0]
        primary = next(member for member in report.members if member.member_name == "primary")
        backup = next(member for member in report.members if member.member_name == "backup")

        assert report.status is ProviderPoolHealthStatus.DEGRADED
        assert primary.status is ProviderPoolMemberHealthStatus.QUARANTINED
        assert primary.consecutive_failures == 1
        assert backup.status is ProviderPoolMemberHealthStatus.HEALTHY
        assert reset_provider_pool_health(pool_name=pool_name, member_name="primary") == 1

        refreshed = snapshot_provider_pool_health(provider_pools)[0]
        refreshed_primary = next(
            member for member in refreshed.members if member.member_name == "primary"
        )
        assert refreshed_primary.status is ProviderPoolMemberHealthStatus.HEALTHY
        assert refreshed.status is ProviderPoolHealthStatus.HEALTHY
    finally:
        primary_server.shutdown()
        backup_server.shutdown()
        primary_server.server_close()
        backup_server.server_close()
        primary_thread.join(timeout=2)
        backup_thread.join(timeout=2)
        reset_provider_pool_health()


def test_provider_pool_health_persists_across_executor_instances_with_shared_db(
    tmp_path: Path,
) -> None:
    reset_provider_pool_health()
    db_path = tmp_path / "archonlab.db"
    captured = {"primary": 0, "backup": 0}

    class PrimaryHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["primary"] += 1
            encoded = json.dumps({"error": "primary unavailable"}).encode("utf-8")
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    class BackupHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["backup"] += 1
            encoded = json.dumps({"output_text": "backup-ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    primary_server = ThreadingHTTPServer(("127.0.0.1", 0), PrimaryHandler)
    backup_server = ThreadingHTTPServer(("127.0.0.1", 0), BackupHandler)
    primary_thread = threading.Thread(target=primary_server.serve_forever, daemon=True)
    backup_thread = threading.Thread(target=backup_server.serve_forever, daemon=True)
    primary_thread.start()
    backup_thread.start()
    try:
        pool_name = "research"
        provider_pools = {
            pool_name: ProviderPoolConfig(
                name=pool_name,
                max_consecutive_failures=1,
                quarantine_seconds=600,
                members=[
                    ProviderPoolMemberConfig(
                        name="primary",
                        base_url=f"http://127.0.0.1:{primary_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                    ProviderPoolMemberConfig(
                        name="backup",
                        base_url=f"http://127.0.0.1:{backup_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                ],
            )
        }

        first_executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
            provider_health_db_path=db_path,
        )
        first = first_executor.execute("prove foo", system_prompt="plan")

        second_executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
            provider_health_db_path=db_path,
        )
        second = second_executor.execute("prove foo again", system_prompt="plan")

        assert first.status is ExecutionStatus.COMPLETED
        assert second.status is ExecutionStatus.COMPLETED
        assert second.telemetry is not None
        assert second.telemetry.provider_member == "backup"
        assert second.telemetry.retry_count == 0
        assert second.telemetry.attempted_members == ["backup"]
        assert captured["primary"] == 1
        assert captured["backup"] == 2

        report = snapshot_provider_pool_health(provider_pools, db_path=db_path)[0]
        primary = next(member for member in report.members if member.member_name == "primary")
        assert primary.status is ProviderPoolMemberHealthStatus.QUARANTINED
        assert reset_provider_pool_health(
            pool_name=pool_name,
            member_name="primary",
            db_path=db_path,
        ) == 1

        third_executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
            provider_health_db_path=db_path,
        )
        third = third_executor.execute("prove foo reset", system_prompt="plan")

        assert third.status is ExecutionStatus.COMPLETED
        assert third.telemetry is not None
        assert third.telemetry.attempted_members == ["primary", "backup"]
        assert captured["primary"] == 2
        assert captured["backup"] == 3
    finally:
        primary_server.shutdown()
        backup_server.shutdown()
        primary_server.server_close()
        backup_server.server_close()
        primary_thread.join(timeout=2)
        backup_thread.join(timeout=2)
        reset_provider_pool_health()
        reset_provider_pool_health(db_path=db_path)


def test_provider_pool_executor_round_robin_rotates_across_shared_db_instances(
    tmp_path: Path,
) -> None:
    reset_provider_pool_health()
    db_path = tmp_path / "archonlab.db"
    captured = {"primary": 0, "backup": 0}

    class PrimaryHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["primary"] += 1
            encoded = json.dumps({"output_text": "primary-ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    class BackupHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["backup"] += 1
            encoded = json.dumps({"output_text": "backup-ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    primary_server = ThreadingHTTPServer(("127.0.0.1", 0), PrimaryHandler)
    backup_server = ThreadingHTTPServer(("127.0.0.1", 0), BackupHandler)
    primary_thread = threading.Thread(target=primary_server.serve_forever, daemon=True)
    backup_thread = threading.Thread(target=backup_server.serve_forever, daemon=True)
    primary_thread.start()
    backup_thread.start()
    try:
        pool_name = "research"
        provider_pools = {
            pool_name: ProviderPoolConfig(
                name=pool_name,
                strategy="round_robin",
                members=[
                    ProviderPoolMemberConfig(
                        name="primary",
                        base_url=f"http://127.0.0.1:{primary_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                    ProviderPoolMemberConfig(
                        name="backup",
                        base_url=f"http://127.0.0.1:{backup_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                ],
            )
        }

        first_executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
            provider_health_db_path=db_path,
        )
        second_executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
            provider_health_db_path=db_path,
        )
        third_executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
            provider_health_db_path=db_path,
        )

        first = first_executor.execute("prove foo", system_prompt="plan")
        second = second_executor.execute("prove bar", system_prompt="plan")
        third = third_executor.execute("prove baz", system_prompt="plan")

        assert first.status is ExecutionStatus.COMPLETED
        assert second.status is ExecutionStatus.COMPLETED
        assert third.status is ExecutionStatus.COMPLETED
        assert first.telemetry is not None
        assert second.telemetry is not None
        assert third.telemetry is not None
        assert first.telemetry.provider_member == "primary"
        assert second.telemetry.provider_member == "backup"
        assert third.telemetry.provider_member == "primary"
        assert first.telemetry.attempted_members == ["primary"]
        assert second.telemetry.attempted_members == ["backup"]
        assert third.telemetry.attempted_members == ["primary"]
        assert captured["primary"] == 2
        assert captured["backup"] == 1
    finally:
        primary_server.shutdown()
        backup_server.shutdown()
        primary_server.server_close()
        backup_server.server_close()
        primary_thread.join(timeout=2)
        backup_thread.join(timeout=2)
        reset_provider_pool_health()
        reset_provider_pool_health(db_path=db_path)


def test_provider_pool_executor_round_robin_continues_across_healthy_members_after_quarantine(
    tmp_path: Path,
) -> None:
    reset_provider_pool_health()
    db_path = tmp_path / "archonlab.db"
    captured = {"primary": 0, "backup": 0, "tertiary": 0}

    class PrimaryHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["primary"] += 1
            encoded = json.dumps({"error": "primary unavailable"}).encode("utf-8")
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    class BackupHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["backup"] += 1
            encoded = json.dumps({"output_text": "backup-ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    class TertiaryHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["tertiary"] += 1
            encoded = json.dumps({"output_text": "tertiary-ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    primary_server = ThreadingHTTPServer(("127.0.0.1", 0), PrimaryHandler)
    backup_server = ThreadingHTTPServer(("127.0.0.1", 0), BackupHandler)
    tertiary_server = ThreadingHTTPServer(("127.0.0.1", 0), TertiaryHandler)
    primary_thread = threading.Thread(target=primary_server.serve_forever, daemon=True)
    backup_thread = threading.Thread(target=backup_server.serve_forever, daemon=True)
    tertiary_thread = threading.Thread(target=tertiary_server.serve_forever, daemon=True)
    primary_thread.start()
    backup_thread.start()
    tertiary_thread.start()
    try:
        pool_name = "research"
        provider_pools = {
            pool_name: ProviderPoolConfig(
                name=pool_name,
                strategy="round_robin",
                max_consecutive_failures=1,
                quarantine_seconds=600,
                members=[
                    ProviderPoolMemberConfig(
                        name="primary",
                        base_url=f"http://127.0.0.1:{primary_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                    ProviderPoolMemberConfig(
                        name="backup",
                        base_url=f"http://127.0.0.1:{backup_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                    ProviderPoolMemberConfig(
                        name="tertiary",
                        base_url=f"http://127.0.0.1:{tertiary_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                ],
            )
        }

        first_executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
            provider_health_db_path=db_path,
        )
        second_executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
            provider_health_db_path=db_path,
        )
        third_executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
            provider_health_db_path=db_path,
        )

        first = first_executor.execute("prove foo", system_prompt="plan")
        second = second_executor.execute("prove bar", system_prompt="plan")
        third = third_executor.execute("prove baz", system_prompt="plan")

        assert first.status is ExecutionStatus.COMPLETED
        assert second.status is ExecutionStatus.COMPLETED
        assert third.status is ExecutionStatus.COMPLETED
        assert first.telemetry is not None
        assert second.telemetry is not None
        assert third.telemetry is not None
        assert first.telemetry.provider_member == "backup"
        assert second.telemetry.provider_member == "tertiary"
        assert third.telemetry.provider_member == "backup"
        assert first.telemetry.attempted_members == ["primary", "backup"]
        assert second.telemetry.attempted_members == ["tertiary"]
        assert third.telemetry.attempted_members == ["backup"]
        assert captured["primary"] == 1
        assert captured["backup"] == 2
        assert captured["tertiary"] == 1

        report = snapshot_provider_pool_health(provider_pools, db_path=db_path)[0]
        primary = next(member for member in report.members if member.member_name == "primary")
        assert primary.status is ProviderPoolMemberHealthStatus.QUARANTINED
    finally:
        primary_server.shutdown()
        backup_server.shutdown()
        tertiary_server.shutdown()
        primary_server.server_close()
        backup_server.server_close()
        tertiary_server.server_close()
        primary_thread.join(timeout=2)
        backup_thread.join(timeout=2)
        tertiary_thread.join(timeout=2)
        reset_provider_pool_health()
        reset_provider_pool_health(db_path=db_path)


def test_provider_pool_executor_honors_pinned_member_without_fallback() -> None:
    captured = {"primary": 0, "backup": 0}

    class PrimaryHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["primary"] += 1
            encoded = json.dumps({"output_text": "primary-ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    class BackupHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["backup"] += 1
            encoded = json.dumps({"output_text": "backup-ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    primary_server = ThreadingHTTPServer(("127.0.0.1", 0), PrimaryHandler)
    backup_server = ThreadingHTTPServer(("127.0.0.1", 0), BackupHandler)
    primary_thread = threading.Thread(target=primary_server.serve_forever, daemon=True)
    backup_thread = threading.Thread(target=backup_server.serve_forever, daemon=True)
    primary_thread.start()
    backup_thread.start()
    try:
        pool_name = "research"
        provider_pools = {
            pool_name: ProviderPoolConfig(
                name=pool_name,
                members=[
                    ProviderPoolMemberConfig(
                        name="primary",
                        base_url=f"http://127.0.0.1:{primary_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                    ProviderPoolMemberConfig(
                        name="backup",
                        base_url=f"http://127.0.0.1:{backup_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                ],
            )
        }

        executor = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name, member_name="backup"),
            provider_pools=provider_pools,
        )
        result = executor.execute("prove foo", system_prompt="plan")

        assert result.status is ExecutionStatus.COMPLETED
        assert result.text == "backup-ok"
        assert result.telemetry is not None
        assert result.telemetry.provider_member == "backup"
        assert result.telemetry.retry_count == 0
        assert result.telemetry.attempted_members == ["backup"]
        assert captured["primary"] == 0
        assert captured["backup"] == 1
    finally:
        primary_server.shutdown()
        backup_server.shutdown()
        primary_server.server_close()
        backup_server.server_close()
        primary_thread.join(timeout=2)
        backup_thread.join(timeout=2)


def test_provider_pool_executor_returns_failed_result_for_unavailable_pinned_member() -> None:
    reset_provider_pool_health()
    captured = {"primary": 0, "backup": 0}

    class PrimaryHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["primary"] += 1
            encoded = json.dumps({"error": "primary unavailable"}).encode("utf-8")
            self.send_response(503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    class BackupHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            captured["backup"] += 1
            encoded = json.dumps({"output_text": "backup-ok"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            del format, args

    primary_server = ThreadingHTTPServer(("127.0.0.1", 0), PrimaryHandler)
    backup_server = ThreadingHTTPServer(("127.0.0.1", 0), BackupHandler)
    primary_thread = threading.Thread(target=primary_server.serve_forever, daemon=True)
    backup_thread = threading.Thread(target=backup_server.serve_forever, daemon=True)
    primary_thread.start()
    backup_thread.start()
    try:
        pool_name = "research"
        provider_pools = {
            pool_name: ProviderPoolConfig(
                name=pool_name,
                max_consecutive_failures=1,
                quarantine_seconds=600,
                members=[
                    ProviderPoolMemberConfig(
                        name="primary",
                        base_url=f"http://127.0.0.1:{primary_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                    ProviderPoolMemberConfig(
                        name="backup",
                        base_url=f"http://127.0.0.1:{backup_server.server_port}/v1",
                        model="gpt-4.1-mini",
                    ),
                ],
            )
        }

        warmup = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name),
            provider_pools=provider_pools,
        )
        warmup_result = warmup.execute("prove foo", system_prompt="plan")
        assert warmup_result.status is ExecutionStatus.COMPLETED
        assert captured["primary"] == 1
        assert captured["backup"] == 1

        pinned = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name, member_name="primary"),
            provider_pools=provider_pools,
        )
        quarantined_result = pinned.execute("prove foo again", system_prompt="plan")

        assert quarantined_result.status is ExecutionStatus.FAILED
        assert quarantined_result.telemetry is not None
        assert quarantined_result.telemetry.provider_member == "primary"
        assert quarantined_result.telemetry.attempted_members == ["primary"]
        assert quarantined_result.telemetry.retry_count == 0
        assert "quarantined" in (quarantined_result.error_message or "")
        assert captured["primary"] == 1
        assert captured["backup"] == 1

        missing = create_executor(
            executor_config=ExecutorConfig(kind=ExecutorKind.OPENAI_COMPATIBLE),
            provider_config=ProviderConfig(pool=pool_name, member_name="missing"),
            provider_pools=provider_pools,
        )
        missing_result = missing.execute("prove missing", system_prompt="plan")

        assert missing_result.status is ExecutionStatus.FAILED
        assert missing_result.telemetry is not None
        assert missing_result.telemetry.provider_member == "missing"
        assert missing_result.telemetry.attempted_members == ["missing"]
        assert "Unknown provider pool member" in (missing_result.error_message or "")
        assert captured["primary"] == 1
        assert captured["backup"] == 1
    finally:
        primary_server.shutdown()
        backup_server.shutdown()
        primary_server.server_close()
        backup_server.server_close()
        primary_thread.join(timeout=2)
        backup_thread.join(timeout=2)
        reset_provider_pool_health()
