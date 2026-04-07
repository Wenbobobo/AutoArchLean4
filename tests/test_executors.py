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
