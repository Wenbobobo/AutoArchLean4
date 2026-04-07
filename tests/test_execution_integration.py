from __future__ import annotations

import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from typer.testing import CliRunner

from archonlab.app import app
from archonlab.config import load_config, load_workspace_config
from archonlab.execution_policy import (
    collect_required_execution_capabilities,
    collect_required_execution_kinds,
)
from archonlab.models import (
    ExecutionCapability,
    ExecutionRequirement,
    ExecutorKind,
    ProviderKind,
    ProviderPoolConfig,
)
from archonlab.services import RunService

runner = CliRunner()


def test_load_config_parses_executor_and_provider_sections(tmp_path: Path) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'artifact_root = "./artifacts"\n'
        "dry_run = false\n\n"
        "[executor]\n"
        'kind = "codex_exec"\n'
        'command = "codex"\n'
        'profile = "research"\n'
        "auto_approve = true\n\n"
        "[provider]\n"
        'model = "gpt-5.4-mini"\n'
        'cost_tier = "cheap"\n'
        'endpoint_class = "lab"\n'
        'base_url = "http://localhost:8000/v1"\n'
        'api_key_env = "LAB_KEY"\n',
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.executor.kind is ExecutorKind.CODEX_EXEC
    assert config.executor.profile == "research"
    assert config.executor.auto_approve is True
    assert config.provider.model == "gpt-5.4-mini"
    assert config.provider.cost_tier == "cheap"
    assert config.provider.endpoint_class == "lab"
    assert config.provider.base_url == "http://localhost:8000/v1"
    assert config.provider.api_key_env == "LAB_KEY"


def test_load_config_parses_named_provider_pools(tmp_path: Path) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'artifact_root = "./artifacts"\n'
        "dry_run = false\n\n"
        "[provider]\n"
        'pool = "research"\n'
        "\n"
        "[provider_pool.research]\n"
        "quarantine_seconds = 120\n"
        "max_consecutive_failures = 1\n"
        "\n"
        "[[provider_pool.research.members]]\n"
        'name = "primary"\n'
        'base_url = "http://localhost:8000/v1"\n'
        'model = "gpt-5.4-mini"\n'
        "\n"
        "[[provider_pool.research.members]]\n"
        'name = "backup"\n'
        'base_url = "http://localhost:9000/v1"\n'
        'model = "gpt-5.4"\n',
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.provider.pool == "research"
    assert isinstance(config.provider_pools["research"], ProviderPoolConfig)
    assert [member.name for member in config.provider_pools["research"].members] == [
        "primary",
        "backup",
    ]


def test_load_config_parses_command_lean_analyzer_section(tmp_path: Path) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    analyzer_script = tmp_path / "fake_analyzer.py"
    analyzer_script.write_text("print('{}')\n", encoding="utf-8")
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'artifact_root = "./artifacts"\n'
        "dry_run = true\n\n"
        "[lean_analyzer]\n"
        'kind = "command"\n'
        f'command = ["{sys.executable}", "{analyzer_script}"]\n'
        "timeout_seconds = 15\n",
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.lean_analyzer.kind == "command"
    assert config.lean_analyzer.command == [sys.executable, str(analyzer_script)]
    assert config.lean_analyzer.timeout_seconds == 15


def test_load_workspace_config_parses_command_lean_analyzer_section(tmp_path: Path) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    analyzer_script = tmp_path / "fake_analyzer.py"
    analyzer_script.write_text("print('{}')\n", encoding="utf-8")
    config_path = tmp_path / "workspace.toml"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'artifact_root = "./artifacts"\n'
        "dry_run = true\n\n"
        "[lean_analyzer]\n"
        'kind = "command"\n'
        f'command = ["{sys.executable}", "{analyzer_script}"]\n'
        "\n"
        "[[projects]]\n"
        'id = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n',
        encoding="utf-8",
    )

    config = load_workspace_config(config_path)

    assert config.lean_analyzer.kind == "command"
    assert config.lean_analyzer.command == [sys.executable, str(analyzer_script)]


def test_run_service_execute_supports_provider_pool_routing(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "archonlab.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = false\n\n"
        "[executor]\n"
        'kind = "dry_run"\n'
        "\n"
        "[provider]\n"
        'pool = "lab"\n'
        "\n"
        "[provider_pool.lab]\n"
        "max_consecutive_failures = 1\n"
        "quarantine_seconds = 60\n"
        "\n"
        "[[provider_pool.lab.members]]\n"
        'name = "member-a"\n'
        'model = "gpt-5.4-mini"\n',
        encoding="utf-8",
    )

    result = RunService(load_config(config_path)).start(dry_run=False)

    assert result.execution is not None
    assert result.execution.telemetry is not None
    assert result.execution.telemetry.provider_pool == "lab"
    assert result.execution.telemetry.provider_member == "member-a"
    assert result.execution.telemetry.retry_count == 0


def test_run_service_persists_provider_pool_health_for_cli(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    class PrimaryHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
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
        config_path = tmp_path / "archonlab.toml"
        artifact_root = tmp_path / "artifacts"
        config_path.write_text(
            "[project]\n"
            'name = "demo"\n'
            f'project_path = "{fake_archon_project}"\n'
            f'archon_path = "{fake_archon_root}"\n\n'
            "[run]\n"
            'workflow = "adaptive_loop"\n'
            f'artifact_root = "{artifact_root}"\n'
            "dry_run = false\n\n"
            "[executor]\n"
            'kind = "openai_compatible"\n'
            "\n"
            "[provider]\n"
            'pool = "lab"\n'
            "\n"
            "[provider_pool.lab]\n"
            "max_consecutive_failures = 1\n"
            "quarantine_seconds = 60\n"
            "\n"
            "[[provider_pool.lab.members]]\n"
            'name = "primary"\n'
            f'base_url = "http://127.0.0.1:{primary_server.server_port}/v1"\n'
            'model = "gpt-5.4-mini"\n'
            "\n"
            "[[provider_pool.lab.members]]\n"
            'name = "backup"\n'
            f'base_url = "http://127.0.0.1:{backup_server.server_port}/v1"\n'
            'model = "gpt-5.4-mini"\n',
            encoding="utf-8",
        )

        result = RunService(load_config(config_path)).start(dry_run=False)

        assert result.execution is not None
        assert result.execution.telemetry is not None
        assert result.execution.telemetry.provider_member == "backup"

        health_result = runner.invoke(
            app,
            ["queue", "provider-health", "--config", str(config_path)],
        )

        assert health_result.exit_code == 0
        assert "lab default | degraded | available=1/2 | quarantined=1" in health_result.output
        assert "primary | quarantined | failures=1 | model=gpt-5.4-mini" in health_result.output
        assert "backup | healthy | failures=0 | model=gpt-5.4-mini" in health_result.output
    finally:
        primary_server.shutdown()
        backup_server.shutdown()
        primary_server.server_close()
        backup_server.server_close()
        primary_thread.join(timeout=2)
        backup_thread.join(timeout=2)


def test_run_service_preview_uses_configured_command_lean_analyzer(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "archonlab.toml"
    artifact_root = tmp_path / "artifacts"
    analyzer_script = tmp_path / "structured_analyzer.py"
    analyzer_script.write_text(
        "from __future__ import annotations\n"
        "import json\n"
        "\n"
        "print(json.dumps({\n"
        f'    "project_id": "{fake_archon_project.name}",\n'
        f'    "project_path": "{fake_archon_project.resolve()}",\n'
        '    "lean_file_count": 1,\n'
        '    "theorem_count": 3,\n'
        '    "sorry_count": 1,\n'
        '    "axiom_count": 0,\n'
        '    "proof_gaps": [\n'
        '        {\n'
        '            "kind": "unsolved_goal",\n'
        '            "theorem_name": "injected_goal",\n'
        '            "file_path": "Injected.lean",\n'
        '            "message": "goal remains"\n'
        '        }\n'
        '    ],\n'
        '    "diagnostics": [\n'
        '        {\n'
        '            "severity": "error",\n'
        '            "message": "type mismatch",\n'
        '            "theorem_name": "injected_goal",\n'
        '            "file_path": "Injected.lean",\n'
        '            "code": "lean.type_mismatch"\n'
        '        }\n'
        '    ],\n'
        '    "declarations": [\n'
        '        {\n'
        '            "name": "injected_goal",\n'
        '            "file_path": "Injected.lean",\n'
        '            "declaration_kind": "theorem",\n'
        '            "dependencies": [],\n'
        '            "blocked_by_sorry": True,\n'
        '            "uses_axiom": False\n'
        '        }\n'
        '    ]\n'
        '}))\n',
        encoding="utf-8",
    )
    (fake_archon_project / "Core.lean").write_text(
        "theorem foo : True := by\n"
        "  trivial\n",
        encoding="utf-8",
    )
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n\n"
        "[lean_analyzer]\n"
        'kind = "command"\n'
        f'command = ["{sys.executable}", "{analyzer_script}"]\n',
        encoding="utf-8",
    )

    preview = RunService(load_config(config_path)).preview()

    assert preview.snapshot.theorem_count == 3
    assert preview.snapshot.sorry_count == 1
    assert preview.snapshot.analysis_backend == "command"
    assert preview.snapshot.analysis_fallback_used is False
    assert preview.snapshot.proof_gap_count == 1
    assert preview.snapshot.diagnostic_count == 1
    node = next(
        node
        for node in preview.task_graph.nodes
        if node.id == "lean:Injected.lean:injected_goal"
    )
    assert node.blockers == [
        "contains_sorry",
        "proof_gap:unsolved_goal",
        "diagnostic:lean.type_mismatch",
    ]


def test_run_service_preview_falls_back_when_command_lean_analyzer_fails(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    config_path = tmp_path / "archonlab.toml"
    artifact_root = tmp_path / "artifacts"
    analyzer_script = tmp_path / "failing_analyzer.py"
    analyzer_script.write_text("raise SystemExit(1)\n", encoding="utf-8")
    (fake_archon_project / "Core.lean").write_text(
        "theorem foo : True := by\n"
        "  sorry\n",
        encoding="utf-8",
    )
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n\n"
        "[lean_analyzer]\n"
        'kind = "command"\n'
        f'command = ["{sys.executable}", "{analyzer_script}"]\n',
        encoding="utf-8",
    )

    preview = RunService(load_config(config_path)).preview()

    assert preview.snapshot.lean_file_count >= 1
    assert preview.snapshot.theorem_count >= 1
    assert preview.snapshot.analysis_backend == "regex"
    assert preview.snapshot.analysis_fallback_used is True
    assert "Lean analyzer command failed" in (preview.snapshot.analysis_fallback_reason or "")
    assert preview.snapshot.proof_gap_count >= 1
    assert preview.snapshot.diagnostic_count == 0
    assert preview.supervisor.action.value == "investigate_infra"
    assert preview.supervisor.reason.value == "analyzer_degraded"


def test_run_service_execute_uses_configured_executor(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "archonlab.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = false\n\n"
        "[executor]\n"
        'kind = "dry_run"\n',
        encoding="utf-8",
    )

    result = RunService(load_config(config_path)).start(dry_run=False)

    assert result.execution is not None
    assert result.execution.executor is ExecutorKind.DRY_RUN
    assert result.execution.output_path is not None
    execution_payload = json.loads(
        (result.artifact_dir / "execution.json").read_text(encoding="utf-8")
    )
    assert execution_payload["executor"] == "dry_run"
    assert execution_payload["status"] == "completed"


def test_collect_required_execution_kinds_includes_phase_and_task_overrides(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'artifact_root = "./artifacts"\n'
        "dry_run = false\n\n"
        "[executor]\n"
        'kind = "codex_exec"\n'
        "\n"
        "[provider]\n"
        'model = "gpt-5.4-mini"\n'
        'cost_tier = "cheap"\n'
        'endpoint_class = "lab"\n'
        "\n"
        "[phase_executor.plan]\n"
        'kind = "dry_run"\n'
        "\n"
        "[phase_provider.plan]\n"
        'model = "gpt-5.4-nano"\n'
        'cost_tier = "nano"\n'
        'endpoint_class = "fast"\n'
        "\n"
        "[task_matcher.core_focus]\n"
        'phase = "prover"\n'
        'theorem_pattern = "^foo$"\n'
        "\n"
        "[task_executor.core_focus]\n"
        'kind = "openai_compatible"\n',
        encoding="utf-8",
    )

    config = load_config(config_path)
    (
        executor_kinds,
        provider_kinds,
        models,
        cost_tiers,
        endpoint_classes,
    ) = collect_required_execution_kinds(
        executor=config.executor,
        provider=config.provider,
        execution_policy=config.execution_policy,
    )

    assert set(executor_kinds) == {
        ExecutorKind.CODEX_EXEC,
        ExecutorKind.DRY_RUN,
        ExecutorKind.OPENAI_COMPATIBLE,
    }
    assert provider_kinds == [ProviderKind.OPENAI_COMPATIBLE]
    assert set(models) == {"gpt-5.4-mini", "gpt-5.4-nano"}
    assert set(cost_tiers) == {"cheap", "nano"}
    assert set(endpoint_classes) == {"fast", "lab"}


def test_collect_required_execution_capabilities_preserves_valid_tuples(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'artifact_root = "./artifacts"\n'
        "dry_run = false\n\n"
        "[executor]\n"
        'kind = "codex_exec"\n'
        "\n"
        "[provider]\n"
        'model = "gpt-5.4-mini"\n'
        'cost_tier = "cheap"\n'
        'endpoint_class = "lab"\n'
        "\n"
        "[phase_executor.plan]\n"
        'kind = "openai_compatible"\n'
        "\n"
        "[phase_provider.plan]\n"
        'model = "gpt-5.4"\n'
        'cost_tier = "premium"\n'
        'endpoint_class = "priority"\n',
        encoding="utf-8",
    )

    config = load_config(config_path)
    capabilities = collect_required_execution_capabilities(
        executor=config.executor,
        provider=config.provider,
        execution_policy=config.execution_policy,
    )

    assert capabilities == sorted(capabilities, key=lambda capability: capability.capability_id)
    assert capabilities == [
        ExecutionCapability(
            executor_kind=ExecutorKind.CODEX_EXEC,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4-mini",
            cost_tier="cheap",
            endpoint_class="lab",
        ),
        ExecutionCapability(
            executor_kind=ExecutorKind.OPENAI_COMPATIBLE,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4",
            cost_tier="premium",
            endpoint_class="priority",
        ),
    ]


def test_execution_requirement_matches_capability_axes_with_wildcard_worker_dimensions() -> None:
    requirement = ExecutionRequirement(
        executor_kinds=[ExecutorKind.OPENAI_COMPATIBLE],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        models=["gpt-5.4-mini"],
        cost_tiers=["cheap"],
        endpoint_classes=["lab"],
    )

    assert requirement.matches_axes(
        executor_kinds=[ExecutorKind.OPENAI_COMPATIBLE],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        models=["gpt-5.4-mini"],
        cost_tiers=["cheap"],
        endpoint_classes=["lab"],
    )
    assert requirement.matches_axes(
        executor_kinds=[ExecutorKind.OPENAI_COMPATIBLE],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        models=[],
        cost_tiers=[],
        endpoint_classes=[],
    )
    assert requirement.matches_capability(
        ExecutionCapability(
            executor_kind=ExecutorKind.OPENAI_COMPATIBLE,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4-mini",
            cost_tier="cheap",
            endpoint_class="lab",
        )
    )
    assert requirement.matches_capability(
        ExecutionCapability(
            executor_kind=ExecutorKind.OPENAI_COMPATIBLE,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model=None,
            cost_tier=None,
            endpoint_class=None,
        )
    )
    assert requirement.matches_capability(
        ExecutionCapability(
            executor_kind=ExecutorKind.OPENAI_COMPATIBLE,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4",
            cost_tier="premium",
            endpoint_class="lab",
        ),
        allow_capability_wildcards=False,
    ) is False
