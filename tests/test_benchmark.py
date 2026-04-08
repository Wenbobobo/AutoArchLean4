from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from archonlab.benchmark import (
    BenchmarkRunService,
    collect_project_snapshot,
    load_benchmark_manifest,
    score_project_snapshot,
)
from archonlab.events import EventStore


def _init_git_repo(repo_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "ArchonLab"], cwd=repo_path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "archonlab@example.com"],
        cwd=repo_path,
        check=True,
    )
    subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "commit", "-m", "initial"],
        cwd=repo_path,
        check=True,
        capture_output=True,
    )


def _write_benchmark_manifest(tmp_path: Path) -> Path:
    project_dir = tmp_path / "DemoProject"
    archon_root = tmp_path / "Archon"
    project_dir.mkdir()
    archon_root.mkdir()
    (archon_root / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    (project_dir / "Core.lean").write_text(
        "theorem helper : True := by\n"
        "  trivial\n\n"
        "theorem foo : True := by\n"
        "  sorry\n",
        encoding="utf-8",
    )

    state_dir = project_dir / ".archon"
    prompts_dir = state_dir / "prompts"
    task_results_dir = state_dir / "task_results"
    prompts_dir.mkdir(parents=True)
    task_results_dir.mkdir()
    (project_dir / "lakefile.lean").write_text("import Lake\n", encoding="utf-8")
    (state_dir / "CLAUDE.md").write_text("# demo\n", encoding="utf-8")
    (prompts_dir / "plan.md").write_text("# plan\n", encoding="utf-8")
    (prompts_dir / "prover-prover.md").write_text("# prover\n", encoding="utf-8")
    (state_dir / "PROGRESS.md").write_text(
        "# Project Progress\n\n"
        "## Current Stage\n"
        "prover\n\n"
        "## Stages\n"
        "- [x] init\n"
        "- [ ] prover\n\n"
        "## Current Objectives\n\n"
        "1. **Core.lean** - fill theorem `foo`\n",
        encoding="utf-8",
    )

    manifest_path = tmp_path / "benchmark.toml"
    manifest_path.write_text(
        "[benchmark]\n"
        'name = "smoke"\n'
        'description = "Minimal benchmark manifest for Phase 3."\n'
        'artifact_root = "./artifacts/benchmarks/smoke"\n\n'
        "[[projects]]\n"
        'id = "demo-project"\n'
        'path = "./DemoProject"\n'
        'archon_path = "./Archon"\n'
        "budget_minutes = 30\n"
        'workflow = "adaptive_loop"\n',
        encoding="utf-8",
    )
    return manifest_path


def test_load_benchmark_manifest_parses_benchmark_and_projects(tmp_path: Path) -> None:
    from archonlab.models import LeanAnalyzerKind, WorkflowMode

    manifest_path = _write_benchmark_manifest(tmp_path)
    analyzer_script = tmp_path / "lean_sidecar.py"
    analyzer_script.write_text("print('{}')\n", encoding="utf-8")
    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8")
        + (
            "\n[lean_analyzer]\n"
            'kind = "command"\n'
            f'command = ["{sys.executable}", "{analyzer_script}"]\n'
            "timeout_seconds = 15\n"
        ),
        encoding="utf-8",
    )

    manifest = load_benchmark_manifest(manifest_path)

    assert manifest.benchmark.name == "smoke"
    assert manifest.benchmark.description == "Minimal benchmark manifest for Phase 3."
    assert manifest.benchmark.artifact_root == (
        tmp_path / "artifacts" / "benchmarks" / "smoke"
    ).resolve()
    assert len(manifest.projects) == 1
    project = manifest.projects[0]
    assert project.id == "demo-project"
    assert project.project_path == (tmp_path / "DemoProject").resolve()
    assert project.archon_path == (tmp_path / "Archon").resolve()
    assert project.budget_minutes == 30
    assert project.workflow is WorkflowMode.ADAPTIVE_LOOP
    assert manifest.lean_analyzer.kind is LeanAnalyzerKind.COMMAND
    assert manifest.lean_analyzer.command == [sys.executable, str(analyzer_script)]
    assert manifest.lean_analyzer.timeout_seconds == 15


def test_collect_project_snapshot_and_score(
    tmp_path: Path,
) -> None:
    project_dir = tmp_path / "DemoProject"
    archon_root = tmp_path / "Archon"
    project_dir.mkdir()
    archon_root.mkdir()
    (archon_root / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    state_dir = project_dir / ".archon"
    prompts_dir = state_dir / "prompts"
    task_results_dir = state_dir / "task_results"
    review_session_dir = state_dir / "proof-journal" / "sessions" / "session-1"
    prompts_dir.mkdir(parents=True)
    task_results_dir.mkdir()
    review_session_dir.mkdir(parents=True)
    (project_dir / "lakefile.lean").write_text("import Lake\n", encoding="utf-8")
    (state_dir / "CLAUDE.md").write_text("# demo\n", encoding="utf-8")
    (prompts_dir / "plan.md").write_text("# plan\n", encoding="utf-8")
    (prompts_dir / "prover-prover.md").write_text("# prover\n", encoding="utf-8")
    (state_dir / "PROGRESS.md").write_text(
        "# Project Progress\n\n"
        "## Current Stage\n"
        "prover\n\n"
        "## Stages\n"
        "- [x] init\n"
        "- [ ] prover\n\n"
        "## Current Objectives\n\n"
        "1. **Core.lean** — fill theorem `foo`\n",
        encoding="utf-8",
    )
    task_result = task_results_dir / "task-1.md"
    task_result.write_text("# task result\n", encoding="utf-8")
    (review_session_dir / "review.md").write_text("# review\n", encoding="utf-8")

    snapshot = collect_project_snapshot(
        project_path=project_dir,
        archon_path=archon_root,
    )
    score = score_project_snapshot(snapshot)

    assert snapshot.project_path == project_dir.resolve()
    assert snapshot.archon_path == archon_root.resolve()
    assert snapshot.progress.stage == "prover"
    assert snapshot.progress.objectives == ["**Core.lean** - fill theorem `foo`"]
    assert snapshot.task_results == [task_result.resolve()]
    assert snapshot.review_sessions == [review_session_dir.resolve()]
    assert score.stage == "prover"
    assert score.objective_count == 1
    assert score.task_result_count == 1
    assert score.review_session_count == 1
    assert score.score >= 1


def test_benchmark_run_service_writes_manifest_copy_and_summary(
    tmp_path: Path,
) -> None:
    manifest_path = _write_benchmark_manifest(tmp_path)

    service = BenchmarkRunService(manifest_path)
    result = service.run()

    assert result.artifact_dir.exists()
    assert result.manifest_copy_path.exists()
    assert result.summary_path.exists()
    assert result.ledger_path is not None
    assert result.ledger_path.exists()
    assert result.manifest_copy_path.read_text(encoding="utf-8") == manifest_path.read_text(
        encoding="utf-8"
    )

    summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
    assert summary["benchmark"]["name"] == "smoke"
    assert summary["projects"][0]["id"] == "demo-project"
    assert summary["projects"][0]["run_status"] == "completed"
    assert summary["projects"][0]["snapshot"]["progress"]["stage"] == "prover"
    assert summary["projects"][0]["snapshot"]["theorem_state_counts"]["contains_sorry"] == 1
    assert summary["projects"][0]["score"]["task_result_count"] == 0
    assert summary["ledger_path"] == str(result.ledger_path)
    assert summary["ledger_summary"]["total_projects"] == 1

    ledger = json.loads(result.ledger_path.read_text(encoding="utf-8"))
    assert ledger["benchmark_name"] == "smoke"
    assert ledger["summary"]["total_projects"] == 1
    assert ledger["outcomes"][0]["project_id"] == "demo-project"
    outcomes = {
        item["theorem_name"]: item for item in summary["projects"][0]["theorem_outcomes"]
    }
    assert outcomes["helper"]["outcome"] == "unchanged"
    assert outcomes["helper"]["after_state"] == "proved"
    assert outcomes["helper"]["failure_categories"] == []
    assert outcomes["foo"]["outcome"] == "unchanged"
    assert outcomes["foo"]["after_state"] == "contains_sorry"
    taxonomy = {
        item["category"]: item for item in summary["projects"][0]["failure_taxonomy"]
    }
    assert taxonomy["contains_sorry"]["count"] == 1
    assert taxonomy["contains_sorry"]["samples"] == ["foo"]


def test_benchmark_run_service_persists_result_in_event_store(tmp_path: Path) -> None:
    manifest_path = _write_benchmark_manifest(tmp_path)

    result = BenchmarkRunService(manifest_path).run()
    store = EventStore(result.benchmark.artifact_root / "archonlab.db")

    listed = store.list_benchmark_runs()
    detail = store.get_benchmark_run(result.run_id)

    assert len(listed) == 1
    assert listed[0].run_id == result.run_id
    assert detail is not None
    assert detail.benchmark.name == "smoke"
    assert detail.ledger_path == result.ledger_path
    assert detail.ledger_summary is not None
    assert detail.ledger_summary.total_projects == 1


def test_benchmark_run_service_can_use_worktrees(tmp_path: Path) -> None:
    manifest_path = _write_benchmark_manifest(tmp_path)
    _init_git_repo(tmp_path / "DemoProject")

    service = BenchmarkRunService(manifest_path)
    result = service.run(use_worktrees=True, cleanup_worktrees=False)

    assert result.projects[0].run_status.value == "completed"
    assert result.projects[0].worktree_path is not None
    assert result.projects[0].lease_path is not None
    assert result.projects[0].worktree_path.exists()
    assert result.projects[0].lease_path.exists()


def test_benchmark_run_service_uses_configured_command_lean_analyzer(tmp_path: Path) -> None:
    manifest_path = _write_benchmark_manifest(tmp_path)
    analyzer_script = tmp_path / "benchmark_sidecar.py"
    analyzer_script.write_text(
        "from __future__ import annotations\n"
        "import json\n"
        "import sys\n"
        "from pathlib import Path\n"
        "\n"
        "args = sys.argv[1:]\n"
        "project_path = Path(args[args.index('--project-path') + 1]).resolve()\n"
        "print(json.dumps({\n"
        '    "project_id": project_path.name,\n'
        '    "project_path": str(project_path),\n'
        '    "lean_file_count": 1,\n'
        '    "theorem_count": 3,\n'
        '    "sorry_count": 0,\n'
        '    "axiom_count": 1,\n'
        '    "declarations": [\n'
        '        {\n'
        '            "name": "sidecar_goal",\n'
        '            "file_path": "Injected.lean",\n'
        '            "declaration_kind": "theorem",\n'
        '            "dependencies": [],\n'
        '            "blocked_by_sorry": False,\n'
        '            "uses_axiom": True\n'
        '        }\n'
        '    ]\n'
        '}))\n',
        encoding="utf-8",
    )
    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8")
        + (
            "\n[lean_analyzer]\n"
            'kind = "command"\n'
            f'command = ["{sys.executable}", "{analyzer_script}"]\n'
        ),
        encoding="utf-8",
    )

    result = BenchmarkRunService(manifest_path).run()

    project_summary = result.projects[0]
    assert project_summary.snapshot.theorem_count == 3
    assert project_summary.snapshot.axiom_count == 1
    assert [outcome.theorem_name for outcome in project_summary.theorem_outcomes] == [
        "sidecar_goal"
    ]
    assert project_summary.theorem_outcomes[0].after_state.value == "uses_axiom"

    assert project_summary.artifact_dir is not None
    run_summary = json.loads(
        (project_summary.artifact_dir / "run-summary.json").read_text(encoding="utf-8")
    )
    assert run_summary["snapshot"]["theorem_count"] == 3
    node_ids = {node["id"] for node in run_summary["task_graph"]["nodes"]}
    assert "lean:Injected.lean:sidecar_goal" in node_ids
