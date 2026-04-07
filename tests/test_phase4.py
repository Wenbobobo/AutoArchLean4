from __future__ import annotations

import subprocess
from pathlib import Path

from archonlab.models import (
    EventRecord,
    ProgressSnapshot,
    ProjectSnapshot,
    SupervisorAction,
    SupervisorReason,
    TaskSource,
    TaskStatus,
)


def _make_fake_project(tmp_path: Path, *, include_sorry: bool) -> tuple[Path, Path, Path]:
    project_path = tmp_path / "DemoProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir(parents=True)
    archon_path.mkdir(parents=True)
    (archon_path / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")

    state_dir = project_path / ".archon"
    prompts_dir = state_dir / "prompts"
    prompts_dir.mkdir(parents=True)
    (state_dir / "CLAUDE.md").write_text("# demo\n", encoding="utf-8")
    (prompts_dir / "plan.md").write_text("# plan\n", encoding="utf-8")
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
    (project_path / "Core.lean").write_text(
        "theorem foo : True := by\n"
        "  trivial\n\n"
        "theorem bar : True := by\n"
        f"  {'sorry' if include_sorry else 'trivial'}\n",
        encoding="utf-8",
    )
    return project_path, archon_path, state_dir


def _init_git_repo(repo_path: Path) -> None:
    subprocess.run(["git", "init"], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "ArchonLab"], cwd=repo_path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "archonlab@example.com"],
        cwd=repo_path,
        check=True,
    )
    (repo_path / "README.md").write_text("# demo\n", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo_path, check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "commit", "-m", "initial"],
        cwd=repo_path,
        check=True,
        capture_output=True,
    )


def _snapshot_for_graph(
    *,
    graph_project_id: str,
    project_path: Path,
    archon_path: Path,
    sorry_count: int,
    axiom_count: int = 0,
    task_results: list[Path] | None = None,
) -> ProjectSnapshot:
    return ProjectSnapshot(
        project_id=graph_project_id,
        project_path=project_path.resolve(),
        archon_path=archon_path.resolve(),
        progress=ProgressSnapshot(stage="prover"),
        task_results=task_results or [],
        review_sessions=[],
        lean_file_count=1,
        theorem_count=2,
        sorry_count=sorry_count,
        axiom_count=axiom_count,
    )


def test_collect_task_graph_extracts_objectives_and_lean_signals(
    tmp_path: Path,
) -> None:
    from archonlab.task_graph import build_task_graph

    project_path, archon_path, _ = _make_fake_project(tmp_path, include_sorry=True)

    graph = build_task_graph(project_path=project_path, archon_path=archon_path)

    assert graph.project_id == "DemoProject"
    assert any(
        node.title == "foo"
        and TaskSource.OBJECTIVE in node.sources
        and node.status is TaskStatus.PENDING
        and node.file_path == Path("Core.lean")
        and node.theorem_name == "foo"
        for node in graph.nodes
    )
    assert any(
        node.theorem_name == "foo"
        and TaskSource.LEAN_DECLARATION in node.sources
        and node.status is TaskStatus.COMPLETED
        for node in graph.nodes
    )
    assert any(
        node.theorem_name == "bar"
        and TaskSource.LEAN_DECLARATION in node.sources
        and node.status is TaskStatus.BLOCKED
        and "contains_sorry" in node.blockers
        for node in graph.nodes
    )
    assert len(graph.edges) >= 1


def test_supervisor_policy_distinguishes_healthy_and_stuck_runs(tmp_path: Path) -> None:
    from archonlab.supervisor import decide_supervisor_action
    from archonlab.task_graph import build_task_graph

    healthy_project_path, healthy_archon_path, _ = _make_fake_project(
        tmp_path / "healthy", include_sorry=False
    )
    healthy_graph = build_task_graph(
        project_path=healthy_project_path, archon_path=healthy_archon_path
    )
    stuck_project_path, stuck_archon_path, _ = _make_fake_project(
        tmp_path / "stuck", include_sorry=True
    )
    stuck_graph = build_task_graph(project_path=stuck_project_path, archon_path=stuck_archon_path)

    healthy_decision = decide_supervisor_action(
        snapshot=_snapshot_for_graph(
            graph_project_id=healthy_graph.project_id,
            project_path=healthy_project_path,
            archon_path=healthy_archon_path,
            sorry_count=0,
        ),
        task_graph=healthy_graph,
        recent_events=[EventRecord(run_id="run-1", kind="run.started", project_id="demo")],
    )

    stuck_events = [
        EventRecord(run_id="run-1", kind="workflow.next_action", project_id="demo"),
        EventRecord(run_id="run-1", kind="workflow.next_action", project_id="demo"),
        EventRecord(run_id="run-1", kind="workflow.next_action", project_id="demo"),
    ]
    stuck_decision = decide_supervisor_action(
        snapshot=_snapshot_for_graph(
            graph_project_id=stuck_graph.project_id,
            project_path=stuck_project_path,
            archon_path=stuck_archon_path,
            sorry_count=1,
        ),
        task_graph=stuck_graph,
        recent_events=stuck_events,
    )

    blocked_decision = decide_supervisor_action(
        snapshot=_snapshot_for_graph(
            graph_project_id=stuck_graph.project_id,
            project_path=stuck_project_path,
            archon_path=stuck_archon_path,
            sorry_count=1,
        ),
        task_graph=stuck_graph.model_copy(
            update={
                "nodes": [
                    node.model_copy(update={"status": TaskStatus.BLOCKED})
                    for node in stuck_graph.nodes
                ]
            }
        ),
        recent_events=[EventRecord(run_id="run-1", kind="run.started", project_id="demo")],
    )

    assert healthy_decision.action is SupervisorAction.CONTINUE
    assert healthy_decision.reason is SupervisorReason.HEALTHY
    assert healthy_decision.summary
    assert stuck_decision.action is SupervisorAction.REROUTE_PLAN
    assert stuck_decision.reason is SupervisorReason.REPEATED_NO_PROGRESS
    assert stuck_decision.summary
    assert blocked_decision.action is SupervisorAction.INVESTIGATE_INFRA
    assert blocked_decision.reason is SupervisorReason.HIGH_BLOCKED_RATIO
    assert blocked_decision.summary


def test_supervisor_policy_treats_axiom_blocked_ratio_as_infra_issue(tmp_path: Path) -> None:
    from archonlab.supervisor import decide_supervisor_action
    from archonlab.task_graph import build_task_graph

    project_path, archon_path, _ = _make_fake_project(tmp_path, include_sorry=False)

    class AxiomAnalyzer:
        def analyze(self, *, project_path: Path, archon_path: Path):  # type: ignore[no-untyped-def]
            del archon_path
            from archonlab.models import LeanAnalysisSnapshot, LeanDeclaration

            return LeanAnalysisSnapshot(
                project_id=project_path.name,
                project_path=project_path,
                lean_file_count=1,
                theorem_count=2,
                sorry_count=0,
                axiom_count=1,
                declarations=[
                    LeanDeclaration(
                        name="helper",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        dependencies=[],
                        uses_axiom=True,
                    ),
                    LeanDeclaration(
                        name="foo",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        dependencies=["helper"],
                    ),
                ],
            )

    graph = build_task_graph(
        project_path=project_path,
        archon_path=archon_path,
        analyzer=AxiomAnalyzer(),
    )

    decision = decide_supervisor_action(
        snapshot=_snapshot_for_graph(
            graph_project_id=graph.project_id,
            project_path=project_path,
            archon_path=archon_path,
            sorry_count=0,
            axiom_count=1,
        ),
        task_graph=graph,
        recent_events=[EventRecord(run_id="run-1", kind="run.started", project_id="demo")],
    )

    assert decision.action is SupervisorAction.INVESTIGATE_INFRA
    assert decision.reason is SupervisorReason.HIGH_BLOCKED_RATIO
    assert decision.summary


def test_worktree_manager_creates_and_releases_isolated_checkout(
    tmp_path: Path,
) -> None:
    from archonlab.worktree import WorktreeManager

    repo_path = tmp_path / "repo"
    worktree_root = tmp_path / "worktrees"
    repo_path.mkdir()
    _init_git_repo(repo_path)

    manager = WorktreeManager(root=worktree_root)
    lease = manager.create(repo_path, name="phase4-run")

    assert lease.repo_path == repo_path.resolve()
    assert lease.worktree_path.exists()
    assert lease.worktree_path != repo_path.resolve()
    assert lease.worktree_path.parent == worktree_root.resolve()
    assert lease.head_sha

    manager.release(lease)

    assert not lease.worktree_path.exists()
