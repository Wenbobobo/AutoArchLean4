from __future__ import annotations

from pathlib import Path

from archonlab.adapter import ArchonAdapter
from archonlab.models import (
    ProjectConfig,
    SupervisorAction,
    SupervisorDecision,
    SupervisorReason,
    WorkflowMode,
)
from archonlab.planner import select_next_action
from archonlab.project_state import collect_project_snapshot
from archonlab.task_graph import build_task_graph


def _make_project(
    tmp_path: Path,
    *,
    with_review_session: bool,
    with_task_results: bool,
    theorem_body: str,
) -> tuple[Path, Path]:
    project_path = tmp_path / "DemoProject"
    archon_path = tmp_path / "Archon"
    state_dir = project_path / ".archon"
    prompts_dir = state_dir / "prompts"
    prompts_dir.mkdir(parents=True)
    archon_path.mkdir(parents=True)
    (archon_path / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
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
    (project_path / "Core.lean").write_text(
        "theorem foo : True := by\n"
        f"  {theorem_body}\n",
        encoding="utf-8",
    )
    if with_review_session:
        (state_dir / "proof-journal" / "sessions" / "session-1").mkdir(parents=True)
    if with_task_results:
        task_results_dir = state_dir / "task_results"
        task_results_dir.mkdir(parents=True)
        (task_results_dir / "task-1.md").write_text("# result\n", encoding="utf-8")
    return project_path, archon_path


def _healthy_supervisor(project_id: str) -> SupervisorDecision:
    return SupervisorDecision(
        project_id=project_id,
        action=SupervisorAction.CONTINUE,
        reason=SupervisorReason.HEALTHY,
        summary="Keep going.",
    )


def test_select_next_action_targets_blocked_theorem_for_prover(tmp_path: Path) -> None:
    project_path, archon_path = _make_project(
        tmp_path,
        with_review_session=True,
        with_task_results=False,
        theorem_body="sorry",
    )
    project = ProjectConfig(name="demo", project_path=project_path, archon_path=archon_path)
    adapter = ArchonAdapter(project)
    snapshot = collect_project_snapshot(project_path=project_path, archon_path=archon_path)
    task_graph = build_task_graph(project_path=project_path, archon_path=archon_path)

    action = select_next_action(
        adapter=adapter,
        workflow=WorkflowMode.ADAPTIVE_LOOP,
        snapshot=snapshot,
        task_graph=task_graph,
        supervisor=_healthy_supervisor(snapshot.project_id),
    )

    assert action.phase == "prover"
    assert action.reason == "task_graph_focus"
    assert action.task_title == "foo"
    assert action.file_path == Path("Core.lean")
    assert "Focus task:" in (action.prompt_preview or "")
    assert "theorem: foo" in (action.prompt_preview or "")


def test_select_next_action_reroutes_to_plan_for_task_results(tmp_path: Path) -> None:
    project_path, archon_path = _make_project(
        tmp_path,
        with_review_session=True,
        with_task_results=True,
        theorem_body="sorry",
    )
    project = ProjectConfig(name="demo", project_path=project_path, archon_path=archon_path)
    adapter = ArchonAdapter(project)
    snapshot = collect_project_snapshot(project_path=project_path, archon_path=archon_path)
    task_graph = build_task_graph(project_path=project_path, archon_path=archon_path)

    action = select_next_action(
        adapter=adapter,
        workflow=WorkflowMode.ADAPTIVE_LOOP,
        snapshot=snapshot,
        task_graph=task_graph,
        supervisor=_healthy_supervisor(snapshot.project_id),
    )

    assert action.phase == "plan"
    assert action.reason == "unprocessed_task_results"


def test_select_next_action_respects_supervisor_reroute(tmp_path: Path) -> None:
    project_path, archon_path = _make_project(
        tmp_path,
        with_review_session=True,
        with_task_results=False,
        theorem_body="trivial",
    )
    project = ProjectConfig(name="demo", project_path=project_path, archon_path=archon_path)
    adapter = ArchonAdapter(project)
    snapshot = collect_project_snapshot(project_path=project_path, archon_path=archon_path)
    task_graph = build_task_graph(project_path=project_path, archon_path=archon_path)
    supervisor = SupervisorDecision(
        project_id=snapshot.project_id,
        action=SupervisorAction.REROUTE_PLAN,
        reason=SupervisorReason.REPEATED_NO_PROGRESS,
        summary="Same loop repeated.",
    )

    action = select_next_action(
        adapter=adapter,
        workflow=WorkflowMode.ADAPTIVE_LOOP,
        snapshot=snapshot,
        task_graph=task_graph,
        supervisor=supervisor,
    )

    assert action.phase == "plan"
    assert action.reason == "supervisor_repeated_no_progress"
    assert action.supervisor_action is SupervisorAction.REROUTE_PLAN
    assert "Supervisor recommendation:" in (action.prompt_preview or "")
