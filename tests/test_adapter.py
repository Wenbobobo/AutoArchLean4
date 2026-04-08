from __future__ import annotations

from pathlib import Path

from archonlab.adapter import ArchonAdapter
from archonlab.models import ProjectConfig, WorkflowMode


def test_archon_adapter_reads_progress(fake_archon_project: Path, fake_archon_root: Path) -> None:
    adapter = ArchonAdapter(
        ProjectConfig(
            name="demo",
            project_path=fake_archon_project,
            archon_path=fake_archon_root,
        )
    )

    progress = adapter.read_progress()

    assert progress.stage == "prover"
    assert progress.objectives == ["**Core.lean** — fill theorem `foo`"]


def test_choose_next_action_bootstraps_with_plan(
    fake_archon_project: Path, fake_archon_root: Path
) -> None:
    adapter = ArchonAdapter(
        ProjectConfig(
            name="demo",
            project_path=fake_archon_project,
            archon_path=fake_archon_root,
        )
    )
    progress = adapter.read_progress()

    action = adapter.choose_next_action(WorkflowMode.ADAPTIVE_LOOP, progress)

    assert action.phase == "plan"
    assert action.reason == "bootstrap_first_iteration"
    assert "You are the plan agent" in (action.prompt_preview or "")


def test_choose_next_action_routes_pending_task_results_to_review(
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    task_result_path = fake_archon_project / ".archon" / "task_results" / "task-1.md"
    task_result_path.write_text("# result\n", encoding="utf-8")
    adapter = ArchonAdapter(
        ProjectConfig(
            name="demo",
            project_path=fake_archon_project,
            archon_path=fake_archon_root,
        )
    )
    progress = adapter.read_progress()

    action = adapter.choose_next_action(WorkflowMode.ADAPTIVE_LOOP, progress)

    assert action.phase == "review"
    assert action.reason == "pending_review"
    assert "You are the review agent" in (action.prompt_preview or "")
