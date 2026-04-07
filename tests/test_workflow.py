from __future__ import annotations

from pathlib import Path

from archonlab import workflow as workflow_mod
from archonlab.adapter import ArchonAdapter
from archonlab.models import (  # noqa: E402
    ProgressSnapshot,
    ProjectConfig,
    ProjectSnapshot,
    SupervisorAction,
    SupervisorDecision,
    SupervisorReason,
    TaskEdge,
    TaskGraph,
    TaskNode,
    TaskSource,
    TaskStatus,
    WorkflowMode,
)
from archonlab.planner import select_next_action


def _task_graph(*, blocked: bool) -> TaskGraph:
    objective_node = TaskNode(
        id="objective:1",
        title="foo",
        status=TaskStatus.PENDING,
        sources=[TaskSource.OBJECTIVE],
        file_path=Path("Core.lean"),
        theorem_name="foo",
        priority=1,
    )
    theorem_node = TaskNode(
        id="lean:Core.lean:foo",
        title="foo",
        status=TaskStatus.BLOCKED if blocked else TaskStatus.COMPLETED,
        sources=[TaskSource.LEAN_DECLARATION],
        file_path=Path("Core.lean"),
        theorem_name="foo",
        priority=2,
        blockers=["contains_sorry"] if blocked else [],
    )
    return TaskGraph(
        project_id="DemoProject",
        nodes=[objective_node, theorem_node],
        edges=[
            TaskEdge(
                source_id=objective_node.id,
                target_id=theorem_node.id,
                kind="objective_targets_theorem",
            )
        ],
    )


def _project_snapshot() -> ProjectSnapshot:
    return ProjectSnapshot(
        project_id="DemoProject",
        project_path=Path("/tmp/DemoProject"),
        archon_path=Path("/tmp/Archon"),
        progress=ProgressSnapshot(stage="prover"),
        lean_file_count=1,
        theorem_count=1,
        sorry_count=0,
        axiom_count=0,
    )


def _supervisor(reason: SupervisorReason) -> SupervisorDecision:
    return SupervisorDecision(
        project_id="DemoProject",
        action=(
            SupervisorAction.REROUTE_PLAN
            if reason is SupervisorReason.REPEATED_NO_PROGRESS
            else SupervisorAction.CONTINUE
        ),
        reason=reason,
        summary=reason.value,
    )


def _adapter() -> ArchonAdapter:
    return ArchonAdapter(
        ProjectConfig(
            name="DemoProject",
            project_path=Path("/tmp/DemoProject"),
            archon_path=Path("/tmp/Archon"),
        )
    )


def test_load_workflow_spec_parses_rules(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.toml"
    spec_path.write_text(
        "[workflow]\n"
        'name = "phase5"\n'
        'description = "Override planning based on supervision or task state."\n\n'
        "[[rules]]\n"
        'name = "reroute_after_no_progress"\n'
        'when_supervisor_reason = "repeated_no_progress"\n'
        'phase = "plan"\n'
        'reason = "reroute_to_plan"\n\n'
        "[[rules]]\n"
        'name = "blocked_task"\n'
        'when_task_status = "blocked"\n'
        'phase = "plan"\n'
        'reason = "blocked_task"\n',
        encoding="utf-8",
    )

    spec = workflow_mod.load_workflow_spec(spec_path)

    assert spec.name == "phase5"
    assert spec.description == "Override planning based on supervision or task state."
    assert len(spec.rules) == 2
    assert spec.rules[0].when_supervisor_reason == SupervisorReason.REPEATED_NO_PROGRESS
    assert spec.rules[1].when_task_status == TaskStatus.BLOCKED


def test_evaluate_workflow_overrides_for_supervisor_reason(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.toml"
    spec_path.write_text(
        "[workflow]\n"
        'name = "phase5"\n'
        'description = "Override planning based on supervision or task state."\n\n'
        "[[rules]]\n"
        'name = "reroute_after_no_progress"\n'
        'when_supervisor_reason = "repeated_no_progress"\n'
        'phase = "plan"\n'
        'reason = "reroute_to_plan"\n',
        encoding="utf-8",
    )
    spec = workflow_mod.load_workflow_spec(spec_path)

    result = select_next_action(
        adapter=_adapter(),
        workflow=WorkflowMode.ADAPTIVE_LOOP,
        snapshot=_project_snapshot_with_review(),
        task_graph=_task_graph(blocked=True),
        supervisor=_supervisor(SupervisorReason.REPEATED_NO_PROGRESS),
        workflow_spec=spec,
    )

    assert result.phase == "plan"
    assert result.reason == "reroute_to_plan"
    assert result.supervisor_reason is SupervisorReason.REPEATED_NO_PROGRESS


def test_evaluate_workflow_overrides_for_blocked_task_status(tmp_path: Path) -> None:
    spec_path = tmp_path / "workflow.toml"
    spec_path.write_text(
        "[workflow]\n"
        'name = "phase5"\n'
        'description = "Override planning based on supervision or task state."\n\n'
        "[[rules]]\n"
        'name = "blocked_task"\n'
        'when_task_status = "blocked"\n'
        'phase = "plan"\n'
        'reason = "blocked_task"\n',
        encoding="utf-8",
    )
    spec = workflow_mod.load_workflow_spec(spec_path)

    result = select_next_action(
        adapter=_adapter(),
        workflow=WorkflowMode.ADAPTIVE_LOOP,
        snapshot=_project_snapshot_with_review(),
        task_graph=_task_graph(blocked=True),
        supervisor=_supervisor(SupervisorReason.HEALTHY),
        workflow_spec=spec,
    )

    assert result.phase == "plan"
    assert result.reason == "blocked_task"
    assert result.task_id == "lean:Core.lean:foo"


def _project_snapshot_with_review() -> ProjectSnapshot:
    return _project_snapshot().model_copy(update={"review_sessions": [Path("session-1")]})
