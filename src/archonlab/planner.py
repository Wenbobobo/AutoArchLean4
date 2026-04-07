from __future__ import annotations

from .adapter import ArchonAdapter
from .models import (
    AdapterAction,
    ProjectSnapshot,
    SupervisorAction,
    SupervisorDecision,
    TaskGraph,
    TaskNode,
    TaskSource,
    TaskStatus,
    WorkflowMode,
)


def select_next_action(
    *,
    adapter: ArchonAdapter,
    workflow: WorkflowMode,
    snapshot: ProjectSnapshot,
    task_graph: TaskGraph,
    supervisor: SupervisorDecision,
) -> AdapterAction:
    progress = snapshot.progress
    focus_task = choose_focus_task(task_graph)

    if progress.stage == "COMPLETE":
        phase = "stop"
        reason = "project_complete"
    elif snapshot.task_results:
        phase = "plan"
        reason = "unprocessed_task_results"
    elif supervisor.action is SupervisorAction.CONTINUE:
        if workflow is WorkflowMode.FIXED_LOOP:
            phase = "plan"
            reason = "fixed_loop_baseline"
        elif not snapshot.review_sessions:
            phase = "plan"
            reason = "bootstrap_first_iteration"
        elif focus_task is None:
            phase = "plan"
            reason = "no_actionable_tasks"
        else:
            phase = "prover"
            reason = "task_graph_focus"
    elif supervisor.action is SupervisorAction.REROUTE_PLAN:
        phase = "plan"
        reason = f"supervisor_{supervisor.reason.value}"
    elif supervisor.action is SupervisorAction.INVESTIGATE_INFRA:
        phase = "plan"
        reason = "supervisor_investigate_infra"
    else:
        phase = "plan"
        reason = "supervisor_request_hint"

    prompt_preview = build_targeted_prompt(
        adapter=adapter,
        phase=phase,
        stage=progress.stage,
        supervisor=supervisor,
        focus_task=focus_task,
    )
    return AdapterAction(
        phase=phase,
        reason=reason,
        stage=progress.stage,
        prompt_preview=prompt_preview,
        task_id=focus_task.id if focus_task is not None else None,
        task_title=focus_task.title if focus_task is not None else None,
        file_path=focus_task.file_path if focus_task is not None else None,
        supervisor_action=supervisor.action,
        supervisor_reason=supervisor.reason,
    )


def choose_focus_task(task_graph: TaskGraph) -> TaskNode | None:
    objective_targets = {
        edge.target_id for edge in task_graph.edges if edge.kind == "objective_targets_theorem"
    }
    declaration_candidates = [
        node
        for node in task_graph.nodes
        if TaskSource.LEAN_DECLARATION in node.sources
        and node.status in {TaskStatus.BLOCKED, TaskStatus.PENDING}
    ]
    declaration_candidates.sort(
        key=lambda node: (
            node.id not in objective_targets,
            node.status is not TaskStatus.BLOCKED,
            -node.priority,
            str(node.file_path or ""),
            node.theorem_name or node.title,
        )
    )
    if declaration_candidates:
        return declaration_candidates[0]

    objective_candidates = [
        node
        for node in task_graph.nodes
        if TaskSource.OBJECTIVE in node.sources
        and node.status in {TaskStatus.BLOCKED, TaskStatus.PENDING}
    ]
    objective_candidates.sort(
        key=lambda node: (-node.priority, str(node.file_path or ""), node.title)
    )
    if objective_candidates:
        return objective_candidates[0]
    return None


def build_targeted_prompt(
    *,
    adapter: ArchonAdapter,
    phase: str,
    stage: str,
    supervisor: SupervisorDecision,
    focus_task: TaskNode | None,
) -> str:
    prompt = adapter.build_prompt(phase=phase, stage=stage)
    if phase == "stop":
        return prompt

    prompt += (
        "\nSupervisor recommendation:\n"
        f"- action: {supervisor.action.value}\n"
        f"- reason: {supervisor.reason.value}\n"
        f"- summary: {supervisor.summary}\n"
    )
    if focus_task is None:
        return prompt

    prompt += "\nFocus task:\n"
    prompt += f"- id: {focus_task.id}\n"
    prompt += f"- title: {focus_task.title}\n"
    if focus_task.file_path is not None:
        prompt += f"- file: {focus_task.file_path}\n"
    if focus_task.theorem_name is not None:
        prompt += f"- theorem: {focus_task.theorem_name}\n"
    prompt += f"- status: {focus_task.status.value}\n"
    if focus_task.blockers:
        prompt += f"- blockers: {', '.join(focus_task.blockers)}\n"
    return prompt
