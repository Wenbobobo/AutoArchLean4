from __future__ import annotations

from .adapter import ArchonAdapter
from .models import (
    ActionPhase,
    AdapterAction,
    ControlState,
    ProjectSnapshot,
    SupervisorAction,
    SupervisorDecision,
    TaskGraph,
    TaskNode,
    TaskSource,
    TaskStatus,
    WorkflowMode,
    WorkflowSpec,
)
from .workflow import match_workflow_rule


def select_next_action(
    *,
    adapter: ArchonAdapter,
    workflow: WorkflowMode,
    snapshot: ProjectSnapshot,
    task_graph: TaskGraph,
    supervisor: SupervisorDecision,
    workflow_spec: WorkflowSpec | None = None,
    control_state: ControlState | None = None,
) -> AdapterAction:
    progress = snapshot.progress
    focus_task = choose_focus_task(task_graph)
    focus_context = describe_focus_task(task_graph, focus_task)

    if control_state is not None and control_state.paused:
        phase = ActionPhase.STOP
        reason = "control_paused"
    elif progress.stage == "COMPLETE":
        phase = ActionPhase.STOP
        reason = "project_complete"
    elif snapshot.task_results:
        phase = ActionPhase.PLAN
        reason = "unprocessed_task_results"
    elif supervisor.action is SupervisorAction.CONTINUE:
        if workflow is WorkflowMode.FIXED_LOOP:
            phase = ActionPhase.PLAN
            reason = "fixed_loop_baseline"
        elif not snapshot.review_sessions:
            phase = ActionPhase.PLAN
            reason = "bootstrap_first_iteration"
        elif focus_task is None:
            phase = ActionPhase.PLAN
            reason = "no_actionable_tasks"
        else:
            phase = ActionPhase.PROVER
            reason = "task_graph_focus"
    elif supervisor.action is SupervisorAction.REROUTE_PLAN:
        phase = ActionPhase.PLAN
        reason = f"supervisor_{supervisor.reason.value}"
    elif supervisor.action is SupervisorAction.INVESTIGATE_INFRA:
        phase = ActionPhase.PLAN
        reason = "supervisor_investigate_infra"
    else:
        phase = ActionPhase.PLAN
        reason = "supervisor_request_hint"

    workflow_rule = match_workflow_rule(
        spec=workflow_spec,
        supervisor=supervisor,
        focus_task=focus_task,
        phase=phase,
        has_task_results=bool(snapshot.task_results),
        has_review_sessions=bool(snapshot.review_sessions),
    )
    if workflow_rule is not None:
        phase = workflow_rule.phase
        reason = workflow_rule.reason

    prompt_preview = build_targeted_prompt(
        adapter=adapter,
        phase=phase.value,
        stage=progress.stage,
        supervisor=supervisor,
        focus_task=focus_task,
        workflow_rule_name=workflow_rule.name if workflow_rule is not None else None,
        control_state=control_state,
    )
    return AdapterAction(
        phase=phase,
        reason=reason,
        stage=progress.stage,
        prompt_preview=prompt_preview,
        task_id=focus_task.id if focus_task is not None else None,
        task_title=focus_task.title if focus_task is not None else None,
        theorem_name=focus_task.theorem_name if focus_task is not None else None,
        file_path=focus_task.file_path if focus_task is not None else None,
        task_status=focus_task.status if focus_task is not None else None,
        task_sources=focus_task.sources if focus_task is not None else [],
        task_priority=focus_task.priority if focus_task is not None else None,
        task_blockers=focus_task.blockers if focus_task is not None else [],
        objective_relevant=focus_context["objective_relevant"] if focus_task is not None else None,
        supervisor_action=supervisor.action,
        supervisor_reason=supervisor.reason,
    )


def choose_focus_task(task_graph: TaskGraph) -> TaskNode | None:
    dependency_map = build_dependency_map(task_graph)
    relevant_targets = collect_relevant_targets(
        {
            edge.target_id
            for edge in task_graph.edges
            if edge.kind == "objective_targets_theorem"
        },
        dependency_map,
    )
    declaration_candidates = [
        node
        for node in task_graph.nodes
        if TaskSource.LEAN_DECLARATION in node.sources
        and node.status in {TaskStatus.BLOCKED, TaskStatus.PENDING}
    ]
    declaration_candidates.sort(
        key=lambda node: (
            not is_actionable(node, task_graph, dependency_map),
            node.id not in relevant_targets,
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


def describe_focus_task(task_graph: TaskGraph, focus_task: TaskNode | None) -> dict[str, bool]:
    if focus_task is None:
        return {"objective_relevant": False}
    dependency_map = build_dependency_map(task_graph)
    relevant_targets = collect_relevant_targets(
        {
            edge.target_id
            for edge in task_graph.edges
            if edge.kind == "objective_targets_theorem"
        },
        dependency_map,
    )
    return {"objective_relevant": focus_task.id in relevant_targets}


def build_targeted_prompt(
    *,
    adapter: ArchonAdapter,
    phase: str,
    stage: str,
    supervisor: SupervisorDecision,
    focus_task: TaskNode | None,
    workflow_rule_name: str | None,
    control_state: ControlState | None,
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
    if workflow_rule_name is not None:
        prompt += f"\nWorkflow override:\n- rule: {workflow_rule_name}\n"
    if control_state is not None:
        prompt += (
            "\nControl state:\n"
            f"- paused: {'yes' if control_state.paused else 'no'}\n"
            f"- hint_count: {len(control_state.hints)}\n"
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


def build_dependency_map(task_graph: TaskGraph) -> dict[str, set[str]]:
    dependency_map: dict[str, set[str]] = {}
    for edge in task_graph.edges:
        if edge.kind != "depends_on":
            continue
        dependency_map.setdefault(edge.source_id, set()).add(edge.target_id)
    return dependency_map


def collect_relevant_targets(
    objective_targets: set[str],
    dependency_map: dict[str, set[str]],
) -> set[str]:
    relevant = set(objective_targets)
    pending = list(objective_targets)
    while pending:
        node_id = pending.pop()
        for dependency in dependency_map.get(node_id, set()):
            if dependency in relevant:
                continue
            relevant.add(dependency)
            pending.append(dependency)
    return relevant


def is_actionable(
    node: TaskNode,
    task_graph: TaskGraph,
    dependency_map: dict[str, set[str]],
) -> bool:
    node_by_id = {candidate.id: candidate for candidate in task_graph.nodes}
    dependencies = dependency_map.get(node.id, set())
    return all(
        node_by_id.get(dependency_id) is not None
        and node_by_id[dependency_id].status is TaskStatus.COMPLETED
        for dependency_id in dependencies
    )
