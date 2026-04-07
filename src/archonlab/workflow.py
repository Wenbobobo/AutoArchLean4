from __future__ import annotations

import tomllib
from pathlib import Path

from .models import ActionPhase, SupervisorDecision, TaskNode, WorkflowRule, WorkflowSpec


def load_workflow_spec(spec_path: Path) -> WorkflowSpec:
    raw = tomllib.loads(spec_path.read_text(encoding="utf-8"))
    workflow_raw = raw["workflow"]
    rules_raw = raw.get("rules", [])
    return WorkflowSpec(
        name=workflow_raw["name"],
        description=workflow_raw.get("description", ""),
        rules=[
            WorkflowRule(
                name=rule["name"],
                when_supervisor_reason=rule.get("when_supervisor_reason"),
                when_task_status=rule.get("when_task_status"),
                when_phase=rule.get("when_phase"),
                when_has_task_results=rule.get("when_has_task_results"),
                when_has_review_sessions=rule.get("when_has_review_sessions"),
                phase=rule["phase"],
                reason=rule["reason"],
            )
            for rule in rules_raw
        ],
    )


def match_workflow_rule(
    *,
    spec: WorkflowSpec | None,
    supervisor: SupervisorDecision,
    focus_task: TaskNode | None,
    phase: str | ActionPhase,
    has_task_results: bool,
    has_review_sessions: bool,
) -> WorkflowRule | None:
    if spec is None:
        return None
    normalized_phase = ActionPhase(phase)
    for rule in spec.rules:
        if (
            rule.when_supervisor_reason is not None
            and rule.when_supervisor_reason != supervisor.reason
        ):
            continue
        if rule.when_task_status is not None and (
            focus_task is None or focus_task.status != rule.when_task_status
        ):
            continue
        if rule.when_phase is not None and rule.when_phase != normalized_phase:
            continue
        if (
            rule.when_has_task_results is not None
            and rule.when_has_task_results != has_task_results
        ):
            continue
        if (
            rule.when_has_review_sessions is not None
            and rule.when_has_review_sessions != has_review_sessions
        ):
            continue
        return rule
    return None
