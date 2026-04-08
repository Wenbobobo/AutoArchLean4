from __future__ import annotations

from collections import Counter
from collections.abc import Sequence

from .models import (
    EventRecord,
    ProjectSnapshot,
    SupervisorAction,
    SupervisorDecision,
    SupervisorReason,
    TaskGraph,
    TaskStatus,
    TheoremState,
)


def decide_supervisor_action(
    *,
    snapshot: ProjectSnapshot,
    task_graph: TaskGraph,
    recent_events: Sequence[EventRecord] = (),
) -> SupervisorDecision:
    blocked_count = sum(1 for node in task_graph.nodes if node.status is TaskStatus.BLOCKED)
    pending_task_results = len(snapshot.task_results)
    repeated_next_actions = _max_repeated_next_actions(recent_events)
    proof_gap_count = max(snapshot.proof_gap_count, snapshot.sorry_count + snapshot.axiom_count)
    diagnostic_count = snapshot.diagnostic_count
    theorem_state_counts = {
        state.value: snapshot.theorem_state_counts.get(state.value, 0)
        for state in TheoremState
    }
    contains_sorry_theorem_count = theorem_state_counts[TheoremState.CONTAINS_SORRY.value]

    if pending_task_results > 0:
        return SupervisorDecision(
            project_id=snapshot.project_id,
            action=SupervisorAction.REROUTE_PLAN,
            reason=SupervisorReason.PENDING_RESULTS_BACKLOG,
            summary="Pending task results should be folded back into planning before more proving.",
            evidence={"task_results_count": pending_task_results},
        )

    if snapshot.analysis_fallback_used:
        return SupervisorDecision(
            project_id=snapshot.project_id,
            action=SupervisorAction.INVESTIGATE_INFRA,
            reason=SupervisorReason.ANALYZER_DEGRADED,
            summary=(
                "The configured Lean analyzer degraded to a fallback backend, so task "
                "dependency and proof-gap signals may be stale."
            ),
            evidence={
                "analysis_backend": snapshot.analysis_backend,
                "analysis_fallback_reason": snapshot.analysis_fallback_reason or "unknown",
                "theorem_state_counts": theorem_state_counts,
            },
        )

    if repeated_next_actions >= 3:
        return SupervisorDecision(
            project_id=snapshot.project_id,
            action=SupervisorAction.REROUTE_PLAN,
            reason=SupervisorReason.REPEATED_NO_PROGRESS,
            summary="The same next-action pattern repeated without visible state change.",
            evidence={"repeated_next_actions": repeated_next_actions},
        )

    node_count = max(1, len(task_graph.nodes))
    blocked_ratio = blocked_count / node_count
    if blocked_ratio >= 0.5 and (proof_gap_count > 0 or diagnostic_count > 0):
        return SupervisorDecision(
            project_id=snapshot.project_id,
            action=SupervisorAction.INVESTIGATE_INFRA,
            reason=SupervisorReason.HIGH_BLOCKED_RATIO,
            summary=(
                "A large share of known tasks are blocked by unresolved proof gaps "
                "or unsound assumptions."
            ),
            evidence={
                "blocked_count": blocked_count,
                "task_count": len(task_graph.nodes),
                "blocked_ratio": round(blocked_ratio, 4),
                "proof_gap_count": proof_gap_count,
                "diagnostic_count": diagnostic_count,
                "sorry_count": snapshot.sorry_count,
                "axiom_count": snapshot.axiom_count,
                "theorem_state_counts": theorem_state_counts,
            },
        )

    if contains_sorry_theorem_count >= 3 and contains_sorry_theorem_count * 2 >= max(
        1, snapshot.theorem_count
    ):
        return SupervisorDecision(
            project_id=snapshot.project_id,
            action=SupervisorAction.REQUEST_HINT,
            reason=SupervisorReason.HIGH_SORRY_LOAD,
            summary=(
                "A large share of declarations are still blocked by sorry, so targeted "
                "hints should be requested before more autonomous retries."
            ),
            evidence={
                "contains_sorry_theorem_count": contains_sorry_theorem_count,
                "theorem_count": snapshot.theorem_count,
                "theorem_state_counts": theorem_state_counts,
            },
        )

    if snapshot.sorry_count >= 10:
        return SupervisorDecision(
            project_id=snapshot.project_id,
            action=SupervisorAction.REQUEST_HINT,
            reason=SupervisorReason.HIGH_SORRY_LOAD,
            summary=(
                "Proof debt is still high enough that curated hints may unlock "
                "progress faster."
            ),
            evidence={
                "sorry_count": snapshot.sorry_count,
                "theorem_state_counts": theorem_state_counts,
            },
        )

    return SupervisorDecision(
        project_id=snapshot.project_id,
        action=SupervisorAction.CONTINUE,
        reason=SupervisorReason.HEALTHY,
        summary="Current state looks healthy enough to continue the planned loop.",
        evidence={
            "task_results_count": pending_task_results,
            "blocked_count": blocked_count,
            "task_count": len(task_graph.nodes),
            "review_sessions": len(snapshot.review_sessions),
            "theorem_state_counts": theorem_state_counts,
        },
    )


def _max_repeated_next_actions(recent_events: Sequence[EventRecord]) -> int:
    relevant = [
        (event.payload.get("phase"), event.payload.get("reason"))
        for event in recent_events
        if event.kind == "workflow.next_action"
    ]
    if not relevant:
        return 0
    counter = Counter(relevant)
    return max(counter.values(), default=0)
