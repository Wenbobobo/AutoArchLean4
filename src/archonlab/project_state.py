from __future__ import annotations

import re
from pathlib import Path

from .adapter import ArchonAdapter
from .models import ProjectConfig, ProjectScore, ProjectSnapshot, SnapshotDelta

THEOREM_PATTERN = re.compile(r"\b(?:theorem|lemma|example)\b")
SORRY_PATTERN = re.compile(r"\bsorry\b")
AXIOM_PATTERN = re.compile(r"\baxiom\b")


def collect_project_snapshot(*, project_path: Path, archon_path: Path) -> ProjectSnapshot:
    resolved_project_path = project_path.resolve()
    resolved_archon_path = archon_path.resolve()
    project = ProjectConfig(
        name=resolved_project_path.name,
        project_path=resolved_project_path,
        archon_path=resolved_archon_path,
    )
    adapter = ArchonAdapter(project)
    adapter.ensure_valid()
    progress = adapter.read_progress()
    progress = progress.model_copy(
        update={"objectives": [objective.replace("—", "-") for objective in progress.objectives]}
    )

    sorry_count = 0
    theorem_count = 0
    axiom_count = 0
    lean_file_count = 0
    for lean_file in sorted(project.project_path.rglob("*.lean")):
        content = lean_file.read_text(encoding="utf-8")
        lean_file_count += 1
        theorem_count += len(THEOREM_PATTERN.findall(content))
        sorry_count += len(SORRY_PATTERN.findall(content))
        axiom_count += len(AXIOM_PATTERN.findall(content))

    return ProjectSnapshot(
        project_id=project.name,
        project_path=resolved_project_path,
        archon_path=resolved_archon_path,
        progress=progress,
        task_results=[path.resolve() for path in adapter.list_task_results()],
        review_sessions=[path.resolve() for path in adapter.list_review_sessions()],
        lean_file_count=lean_file_count,
        theorem_count=theorem_count,
        sorry_count=sorry_count,
        axiom_count=axiom_count,
    )


def score_project_snapshot(snapshot: ProjectSnapshot) -> ProjectScore:
    checklist_done = sum(1 for item in snapshot.progress.checklist if item.done)
    checklist_total = len(snapshot.progress.checklist)
    progress_ratio = checklist_done / checklist_total if checklist_total else 0.0
    task_result_count = len(snapshot.task_results)
    review_session_count = len(snapshot.review_sessions)
    backlog_penalty = task_result_count * 5
    proof_gap_penalty = snapshot.sorry_count * 3
    axiom_penalty = snapshot.axiom_count * 10
    score = max(
        0.0,
        round(
            100 * progress_ratio
            + review_session_count * 2
            - backlog_penalty
            - proof_gap_penalty
            - axiom_penalty,
            2,
        ),
    )
    return ProjectScore(
        project_id=snapshot.project_id,
        stage=snapshot.progress.stage,
        objective_count=len(snapshot.progress.objectives),
        task_result_count=task_result_count,
        review_session_count=review_session_count,
        progress_ratio=round(progress_ratio, 4),
        backlog_penalty=backlog_penalty,
        proof_gap_penalty=proof_gap_penalty,
        axiom_penalty=axiom_penalty,
        score=score,
    )


def diff_snapshots(before: ProjectSnapshot, after: ProjectSnapshot) -> SnapshotDelta:
    before_score = score_project_snapshot(before)
    after_score = score_project_snapshot(after)
    before_checklist_done = sum(1 for item in before.progress.checklist if item.done)
    after_checklist_done = sum(1 for item in after.progress.checklist if item.done)
    return SnapshotDelta(
        sorry_delta=after.sorry_count - before.sorry_count,
        axiom_delta=after.axiom_count - before.axiom_count,
        review_session_delta=len(after.review_sessions) - len(before.review_sessions),
        task_results_delta=len(after.task_results) - len(before.task_results),
        checklist_done_delta=after_checklist_done - before_checklist_done,
        score_delta=round(after_score.score - before_score.score, 2),
    )
