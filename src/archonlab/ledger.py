from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, cast

from .models import (
    BenchmarkLedger,
    BenchmarkLedgerFailureKind,
    BenchmarkLedgerOutcomeKind,
    BenchmarkLedgerRecord,
    BenchmarkLedgerSummary,
    BenchmarkProjectResult,
    BenchmarkResult,
    ExecutionResult,
    ExecutionStatus,
    RunStatus,
)

THEOREM_OBJECTIVE_PATTERN = re.compile(r"theorem `([^`]+)`")


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _load_execution_result(artifact_dir: Path | None) -> ExecutionResult | None:
    if artifact_dir is None:
        return None
    execution_path = artifact_dir / "execution.json"
    if not execution_path.exists():
        return None
    return ExecutionResult.model_validate_json(execution_path.read_text(encoding="utf-8"))


def _load_run_summary(artifact_dir: Path | None) -> dict[str, Any] | None:
    if artifact_dir is None:
        return None
    return _load_json(artifact_dir / "run-summary.json")


def _extract_target_theorem(
    project_result: BenchmarkProjectResult,
    run_summary: dict[str, Any] | None,
) -> str | None:
    next_action = run_summary.get("next_action", {}) if isinstance(run_summary, dict) else {}
    theorem_name = next_action.get("theorem_name")
    if theorem_name:
        return str(theorem_name)

    objective_lists = [
        run_summary.get("progress", {}).get("objectives", [])
        if isinstance(run_summary, dict)
        else [],
        run_summary.get("snapshot", {}).get("progress", {}).get("objectives", [])
        if isinstance(run_summary, dict)
        else [],
        project_result.snapshot.progress.objectives,
    ]
    for objectives in objective_lists:
        if not isinstance(objectives, list):
            continue
        for objective in objectives:
            match = THEOREM_OBJECTIVE_PATTERN.search(str(objective))
            if match is not None:
                return match.group(1)
    return None


def _extract_file_path(run_summary: dict[str, Any] | None) -> Path | None:
    if not isinstance(run_summary, dict):
        return None
    next_action = run_summary.get("next_action", {})
    raw_path = next_action.get("file_path")
    if raw_path is None:
        return None
    return Path(str(raw_path))


def _extract_action_field(run_summary: dict[str, Any] | None, key: str) -> str | None:
    if not isinstance(run_summary, dict):
        return None
    next_action = run_summary.get("next_action", {})
    value = next_action.get(key)
    return None if value is None else str(value)


def _classify_failure_kind(
    *,
    project_result: BenchmarkProjectResult,
    theorem_name: str | None,
    execution_result: ExecutionResult | None,
) -> BenchmarkLedgerFailureKind:
    if project_result.run_status is RunStatus.FAILED:
        if (
            execution_result is not None
            and execution_result.status is ExecutionStatus.FAILED
        ):
            return BenchmarkLedgerFailureKind.EXECUTOR_FAILED
        if project_result.artifact_dir is None or not project_result.artifact_dir.exists():
            return BenchmarkLedgerFailureKind.ARTIFACT_MISSING
        return BenchmarkLedgerFailureKind.RUN_ERROR
    if theorem_name is None:
        return BenchmarkLedgerFailureKind.MISSING_TARGET_THEOREM
    return BenchmarkLedgerFailureKind.NONE


def _classify_outcome(
    *,
    result: BenchmarkResult,
    project_result: BenchmarkProjectResult,
    theorem_name: str | None,
) -> BenchmarkLedgerOutcomeKind:
    if project_result.run_status is RunStatus.FAILED:
        return BenchmarkLedgerOutcomeKind.FAILED
    if theorem_name is None:
        return BenchmarkLedgerOutcomeKind.MISSING_TARGET
    if result.dry_run:
        return BenchmarkLedgerOutcomeKind.DRY_RUN
    if project_result.delta.sorry_delta < 0:
        return BenchmarkLedgerOutcomeKind.PROOF_PROGRESS
    if (
        project_result.delta.task_results_delta > 0
        or project_result.delta.review_session_delta > 0
        or project_result.delta.score_delta > 0
    ):
        return BenchmarkLedgerOutcomeKind.ARTIFACT_PROGRESS
    return BenchmarkLedgerOutcomeKind.NO_PROGRESS


def build_benchmark_ledger(result: BenchmarkResult) -> BenchmarkLedger:
    outcomes: list[BenchmarkLedgerRecord] = []
    outcome_counts: Counter[str] = Counter()
    failure_counts: Counter[str] = Counter()
    total_estimated_cost = 0.0

    for project_result in result.projects:
        run_summary = _load_run_summary(project_result.artifact_dir)
        execution_result = _load_execution_result(project_result.artifact_dir)
        theorem_name = _extract_target_theorem(project_result, run_summary)
        failure_kind = _classify_failure_kind(
            project_result=project_result,
            theorem_name=theorem_name,
            execution_result=execution_result,
        )
        outcome = _classify_outcome(
            result=result,
            project_result=project_result,
            theorem_name=theorem_name,
        )
        telemetry = execution_result.telemetry if execution_result is not None else None
        cost_estimate = telemetry.cost_estimate if telemetry is not None else None
        if cost_estimate is not None:
            total_estimated_cost += cost_estimate
        record = BenchmarkLedgerRecord(
            project_id=project_result.id,
            run_id=project_result.run_id,
            theorem_name=theorem_name,
            file_path=_extract_file_path(run_summary),
            workflow=project_result.workflow,
            run_status=project_result.run_status,
            mode="dry_run" if result.dry_run else "execute",
            action_phase=_extract_action_field(run_summary, "phase"),
            action_reason=_extract_action_field(run_summary, "reason"),
            outcome=outcome,
            failure_kind=failure_kind,
            error_message=(
                project_result.error_message
                or (execution_result.error_message if execution_result is not None else None)
            ),
            score_after=project_result.score.score,
            score_delta=project_result.delta.score_delta,
            sorry_delta=project_result.delta.sorry_delta,
            task_results_delta=project_result.delta.task_results_delta,
            review_session_delta=project_result.delta.review_session_delta,
            executor=(execution_result.executor if execution_result is not None else None),
            provider=(
                execution_result.provider if execution_result is not None else None
            ),
            provider_pool=(telemetry.provider_pool if telemetry is not None else None),
            provider_member=(telemetry.provider_member if telemetry is not None else None),
            latency_ms=(telemetry.latency_ms if telemetry is not None else None),
            cost_estimate=cost_estimate,
            artifact_dir=project_result.artifact_dir,
        )
        outcomes.append(record)
        outcome_counts[record.outcome.value] += 1
        if record.failure_kind is not BenchmarkLedgerFailureKind.NONE:
            failure_counts[record.failure_kind.value] += 1

    return BenchmarkLedger(
        benchmark_name=result.benchmark.name,
        benchmark_run_id=result.run_id,
        dry_run=result.dry_run,
        summary=BenchmarkLedgerSummary(
            total_projects=len(result.projects),
            targeted_projects=sum(1 for item in outcomes if item.theorem_name is not None),
            outcome_counts=dict(outcome_counts),
            failure_counts=dict(failure_counts),
            total_estimated_cost=total_estimated_cost,
        ),
        outcomes=outcomes,
    )


def load_benchmark_ledger(summary_path: Path) -> BenchmarkLedger:
    result = BenchmarkResult.model_validate_json(summary_path.read_text(encoding="utf-8"))
    return build_benchmark_ledger(result)
