from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from archonlab.ledger import build_benchmark_ledger
from archonlab.models import (
    BenchmarkConfig,
    BenchmarkProjectResult,
    BenchmarkResult,
    BenchmarkRunStatus,
    ExecutionResult,
    ExecutionStatus,
    ExecutionTelemetry,
    ExecutorKind,
    ProgressSnapshot,
    ProjectScore,
    ProjectSnapshot,
    RunStatus,
    SnapshotDelta,
    WorkflowMode,
)


def _make_snapshot(
    project_id: str,
    project_path: Path,
    archon_path: Path,
    *,
    objectives: list[str],
    sorry_count: int = 0,
) -> ProjectSnapshot:
    return ProjectSnapshot(
        project_id=project_id,
        project_path=project_path,
        archon_path=archon_path,
        progress=ProgressSnapshot(stage="prover", objectives=objectives),
        lean_file_count=1,
        theorem_count=1,
        sorry_count=sorry_count,
        axiom_count=0,
    )


def _make_score(project_id: str, score: float) -> ProjectScore:
    return ProjectScore(
        project_id=project_id,
        stage="prover",
        objective_count=1,
        task_result_count=0,
        review_session_count=0,
        progress_ratio=0.5,
        backlog_penalty=0,
        proof_gap_penalty=0,
        axiom_penalty=0,
        score=score,
    )


def _write_run_summary(
    artifact_dir: Path,
    *,
    theorem_name: str | None,
    file_path: Path | None,
    phase: str = "prover",
    reason: str = "task_graph_focus",
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "run-summary.json").write_text(
        json.dumps(
            {
                "next_action": {
                    "theorem_name": theorem_name,
                    "file_path": str(file_path) if file_path is not None else None,
                    "phase": phase,
                    "reason": reason,
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_execution(
    artifact_dir: Path,
    *,
    status: ExecutionStatus,
    error_message: str | None = None,
    cost_estimate: float | None = None,
    latency_ms: int | None = None,
    provider_pool: str | None = None,
    provider_member: str | None = None,
) -> None:
    execution = ExecutionResult(
        executor=ExecutorKind.OPENAI_COMPATIBLE,
        status=status,
        error_message=error_message,
        metadata={"provider": "openai_compatible"},
        telemetry=ExecutionTelemetry(
            cost_estimate=cost_estimate,
            latency_ms=latency_ms,
            provider_pool=provider_pool,
            provider_member=provider_member,
        ),
    )
    (artifact_dir / "execution.json").write_text(
        execution.model_dump_json(indent=2),
        encoding="utf-8",
    )


def test_build_benchmark_ledger_classifies_theorem_outcomes_and_failures(
    tmp_path: Path,
) -> None:
    bench_dir = tmp_path / "artifacts" / "bench"
    archon_path = tmp_path / "Archon"
    archon_path.mkdir()
    alpha_artifact = bench_dir / "alpha-run"
    beta_artifact = bench_dir / "beta-run"
    gamma_artifact = bench_dir / "gamma-run"
    alpha_project = tmp_path / "Alpha"
    beta_project = tmp_path / "Beta"
    gamma_project = tmp_path / "Gamma"
    for project in [alpha_project, beta_project, gamma_project]:
        project.mkdir()

    _write_run_summary(
        alpha_artifact,
        theorem_name="foo",
        file_path=alpha_project / "Core.lean",
    )
    _write_execution(
        alpha_artifact,
        status=ExecutionStatus.COMPLETED,
        cost_estimate=1.25,
        latency_ms=1200,
        provider_pool="premium",
        provider_member="primary",
    )
    _write_run_summary(
        beta_artifact,
        theorem_name="bar",
        file_path=beta_project / "Core.lean",
    )
    _write_execution(
        beta_artifact,
        status=ExecutionStatus.FAILED,
        error_message="provider timeout",
        provider_pool="premium",
        provider_member="fallback",
    )
    _write_run_summary(
        gamma_artifact,
        theorem_name=None,
        file_path=None,
        reason="bootstrap_first_iteration",
    )

    result = BenchmarkResult(
        benchmark=BenchmarkConfig(
            name="smoke",
            description="",
            artifact_root=bench_dir,
            worker_slots=1,
        ),
        manifest_path=tmp_path / "benchmark.toml",
        run_id="benchmark-1",
        status=BenchmarkRunStatus.PARTIAL,
        dry_run=False,
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
        artifact_dir=bench_dir,
        manifest_copy_path=bench_dir / "manifest.toml",
        summary_path=bench_dir / "summary.json",
        projects=[
            BenchmarkProjectResult(
                id="alpha",
                workflow=WorkflowMode.ADAPTIVE_LOOP,
                budget_minutes=30,
                run_id="run-alpha",
                run_status=RunStatus.COMPLETED,
                snapshot=_make_snapshot(
                    "alpha",
                    alpha_project,
                    archon_path,
                    objectives=["1. **Core.lean** - fill theorem `foo`"],
                    sorry_count=1,
                ),
                score=_make_score("alpha", 8.5),
                delta=SnapshotDelta(
                    sorry_delta=-1,
                    axiom_delta=0,
                    review_session_delta=0,
                    task_results_delta=1,
                    checklist_done_delta=0,
                    score_delta=2.0,
                ),
                artifact_dir=alpha_artifact,
            ),
            BenchmarkProjectResult(
                id="beta",
                workflow=WorkflowMode.ADAPTIVE_LOOP,
                budget_minutes=30,
                run_id="run-beta",
                run_status=RunStatus.FAILED,
                snapshot=_make_snapshot(
                    "beta",
                    beta_project,
                    archon_path,
                    objectives=["1. **Core.lean** - fill theorem `bar`"],
                    sorry_count=2,
                ),
                score=_make_score("beta", 1.0),
                delta=SnapshotDelta(
                    sorry_delta=0,
                    axiom_delta=0,
                    review_session_delta=0,
                    task_results_delta=0,
                    checklist_done_delta=0,
                    score_delta=0.0,
                ),
                artifact_dir=beta_artifact,
                error_message="provider timeout",
            ),
            BenchmarkProjectResult(
                id="gamma",
                workflow=WorkflowMode.ADAPTIVE_LOOP,
                budget_minutes=30,
                run_id="run-gamma",
                run_status=RunStatus.COMPLETED,
                snapshot=_make_snapshot(
                    "gamma",
                    gamma_project,
                    archon_path,
                    objectives=["Investigate build error"],
                    sorry_count=3,
                ),
                score=_make_score("gamma", 0.5),
                delta=SnapshotDelta(
                    sorry_delta=0,
                    axiom_delta=0,
                    review_session_delta=0,
                    task_results_delta=0,
                    checklist_done_delta=0,
                    score_delta=0.0,
                ),
                artifact_dir=gamma_artifact,
            ),
        ],
    )

    ledger = build_benchmark_ledger(result)

    assert ledger.summary.total_projects == 3
    assert ledger.summary.targeted_projects == 2
    assert ledger.summary.outcome_counts["proof_progress"] == 1
    assert ledger.summary.outcome_counts["failed"] == 1
    assert ledger.summary.outcome_counts["missing_target"] == 1
    assert ledger.summary.failure_counts["executor_failed"] == 1
    assert ledger.summary.failure_counts["missing_target_theorem"] == 1
    assert ledger.summary.total_estimated_cost == 1.25

    alpha = next(item for item in ledger.outcomes if item.project_id == "alpha")
    beta = next(item for item in ledger.outcomes if item.project_id == "beta")
    gamma = next(item for item in ledger.outcomes if item.project_id == "gamma")

    assert alpha.theorem_name == "foo"
    assert alpha.outcome == "proof_progress"
    assert alpha.failure_kind == "none"
    assert alpha.provider_pool == "premium"
    assert alpha.provider_member == "primary"
    assert alpha.latency_ms == 1200

    assert beta.theorem_name == "bar"
    assert beta.outcome == "failed"
    assert beta.failure_kind == "executor_failed"
    assert beta.error_message == "provider timeout"

    assert gamma.theorem_name is None
    assert gamma.outcome == "missing_target"
    assert gamma.failure_kind == "missing_target_theorem"
