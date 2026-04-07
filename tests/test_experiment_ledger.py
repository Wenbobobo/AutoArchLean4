from __future__ import annotations

from pathlib import Path

from archonlab.experiment_ledger import (
    build_experiment_ledger_comparison,
    build_experiment_replay,
    build_failure_taxonomy,
    build_theorem_outcome_ledger,
)
from archonlab.models import (
    ExperimentLedger,
    ExperimentLedgerSummary,
    ExperimentProjectLedger,
    ExperimentReplayPayload,
    FailureCategory,
    LeanAnalysisSnapshot,
    LeanDeclaration,
    RunStatus,
    TheoremOutcome,
    TheoremOutcomeKind,
    TheoremState,
)


def test_theorem_outcome_ledger_classifies_changes_and_failures() -> None:
    before = LeanAnalysisSnapshot(
        project_id="demo",
        project_path=Path("/tmp/demo"),
        declarations=[
            LeanDeclaration(
                name="foo",
                file_path=Path("Core.lean"),
                declaration_kind="theorem",
                blocked_by_sorry=True,
            ),
            LeanDeclaration(
                name="bar",
                file_path=Path("Core.lean"),
                declaration_kind="theorem",
            ),
        ],
    )
    after = LeanAnalysisSnapshot(
        project_id="demo",
        project_path=Path("/tmp/demo"),
        declarations=[
            LeanDeclaration(
                name="foo",
                file_path=Path("Core.lean"),
                declaration_kind="theorem",
            ),
            LeanDeclaration(
                name="bar",
                file_path=Path("Core.lean"),
                declaration_kind="theorem",
                uses_axiom=True,
            ),
            LeanDeclaration(
                name="baz",
                file_path=Path("Extra.lean"),
                declaration_kind="lemma",
                blocked_by_sorry=True,
            ),
        ],
    )

    outcomes = build_theorem_outcome_ledger(before, after)
    taxonomy = build_failure_taxonomy(outcomes, error_message="worker timeout")
    outcome_by_name = {item.theorem_name: item for item in outcomes}
    taxonomy_by_category = {item.category: item for item in taxonomy}

    assert outcome_by_name["foo"].outcome.value == "improved"
    assert outcome_by_name["foo"].before_state.value == "contains_sorry"
    assert outcome_by_name["foo"].after_state.value == "proved"
    assert outcome_by_name["foo"].failure_categories == []

    assert outcome_by_name["bar"].outcome.value == "regressed"
    assert outcome_by_name["bar"].after_state.value == "uses_axiom"
    assert [item.value for item in outcome_by_name["bar"].failure_categories] == [
        "uses_axiom"
    ]

    assert outcome_by_name["baz"].outcome.value == "new"
    assert outcome_by_name["baz"].after_state.value == "contains_sorry"
    assert [item.value for item in outcome_by_name["baz"].failure_categories] == [
        "contains_sorry"
    ]

    assert taxonomy_by_category[FailureCategory.USES_AXIOM].count == 1
    assert taxonomy_by_category[FailureCategory.USES_AXIOM].samples == ["bar"]
    assert taxonomy_by_category[FailureCategory.CONTAINS_SORRY].count == 1
    assert taxonomy_by_category[FailureCategory.CONTAINS_SORRY].samples == ["baz"]
    assert taxonomy_by_category[FailureCategory.RUN_ERROR].count == 1
    assert taxonomy_by_category[FailureCategory.RUN_ERROR].samples == ["worker timeout"]


def test_experiment_ledger_comparison_reports_theorem_level_changes() -> None:
    baseline = ExperimentLedger(
        benchmark_name="baseline",
        benchmark_run_id="run-a",
        summary=ExperimentLedgerSummary(total_projects=1, total_theorems=2, unchanged=2),
        outcomes=[
            ExperimentProjectLedger(
                project_id="demo",
                run_id="run-a",
                run_status=RunStatus.COMPLETED,
                theorem_outcomes=[
                    TheoremOutcome(
                        theorem_name="foo",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        before_state=TheoremState.CONTAINS_SORRY,
                        after_state=TheoremState.CONTAINS_SORRY,
                        outcome=TheoremOutcomeKind.UNCHANGED,
                    ),
                    TheoremOutcome(
                        theorem_name="helper",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        before_state=TheoremState.PROVED,
                        after_state=TheoremState.PROVED,
                        outcome=TheoremOutcomeKind.UNCHANGED,
                    ),
                ],
            )
        ],
    )
    candidate = ExperimentLedger(
        benchmark_name="candidate",
        benchmark_run_id="run-b",
        summary=ExperimentLedgerSummary(
            total_projects=1,
            total_theorems=3,
            unchanged=1,
            improved=1,
            new=1,
        ),
        outcomes=[
            ExperimentProjectLedger(
                project_id="demo",
                run_id="run-b",
                run_status=RunStatus.COMPLETED,
                theorem_outcomes=[
                    TheoremOutcome(
                        theorem_name="foo",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        before_state=TheoremState.CONTAINS_SORRY,
                        after_state=TheoremState.PROVED,
                        outcome=TheoremOutcomeKind.IMPROVED,
                    ),
                    TheoremOutcome(
                        theorem_name="helper",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        before_state=TheoremState.PROVED,
                        after_state=TheoremState.PROVED,
                        outcome=TheoremOutcomeKind.UNCHANGED,
                    ),
                    TheoremOutcome(
                        theorem_name="bar",
                        file_path=Path("Extra.lean"),
                        declaration_kind="lemma",
                        before_state=TheoremState.MISSING,
                        after_state=TheoremState.CONTAINS_SORRY,
                        outcome=TheoremOutcomeKind.NEW,
                        failure_categories=[FailureCategory.CONTAINS_SORRY],
                    ),
                ],
            )
        ],
    )

    comparison = build_experiment_ledger_comparison(
        baseline_ledger=baseline,
        candidate_ledger=candidate,
    )

    assert comparison.baseline_benchmark == "baseline"
    assert comparison.candidate_benchmark == "candidate"
    assert comparison.summary.improved == 1
    assert comparison.summary.new == 1
    change_by_theorem = {item.theorem_name: item for item in comparison.changes}
    assert change_by_theorem["foo"].change is TheoremOutcomeKind.IMPROVED
    assert change_by_theorem["foo"].baseline_state is TheoremState.CONTAINS_SORRY
    assert change_by_theorem["foo"].candidate_state is TheoremState.PROVED
    assert change_by_theorem["bar"].change is TheoremOutcomeKind.NEW


def test_build_experiment_replay_exposes_artifact_paths_and_theorem_filter(
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "run-demo"
    artifact_dir.mkdir()
    (artifact_dir / "run-summary.json").write_text("{}", encoding="utf-8")
    (artifact_dir / "execution.json").write_text("{}", encoding="utf-8")
    ledger = ExperimentLedger(
        benchmark_name="demo-benchmark",
        benchmark_run_id="run-demo",
        summary=ExperimentLedgerSummary(total_projects=1, total_theorems=2),
        outcomes=[
            ExperimentProjectLedger(
                project_id="demo",
                run_id="run-demo",
                run_status=RunStatus.COMPLETED,
                artifact_dir=artifact_dir,
                theorem_outcomes=[
                    TheoremOutcome(
                        theorem_name="foo",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        before_state=TheoremState.CONTAINS_SORRY,
                        after_state=TheoremState.PROVED,
                        outcome=TheoremOutcomeKind.IMPROVED,
                    ),
                    TheoremOutcome(
                        theorem_name="helper",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        before_state=TheoremState.PROVED,
                        after_state=TheoremState.PROVED,
                        outcome=TheoremOutcomeKind.UNCHANGED,
                    ),
                ],
            )
        ],
    )

    replay = build_experiment_replay(
        experiment_ledger=ledger,
        project_id="demo",
        theorem_name="foo",
    )

    assert isinstance(replay, ExperimentReplayPayload)
    assert replay.project_id == "demo"
    assert replay.artifact_dir == artifact_dir
    assert replay.run_summary_path == artifact_dir / "run-summary.json"
    assert replay.execution_path == artifact_dir / "execution.json"
    assert len(replay.theorem_outcomes) == 1
    assert replay.theorem_outcomes[0].theorem_name == "foo"
