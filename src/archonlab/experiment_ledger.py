from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from .models import (
    BenchmarkProjectResult,
    ExperimentLedger,
    ExperimentLedgerSummary,
    ExperimentProjectLedger,
    FailureCategory,
    FailureCategorySummary,
    LeanAnalysisSnapshot,
    LeanDeclaration,
    TheoremOutcome,
    TheoremOutcomeKind,
    TheoremState,
)


def _theorem_state(declaration: LeanDeclaration | None) -> TheoremState:
    if declaration is None:
        return TheoremState.MISSING
    if declaration.uses_axiom:
        return TheoremState.USES_AXIOM
    if declaration.blocked_by_sorry:
        return TheoremState.CONTAINS_SORRY
    return TheoremState.PROVED


def _theorem_severity(state: TheoremState) -> int:
    return {
        TheoremState.PROVED: 0,
        TheoremState.CONTAINS_SORRY: 1,
        TheoremState.USES_AXIOM: 2,
        TheoremState.MISSING: 3,
    }[state]


def _outcome_kind(
    before: LeanDeclaration | None,
    after: LeanDeclaration | None,
) -> TheoremOutcomeKind:
    if before is None:
        return TheoremOutcomeKind.NEW
    if after is None:
        return TheoremOutcomeKind.REMOVED
    before_state = _theorem_state(before)
    after_state = _theorem_state(after)
    if before_state is after_state:
        return TheoremOutcomeKind.UNCHANGED
    if _theorem_severity(after_state) < _theorem_severity(before_state):
        return TheoremOutcomeKind.IMPROVED
    return TheoremOutcomeKind.REGRESSED


def _failure_categories(
    *,
    after: LeanDeclaration | None,
    after_state: TheoremState,
) -> list[FailureCategory]:
    if after is None or after_state is TheoremState.MISSING:
        return [FailureCategory.REMOVED_DECLARATION]
    if after_state is TheoremState.CONTAINS_SORRY:
        return [FailureCategory.CONTAINS_SORRY]
    if after_state is TheoremState.USES_AXIOM:
        return [FailureCategory.USES_AXIOM]
    return []


def build_theorem_outcome_ledger(
    before: LeanAnalysisSnapshot,
    after: LeanAnalysisSnapshot,
) -> list[TheoremOutcome]:
    before_by_name = {declaration.name: declaration for declaration in before.declarations}
    after_by_name = {declaration.name: declaration for declaration in after.declarations}
    theorem_names = sorted(set(before_by_name) | set(after_by_name))

    outcomes: list[TheoremOutcome] = []
    for theorem_name in theorem_names:
        before_declaration = before_by_name.get(theorem_name)
        after_declaration = after_by_name.get(theorem_name)
        after_state = _theorem_state(after_declaration)
        outcomes.append(
            TheoremOutcome(
                theorem_name=theorem_name,
                file_path=(
                    after_declaration.file_path
                    if after_declaration is not None
                    else (
                        before_declaration.file_path
                        if before_declaration is not None
                        else None
                    )
                ),
                declaration_kind=(
                    after_declaration.declaration_kind
                    if after_declaration is not None
                    else (
                        before_declaration.declaration_kind
                        if before_declaration is not None
                        else None
                    )
                ),
                before_state=_theorem_state(before_declaration),
                after_state=after_state,
                outcome=_outcome_kind(before_declaration, after_declaration),
                failure_categories=_failure_categories(
                    after=after_declaration,
                    after_state=after_state,
                ),
            )
        )
    return outcomes


def build_failure_taxonomy(
    outcomes: list[TheoremOutcome],
    *,
    error_message: str | None = None,
) -> list[FailureCategorySummary]:
    samples_by_category: dict[FailureCategory, list[str]] = defaultdict(list)
    for outcome in outcomes:
        for category in outcome.failure_categories:
            samples_by_category[category].append(outcome.theorem_name)
    if error_message:
        samples_by_category[FailureCategory.RUN_ERROR].append(error_message)

    return [
        FailureCategorySummary(
            category=category,
            count=len(samples),
            samples=samples,
        )
        for category, samples in sorted(
            samples_by_category.items(),
            key=lambda item: item[0].value,
        )
    ]


def build_experiment_ledger(
    *,
    benchmark_name: str,
    benchmark_run_id: str,
    project_results: list[BenchmarkProjectResult],
) -> ExperimentLedger:
    all_outcomes = [
        outcome
        for project_result in project_results
        for outcome in project_result.theorem_outcomes
    ]
    failure_counts: dict[FailureCategory, int] = defaultdict(int)
    failure_samples: dict[FailureCategory, list[str]] = defaultdict(list)
    for project_result in project_results:
        for taxonomy_entry in project_result.failure_taxonomy:
            failure_counts[taxonomy_entry.category] += taxonomy_entry.count
            failure_samples[taxonomy_entry.category].extend(taxonomy_entry.samples)

    summary = ExperimentLedgerSummary(
        total_projects=len(project_results),
        total_theorems=len(all_outcomes),
        unchanged=sum(
            1
            for outcome in all_outcomes
            if outcome.outcome is TheoremOutcomeKind.UNCHANGED
        ),
        improved=sum(
            1
            for outcome in all_outcomes
            if outcome.outcome is TheoremOutcomeKind.IMPROVED
        ),
        regressed=sum(
            1
            for outcome in all_outcomes
            if outcome.outcome is TheoremOutcomeKind.REGRESSED
        ),
        new=sum(1 for outcome in all_outcomes if outcome.outcome is TheoremOutcomeKind.NEW),
        removed=sum(
            1
            for outcome in all_outcomes
            if outcome.outcome is TheoremOutcomeKind.REMOVED
        ),
        failure_taxonomy=[
            FailureCategorySummary(
                category=category,
                count=count,
                samples=samples,
            )
            for category, count, samples in sorted(
                (
                    (category, count, failure_samples[category])
                    for category, count in failure_counts.items()
                ),
                key=lambda item: item[0].value,
            )
        ],
    )
    return ExperimentLedger(
        benchmark_name=benchmark_name,
        benchmark_run_id=benchmark_run_id,
        generated_at=datetime.now(UTC),
        summary=summary,
        outcomes=[
            ExperimentProjectLedger(
                project_id=project_result.id,
                run_id=project_result.run_id,
                run_status=project_result.run_status,
                theorem_outcomes=project_result.theorem_outcomes,
                failure_taxonomy=project_result.failure_taxonomy,
            )
            for project_result in project_results
        ],
    )
