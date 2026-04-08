from __future__ import annotations

from collections import Counter

from .models import (
    FailureCategory,
    LeanAnalysisSnapshot,
    LeanDeclaration,
    TheoremOutcome,
    TheoremOutcomeKind,
    TheoremState,
    TheoremStateRecord,
)


def theorem_state_from_declaration(declaration: LeanDeclaration | None) -> TheoremState:
    if declaration is None:
        return TheoremState.MISSING
    if declaration.uses_axiom:
        return TheoremState.USES_AXIOM
    if declaration.blocked_by_sorry:
        return TheoremState.CONTAINS_SORRY
    return TheoremState.PROVED


def theorem_state_severity(state: TheoremState) -> int:
    return {
        TheoremState.PROVED: 0,
        TheoremState.CONTAINS_SORRY: 1,
        TheoremState.USES_AXIOM: 2,
        TheoremState.MISSING: 3,
    }[state]


def build_theorem_state_records(analysis: LeanAnalysisSnapshot) -> list[TheoremStateRecord]:
    return sorted(
        [
            TheoremStateRecord(
                theorem_name=declaration.name,
                file_path=declaration.file_path,
                declaration_kind=declaration.declaration_kind,
                state=theorem_state_from_declaration(declaration),
            )
            for declaration in analysis.declarations
        ],
        key=lambda record: (str(record.file_path or ""), record.theorem_name),
    )


def count_theorem_states(records: list[TheoremStateRecord]) -> dict[str, int]:
    counts = {state.value: 0 for state in TheoremState}
    for record in records:
        counts[record.state.value] += 1
    return counts


def build_theorem_outcomes_from_records(
    before_records: list[TheoremStateRecord],
    after_records: list[TheoremStateRecord],
) -> list[TheoremOutcome]:
    before_by_name = {record.theorem_name: record for record in before_records}
    after_by_name = {record.theorem_name: record for record in after_records}
    theorem_names = sorted(set(before_by_name) | set(after_by_name))

    outcomes: list[TheoremOutcome] = []
    for theorem_name in theorem_names:
        before_record = before_by_name.get(theorem_name)
        after_record = after_by_name.get(theorem_name)
        before_state = (
            before_record.state if before_record is not None else TheoremState.MISSING
        )
        after_state = after_record.state if after_record is not None else TheoremState.MISSING
        outcomes.append(
            TheoremOutcome(
                theorem_name=theorem_name,
                file_path=(
                    after_record.file_path
                    if after_record is not None
                    else (before_record.file_path if before_record is not None else None)
                ),
                declaration_kind=(
                    after_record.declaration_kind
                    if after_record is not None
                    else (
                        before_record.declaration_kind
                        if before_record is not None
                        else None
                    )
                ),
                before_state=before_state,
                after_state=after_state,
                outcome=_theorem_outcome_kind(
                    before_record=before_record,
                    after_record=after_record,
                ),
                failure_categories=_failure_categories(
                    after_record=after_record,
                    after_state=after_state,
                ),
            )
        )
    return outcomes


def count_theorem_outcomes(outcomes: list[TheoremOutcome]) -> dict[str, int]:
    counts = {kind.value: 0 for kind in TheoremOutcomeKind}
    counts.update(Counter(outcome.outcome.value for outcome in outcomes))
    return counts


def _theorem_outcome_kind(
    *,
    before_record: TheoremStateRecord | None,
    after_record: TheoremStateRecord | None,
) -> TheoremOutcomeKind:
    if before_record is None:
        return TheoremOutcomeKind.NEW
    if after_record is None:
        return TheoremOutcomeKind.REMOVED
    if before_record.state is after_record.state:
        return TheoremOutcomeKind.UNCHANGED
    if theorem_state_severity(after_record.state) < theorem_state_severity(before_record.state):
        return TheoremOutcomeKind.IMPROVED
    return TheoremOutcomeKind.REGRESSED


def _failure_categories(
    *,
    after_record: TheoremStateRecord | None,
    after_state: TheoremState,
) -> list[FailureCategory]:
    if after_record is None or after_state is TheoremState.MISSING:
        return [FailureCategory.REMOVED_DECLARATION]
    if after_state is TheoremState.CONTAINS_SORRY:
        return [FailureCategory.CONTAINS_SORRY]
    if after_state is TheoremState.USES_AXIOM:
        return [FailureCategory.USES_AXIOM]
    return []
