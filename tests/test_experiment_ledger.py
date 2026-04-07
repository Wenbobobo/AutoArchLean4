from __future__ import annotations

from pathlib import Path

from archonlab.experiment_ledger import (
    build_failure_taxonomy,
    build_theorem_outcome_ledger,
)
from archonlab.models import (
    FailureCategory,
    LeanAnalysisSnapshot,
    LeanDeclaration,
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
