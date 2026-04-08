from __future__ import annotations

from pathlib import Path

from archonlab.models import (
    LeanAnalysisSnapshot,
    LeanDeclaration,
    ProgressSnapshot,
    ProjectSnapshot,
    TheoremState,
    TheoremStateRecord,
)
from archonlab.project_state import collect_project_snapshot, diff_snapshots


class StructuredAnalyzer:
    def analyze(self, *, project_path: Path, archon_path: Path) -> LeanAnalysisSnapshot:
        del archon_path
        return LeanAnalysisSnapshot(
            project_id=project_path.name,
            project_path=project_path,
            backend="structured-test",
            lean_file_count=1,
            theorem_count=3,
            sorry_count=1,
            axiom_count=1,
            declarations=[
                LeanDeclaration(
                    name="helper",
                    file_path=Path("Core.lean"),
                    declaration_kind="theorem",
                ),
                LeanDeclaration(
                    name="foo",
                    file_path=Path("Core.lean"),
                    declaration_kind="theorem",
                    blocked_by_sorry=True,
                ),
                LeanDeclaration(
                    name="unsafeLemma",
                    file_path=Path("Extra.lean"),
                    declaration_kind="lemma",
                    uses_axiom=True,
                ),
            ],
        )


def test_collect_project_snapshot_includes_theorem_states_and_counts(
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    snapshot = collect_project_snapshot(
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
        analyzer=StructuredAnalyzer(),
    )

    record_by_name = {record.theorem_name: record for record in snapshot.theorem_states}

    assert snapshot.analysis_backend == "structured-test"
    assert len(snapshot.theorem_states) == 3
    assert record_by_name["helper"].state is TheoremState.PROVED
    assert record_by_name["foo"].state is TheoremState.CONTAINS_SORRY
    assert record_by_name["unsafeLemma"].state is TheoremState.USES_AXIOM
    assert snapshot.theorem_state_counts == {
        "proved": 1,
        "contains_sorry": 1,
        "uses_axiom": 1,
        "missing": 0,
    }


def test_diff_snapshots_reports_theorem_level_outcomes() -> None:
    before = ProjectSnapshot(
        project_id="demo",
        project_path=Path("/tmp/demo"),
        archon_path=Path("/tmp/archon"),
        progress=ProgressSnapshot(stage="prover"),
        lean_file_count=1,
        theorem_count=3,
        sorry_count=1,
        axiom_count=1,
        theorem_states=[
            TheoremStateRecord(
                theorem_name="foo",
                file_path=Path("Core.lean"),
                declaration_kind="theorem",
                state=TheoremState.CONTAINS_SORRY,
            ),
            TheoremStateRecord(
                theorem_name="helper",
                file_path=Path("Core.lean"),
                declaration_kind="theorem",
                state=TheoremState.PROVED,
            ),
            TheoremStateRecord(
                theorem_name="legacy",
                file_path=Path("Legacy.lean"),
                declaration_kind="lemma",
                state=TheoremState.USES_AXIOM,
            ),
            TheoremStateRecord(
                theorem_name="regress",
                file_path=Path("Extra.lean"),
                declaration_kind="theorem",
                state=TheoremState.PROVED,
            ),
        ],
    )
    after = ProjectSnapshot(
        project_id="demo",
        project_path=Path("/tmp/demo"),
        archon_path=Path("/tmp/archon"),
        progress=ProgressSnapshot(stage="prover"),
        lean_file_count=1,
        theorem_count=4,
        sorry_count=2,
        axiom_count=0,
        theorem_states=[
            TheoremStateRecord(
                theorem_name="foo",
                file_path=Path("Core.lean"),
                declaration_kind="theorem",
                state=TheoremState.PROVED,
            ),
            TheoremStateRecord(
                theorem_name="helper",
                file_path=Path("Core.lean"),
                declaration_kind="theorem",
                state=TheoremState.PROVED,
            ),
            TheoremStateRecord(
                theorem_name="newLemma",
                file_path=Path("New.lean"),
                declaration_kind="lemma",
                state=TheoremState.CONTAINS_SORRY,
            ),
            TheoremStateRecord(
                theorem_name="regress",
                file_path=Path("Extra.lean"),
                declaration_kind="theorem",
                state=TheoremState.CONTAINS_SORRY,
            ),
        ],
    )

    delta = diff_snapshots(before, after)

    assert delta.sorry_delta == 1
    assert delta.axiom_delta == -1
    assert delta.theorem_improved_count == 1
    assert delta.theorem_regressed_count == 1
    assert delta.theorem_new_count == 1
    assert delta.theorem_removed_count == 1
    assert delta.theorem_unchanged_count == 1
