from __future__ import annotations

from pathlib import Path

from archonlab.lean_analyzer import collect_lean_analysis
from archonlab.models import LeanAnalysisSnapshot, LeanDeclaration, LeanDiagnostic, LeanProofGap
from archonlab.project_state import collect_project_snapshot
from archonlab.task_graph import build_task_graph


class StaticAnalyzer:
    def analyze(self, *, project_path: Path, archon_path: Path) -> LeanAnalysisSnapshot:
        del archon_path
        return LeanAnalysisSnapshot(
            project_id=project_path.name,
            project_path=project_path,
            lean_file_count=1,
            theorem_count=2,
            sorry_count=1,
            axiom_count=0,
            declarations=[
                LeanDeclaration(
                    name="helper",
                    file_path=Path("Core.lean"),
                    declaration_kind="theorem",
                    dependencies=[],
                    blocked_by_sorry=False,
                ),
                LeanDeclaration(
                    name="foo",
                    file_path=Path("Core.lean"),
                    declaration_kind="theorem",
                    dependencies=["helper"],
                    blocked_by_sorry=True,
                ),
            ],
        )


class FailingAnalyzer:
    def analyze(self, *, project_path: Path, archon_path: Path) -> LeanAnalysisSnapshot:
        del project_path, archon_path
        raise RuntimeError("sidecar unavailable")


def test_build_task_graph_uses_structured_analyzer_output(
    fake_archon_project: Path, fake_archon_root: Path
) -> None:
    graph = build_task_graph(
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
        analyzer=StaticAnalyzer(),
    )

    node_ids = {node.id for node in graph.nodes}
    assert "lean:Core.lean:helper" in node_ids
    assert "lean:Core.lean:foo" in node_ids
    foo_node = next(node for node in graph.nodes if node.id == "lean:Core.lean:foo")
    assert foo_node.status.value == "blocked"
    assert foo_node.blockers == ["contains_sorry"]
    assert any(
        edge.source_id == "lean:Core.lean:foo"
        and edge.target_id == "lean:Core.lean:helper"
        and edge.kind == "depends_on"
        for edge in graph.edges
    )


def test_build_task_graph_propagates_axiom_and_dependency_blockers(
    fake_archon_project: Path, fake_archon_root: Path
) -> None:
    class AxiomAnalyzer:
        def analyze(self, *, project_path: Path, archon_path: Path) -> LeanAnalysisSnapshot:
            del archon_path
            return LeanAnalysisSnapshot(
                project_id=project_path.name,
                project_path=project_path,
                lean_file_count=1,
                theorem_count=2,
                sorry_count=0,
                axiom_count=1,
                declarations=[
                    LeanDeclaration(
                        name="helper",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        dependencies=[],
                        uses_axiom=True,
                    ),
                    LeanDeclaration(
                        name="foo",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        dependencies=["helper"],
                    ),
                ],
            )

    graph = build_task_graph(
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
        analyzer=AxiomAnalyzer(),
    )

    helper_node = next(node for node in graph.nodes if node.id == "lean:Core.lean:helper")
    foo_node = next(node for node in graph.nodes if node.id == "lean:Core.lean:foo")
    assert helper_node.status.value == "blocked"
    assert helper_node.blockers == ["uses_axiom"]
    assert foo_node.status.value == "blocked"
    assert foo_node.blockers == ["depends_on:helper", "uses_axiom"]


def test_build_task_graph_uses_structured_proof_gaps_and_diagnostics(
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    class StructuredAnalyzer:
        def analyze(self, *, project_path: Path, archon_path: Path) -> LeanAnalysisSnapshot:
            del archon_path
            return LeanAnalysisSnapshot(
                project_id=project_path.name,
                project_path=project_path,
                lean_file_count=1,
                theorem_count=1,
                sorry_count=0,
                axiom_count=0,
                declarations=[
                    LeanDeclaration(
                        name="foo",
                        file_path=Path("Core.lean"),
                        declaration_kind="theorem",
                        dependencies=[],
                    ),
                ],
                proof_gaps=[
                    LeanProofGap(
                        kind="unsolved_goal",
                        theorem_name="foo",
                        file_path=Path("Core.lean"),
                        message="stuck on goal",
                    )
                ],
                diagnostics=[
                    LeanDiagnostic(
                        severity="error",
                        message="type mismatch",
                        theorem_name="foo",
                        file_path=Path("Core.lean"),
                        code="lean.type_mismatch",
                    )
                ],
            )

    graph = build_task_graph(
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
        analyzer=StructuredAnalyzer(),
    )

    foo_node = next(node for node in graph.nodes if node.id == "lean:Core.lean:foo")
    assert foo_node.status.value == "blocked"
    assert foo_node.blockers == ["proof_gap:unsolved_goal", "diagnostic:lean.type_mismatch"]
    assert foo_node.metadata["proof_gap_count"] == 1
    assert foo_node.metadata["diagnostic_count"] == 1


def test_collect_project_snapshot_falls_back_when_analyzer_fails(
    fake_archon_project: Path, fake_archon_root: Path
) -> None:
    (fake_archon_project / "Core.lean").write_text(
        "theorem foo : True := by\n"
        "  sorry\n",
        encoding="utf-8",
    )

    snapshot = collect_project_snapshot(
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
        analyzer=FailingAnalyzer(),
    )

    assert snapshot.lean_file_count == 2
    assert snapshot.theorem_count == 1
    assert snapshot.sorry_count == 1
    assert snapshot.axiom_count == 0
    assert snapshot.analysis_backend == "regex"
    assert snapshot.analysis_fallback_used is True
    assert snapshot.analysis_fallback_reason == "sidecar unavailable"
    assert snapshot.proof_gap_count == 1
    assert snapshot.diagnostic_count == 0
    assert snapshot.theorem_state_counts["contains_sorry"] == 1
    assert snapshot.theorem_states[0].theorem_name == "foo"


def test_collect_lean_analysis_marks_fallback_metadata_when_analyzer_fails(
    fake_archon_project: Path, fake_archon_root: Path
) -> None:
    (fake_archon_project / "Core.lean").write_text(
        "theorem foo : True := by\n"
        "  sorry\n",
        encoding="utf-8",
    )

    snapshot = collect_lean_analysis(
        project_path=fake_archon_project,
        archon_path=fake_archon_root,
        analyzer=FailingAnalyzer(),
    )

    assert snapshot.backend == "regex"
    assert snapshot.fallback_used is True
    assert snapshot.fallback_reason == "sidecar unavailable"
    assert snapshot.proof_gaps[0].kind == "contains_sorry"
