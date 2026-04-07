from __future__ import annotations

from pathlib import Path

from archonlab.checks import gather_doctor_report
from archonlab.models import LeanAnalyzerConfig, LeanAnalyzerKind, ProjectConfig


def test_gather_doctor_report_describes_configured_command_lean_analyzer(
    tmp_path: Path,
) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    state_dir = project_path / ".archon"
    project_path.mkdir()
    archon_path.mkdir()
    state_dir.mkdir(parents=True)
    (archon_path / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    (state_dir / "PROGRESS.md").write_text("# progress\n", encoding="utf-8")

    report = gather_doctor_report(
        ProjectConfig(
            name="demo",
            project_path=project_path,
            archon_path=archon_path,
        ),
        lean_analyzer=LeanAnalyzerConfig(
            kind=LeanAnalyzerKind.COMMAND,
            command=["python", "lean-sidecar.py"],
        ),
    )

    analyzer_status = next(path for path in report.paths if path.name == "lean_analyzer")
    assert analyzer_status.ok is True
    assert analyzer_status.detail.startswith("CommandLeanAnalyzer")
    assert "python lean-sidecar.py" in analyzer_status.detail
