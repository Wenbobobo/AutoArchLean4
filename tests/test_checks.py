from __future__ import annotations

import sys
from pathlib import Path

from archonlab.checks import ToolStatus, gather_doctor_report
from archonlab.models import LeanAnalyzerConfig, LeanAnalyzerKind, ProjectConfig


def test_gather_doctor_report_smoke_checks_configured_command_lean_analyzer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("archonlab.checks._read_version", lambda executable: "stub-version")
    monkeypatch.setattr(
        "archonlab.checks._tool_status",
        lambda name, *, required: ToolStatus(
            name=name,
            required=required,
            available=True,
            path=f"/mock/{name}",
            version="stub-version",
        ),
    )
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    state_dir = project_path / ".archon"
    analyzer_script = tmp_path / "lean_sidecar.py"
    project_path.mkdir()
    archon_path.mkdir()
    state_dir.mkdir(parents=True)
    (archon_path / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    (state_dir / "PROGRESS.md").write_text("# progress\n", encoding="utf-8")
    (project_path / "Core.lean").write_text(
        "theorem demo : True := by\n  trivial\n",
        encoding="utf-8",
    )
    analyzer_script.write_text(
        "from __future__ import annotations\n"
        "import json\n"
        "\n"
        "print(json.dumps({\n"
        f'    "project_id": "{project_path.name}",\n'
        f'    "project_path": "{project_path.resolve()}",\n'
        '    "lean_file_count": 1,\n'
        '    "theorem_count": 3,\n'
        '    "sorry_count": 0,\n'
        '    "axiom_count": 0,\n'
        '    "declarations": []\n'
        '}))\n',
        encoding="utf-8",
    )

    report = gather_doctor_report(
        ProjectConfig(
            name="demo",
            project_path=project_path,
            archon_path=archon_path,
        ),
        lean_analyzer=LeanAnalyzerConfig(
            kind=LeanAnalyzerKind.COMMAND,
            command=[sys.executable, str(analyzer_script)],
        ),
    )

    analyzer_status = next(path for path in report.paths if path.name == "lean_analyzer")
    assert analyzer_status.ok is True
    assert analyzer_status.detail.startswith("CommandLeanAnalyzer")
    assert "backend=command" in analyzer_status.detail
    assert "theorem_count=3" in analyzer_status.detail
    assert "fallback=no" in analyzer_status.detail


def test_gather_doctor_report_marks_failed_command_lean_analyzer_with_regex_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr("archonlab.checks._read_version", lambda executable: "stub-version")
    monkeypatch.setattr(
        "archonlab.checks._tool_status",
        lambda name, *, required: ToolStatus(
            name=name,
            required=required,
            available=True,
            path=f"/mock/{name}",
            version="stub-version",
        ),
    )
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    state_dir = project_path / ".archon"
    analyzer_script = tmp_path / "failing_sidecar.py"
    project_path.mkdir()
    archon_path.mkdir()
    state_dir.mkdir(parents=True)
    (archon_path / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    (state_dir / "PROGRESS.md").write_text("# progress\n", encoding="utf-8")
    (project_path / "Core.lean").write_text(
        "theorem demo : True := by\n  sorry\n",
        encoding="utf-8",
    )
    analyzer_script.write_text("raise SystemExit(1)\n", encoding="utf-8")

    report = gather_doctor_report(
        ProjectConfig(
            name="demo",
            project_path=project_path,
            archon_path=archon_path,
        ),
        lean_analyzer=LeanAnalyzerConfig(
            kind=LeanAnalyzerKind.COMMAND,
            command=[sys.executable, str(analyzer_script)],
        ),
    )

    analyzer_status = next(path for path in report.paths if path.name == "lean_analyzer")
    assert analyzer_status.ok is False
    assert "fallback=yes" in analyzer_status.detail
    assert "backend=regex" in analyzer_status.detail
    assert "failing_sidecar.py" in analyzer_status.detail
