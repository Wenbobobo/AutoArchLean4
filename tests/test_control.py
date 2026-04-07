from __future__ import annotations

import json
from pathlib import Path

from archonlab.control import ControlService
from archonlab.models import ProjectConfig


def _make_project(tmp_path: Path) -> Path:
    project_path = tmp_path / "DemoProject"
    state_dir = project_path / ".archon"
    state_dir.mkdir(parents=True)
    (state_dir / "PROGRESS.md").write_text(
        "# Project Progress\n\n"
        "## Current Stage\n"
        "prover\n",
        encoding="utf-8",
    )
    return project_path


def test_control_state_service_pause_resume_and_hint_injection(tmp_path: Path) -> None:
    project_path = _make_project(tmp_path)
    service = ControlService(root=tmp_path / "control-state")
    project = ProjectConfig(
        name="DemoProject",
        project_path=project_path,
        archon_path=tmp_path / "Archon",
    )

    paused = service.pause(project, reason="manual_hold")
    assert paused.paused is True
    assert paused.pause_reason == "manual_hold"
    state_path = tmp_path / "control-state" / "control" / "DemoProject.json"
    assert state_path.exists()
    stored = json.loads(state_path.read_text(encoding="utf-8"))
    assert stored["paused"] is True
    assert stored["pause_reason"] == "manual_hold"

    resumed = service.resume(project)
    assert resumed.paused is False
    assert resumed.pause_reason is None

    updated = service.add_hint(
        project,
        text="Try unfolding `foo` before `simp`.",
        author="mentor",
    )
    expected_hints_path = project_path / ".archon" / "USER_HINTS.md"
    assert updated.hints[0].text == "Try unfolding `foo` before `simp`."
    assert updated.hints[0].author == "mentor"
    assert expected_hints_path.exists()
    content = expected_hints_path.read_text(encoding="utf-8")
    assert "Try unfolding `foo` before `simp`." in content
    assert "mentor" in content
