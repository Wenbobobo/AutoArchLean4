from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture()
def fake_archon_root(tmp_path: Path) -> Path:
    archon_root = tmp_path / "Archon"
    archon_root.mkdir()
    (archon_root / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    return archon_root


@pytest.fixture()
def fake_archon_project(tmp_path: Path) -> Path:
    project = tmp_path / "DemoProject"
    state_dir = project / ".archon"
    prompts_dir = state_dir / "prompts"
    task_results_dir = state_dir / "task_results"
    prompts_dir.mkdir(parents=True)
    task_results_dir.mkdir()
    (project / "lakefile.lean").write_text("import Lake\n", encoding="utf-8")
    (state_dir / "CLAUDE.md").write_text("# demo\n", encoding="utf-8")
    (prompts_dir / "plan.md").write_text("# plan\n", encoding="utf-8")
    (prompts_dir / "prover-prover.md").write_text("# prover\n", encoding="utf-8")
    (state_dir / "PROGRESS.md").write_text(
        "# Project Progress\n\n"
        "## Current Stage\n"
        "prover\n\n"
        "## Stages\n"
        "- [x] init\n"
        "- [ ] prover\n\n"
        "## Current Objectives\n\n"
        "1. **Core.lean** — fill theorem `foo`\n",
        encoding="utf-8",
    )
    return project

