from __future__ import annotations

import json
from pathlib import Path

from archonlab.config import load_config
from archonlab.services import RunService


def test_run_service_uses_history_to_reroute_repeated_no_progress(
    tmp_path: Path, fake_archon_project: Path, fake_archon_root: Path
) -> None:
    config_path = tmp_path / "archonlab.toml"
    artifact_root = tmp_path / "artifacts"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        f'project_path = "{fake_archon_project}"\n'
        f'archon_path = "{fake_archon_root}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n",
        encoding="utf-8",
    )

    service = RunService(load_config(config_path))
    first = service.start(dry_run=True)
    second = service.start(dry_run=True)
    third = service.start(dry_run=True)

    assert first.action.reason == "bootstrap_first_iteration"
    assert second.action.reason == "bootstrap_first_iteration"
    assert third.action.reason == "supervisor_repeated_no_progress"

    summary = json.loads((third.artifact_dir / "supervisor.json").read_text(encoding="utf-8"))
    assert summary["reason"] == "repeated_no_progress"
