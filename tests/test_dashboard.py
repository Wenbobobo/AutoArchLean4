from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest
from fastapi.testclient import TestClient  # noqa: E402

from archonlab.dashboard import create_dashboard_app  # noqa: E402
from archonlab.events import EventStore  # noqa: E402
from archonlab.models import EventRecord, RunStatus, RunSummary, WorkflowMode  # noqa: E402


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


def _seed_run(artifact_root: Path) -> None:
    store = EventStore(artifact_root / "archonlab.db")
    summary = RunSummary(
        run_id="run-1",
        project_id="DemoProject",
        workflow=WorkflowMode.ADAPTIVE_LOOP,
        status=RunStatus.STARTED,
        stage="prover",
        dry_run=True,
        started_at=datetime.now(UTC),
        artifact_dir=artifact_root / "runs" / "run-1",
    )
    store.register_run(summary)
    store.append(
        EventRecord(
            run_id="run-1",
            kind="run.started",
            project_id="DemoProject",
            payload={"stage": "prover"},
        )
    )


def test_dashboard_api_lists_runs_and_supports_control_actions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_path = _make_project(tmp_path)
    artifact_root = tmp_path / "artifacts"
    _seed_run(artifact_root)

    original_connect = sqlite3.connect

    def connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        options = dict(kwargs)
        options.setdefault("check_same_thread", False)
        return original_connect(*args, **options)

    monkeypatch.setattr(sqlite3, "connect", connect)

    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "DemoProject"\n'
        f'project_path = "{project_path}"\n'
        f'archon_path = "{tmp_path / "Archon"}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n",
        encoding="utf-8",
    )

    app = create_dashboard_app(config_path)
    client = TestClient(app)

    runs_response = client.get("/api/runs")
    assert runs_response.status_code == 200
    runs = runs_response.json()
    assert runs[0]["run_id"] == "run-1"
    assert runs[0]["status"] == "started"

    detail_response = client.get("/api/runs/run-1")
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["run"]["run_id"] == "run-1"
    assert detail["events"][0]["kind"] == "run.started"

    control_response = client.get("/api/projects/DemoProject/control")
    assert control_response.status_code == 200
    control = control_response.json()
    assert control["paused"] is False

    pause_response = client.post(
        "/api/projects/DemoProject/pause",
        json={"reason": "manual_hold"},
    )
    assert pause_response.status_code == 200
    paused = pause_response.json()
    assert paused["paused"] is True
    assert paused["pause_reason"] == "manual_hold"

    resume_response = client.post("/api/projects/DemoProject/resume")
    assert resume_response.status_code == 200
    resumed = resume_response.json()
    assert resumed["paused"] is False
    assert resumed["pause_reason"] is None

    hint_response = client.post(
        "/api/projects/DemoProject/hint",
        json={"text": "Try `simp` after `rw`.", "author": "mentor"},
    )
    assert hint_response.status_code == 200
    hint_payload = hint_response.json()
    assert hint_payload["hints"][-1]["author"] == "mentor"
    assert (project_path / ".archon" / "USER_HINTS.md").exists()
