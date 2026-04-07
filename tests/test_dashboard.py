from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient  # noqa: E402

from archonlab.dashboard import create_dashboard_app  # noqa: E402
from archonlab.events import EventStore  # noqa: E402
from archonlab.models import (  # noqa: E402
    ActionPhase,
    BatchRunReport,
    EventRecord,
    ExecutorKind,
    ProviderKind,
    QueueJobPreview,
    RunStatus,
    RunSummary,
    SupervisorAction,
    SupervisorReason,
    WorkflowMode,
)
from archonlab.queue import QueueStore  # noqa: E402


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

    queue_store = QueueStore(artifact_root / "archonlab.db")
    queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": str(tmp_path / "demo.toml")},
        project_id="demo-project",
        priority=10,
        required_executor_kinds=[ExecutorKind.DRY_RUN],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        required_models=["gpt-5.4"],
        required_cost_tiers=["premium"],
        required_endpoint_classes=["lab"],
        preview=QueueJobPreview(
            phase=ActionPhase.PROVER,
            reason="task_graph_focus",
            stage="prover",
            supervisor_action=SupervisorAction.CONTINUE,
            supervisor_reason=SupervisorReason.HEALTHY,
            supervisor_summary="Current state looks healthy enough to continue the planned loop.",
            theorem_name="foo",
            base_priority=3,
            task_priority_bonus=2,
            objective_relevance_bonus=5,
            final_priority=10,
            executor_kind=ExecutorKind.DRY_RUN,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4",
            cost_tier="premium",
            endpoint_class="lab",
        ),
    )
    queue_store.register_worker(slot_index=1, worker_id="worker-test")
    stale_heartbeat = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
    queue_store._conn.execute(
        "UPDATE queue_workers SET heartbeat_at = ? WHERE worker_id = ?",
        (stale_heartbeat, "worker-test"),
    )
    queue_store._conn.commit()
    workers_response = client.get("/api/queue/workers")
    assert workers_response.status_code == 200
    workers = workers_response.json()
    assert workers[0]["worker_id"] == "worker-test"
    assert workers[0]["stale"] is True

    jobs_response = client.get("/api/queue/jobs")
    assert jobs_response.status_code == 200
    jobs = jobs_response.json()
    assert jobs[0]["preview"]["phase"] == "prover"
    assert jobs[0]["preview"]["reason"] == "task_graph_focus"
    assert jobs[0]["preview"]["stage"] == "prover"
    assert jobs[0]["preview"]["final_priority"] == 10
    assert jobs[0]["preview"]["model"] == "gpt-5.4"
    assert jobs[0]["preview"]["supervisor_summary"].startswith("Current state looks healthy")

    sweep_response = client.post(
        "/api/queue/workers/sweep",
        json={"stale_after_seconds": 60, "requeue_running_jobs": True},
    )
    assert sweep_response.status_code == 200
    swept = sweep_response.json()
    assert swept[0]["worker_id"] == "worker-test"
    assert swept[0]["status"] == "failed"

    monkeypatch.setattr(
        "archonlab.dashboard.BatchRunner.run_fleet",
        lambda self, **kwargs: BatchRunReport(
            processed_job_ids=["job-1"],
            worker_ids=["worker-auto-1"],
        ),
    )
    fleet_response = client.post("/api/queue/fleet", json={})
    assert fleet_response.status_code == 200
    fleet_payload = fleet_response.json()
    assert fleet_payload["processed_job_ids"] == ["job-1"]
    assert fleet_payload["worker_ids"] == ["worker-auto-1"]
