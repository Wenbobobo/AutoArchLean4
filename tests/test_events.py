from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from archonlab.events import EventStore
from archonlab.models import (
    ActionPhase,
    EventRecord,
    ProjectSession,
    RunStatus,
    RunSummary,
    SessionIteration,
    SessionStatus,
    WorkflowMode,
)


def test_event_store_round_trip(tmp_path: Path) -> None:
    db_path = tmp_path / "artifacts" / "archonlab.db"
    store = EventStore(db_path)
    summary = RunSummary(
        run_id="run-1",
        project_id="demo",
        workflow=WorkflowMode.ADAPTIVE_LOOP,
        status=RunStatus.STARTED,
        stage="prover",
        dry_run=True,
        started_at=datetime.now(UTC),
        artifact_dir=tmp_path / "artifacts" / "runs" / "run-1",
    )
    store.register_run(summary)
    jsonl_path = tmp_path / "artifacts" / "runs" / "run-1" / "events.jsonl"
    store.append(
        EventRecord(
            run_id="run-1",
            kind="run.started",
            project_id="demo",
            payload={"hello": "world"},
        ),
        jsonl_path=jsonl_path,
    )
    store.complete_run("run-1", RunStatus.COMPLETED)

    runs = store.list_runs()
    events = store.get_run_events("run-1")

    assert len(runs) == 1
    assert runs[0].status is RunStatus.COMPLETED
    assert len(events) == 1
    assert events[0].payload["hello"] == "world"
    assert jsonl_path.exists()


def test_event_store_lists_recent_project_events_in_order(tmp_path: Path) -> None:
    store = EventStore(tmp_path / "artifacts" / "archonlab.db")
    for run_id in ["run-1", "run-2"]:
        store.register_run(
            RunSummary(
                run_id=run_id,
                project_id="demo",
                workflow=WorkflowMode.ADAPTIVE_LOOP,
                status=RunStatus.STARTED,
                stage="prover",
                dry_run=True,
                started_at=datetime.now(UTC),
                artifact_dir=tmp_path / "artifacts" / "runs" / run_id,
            )
        )
    store.append(
        EventRecord(
            run_id="run-1",
            kind="workflow.next_action",
            project_id="demo",
            payload={"phase": "plan", "reason": "bootstrap_first_iteration"},
        )
    )
    store.append(
        EventRecord(
            run_id="run-2",
            kind="workflow.next_action",
            project_id="demo",
            payload={"phase": "plan", "reason": "bootstrap_first_iteration"},
        )
    )

    recent = store.list_recent_project_events("demo", limit=10)

    assert [event.run_id for event in recent] == ["run-1", "run-2"]


def test_event_store_tracks_project_sessions_and_iterations(tmp_path: Path) -> None:
    store = EventStore(tmp_path / "artifacts" / "archonlab.db")
    session = ProjectSession(
        session_id="session-alpha-1",
        workspace_id="demo-workspace",
        project_id="alpha",
        workflow=WorkflowMode.ADAPTIVE_LOOP,
        dry_run=True,
        max_iterations=8,
    )
    store.register_session(session)
    store.append_session_iteration(
        SessionIteration(
            session_id=session.session_id,
            iteration_index=1,
            project_id=session.project_id,
            run_id="run-1",
            status=RunStatus.COMPLETED,
            action_phase=ActionPhase.PLAN,
            action_reason="bootstrap_first_iteration",
            finished_at=datetime.now(UTC),
        )
    )
    updated = store.update_session(
        session.session_id,
        status=SessionStatus.RUNNING,
        completed_iterations=1,
        last_run_id="run-1",
    )

    listed = store.list_sessions(workspace_id="demo-workspace")
    iterations = store.list_session_iterations(session.session_id)

    assert updated.status is SessionStatus.RUNNING
    assert updated.completed_iterations == 1
    assert updated.started_at is not None
    assert len(listed) == 1
    assert listed[0].last_run_id == "run-1"
    assert len(iterations) == 1
    assert iterations[0].action_phase is ActionPhase.PLAN
