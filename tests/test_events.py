from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from archonlab.events import EventStore
from archonlab.models import EventRecord, RunStatus, RunSummary, WorkflowMode


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
