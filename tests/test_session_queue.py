from __future__ import annotations

import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

from archonlab.batch import BatchRunner
from archonlab.control import ControlService
from archonlab.events import EventStore
from archonlab.models import (
    ExecutorKind,
    ProviderKind,
    QueueJobKind,
    QueueJobStatus,
    SessionStatus,
)
from archonlab.queue import QueueStore


def _clone_project(source: Path, target: Path) -> Path:
    shutil.copytree(source, target)
    return target


def _write_workspace_config(
    path: Path,
    *,
    artifact_root: Path,
    archon_path: Path,
    projects: list[tuple[str, Path]],
) -> Path:
    content = (
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 2\n\n"
    )
    for project_id, project_path in projects:
        content += (
            "[[projects]]\n"
            f'id = "{project_id}"\n'
            f'project_path = "{project_path}"\n'
            f'archon_path = "{archon_path}"\n\n'
        )
    path.write_text(content, encoding="utf-8")
    return path


def test_queue_store_enqueue_workspace_sessions_creates_session_jobs(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    workspace_path = _write_workspace_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        archon_path=fake_archon_root,
        projects=[("demo-project", fake_archon_project)],
    )

    queue_store = QueueStore(artifact_root / "archonlab.db")

    jobs = queue_store.enqueue_workspace_sessions(workspace_path)

    assert len(jobs) == 1
    job = jobs[0]
    assert job.kind is QueueJobKind.SESSION_QUANTUM
    assert job.status is QueueJobStatus.QUEUED
    assert job.workspace_id == "demo-workspace"
    assert job.session_id is not None
    assert job.preview is not None

    stored_jobs = queue_store.list_jobs()
    assert len(stored_jobs) == 1
    assert stored_jobs[0].session_id == job.session_id

    sessions = EventStore(artifact_root / "archonlab.db").list_sessions(
        workspace_id="demo-workspace"
    )
    assert len(sessions) == 1
    assert sessions[0].session_id == job.session_id
    assert sessions[0].status is SessionStatus.PENDING


def test_queue_store_enqueue_session_quantum_deduplicates_active_session_jobs(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    workspace_path = _write_workspace_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        archon_path=fake_archon_root,
        projects=[("demo-project", fake_archon_project)],
    )

    queue_store = QueueStore(artifact_root / "archonlab.db")
    first = queue_store.enqueue_workspace_sessions(workspace_path)[0]

    second = queue_store.enqueue_session_quantum(
        workspace_path,
        session_id=first.session_id or "",
    )

    assert second.id == first.id
    assert len(queue_store.list_jobs()) == 1


def test_batch_runner_processes_session_quantum_and_reenqueues_follow_up(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    workspace_path = _write_workspace_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        archon_path=fake_archon_root,
        projects=[("demo-project", fake_archon_project)],
    )
    queue_store = QueueStore(artifact_root / "archonlab.db")
    initial_job = queue_store.enqueue_workspace_sessions(workspace_path)[0]
    runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(artifact_root),
        artifact_root=artifact_root,
        slot_limit=1,
    )

    report = runner.run_worker(
        slot_index=1,
        max_jobs=1,
        poll_seconds=0.01,
        idle_timeout_seconds=0.1,
        executor_kinds=[ExecutorKind.DRY_RUN],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
    )

    assert report.processed_job_ids == [initial_job.id]
    session = EventStore(artifact_root / "archonlab.db").get_session(initial_job.session_id or "")
    assert session is not None
    assert session.completed_iterations == 1
    assert session.status is SessionStatus.PENDING
    assert session.owner_worker_id is None
    assert session.owner_job_id is None

    jobs = queue_store.list_jobs(limit=10)
    completed = next(job for job in jobs if job.id == initial_job.id)
    queued = next(
        job
        for job in jobs
        if job.session_id == initial_job.session_id and job.id != initial_job.id
    )
    assert completed.status is QueueJobStatus.COMPLETED
    assert queued.status is QueueJobStatus.QUEUED
    assert queued.kind is QueueJobKind.SESSION_QUANTUM
    assert queued.result_path is None


def test_batch_runner_session_quanta_are_fair_across_projects(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    project_b = _clone_project(fake_archon_project, tmp_path / "DemoProjectB")
    artifact_root = tmp_path / "artifacts"
    workspace_path = _write_workspace_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        archon_path=fake_archon_root,
        projects=[
            ("alpha", fake_archon_project),
            ("beta", project_b),
        ],
    )
    queue_store = QueueStore(artifact_root / "archonlab.db")
    queue_store.enqueue_workspace_sessions(workspace_path)
    runner = BatchRunner(
        queue_store=queue_store,
        control_service=ControlService(artifact_root),
        artifact_root=artifact_root,
        slot_limit=1,
    )

    report = runner.run_worker(
        slot_index=1,
        max_jobs=2,
        poll_seconds=0.01,
        idle_timeout_seconds=0.1,
        executor_kinds=[ExecutorKind.DRY_RUN],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
    )

    assert len(report.processed_job_ids) == 2
    sessions = EventStore(artifact_root / "archonlab.db").list_sessions(
        workspace_id="demo-workspace"
    )
    assert len(sessions) == 2
    assert {session.completed_iterations for session in sessions} == {1}
    assert all(session.status is SessionStatus.PENDING for session in sessions)

    queued_jobs = queue_store.list_jobs(status=QueueJobStatus.QUEUED, limit=10)
    assert len(queued_jobs) == 2
    assert {job.session_id for job in queued_jobs} == {session.session_id for session in sessions}


def test_queue_store_reaps_stale_worker_and_recovers_running_session_claim(
    tmp_path: Path,
    fake_archon_project: Path,
    fake_archon_root: Path,
) -> None:
    artifact_root = tmp_path / "artifacts"
    workspace_path = _write_workspace_config(
        tmp_path / "workspace.toml",
        artifact_root=artifact_root,
        archon_path=fake_archon_root,
        projects=[("demo-project", fake_archon_project)],
    )
    queue_store = QueueStore(artifact_root / "archonlab.db")
    job = queue_store.enqueue_workspace_sessions(workspace_path)[0]
    worker = queue_store.register_worker(slot_index=1, worker_id="worker-stale")
    claimed = queue_store.claim_next_job(worker_id=worker.worker_id)
    assert claimed is not None
    queue_store.assign_job_to_worker(worker.worker_id, claimed.id)

    event_store = EventStore(artifact_root / "archonlab.db")
    event_store.claim_session(
        job.session_id or "",
        owner_worker_id=worker.worker_id,
        owner_job_id=job.id,
    )
    event_store.update_session(
        job.session_id or "",
        status=SessionStatus.RUNNING,
        expected_owner_worker_id=worker.worker_id,
        expected_owner_job_id=job.id,
    )

    stale_heartbeat = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
    queue_store._conn.execute(
        "UPDATE queue_workers SET heartbeat_at = ? WHERE worker_id = ?",
        (stale_heartbeat, worker.worker_id),
    )
    queue_store._conn.commit()

    reaped = queue_store.reap_stale_workers(stale_after_seconds=60)

    assert [lease.worker_id for lease in reaped] == [worker.worker_id]
    updated_session = event_store.get_session(job.session_id or "")
    assert updated_session is not None
    assert updated_session.status is SessionStatus.PENDING
    assert updated_session.owner_worker_id is None
    assert updated_session.owner_job_id is None
    assert updated_session.last_stop_reason == "recovered_from_stale_worker:worker-stale"

    updated_job = queue_store.get_job(job.id)
    assert updated_job is not None
    assert updated_job.status is QueueJobStatus.QUEUED
    assert updated_job.worker_id is None
