from __future__ import annotations

import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

from archonlab.batch import BatchRunner
from archonlab.control import ControlService
from archonlab.events import EventStore
from archonlab.models import (
    ExecutorKind,
    ProjectConfig,
    ProjectSession,
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


def test_queue_store_plan_fleet_can_scope_to_target_session_ids(
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
    jobs = queue_store.enqueue_workspace_sessions(workspace_path)
    beta_job = next(job for job in jobs if job.project_id == "beta")

    scoped_plan = queue_store.plan_fleet(
        target_jobs_per_worker=1,
        allowed_session_ids=[beta_job.session_id or ""],
    )

    assert scoped_plan.active_jobs == 1
    assert scoped_plan.queued_jobs == 1
    assert len(scoped_plan.profiles) == 1
    assert scoped_plan.profiles[0].project_ids == ["beta"]


def test_batch_runner_run_worker_claims_only_target_session_ids(
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
    jobs = queue_store.enqueue_workspace_sessions(workspace_path)
    alpha_job = next(job for job in jobs if job.project_id == "alpha")
    beta_job = next(job for job in jobs if job.project_id == "beta")
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
        allowed_session_ids=[beta_job.session_id or ""],
        executor_kinds=[ExecutorKind.DRY_RUN],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
    )

    assert report.processed_job_ids == [beta_job.id]
    alpha_current = queue_store.get_job(alpha_job.id)
    assert alpha_current is not None
    assert alpha_current.status is QueueJobStatus.QUEUED

    event_store = EventStore(artifact_root / "archonlab.db")
    alpha_session = event_store.get_session(alpha_job.session_id or "")
    beta_session = event_store.get_session(beta_job.session_id or "")
    assert alpha_session is not None
    assert beta_session is not None
    assert alpha_session.completed_iterations == 0
    assert beta_session.completed_iterations == 1


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


def test_queue_store_resume_workspace_sessions_resumes_paused_and_failed_sessions(
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
    event_store = EventStore(artifact_root / "archonlab.db")
    event_store.register_session(
        ProjectSession(
            session_id="session-alpha-1",
            workspace_id="demo-workspace",
            project_id="alpha",
            status=SessionStatus.PAUSED,
            max_iterations=1,
            completed_iterations=1,
            last_stop_reason="max_iterations_reached",
        )
    )
    event_store.register_session(
        ProjectSession(
            session_id="session-beta-1",
            workspace_id="demo-workspace",
            project_id="beta",
            status=SessionStatus.FAILED,
            max_iterations=2,
            completed_iterations=1,
            error_message="executor failed",
            last_stop_reason="run_failed",
            consecutive_failures=2,
            max_consecutive_failures=3,
            failure_cooldown_seconds=60,
            last_failure_at=datetime.now(UTC) - timedelta(seconds=120),
            cooldown_until=datetime.now(UTC) + timedelta(seconds=60),
        )
    )
    queue_store = QueueStore(artifact_root / "archonlab.db")

    result = queue_store.resume_workspace_sessions(
        workspace_path,
        max_iterations=3,
        resume_reason="workspace_batch_resume",
    )

    assert [session.project_id for session, _ in result.resumed] == ["alpha", "beta"]
    assert not result.skipped

    resumed_alpha = event_store.get_session("session-alpha-1")
    resumed_beta = event_store.get_session("session-beta-1")
    assert resumed_alpha is not None
    assert resumed_beta is not None
    assert resumed_alpha.status is SessionStatus.PENDING
    assert resumed_beta.status is SessionStatus.PENDING
    assert resumed_alpha.max_iterations == 3
    assert resumed_beta.max_iterations == 3
    assert resumed_alpha.last_resume_reason == "workspace_batch_resume"
    assert resumed_beta.last_resume_reason == "workspace_batch_resume"
    assert resumed_alpha.last_stop_reason is None
    assert resumed_beta.last_stop_reason is None
    assert resumed_beta.error_message is None
    assert resumed_beta.consecutive_failures == 0
    assert resumed_beta.cooldown_until is None

    jobs = queue_store.list_jobs(limit=10)
    assert len(jobs) == 2
    assert {job.session_id for job in jobs} == {"session-alpha-1", "session-beta-1"}


def test_queue_store_enqueue_workspace_sessions_resumes_failed_session_instead_of_restarting(
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
    event_store = EventStore(artifact_root / "archonlab.db")
    event_store.register_session(
        ProjectSession(
            session_id="session-demo-project-1",
            workspace_id="demo-workspace",
            project_id="demo-project",
            status=SessionStatus.FAILED,
            max_iterations=3,
            completed_iterations=1,
            error_message="executor failed",
            last_stop_reason="run_failed",
            consecutive_failures=1,
            max_consecutive_failures=3,
            failure_cooldown_seconds=60,
            last_failure_at=datetime.now(UTC) - timedelta(seconds=180),
            cooldown_until=datetime.now(UTC) - timedelta(seconds=120),
        )
    )
    queue_store = QueueStore(artifact_root / "archonlab.db")

    jobs = queue_store.enqueue_workspace_sessions(workspace_path)

    assert len(jobs) == 1
    assert jobs[0].session_id == "session-demo-project-1"
    updated = event_store.get_session("session-demo-project-1")
    assert updated is not None
    assert updated.status is SessionStatus.PENDING
    assert updated.error_message is None
    assert updated.consecutive_failures == 1
    assert updated.cooldown_until is None
    assert updated.last_resume_reason == "workspace_enqueue_resume"
    sessions = event_store.list_sessions(workspace_id="demo-workspace")
    assert len(sessions) == 1


def test_queue_store_enqueue_workspace_sessions_skips_failed_session_during_cooldown(
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
    event_store = EventStore(artifact_root / "archonlab.db")
    event_store.register_session(
        ProjectSession(
            session_id="session-demo-project-cooling",
            workspace_id="demo-workspace",
            project_id="demo-project",
            status=SessionStatus.FAILED,
            max_iterations=3,
            completed_iterations=1,
            error_message="executor failed",
            last_stop_reason="run_failed",
            consecutive_failures=1,
            max_consecutive_failures=3,
            failure_cooldown_seconds=60,
            last_failure_at=datetime.now(UTC) - timedelta(seconds=5),
            cooldown_until=datetime.now(UTC) + timedelta(seconds=60),
        )
    )
    queue_store = QueueStore(artifact_root / "archonlab.db")

    result = queue_store.enqueue_workspace_sessions_detailed(workspace_path)

    assert result.jobs == []
    assert [(item.session_id, item.reason) for item in result.skipped] == [
        ("session-demo-project-cooling", "failure_cooldown_active")
    ]
    updated = event_store.get_session("session-demo-project-cooling")
    assert updated is not None
    assert updated.status is SessionStatus.FAILED
    assert updated.consecutive_failures == 1
    assert queue_store.list_jobs(limit=10) == []


def test_queue_store_enqueue_workspace_sessions_skips_failed_session_after_retry_budget_exhausted(
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
    event_store = EventStore(artifact_root / "archonlab.db")
    event_store.register_session(
        ProjectSession(
            session_id="session-demo-project-exhausted",
            workspace_id="demo-workspace",
            project_id="demo-project",
            status=SessionStatus.FAILED,
            max_iterations=3,
            completed_iterations=1,
            error_message="executor failed",
            last_stop_reason="run_failed",
            consecutive_failures=3,
            max_consecutive_failures=3,
            failure_cooldown_seconds=60,
            last_failure_at=datetime.now(UTC) - timedelta(seconds=180),
            cooldown_until=datetime.now(UTC) - timedelta(seconds=120),
        )
    )
    queue_store = QueueStore(artifact_root / "archonlab.db")

    result = queue_store.enqueue_workspace_sessions_detailed(workspace_path)

    assert result.jobs == []
    assert [(item.session_id, item.reason) for item in result.skipped] == [
        ("session-demo-project-exhausted", "failure_budget_exhausted")
    ]
    updated = event_store.get_session("session-demo-project-exhausted")
    assert updated is not None
    assert updated.status is SessionStatus.FAILED
    assert updated.consecutive_failures == 3
    assert queue_store.list_jobs(limit=10) == []


def test_queue_store_enqueue_workspace_sessions_skips_control_paused_session(
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
    event_store = EventStore(artifact_root / "archonlab.db")
    event_store.register_session(
        ProjectSession(
            session_id="session-demo-project-control",
            workspace_id="demo-workspace",
            project_id="demo-project",
            status=SessionStatus.PAUSED,
            max_iterations=2,
            completed_iterations=0,
            last_stop_reason="stop:control_paused",
        )
    )
    queue_store = QueueStore(artifact_root / "archonlab.db")

    jobs = queue_store.enqueue_workspace_sessions(workspace_path)

    assert jobs == []
    updated = event_store.get_session("session-demo-project-control")
    assert updated is not None
    assert updated.status is SessionStatus.PAUSED
    assert updated.last_stop_reason == "stop:control_paused"
    assert queue_store.list_jobs(limit=10) == []


def test_batch_runner_control_paused_session_does_not_consume_budget(
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
    ControlService(artifact_root).pause(
        ProjectConfig(
            name="demo-project",
            project_path=fake_archon_project,
            archon_path=fake_archon_root,
        ),
        reason="operator_pause",
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

    assert report.processed_job_ids == []
    assert report.paused_job_ids == [initial_job.id]
    session = EventStore(artifact_root / "archonlab.db").get_session(initial_job.session_id or "")
    assert session is not None
    assert session.completed_iterations == 0
    assert session.status is SessionStatus.PAUSED
    assert session.last_stop_reason == "stop:control_paused"
    jobs = queue_store.list_jobs(limit=10)
    updated_job = next(job for job in jobs if job.id == initial_job.id)
    assert updated_job.status is QueueJobStatus.PAUSED
