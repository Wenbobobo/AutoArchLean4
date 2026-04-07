from __future__ import annotations

from pathlib import Path

from archonlab.models import (
    ActionPhase,
    ExecutorKind,
    ProviderKind,
    QueueJobPreview,
    SupervisorAction,
    SupervisorReason,
)
from archonlab.queue import QueueJobStatus, QueueStore


def test_queue_store_enqueues_in_order_and_persists_updates(tmp_path: Path) -> None:
    store = QueueStore(tmp_path / "queue.db")

    first = store.enqueue(
        "benchmark",
        {"manifest_path": str(tmp_path / "benchmarks" / "first.toml")},
    )
    second = store.enqueue(
        "benchmark",
        {"manifest_path": str(tmp_path / "benchmarks" / "second.toml")},
    )

    assert first.status is QueueJobStatus.QUEUED
    assert second.status is QueueJobStatus.QUEUED
    assert [job.id for job in store.list_jobs()] == [first.id, second.id]

    updated = store.update_status(first.id, QueueJobStatus.RUNNING)
    assert updated.status is QueueJobStatus.RUNNING

    store.update_status(first.id, QueueJobStatus.COMPLETED)
    reopened = QueueStore(tmp_path / "queue.db")
    jobs = reopened.list_jobs()

    assert [job.id for job in jobs] == [first.id, second.id]
    assert jobs[0].status is QueueJobStatus.COMPLETED
    assert jobs[0].payload["manifest_path"].endswith("first.toml")
    assert jobs[1].status is QueueJobStatus.QUEUED


def test_queue_store_marks_paused_and_canceled_jobs(tmp_path: Path) -> None:
    store = QueueStore(tmp_path / "queue.db")

    paused = store.enqueue("benchmark", {"manifest_path": "paused.toml"})
    canceled = store.enqueue("benchmark", {"manifest_path": "canceled.toml"})

    store.pause(paused.id, reason="manual_hold")
    store.cancel(canceled.id, reason="obsolete_manifest")

    reopened = QueueStore(tmp_path / "queue.db")
    jobs = {job.id: job for job in reopened.list_jobs()}

    assert jobs[paused.id].status is QueueJobStatus.PAUSED
    assert jobs[paused.id].pause_reason == "manual_hold"
    assert jobs[canceled.id].status is QueueJobStatus.CANCELED
    assert jobs[canceled.id].cancel_reason == "obsolete_manifest"


def test_queue_store_requeues_terminal_job_and_clears_terminal_metadata(
    tmp_path: Path,
) -> None:
    store = QueueStore(tmp_path / "queue.db")
    job = store.enqueue("benchmark", {"manifest_path": "failed.toml"}, priority=7)
    failed = store.finish_job(
        job.id,
        status=QueueJobStatus.FAILED,
        artifact_dir=tmp_path / "artifacts" / job.id,
        result_path=tmp_path / "artifacts" / job.id / "error.json",
        error_message="executor_crashed",
        worker_id="worker-1",
    )

    requeued = store.requeue(failed.id)

    assert requeued.status is QueueJobStatus.QUEUED
    assert requeued.priority == 7
    assert requeued.artifact_dir is None
    assert requeued.result_path is None
    assert requeued.error_message is None
    assert requeued.pause_reason is None
    assert requeued.cancel_reason is None
    assert requeued.worker_id is None


def test_queue_store_builds_fleet_plan_from_active_jobs_and_dedicated_workers(
    tmp_path: Path,
) -> None:
    store = QueueStore(tmp_path / "queue.db")

    cheap_job = store.enqueue(
        "benchmark",
        {"manifest_path": "cheap-a.toml"},
        project_id="demo-a",
        priority=6,
        required_executor_kinds=[ExecutorKind.DRY_RUN],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        required_models=["gpt-5.4-mini"],
        required_cost_tiers=["cheap"],
        required_endpoint_classes=["lab"],
        preview=QueueJobPreview(
            phase=ActionPhase.PLAN,
            reason="bootstrap_first_iteration",
            stage="plan",
            supervisor_action=SupervisorAction.CONTINUE,
            supervisor_reason=SupervisorReason.HEALTHY,
            theorem_name="alpha",
            final_priority=6,
            executor_kind=ExecutorKind.DRY_RUN,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4-mini",
            cost_tier="cheap",
            endpoint_class="lab",
        ),
    )
    cheap_running = store.enqueue(
        "benchmark",
        {"manifest_path": "cheap-b.toml"},
        project_id="demo-b",
        priority=8,
        required_executor_kinds=[ExecutorKind.DRY_RUN],
        required_provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        required_models=["gpt-5.4-mini"],
        required_cost_tiers=["cheap"],
        required_endpoint_classes=["lab"],
        preview=QueueJobPreview(
            phase=ActionPhase.PLAN,
            reason="task_graph_focus",
            stage="plan",
            supervisor_action=SupervisorAction.CONTINUE,
            supervisor_reason=SupervisorReason.HEALTHY,
            theorem_name="beta",
            final_priority=8,
            executor_kind=ExecutorKind.DRY_RUN,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4-mini",
            cost_tier="cheap",
            endpoint_class="lab",
        ),
    )
    premium_job = store.enqueue(
        "benchmark",
        {"manifest_path": "premium.toml"},
        project_id="demo-c",
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
            theorem_name="gamma",
            final_priority=10,
            executor_kind=ExecutorKind.DRY_RUN,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4",
            cost_tier="premium",
            endpoint_class="lab",
        ),
    )
    store.update_status(cheap_running.id, QueueJobStatus.RUNNING)

    store.register_worker(
        slot_index=1,
        worker_id="worker-cheap",
        executor_kinds=[ExecutorKind.DRY_RUN],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        models=["gpt-5.4-mini"],
        cost_tiers=["cheap"],
        endpoint_classes=["lab"],
    )
    store.register_worker(slot_index=2, worker_id="worker-generic")

    plan = store.plan_fleet(target_jobs_per_worker=2, stale_after_seconds=60.0)

    assert cheap_job.id != premium_job.id
    assert plan.active_jobs == 3
    assert plan.queued_jobs == 2
    assert plan.running_jobs == 1
    assert plan.total_profiles == 2
    assert plan.active_workers == 2
    assert plan.dedicated_workers == 1
    assert plan.generic_workers == 1
    assert plan.recommended_total_workers == 2
    assert plan.recommended_additional_workers == 0

    cheap_profile = next(
        profile for profile in plan.profiles if profile.required_models == ["gpt-5.4-mini"]
    )
    assert cheap_profile.queued_jobs == 1
    assert cheap_profile.running_jobs == 1
    assert cheap_profile.active_jobs == 2
    assert cheap_profile.dedicated_workers == 1
    assert cheap_profile.recommended_total_workers == 1
    assert cheap_profile.recommended_additional_workers == 0
    assert cheap_profile.dominant_phase is ActionPhase.PLAN
    assert cheap_profile.project_ids == ["demo-a", "demo-b"]
    assert cheap_profile.focus_examples == ["alpha", "beta"]

    premium_profile = next(
        profile for profile in plan.profiles if profile.required_models == ["gpt-5.4"]
    )
    assert premium_profile.queued_jobs == 1
    assert premium_profile.running_jobs == 0
    assert premium_profile.active_jobs == 1
    assert premium_profile.dedicated_workers == 0
    assert premium_profile.recommended_total_workers == 1
    assert premium_profile.recommended_additional_workers == 0
    assert premium_profile.dominant_phase is ActionPhase.PROVER
    assert premium_profile.project_ids == ["demo-c"]
