from __future__ import annotations

from pathlib import Path

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
