from __future__ import annotations

import concurrent.futures
import json
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol, cast

from pydantic import ValidationError

from .benchmark import (
    BenchmarkRunService,
    load_benchmark_manifest,
    run_benchmark_project,
)
from .control import ControlService
from .models import (
    BatchRunReport,
    BenchmarkManifest,
    BenchmarkProjectConfig,
    ExecutorKind,
    ProjectConfig,
    ProviderKind,
    QueueBenchmarkPayload,
    QueueFleetProfile,
    QueueJob,
    QueueJobStatus,
    QueueWorkerLease,
    WorkerStatus,
)
from .queue import QueueStore


class BenchmarkRunnerResult(Protocol):
    run_id: str
    status: object
    artifact_dir: Path
    summary_path: Path | None


class BenchmarkRunnerProtocol(Protocol):
    def __init__(self, manifest_path: Path) -> None: ...

    def run(
        self,
        *,
        dry_run: bool = True,
        use_worktrees: bool = False,
        worker_slots: int | None = None,
    ) -> BenchmarkRunnerResult: ...


class BatchRunner:
    def __init__(
        self,
        *,
        queue_store: QueueStore,
        control_service: ControlService,
        artifact_root: Path,
        benchmark_runner_cls: Callable[[Path], Any] = BenchmarkRunService,
        slot_limit: int = 1,
        heartbeat_interval_seconds: float = 1.0,
    ) -> None:
        self.queue_store = queue_store
        self.control_service = control_service
        self.artifact_root = artifact_root.resolve()
        self.benchmark_runner_cls = benchmark_runner_cls
        self.slot_limit = max(1, slot_limit)
        self.heartbeat_interval_seconds = heartbeat_interval_seconds

    def run_pending(self, *, max_jobs: int | None = None) -> BatchRunReport:
        report = BatchRunReport()
        report_lock = threading.Lock()
        claim_lock = threading.Lock()
        claimed_jobs = 0

        def worker_loop(slot_index: int) -> None:
            nonlocal claimed_jobs
            worker_id = f"worker-slot-{slot_index}-{uuid.uuid4().hex[:8]}"
            worker = self.queue_store.register_worker(
                slot_index=slot_index,
                worker_id=worker_id,
                thread_name=threading.current_thread().name,
                worktree_root_factory=lambda resolved_worker_id, resolved_slot_index: (
                    self._worker_worktree_root(
                        worker_id=resolved_worker_id,
                        slot_index=resolved_slot_index,
                    )
                ),
                executor_kinds=list(ExecutorKind),
                provider_kinds=list(ProviderKind),
            )
            with report_lock:
                report.worker_ids.append(worker.worker_id)
            while True:
                with claim_lock:
                    if max_jobs is not None and claimed_jobs >= max_jobs:
                        self.queue_store.stop_worker(worker.worker_id)
                        return
                    try:
                        job = self.queue_store.claim_next_job(worker_id=worker.worker_id)
                    except TypeError:
                        job = self.queue_store.claim_next_job()
                    if job is None:
                        self.queue_store.stop_worker(worker.worker_id)
                        return
                    claimed_jobs += 1
                self.queue_store.assign_job_to_worker(worker.worker_id, job.id)
                heartbeat_stop = threading.Event()
                heartbeat_thread = threading.Thread(
                    target=self._heartbeat_loop,
                    args=(worker.worker_id, job.id, heartbeat_stop),
                    daemon=True,
                    name=f"{worker.worker_id}-heartbeat",
                )
                heartbeat_thread.start()
                try:
                    self._run_job(
                        job,
                        report,
                        report_lock,
                        worker_id=worker.worker_id,
                    )
                finally:
                    heartbeat_stop.set()
                    heartbeat_thread.join(timeout=self.heartbeat_interval_seconds * 2)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.slot_limit) as pool:
            futures = [
                pool.submit(worker_loop, slot_index)
                for slot_index in range(1, self.slot_limit + 1)
            ]
            for future in futures:
                future.result()
        return report

    def run_worker(
        self,
        *,
        slot_index: int | None,
        max_jobs: int | None = None,
        poll_seconds: float = 2.0,
        idle_timeout_seconds: float = 30.0,
        worker_id: str | None = None,
        note: str | None = "external_worker",
        stale_after_seconds: float | None = None,
        executor_kinds: list[ExecutorKind] | None = None,
        provider_kinds: list[ProviderKind] | None = None,
        models: list[str] | None = None,
        cost_tiers: list[str] | None = None,
        endpoint_classes: list[str] | None = None,
    ) -> BatchRunReport:
        report = BatchRunReport()
        report_lock = threading.Lock()
        slot_label = f"slot-{slot_index}" if slot_index is not None else "auto"
        resolved_worker_id = worker_id or f"worker-{slot_label}-{uuid.uuid4().hex[:8]}"
        worker = self.queue_store.register_worker(
            slot_index=slot_index,
            worker_id=resolved_worker_id,
            thread_name=threading.current_thread().name,
            note=note,
            worktree_root_factory=lambda assigned_worker_id, assigned_slot_index: (
                self._worker_worktree_root(
                    worker_id=assigned_worker_id,
                    slot_index=assigned_slot_index,
                )
            ),
            stale_after_seconds=stale_after_seconds,
            executor_kinds=executor_kinds,
            provider_kinds=provider_kinds,
            models=models,
            cost_tiers=cost_tiers,
            endpoint_classes=endpoint_classes,
        )
        report.worker_ids.append(worker.worker_id)
        processed_count = 0
        idle_started = time.monotonic()
        try:
            while max_jobs is None or processed_count < max_jobs:
                self.queue_store.heartbeat_worker(
                    worker.worker_id,
                    status=WorkerStatus.IDLE,
                    note=note,
                )
                job = self.queue_store.claim_next_job(worker_id=worker.worker_id)
                if job is None:
                    if time.monotonic() - idle_started >= idle_timeout_seconds:
                        break
                    time.sleep(poll_seconds)
                    continue
                idle_started = time.monotonic()
                processed_count += 1
                self.queue_store.assign_job_to_worker(worker.worker_id, job.id)
                heartbeat_stop = threading.Event()
                heartbeat_thread = threading.Thread(
                    target=self._heartbeat_loop,
                    args=(worker.worker_id, job.id, heartbeat_stop),
                    daemon=True,
                    name=f"{worker.worker_id}-heartbeat",
                )
                heartbeat_thread.start()
                try:
                    self._run_job(
                        job,
                        report,
                        report_lock,
                        worker_id=worker.worker_id,
                    )
                finally:
                    heartbeat_stop.set()
                    heartbeat_thread.join(timeout=self.heartbeat_interval_seconds * 2)
        except Exception as exc:  # noqa: BLE001
            self.queue_store.stop_worker(worker.worker_id, failed=True, note=str(exc))
            raise
        self.queue_store.stop_worker(worker.worker_id, note=note)
        return report

    def run_fleet(
        self,
        *,
        worker_count: int | None = None,
        plan_driven: bool = False,
        target_jobs_per_worker: int = 2,
        max_jobs_per_worker: int | None = None,
        poll_seconds: float = 2.0,
        idle_timeout_seconds: float = 30.0,
        stale_after_seconds: float | None = None,
        executor_kinds: list[ExecutorKind] | None = None,
        provider_kinds: list[ProviderKind] | None = None,
        models: list[str] | None = None,
        cost_tiers: list[str] | None = None,
        endpoint_classes: list[str] | None = None,
    ) -> BatchRunReport:
        aggregate = BatchRunReport()
        aggregate_lock = threading.Lock()
        launch_specs = (
            self._planned_fleet_launch_specs(
                worker_count=worker_count,
                target_jobs_per_worker=target_jobs_per_worker,
                max_jobs_per_worker=max_jobs_per_worker,
                poll_seconds=poll_seconds,
                idle_timeout_seconds=idle_timeout_seconds,
                stale_after_seconds=stale_after_seconds,
            )
            if plan_driven
            else [
                {
                    "slot_index": None,
                    "max_jobs": max_jobs_per_worker,
                    "poll_seconds": poll_seconds,
                    "idle_timeout_seconds": idle_timeout_seconds,
                    "note": f"fleet_worker_{index}",
                    "stale_after_seconds": stale_after_seconds,
                    "executor_kinds": executor_kinds,
                    "provider_kinds": provider_kinds,
                    "models": models,
                    "cost_tiers": cost_tiers,
                    "endpoint_classes": endpoint_classes,
                }
                for index in range(1, max(1, worker_count or self.slot_limit) + 1)
            ]
        )
        if not launch_specs:
            return aggregate

        def fleet_worker(spec: dict[str, Any]) -> None:
            report = self.run_worker(**spec)
            self._merge_report(aggregate, report, aggregate_lock)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(launch_specs)) as pool:
            futures = [pool.submit(fleet_worker, spec) for spec in launch_specs]
            for future in futures:
                future.result()
        return aggregate

    def _run_job(
        self,
        job: QueueJob,
        report: BatchRunReport,
        report_lock: threading.Lock,
        worker_id: str,
    ) -> None:
        manifest_path = Path(str(job.payload["manifest_path"])).resolve()
        artifact_dir = self.artifact_root / "jobs" / job.id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        try:
            manifest = load_benchmark_manifest(manifest_path)
            project_payload = self._project_payload(job)
            projects = (
                [project_payload.project]
                if project_payload is not None
                else manifest.projects
            )
            paused_reason = self._paused_reason(projects)
            if paused_reason is not None:
                status_path = artifact_dir / "status.json"
                status_path.write_text(
                    json.dumps(
                        {"job_id": job.id, "reason": paused_reason},
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                paused_job = self.queue_store.finish_job(
                    job.id,
                    status=QueueJobStatus.PAUSED,
                    artifact_dir=artifact_dir,
                    result_path=status_path,
                    pause_reason=paused_reason,
                    worker_id=worker_id,
                )
                if not self._job_finish_applied(
                    paused_job,
                    expected_status=QueueJobStatus.PAUSED,
                    worker_id=worker_id,
                ):
                    return
                self.queue_store.release_job_from_worker(
                    worker_id,
                    job_id=job.id,
                    failed=False,
                )
                with report_lock:
                    report.paused_job_ids.append(job.id)
                return

            if project_payload is not None:
                result_status, summary_path = self._run_project_job(
                    artifact_dir=artifact_dir,
                    manifest=manifest,
                    payload=project_payload,
                    worker_id=worker_id,
                )
            else:
                result_status, summary_path = self._run_manifest_job(
                    artifact_dir=artifact_dir,
                    manifest_path=manifest_path,
                    job=job,
                )

            finished_job = self.queue_store.finish_job(
                job.id,
                status=result_status,
                artifact_dir=artifact_dir,
                result_path=summary_path,
                worker_id=worker_id,
            )
            if not self._job_finish_applied(
                finished_job,
                expected_status=result_status,
                worker_id=worker_id,
            ):
                return
            self.queue_store.release_job_from_worker(
                worker_id,
                job_id=job.id,
                failed=result_status is QueueJobStatus.FAILED,
            )
        except Exception as exc:  # noqa: BLE001
            error_path = artifact_dir / "error.json"
            error_path.write_text(
                json.dumps(
                    {"job_id": job.id, "error": str(exc)},
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            finished_job = self.queue_store.finish_job(
                job.id,
                status=QueueJobStatus.FAILED,
                artifact_dir=artifact_dir,
                result_path=error_path,
                error_message=str(exc),
                worker_id=worker_id,
            )
            if not self._job_finish_applied(
                finished_job,
                expected_status=QueueJobStatus.FAILED,
                worker_id=worker_id,
            ):
                return
            self.queue_store.release_job_from_worker(
                worker_id,
                job_id=job.id,
                failed=True,
            )
            result_status = QueueJobStatus.FAILED
        with report_lock:
            if result_status is QueueJobStatus.PAUSED:
                report.paused_job_ids.append(job.id)
            elif result_status is QueueJobStatus.FAILED:
                report.failed_job_ids.append(job.id)
            else:
                report.processed_job_ids.append(job.id)

    def list_worker_leases(self) -> list[QueueWorkerLease]:
        return self.queue_store.list_workers()

    @staticmethod
    def _merge_report(
        aggregate: BatchRunReport,
        report: BatchRunReport,
        aggregate_lock: threading.Lock,
    ) -> None:
        with aggregate_lock:
            aggregate.processed_job_ids.extend(report.processed_job_ids)
            aggregate.paused_job_ids.extend(report.paused_job_ids)
            aggregate.failed_job_ids.extend(report.failed_job_ids)
            aggregate.worker_ids.extend(report.worker_ids)

    def _planned_fleet_launch_specs(
        self,
        *,
        worker_count: int | None,
        target_jobs_per_worker: int,
        max_jobs_per_worker: int | None,
        poll_seconds: float,
        idle_timeout_seconds: float,
        stale_after_seconds: float | None,
    ) -> list[dict[str, Any]]:
        plan = self.queue_store.plan_fleet(
            target_jobs_per_worker=target_jobs_per_worker,
            stale_after_seconds=stale_after_seconds,
        )
        launch_specs: list[dict[str, Any]] = []
        for profile in plan.profiles:
            if (
                profile.recommended_additional_workers < 1
                or self._profile_is_generic(profile)
            ):
                continue
            for launch_index in range(1, profile.recommended_additional_workers + 1):
                launch_specs.append(
                    {
                        "slot_index": None,
                        "max_jobs": max_jobs_per_worker,
                        "poll_seconds": poll_seconds,
                        "idle_timeout_seconds": idle_timeout_seconds,
                        "note": f"planned_fleet:{profile.profile_id}:{launch_index}",
                        "stale_after_seconds": stale_after_seconds,
                        "executor_kinds": profile.required_executor_kinds,
                        "provider_kinds": profile.required_provider_kinds,
                        "models": profile.required_models,
                        "cost_tiers": profile.required_cost_tiers,
                        "endpoint_classes": profile.required_endpoint_classes,
                    }
                )
                if worker_count is not None and len(launch_specs) >= worker_count:
                    return launch_specs
        return launch_specs

    @staticmethod
    def _profile_is_generic(profile: QueueFleetProfile) -> bool:
        return not (
            profile.required_executor_kinds
            or profile.required_provider_kinds
            or profile.required_models
            or profile.required_cost_tiers
            or profile.required_endpoint_classes
        )

    @staticmethod
    def _job_finish_applied(
        job: QueueJob,
        *,
        expected_status: QueueJobStatus,
        worker_id: str,
    ) -> bool:
        return job.status is expected_status and job.worker_id == worker_id

    def _worker_worktree_root(self, *, worker_id: str | None, slot_index: int) -> Path:
        label = worker_id or f"slot-{slot_index}"
        return self.artifact_root / "queue-worktrees" / label

    def _heartbeat_loop(
        self,
        worker_id: str,
        job_id: str,
        stop_event: threading.Event,
    ) -> None:
        while not stop_event.wait(self.heartbeat_interval_seconds):
            self.queue_store.heartbeat_worker(
                worker_id,
                status=WorkerStatus.RUNNING,
                current_job_id=job_id,
            )

    def _run_project_job(
        self,
        *,
        artifact_dir: Path,
        manifest: BenchmarkManifest,
        payload: QueueBenchmarkPayload,
        worker_id: str,
    ) -> tuple[QueueJobStatus, Path]:
        worker = self.queue_store.get_worker(worker_id)
        worktree_root = worker.worktree_root if worker is not None else None
        result = run_benchmark_project(
            payload.project,
            artifact_root=self.artifact_root,
            dry_run=payload.dry_run,
            worktree_root=worktree_root if payload.use_worktrees else None,
            cleanup_worktrees=payload.cleanup_worktrees,
            executor=manifest.executor,
            provider=manifest.provider,
            execution_policy=manifest.execution_policy,
        )
        summary_path = artifact_dir / "summary.json"
        summary_path.write_text(
            json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        status = (
            QueueJobStatus.COMPLETED
            if result.run_status.value == "completed"
            else QueueJobStatus.FAILED
        )
        return status, summary_path

    def _run_manifest_job(
        self,
        *,
        artifact_dir: Path,
        manifest_path: Path,
        job: QueueJob,
    ) -> tuple[QueueJobStatus, Path]:
        runner = cast(BenchmarkRunnerProtocol, self.benchmark_runner_cls(manifest_path))
        dry_run = bool(job.payload.get("dry_run", True))
        use_worktrees = bool(job.payload.get("use_worktrees", False))
        try:
            result = runner.run(
                dry_run=dry_run,
                use_worktrees=use_worktrees,
                worker_slots=1,
            )
        except TypeError:
            result = runner.run(
                dry_run=dry_run,
                use_worktrees=use_worktrees,
            )
        summary_path = artifact_dir / "summary.json"
        source_summary_path = getattr(result, "summary_path", None)
        if source_summary_path is not None:
            summary_path.write_text(
                Path(source_summary_path).read_text(encoding="utf-8"),
                encoding="utf-8",
            )
        else:
            summary_path.write_text(
                json.dumps(
                    {
                        "run_id": result.run_id,
                        "status": getattr(result.status, "value", "unknown"),
                        "artifact_dir": str(result.artifact_dir),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        result_status = getattr(result.status, "value", "unknown")
        final_status = (
            QueueJobStatus.COMPLETED
            if result_status == "completed"
            else QueueJobStatus.FAILED
        )
        return final_status, summary_path

    def _paused_reason(self, projects: list[BenchmarkProjectConfig]) -> str | None:
        for project in projects:
            state = self.control_service.read(project_to_project_config(project))
            if state.paused:
                return state.pause_reason or "project_paused"
        return None

    @staticmethod
    def _project_payload(job: QueueJob) -> QueueBenchmarkPayload | None:
        try:
            return QueueBenchmarkPayload.model_validate(job.payload)
        except ValidationError:
            return None


def project_to_project_config(project: BenchmarkProjectConfig) -> ProjectConfig:
    return ProjectConfig(
        name=project.id,
        project_path=project.project_path,
        archon_path=project.archon_path,
    )
