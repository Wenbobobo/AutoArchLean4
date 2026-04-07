from __future__ import annotations

import concurrent.futures
import json
import threading
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
    ProjectConfig,
    QueueBenchmarkPayload,
    QueueJob,
    QueueJobStatus,
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
    ) -> None:
        self.queue_store = queue_store
        self.control_service = control_service
        self.artifact_root = artifact_root.resolve()
        self.benchmark_runner_cls = benchmark_runner_cls
        self.slot_limit = max(1, slot_limit)

    def run_pending(self, *, max_jobs: int | None = None) -> BatchRunReport:
        report = BatchRunReport()
        report_lock = threading.Lock()
        claim_lock = threading.Lock()
        claimed_jobs = 0

        def worker_loop() -> None:
            nonlocal claimed_jobs
            while True:
                with claim_lock:
                    if max_jobs is not None and claimed_jobs >= max_jobs:
                        return
                    job = self.queue_store.claim_next_job()
                    if job is None:
                        return
                    claimed_jobs += 1
                self._run_job(job, report, report_lock)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.slot_limit) as pool:
            futures = [pool.submit(worker_loop) for _ in range(self.slot_limit)]
            for future in futures:
                future.result()
        return report

    def _run_job(
        self,
        job: QueueJob,
        report: BatchRunReport,
        report_lock: threading.Lock,
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
                self.queue_store.finish_job(
                    job.id,
                    status=QueueJobStatus.PAUSED,
                    artifact_dir=artifact_dir,
                    result_path=status_path,
                    pause_reason=paused_reason,
                )
                with report_lock:
                    report.paused_job_ids.append(job.id)
                return

            if project_payload is not None:
                result_status, summary_path = self._run_project_job(
                    artifact_dir=artifact_dir,
                    manifest=manifest,
                    payload=project_payload,
                )
            else:
                result_status, summary_path = self._run_manifest_job(
                    artifact_dir=artifact_dir,
                    manifest_path=manifest_path,
                    job=job,
                )

            self.queue_store.finish_job(
                job.id,
                status=result_status,
                artifact_dir=artifact_dir,
                result_path=summary_path,
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
            self.queue_store.finish_job(
                job.id,
                status=QueueJobStatus.FAILED,
                artifact_dir=artifact_dir,
                result_path=error_path,
                error_message=str(exc),
            )
            result_status = QueueJobStatus.FAILED
        with report_lock:
            if result_status is QueueJobStatus.PAUSED:
                report.paused_job_ids.append(job.id)
            else:
                report.processed_job_ids.append(job.id)

    def _run_project_job(
        self,
        *,
        artifact_dir: Path,
        manifest: BenchmarkManifest,
        payload: QueueBenchmarkPayload,
    ) -> tuple[QueueJobStatus, Path]:
        result = run_benchmark_project(
            payload.project,
            artifact_root=self.artifact_root,
            dry_run=payload.dry_run,
            worktree_root=(
                self.artifact_root / "queue-worktrees"
                if payload.use_worktrees
                else None
            ),
            cleanup_worktrees=payload.cleanup_worktrees,
            executor=manifest.executor,
            provider=manifest.provider,
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
