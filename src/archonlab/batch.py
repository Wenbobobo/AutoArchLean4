from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol, cast

from .benchmark import BenchmarkRunService, load_benchmark_manifest
from .control import ControlService
from .models import BatchRunReport, BenchmarkManifest, ProjectConfig, QueueJob, QueueJobStatus
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
        self.slot_limit = slot_limit

    def run_pending(self, *, max_jobs: int | None = None) -> BatchRunReport:
        del self.slot_limit
        report = BatchRunReport()
        while max_jobs is None or len(report.processed_job_ids) < max_jobs:
            job = self.queue_store.claim_next_job()
            if job is None:
                break
            self._run_job(job, report)
        return report

    def _run_job(self, job: QueueJob, report: BatchRunReport) -> None:
        manifest_path = Path(str(job.payload["manifest_path"])).resolve()
        manifest = load_benchmark_manifest(manifest_path)
        artifact_dir = self.artifact_root / "jobs" / job.id
        artifact_dir.mkdir(parents=True, exist_ok=True)

        paused_reason = self._paused_reason(manifest)
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
            self.queue_store.pause(
                job.id,
                reason=paused_reason,
            )
            self.queue_store.finish_job(
                job.id,
                status=QueueJobStatus.PAUSED,
                artifact_dir=artifact_dir,
                result_path=status_path,
                pause_reason=paused_reason,
            )
            report.paused_job_ids.append(job.id)
            return

        runner = cast(BenchmarkRunnerProtocol, self.benchmark_runner_cls(manifest_path))
        result = runner.run(
            dry_run=bool(job.payload.get("dry_run", True)),
            use_worktrees=bool(job.payload.get("use_worktrees", False)),
        )
        summary_path = artifact_dir / "summary.json"
        source_summary_path = getattr(result, "summary_path", None)
        if source_summary_path is not None:
            shutil.copy2(Path(source_summary_path), summary_path)
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
        self.queue_store.finish_job(
            job.id,
            status=final_status,
            artifact_dir=artifact_dir,
            result_path=summary_path,
        )
        report.processed_job_ids.append(job.id)

    def _paused_reason(self, manifest: BenchmarkManifest) -> str | None:
        for project in manifest.projects:
            state = self.control_service.read(
                ProjectConfig(
                    name=project.id,
                    project_path=project.project_path,
                    archon_path=project.archon_path,
                )
            )
            if state.paused:
                return state.pause_reason or "project_paused"
        return None
