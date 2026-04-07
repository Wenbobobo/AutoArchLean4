from __future__ import annotations

import json
import re
import tomllib
import uuid
from datetime import UTC, datetime
from pathlib import Path

from .adapter import ArchonAdapter
from .models import (
    AppConfig,
    BenchmarkConfig,
    BenchmarkManifest,
    BenchmarkProjectConfig,
    BenchmarkProjectResult,
    BenchmarkResult,
    BenchmarkRunStatus,
    ProjectConfig,
    ProjectScore,
    ProjectSnapshot,
    RunConfig,
    RunStatus,
    SnapshotDelta,
)
from .services import RunService

THEOREM_PATTERN = re.compile(r"\b(?:theorem|lemma|example)\b")
SORRY_PATTERN = re.compile(r"\bsorry\b")
AXIOM_PATTERN = re.compile(r"\baxiom\b")


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


def load_benchmark_manifest(manifest_path: Path) -> BenchmarkManifest:
    raw = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    base_dir = manifest_path.parent.resolve()
    benchmark_raw = raw["benchmark"]
    projects_raw = raw["projects"]

    projects = [
        BenchmarkProjectConfig(
            id=project_raw["id"],
            project_path=_resolve_path(base_dir, project_raw["path"]),
            archon_path=_resolve_path(base_dir, project_raw["archon_path"]),
            budget_minutes=project_raw.get("budget_minutes", 30),
            workflow=project_raw.get("workflow", "adaptive_loop"),
            max_iterations=project_raw.get("max_iterations", 10),
            tags=project_raw.get("tags", []),
        )
        for project_raw in projects_raw
    ]
    if not projects:
        raise ValueError("Benchmark manifest must define at least one [[projects]] entry.")

    return BenchmarkManifest(
        benchmark=BenchmarkConfig(
            name=benchmark_raw["name"],
            description=benchmark_raw.get("description", ""),
            artifact_root=_resolve_path(
                base_dir, benchmark_raw.get("artifact_root", "artifacts/benchmarks")
            ),
        ),
        projects=projects,
    )


def collect_project_snapshot(*, project_path: Path, archon_path: Path) -> ProjectSnapshot:
    resolved_project_path = project_path.resolve()
    resolved_archon_path = archon_path.resolve()
    project = ProjectConfig(
        name=resolved_project_path.name,
        project_path=resolved_project_path,
        archon_path=resolved_archon_path,
    )
    adapter = ArchonAdapter(project)
    adapter.ensure_valid()
    progress = adapter.read_progress()
    progress = progress.model_copy(
        update={"objectives": [objective.replace("—", "-") for objective in progress.objectives]}
    )

    sorry_count = 0
    theorem_count = 0
    axiom_count = 0
    lean_file_count = 0
    for lean_file in sorted(project.project_path.rglob("*.lean")):
        content = lean_file.read_text(encoding="utf-8")
        lean_file_count += 1
        theorem_count += len(THEOREM_PATTERN.findall(content))
        sorry_count += len(SORRY_PATTERN.findall(content))
        axiom_count += len(AXIOM_PATTERN.findall(content))

    return ProjectSnapshot(
        project_id=project.name,
        project_path=resolved_project_path,
        archon_path=resolved_archon_path,
        progress=progress,
        task_results=[path.resolve() for path in adapter.list_task_results()],
        review_sessions=[path.resolve() for path in adapter.list_review_sessions()],
        lean_file_count=lean_file_count,
        theorem_count=theorem_count,
        sorry_count=sorry_count,
        axiom_count=axiom_count,
    )


def score_project_snapshot(snapshot: ProjectSnapshot) -> ProjectScore:
    checklist_done = sum(1 for item in snapshot.progress.checklist if item.done)
    checklist_total = len(snapshot.progress.checklist)
    progress_ratio = (
        checklist_done / checklist_total if checklist_total else 0.0
    )
    task_result_count = len(snapshot.task_results)
    review_session_count = len(snapshot.review_sessions)
    backlog_penalty = task_result_count * 5
    proof_gap_penalty = snapshot.sorry_count * 3
    axiom_penalty = snapshot.axiom_count * 10
    score = max(
        0.0,
        round(
            100 * progress_ratio
            + review_session_count * 2
            - backlog_penalty
            - proof_gap_penalty
            - axiom_penalty,
            2,
        ),
    )
    return ProjectScore(
        project_id=snapshot.project_id,
        stage=snapshot.progress.stage,
        objective_count=len(snapshot.progress.objectives),
        task_result_count=task_result_count,
        review_session_count=review_session_count,
        progress_ratio=round(progress_ratio, 4),
        backlog_penalty=backlog_penalty,
        proof_gap_penalty=proof_gap_penalty,
        axiom_penalty=axiom_penalty,
        score=score,
    )


def diff_snapshots(before: ProjectSnapshot, after: ProjectSnapshot) -> SnapshotDelta:
    before_score = score_project_snapshot(before)
    after_score = score_project_snapshot(after)
    before_checklist_done = sum(1 for item in before.progress.checklist if item.done)
    after_checklist_done = sum(1 for item in after.progress.checklist if item.done)
    return SnapshotDelta(
        sorry_delta=after.sorry_count - before.sorry_count,
        axiom_delta=after.axiom_count - before.axiom_count,
        review_session_delta=len(after.review_sessions) - len(before.review_sessions),
        task_results_delta=len(after.task_results) - len(before.task_results),
        checklist_done_delta=after_checklist_done - before_checklist_done,
        score_delta=round(after_score.score - before_score.score, 2),
    )


class BenchmarkRunService:
    def __init__(self, manifest_path: Path) -> None:
        self.manifest_path = manifest_path.resolve()
        self.manifest = load_benchmark_manifest(self.manifest_path)

    def run(self, *, dry_run: bool = True) -> BenchmarkResult:
        started_at = datetime.now(UTC)
        run_id = self._new_run_id()
        artifact_dir = self.manifest.benchmark.artifact_root / "runs" / run_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        project_run_root = artifact_dir / "project-runs"
        project_run_root.mkdir(parents=True, exist_ok=True)

        manifest_copy_path = artifact_dir / "manifest.toml"
        manifest_copy_path.write_text(
            self.manifest_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (artifact_dir / "manifest.resolved.json").write_text(
            json.dumps(self.manifest.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        project_results: list[BenchmarkProjectResult] = []
        for project in self.manifest.projects:
            project_results.append(
                self._run_project(
                    project,
                    artifact_root=project_run_root,
                    dry_run=dry_run,
                )
            )

        finished_at = datetime.now(UTC)
        status = self._benchmark_status(project_results)
        summary_path = artifact_dir / "summary.json"
        result = BenchmarkResult(
            benchmark=self.manifest.benchmark,
            manifest_path=self.manifest_path,
            run_id=run_id,
            status=status,
            dry_run=dry_run,
            started_at=started_at,
            finished_at=finished_at,
            artifact_dir=artifact_dir,
            manifest_copy_path=manifest_copy_path,
            summary_path=summary_path,
            projects=project_results,
        )
        summary_path.write_text(
            json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

    def _run_project(
        self,
        benchmark_project: BenchmarkProjectConfig,
        *,
        artifact_root: Path,
        dry_run: bool,
    ) -> BenchmarkProjectResult:
        project_config = ProjectConfig(
            name=benchmark_project.id,
            project_path=benchmark_project.project_path,
            archon_path=benchmark_project.archon_path,
        )
        snapshot = collect_project_snapshot(
            project_path=benchmark_project.project_path,
            archon_path=benchmark_project.archon_path,
        )
        run_status = RunStatus.FAILED
        run_id: str | None = None
        run_artifact_dir: Path | None = None
        error_message: str | None = None

        try:
            service = RunService(
                AppConfig(
                    project=project_config,
                    run=RunConfig(
                        workflow=benchmark_project.workflow,
                        max_iterations=benchmark_project.max_iterations,
                        dry_run=dry_run,
                        artifact_root=artifact_root,
                    ),
                )
            )
            run_result = service.start(dry_run=dry_run)
            run_status = run_result.status
            run_id = run_result.run_id
            run_artifact_dir = run_result.artifact_dir
        except Exception as exc:
            error_message = str(exc)

        final_snapshot = collect_project_snapshot(
            project_path=benchmark_project.project_path,
            archon_path=benchmark_project.archon_path,
        )
        return BenchmarkProjectResult(
            id=benchmark_project.id,
            workflow=benchmark_project.workflow,
            budget_minutes=benchmark_project.budget_minutes,
            run_id=run_id,
            run_status=run_status,
            snapshot=final_snapshot,
            score=score_project_snapshot(final_snapshot),
            delta=diff_snapshots(snapshot, final_snapshot),
            artifact_dir=run_artifact_dir,
            error_message=error_message,
        )

    @staticmethod
    def _benchmark_status(project_results: list[BenchmarkProjectResult]) -> BenchmarkRunStatus:
        statuses = {result.run_status for result in project_results}
        if statuses == {RunStatus.COMPLETED}:
            return BenchmarkRunStatus.COMPLETED
        if RunStatus.COMPLETED in statuses:
            return BenchmarkRunStatus.PARTIAL
        return BenchmarkRunStatus.FAILED

    @staticmethod
    def _new_run_id() -> str:
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"benchmark-{timestamp}-{uuid.uuid4().hex[:8]}"


BenchmarkService = BenchmarkRunService
score_snapshot = score_project_snapshot
