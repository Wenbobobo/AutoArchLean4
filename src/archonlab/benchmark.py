from __future__ import annotations

import concurrent.futures
import json
import tomllib
import uuid
from datetime import UTC, datetime
from pathlib import Path

from .execution_policy import build_execution_policy
from .experiment_ledger import (
    build_experiment_ledger,
    build_failure_taxonomy,
    build_theorem_outcome_ledger,
)
from .lean_analyzer import build_lean_analyzer, collect_lean_analysis
from .models import (
    AppConfig,
    BenchmarkConfig,
    BenchmarkManifest,
    BenchmarkProjectConfig,
    BenchmarkProjectResult,
    BenchmarkResult,
    BenchmarkRunStatus,
    ExecutionPolicy,
    ExecutorConfig,
    LeanAnalyzerConfig,
    LeanAnalyzerKind,
    ProjectConfig,
    ProviderConfig,
    RunConfig,
    RunStatus,
)
from .project_state import collect_project_snapshot, diff_snapshots, score_project_snapshot
from .services import RunService
from .worktree import WorktreeManager


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path.resolve()
    return (base_dir / path).resolve()


def _get_command_list(raw: dict[str, object], key: str) -> list[str]:
    value = raw.get(key, [])
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _load_lean_analyzer_config(raw: object) -> LeanAnalyzerConfig:
    if not isinstance(raw, dict):
        return LeanAnalyzerConfig()
    return LeanAnalyzerConfig(
        kind=LeanAnalyzerKind(str(raw.get("kind", LeanAnalyzerKind.REGEX.value))),
        command=_get_command_list(raw, "command"),
        timeout_seconds=int(raw.get("timeout_seconds", 60)),
    )


def load_benchmark_manifest(manifest_path: Path) -> BenchmarkManifest:
    raw = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    base_dir = manifest_path.parent.resolve()
    benchmark_raw = raw["benchmark"]
    projects_raw = raw["projects"]
    executor_raw = raw.get("executor", {})
    provider_raw = raw.get("provider", {})
    phase_executor_raw = raw.get("phase_executor", {})
    phase_provider_raw = raw.get("phase_provider", {})
    task_matcher_raw = raw.get("task_matcher", {})
    task_executor_raw = raw.get("task_executor", {})
    task_provider_raw = raw.get("task_provider", {})
    lean_analyzer_raw = raw.get("lean_analyzer", {})

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

    executor = ExecutorConfig(
        kind=executor_raw.get("kind", "dry_run"),
        command=executor_raw.get("command", "codex"),
        profile=executor_raw.get("profile"),
        auto_approve=executor_raw.get("auto_approve", False),
        skip_git_repo_check=executor_raw.get("skip_git_repo_check", True),
        sandbox=executor_raw.get("sandbox"),
        color=executor_raw.get("color", "never"),
        extra_args=executor_raw.get("extra_args", []),
        timeout_seconds=executor_raw.get("timeout_seconds", 600),
    )
    provider = ProviderConfig(
        kind=provider_raw.get("kind", "openai_compatible"),
        model=provider_raw.get("model"),
        cost_tier=provider_raw.get("cost_tier"),
        endpoint_class=provider_raw.get("endpoint_class"),
        base_url=provider_raw.get("base_url"),
        api_key_env=provider_raw.get("api_key_env", "OPENAI_API_KEY"),
        endpoint_path=provider_raw.get(
            "endpoint_path",
            provider_raw.get("chat_completions_path", "/v1/responses"),
        ),
        headers=provider_raw.get("headers", {}),
    )

    return BenchmarkManifest(
        benchmark=BenchmarkConfig(
            name=benchmark_raw["name"],
            description=benchmark_raw.get("description", ""),
            artifact_root=_resolve_path(
                base_dir, benchmark_raw.get("artifact_root", "artifacts/benchmarks")
            ),
            worker_slots=benchmark_raw.get("worker_slots", 1),
        ),
        projects=projects,
        lean_analyzer=_load_lean_analyzer_config(lean_analyzer_raw),
        executor=executor,
        provider=provider,
        execution_policy=build_execution_policy(
            base_executor=executor,
            base_provider=provider,
            phase_executor_raw=phase_executor_raw if isinstance(phase_executor_raw, dict) else {},
            phase_provider_raw=phase_provider_raw if isinstance(phase_provider_raw, dict) else {},
            task_matcher_raw=task_matcher_raw if isinstance(task_matcher_raw, dict) else {},
            task_executor_raw=task_executor_raw if isinstance(task_executor_raw, dict) else {},
            task_provider_raw=task_provider_raw if isinstance(task_provider_raw, dict) else {},
        ),
    )

class BenchmarkRunService:
    def __init__(self, manifest_path: Path) -> None:
        self.manifest_path = manifest_path.resolve()
        self.manifest = load_benchmark_manifest(self.manifest_path)

    def run(
        self,
        *,
        dry_run: bool = True,
        use_worktrees: bool = False,
        cleanup_worktrees: bool = True,
        worker_slots: int | None = None,
    ) -> BenchmarkResult:
        started_at = datetime.now(UTC)
        run_id = self._new_run_id()
        artifact_dir = self.manifest.benchmark.artifact_root / "runs" / run_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        project_run_root = artifact_dir / "project-runs"
        project_run_root.mkdir(parents=True, exist_ok=True)
        worktree_root = artifact_dir / "worktrees"

        manifest_copy_path = artifact_dir / "manifest.toml"
        manifest_copy_path.write_text(
            self.manifest_path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (artifact_dir / "manifest.resolved.json").write_text(
            json.dumps(self.manifest.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        slot_count = max(1, worker_slots or self.manifest.benchmark.worker_slots)
        if slot_count == 1:
            project_results = [
                run_benchmark_project(
                    project,
                    artifact_root=project_run_root,
                    dry_run=dry_run,
                    worktree_root=worktree_root if use_worktrees else None,
                    cleanup_worktrees=cleanup_worktrees,
                    executor=self.manifest.executor,
                    provider=self.manifest.provider,
                    execution_policy=self.manifest.execution_policy,
                    lean_analyzer=self.manifest.lean_analyzer,
                )
                for project in self.manifest.projects
            ]
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=slot_count) as pool:
                futures = [
                    pool.submit(
                        run_benchmark_project,
                        project,
                        artifact_root=project_run_root,
                        dry_run=dry_run,
                        worktree_root=worktree_root if use_worktrees else None,
                        cleanup_worktrees=cleanup_worktrees,
                        executor=self.manifest.executor,
                        provider=self.manifest.provider,
                        execution_policy=self.manifest.execution_policy,
                        lean_analyzer=self.manifest.lean_analyzer,
                    )
                    for project in self.manifest.projects
                ]
                project_results = [future.result() for future in futures]

        finished_at = datetime.now(UTC)
        status = self._benchmark_status(project_results)
        ledger = build_experiment_ledger(
            benchmark_name=self.manifest.benchmark.name,
            benchmark_run_id=run_id,
            project_results=project_results,
        )
        ledger_path = artifact_dir / "experiment-ledger.json"
        ledger_path.write_text(
            json.dumps(ledger.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
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
            ledger_path=ledger_path,
            ledger_summary=ledger.summary,
            projects=project_results,
        )
        summary_path.write_text(
            json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return result

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


def run_benchmark_project(
    benchmark_project: BenchmarkProjectConfig,
    *,
    artifact_root: Path,
    dry_run: bool,
    worktree_root: Path | None,
    cleanup_worktrees: bool,
    executor: ExecutorConfig,
    provider: ProviderConfig,
    execution_policy: ExecutionPolicy,
    lean_analyzer: LeanAnalyzerConfig,
) -> BenchmarkProjectResult:
    analyzer = build_lean_analyzer(lean_analyzer)
    baseline_analysis = collect_lean_analysis(
        project_path=benchmark_project.project_path,
        archon_path=benchmark_project.archon_path,
        analyzer=analyzer,
    )
    baseline_snapshot = collect_project_snapshot(
        project_path=benchmark_project.project_path,
        archon_path=benchmark_project.archon_path,
        analyzer=analyzer,
        analysis=baseline_analysis,
    )
    effective_project_path = benchmark_project.project_path
    lease = None
    lease_path: Path | None = None
    worktree_path: Path | None = None
    manager = WorktreeManager(root=worktree_root) if worktree_root is not None else None
    run_status = RunStatus.FAILED
    run_id: str | None = None
    run_artifact_dir: Path | None = None
    error_message: str | None = None
    final_snapshot = baseline_snapshot
    final_analysis = baseline_analysis

    try:
        if manager is not None:
            lease = manager.create(
                benchmark_project.project_path,
                name=f"{benchmark_project.id}-{uuid.uuid4().hex[:8]}",
            )
            worktree_path = lease.worktree_path
            effective_project_path = lease.worktree_path
            lease_path = artifact_root / f"{lease.lease_id}.json"
            lease_path.write_text(
                json.dumps(lease.model_dump(mode="json"), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        project_config = ProjectConfig(
            name=benchmark_project.id,
            project_path=effective_project_path,
            archon_path=benchmark_project.archon_path,
        )
        service = RunService(
            AppConfig(
                project=project_config,
                run=RunConfig(
                    workflow=benchmark_project.workflow,
                    max_iterations=benchmark_project.max_iterations,
                    dry_run=dry_run,
                    artifact_root=artifact_root,
                ),
                lean_analyzer=lean_analyzer,
                executor=executor,
                provider=provider,
                execution_policy=execution_policy,
            )
        )
        run_result = service.start(dry_run=dry_run)
        run_status = run_result.status
        run_id = run_result.run_id
        run_artifact_dir = run_result.artifact_dir
        final_analysis = collect_lean_analysis(
            project_path=effective_project_path,
            archon_path=benchmark_project.archon_path,
            analyzer=analyzer,
        )
        final_snapshot = collect_project_snapshot(
            project_path=effective_project_path,
            archon_path=benchmark_project.archon_path,
            analyzer=analyzer,
            analysis=final_analysis,
        )
    except Exception as exc:
        error_message = str(exc)
    finally:
        if lease is not None and manager is not None and cleanup_worktrees:
            manager.release(lease, force=True)
    theorem_outcomes = build_theorem_outcome_ledger(
        baseline_analysis,
        final_analysis,
    )
    failure_taxonomy = build_failure_taxonomy(
        theorem_outcomes,
        error_message=error_message,
    )
    return BenchmarkProjectResult(
        id=benchmark_project.id,
        workflow=benchmark_project.workflow,
        budget_minutes=benchmark_project.budget_minutes,
        run_id=run_id,
        run_status=run_status,
        snapshot=final_snapshot,
        score=score_project_snapshot(final_snapshot),
        delta=diff_snapshots(baseline_snapshot, final_snapshot),
        artifact_dir=run_artifact_dir,
        worktree_path=worktree_path,
        lease_path=lease_path,
        error_message=error_message,
        theorem_outcomes=theorem_outcomes,
        failure_taxonomy=failure_taxonomy,
    )
