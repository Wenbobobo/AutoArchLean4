from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class WorkflowMode(StrEnum):
    FIXED_LOOP = "fixed_loop"
    ADAPTIVE_LOOP = "adaptive_loop"


class RunStatus(StrEnum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"


class BenchmarkRunStatus(StrEnum):
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


class ChecklistItem(BaseModel):
    label: str
    done: bool


class ProgressSnapshot(BaseModel):
    stage: str
    objectives: list[str] = Field(default_factory=list)
    checklist: list[ChecklistItem] = Field(default_factory=list)


class ProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    project_path: Path
    archon_path: Path
    backend: str = "archon"


class RunConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow: WorkflowMode = WorkflowMode.ADAPTIVE_LOOP
    stage_policy: str = "auto"
    max_iterations: int = 10
    max_parallel: int = 8
    review: bool = True
    dry_run: bool = True
    artifact_root: Path = Path("artifacts")


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project: ProjectConfig
    run: RunConfig


class AdapterAction(BaseModel):
    phase: str
    reason: str
    stage: str
    prompt_preview: str | None = None


class EventRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    kind: str
    project_id: str
    task_id: str | None = None
    worktree_id: str | None = None
    policy_version: str = "v0"
    payload: dict[str, Any] = Field(default_factory=dict)
    ts: datetime = Field(default_factory=lambda: datetime.now(UTC))


class RunSummary(BaseModel):
    run_id: str
    project_id: str
    workflow: WorkflowMode
    status: RunStatus
    stage: str
    dry_run: bool
    started_at: datetime
    finished_at: datetime | None = None
    artifact_dir: Path


class RunResult(BaseModel):
    run_id: str
    status: RunStatus
    action: AdapterAction
    artifact_dir: Path
    prompt_path: Path


class BenchmarkConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""
    artifact_root: Path


class BenchmarkProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    project_path: Path
    archon_path: Path
    budget_minutes: int = 30
    workflow: WorkflowMode = WorkflowMode.ADAPTIVE_LOOP
    max_iterations: int = 10
    tags: list[str] = Field(default_factory=list)


class BenchmarkManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    benchmark: BenchmarkConfig
    projects: list[BenchmarkProjectConfig]


class ProjectSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    project_path: Path
    archon_path: Path
    progress: ProgressSnapshot
    task_results: list[Path] = Field(default_factory=list)
    review_sessions: list[Path] = Field(default_factory=list)
    lean_file_count: int
    theorem_count: int
    sorry_count: int
    axiom_count: int
    ts: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ProjectScore(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    stage: str
    objective_count: int
    task_result_count: int
    review_session_count: int
    progress_ratio: float
    backlog_penalty: int
    proof_gap_penalty: int
    axiom_penalty: int
    score: float


class SnapshotDelta(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sorry_delta: int
    axiom_delta: int
    review_session_delta: int
    task_results_delta: int
    checklist_done_delta: int
    score_delta: float


class BenchmarkProjectResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    workflow: WorkflowMode
    budget_minutes: int
    run_id: str | None = None
    run_status: RunStatus
    snapshot: ProjectSnapshot
    score: ProjectScore
    delta: SnapshotDelta
    artifact_dir: Path | None = None
    error_message: str | None = None


class BenchmarkResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    benchmark: BenchmarkConfig
    manifest_path: Path
    run_id: str
    status: BenchmarkRunStatus
    dry_run: bool
    started_at: datetime
    finished_at: datetime
    artifact_dir: Path
    manifest_copy_path: Path
    summary_path: Path
    projects: list[BenchmarkProjectResult]
