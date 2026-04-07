from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class WorkflowMode(StrEnum):
    FIXED_LOOP = "fixed_loop"
    ADAPTIVE_LOOP = "adaptive_loop"


class ActionPhase(StrEnum):
    PLAN = "plan"
    PROVER = "prover"
    REVIEW = "review"
    STOP = "stop"


class TaskSource(StrEnum):
    OBJECTIVE = "objective"
    LEAN_DECLARATION = "lean_declaration"


class TaskStatus(StrEnum):
    PENDING = "pending"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    UNKNOWN = "unknown"


class SupervisorAction(StrEnum):
    CONTINUE = "continue"
    REROUTE_PLAN = "reroute_plan"
    INVESTIGATE_INFRA = "investigate_infra"
    REQUEST_HINT = "request_hint"


class SupervisorReason(StrEnum):
    HEALTHY = "healthy"
    PENDING_RESULTS_BACKLOG = "pending_results_backlog"
    REPEATED_NO_PROGRESS = "repeated_no_progress"
    HIGH_BLOCKED_RATIO = "high_blocked_ratio"
    HIGH_SORRY_LOAD = "high_sorry_load"


class RunStatus(StrEnum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"


class BenchmarkRunStatus(StrEnum):
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


class QueueJobKind(StrEnum):
    BENCHMARK_PROJECT = "benchmark_project"


class QueueJobStatus(StrEnum):
    QUEUED = "queued"
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELED = "canceled"


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
    workflow_spec: Path | None = None
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
    phase: str | ActionPhase
    reason: str
    stage: str
    prompt_preview: str | None = None
    task_id: str | None = None
    task_title: str | None = None
    file_path: Path | None = None
    supervisor_action: SupervisorAction | None = None
    supervisor_reason: SupervisorReason | None = None


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
    task_graph_path: Path | None = None
    supervisor_path: Path | None = None


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
    worktree_path: Path | None = None
    lease_path: Path | None = None
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


class TaskNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    title: str
    status: TaskStatus
    sources: list[TaskSource] = Field(default_factory=list)
    file_path: Path | None = None
    theorem_name: str | None = None
    priority: int = 0
    blockers: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class TaskEdge(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: str
    target_id: str
    kind: str


class TaskGraph(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    nodes: list[TaskNode] = Field(default_factory=list)
    edges: list[TaskEdge] = Field(default_factory=list)


class WorkflowRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    when_supervisor_reason: SupervisorReason | None = None
    when_task_status: TaskStatus | None = None
    when_phase: ActionPhase | None = None
    when_has_task_results: bool | None = None
    when_has_review_sessions: bool | None = None
    phase: ActionPhase
    reason: str


class WorkflowSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""
    rules: list[WorkflowRule] = Field(default_factory=list)


class SupervisorDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    action: SupervisorAction
    reason: SupervisorReason
    summary: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class HintRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str
    author: str = "user"
    ts: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ControlState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    paused: bool = False
    pause_reason: str | None = None
    hints: list[HintRecord] = Field(default_factory=list)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class WorktreeLease(BaseModel):
    model_config = ConfigDict(extra="forbid")

    lease_id: str
    repo_path: Path
    worktree_path: Path
    head_sha: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class QueueBenchmarkPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    benchmark_name: str
    manifest_path: Path
    project: BenchmarkProjectConfig
    dry_run: bool = True
    use_worktrees: bool = False
    cleanup_worktrees: bool = True


class QueueJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    batch_id: str | None = None
    kind: QueueJobKind
    project_id: str
    status: QueueJobStatus
    priority: int = 0
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    artifact_dir: Path | None = None
    result_path: Path | None = None
    error_message: str | None = None
    pause_reason: str | None = None
    cancel_reason: str | None = None

    @property
    def id(self) -> str:
        return self.job_id


class BatchRunReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    processed_job_ids: list[str] = Field(default_factory=list)
    paused_job_ids: list[str] = Field(default_factory=list)
