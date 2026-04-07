from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class WorkflowMode(StrEnum):
    FIXED_LOOP = "fixed_loop"
    ADAPTIVE_LOOP = "adaptive_loop"


class ProviderKind(StrEnum):
    OPENAI_COMPATIBLE = "openai_compatible"


class ExecutorKind(StrEnum):
    DRY_RUN = "dry_run"
    OPENAI_COMPATIBLE = "openai_compatible"
    CODEX_EXEC = "codex_exec"


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


class BenchmarkLedgerOutcomeKind(StrEnum):
    PROOF_PROGRESS = "proof_progress"
    ARTIFACT_PROGRESS = "artifact_progress"
    NO_PROGRESS = "no_progress"
    DRY_RUN = "dry_run"
    FAILED = "failed"
    MISSING_TARGET = "missing_target"


class BenchmarkLedgerFailureKind(StrEnum):
    NONE = "none"
    EXECUTOR_FAILED = "executor_failed"
    RUN_ERROR = "run_error"
    ARTIFACT_MISSING = "artifact_missing"
    MISSING_TARGET_THEOREM = "missing_target_theorem"


class ExecutionStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


class QueueJobKind(StrEnum):
    BENCHMARK_PROJECT = "benchmark_project"
    SESSION_QUANTUM = "session_quantum"


class QueueJobStatus(StrEnum):
    QUEUED = "queued"
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"
    CANCELED = "canceled"


class WorkerStatus(StrEnum):
    IDLE = "idle"
    RUNNING = "running"
    STOPPED = "stopped"
    FAILED = "failed"


class SessionStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
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


class WorkspaceProjectConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    project_path: Path
    archon_path: Path
    workflow: WorkflowMode | None = None
    workflow_spec: Path | None = None
    max_iterations: int | None = None
    dry_run: bool | None = None
    backend: str = "archon"
    enabled: bool = True
    tags: list[str] = Field(default_factory=list)

    def as_project_config(self) -> ProjectConfig:
        return ProjectConfig(
            name=self.id,
            project_path=self.project_path,
            archon_path=self.archon_path,
            backend=self.backend,
        )


class ProviderConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: ProviderKind = ProviderKind.OPENAI_COMPATIBLE
    pool: str | None = None
    member_name: str | None = None
    model: str | None = None
    cost_tier: str | None = None
    endpoint_class: str | None = None
    base_url: str | None = None
    api_key_env: str = "OPENAI_API_KEY"
    endpoint_path: str = "/v1/responses"
    headers: dict[str, str] = Field(default_factory=dict)
    input_cost_per_1k_tokens: float | None = None
    output_cost_per_1k_tokens: float | None = None


class ProviderPoolMemberConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    enabled: bool = True
    priority: int = 0
    kind: ProviderKind | None = None
    model: str | None = None
    cost_tier: str | None = None
    endpoint_class: str | None = None
    base_url: str | None = None
    api_key_env: str | None = None
    endpoint_path: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    input_cost_per_1k_tokens: float | None = None
    output_cost_per_1k_tokens: float | None = None

    def as_provider_config(self, base: ProviderConfig) -> ProviderConfig:
        return base.model_copy(
            update={
                "kind": self.kind or base.kind,
                "pool": base.pool,
                "member_name": self.name,
                "model": self.model if self.model is not None else base.model,
                "cost_tier": (
                    self.cost_tier if self.cost_tier is not None else base.cost_tier
                ),
                "endpoint_class": (
                    self.endpoint_class
                    if self.endpoint_class is not None
                    else base.endpoint_class
                ),
                "base_url": self.base_url if self.base_url is not None else base.base_url,
                "api_key_env": (
                    self.api_key_env if self.api_key_env is not None else base.api_key_env
                ),
                "endpoint_path": (
                    self.endpoint_path
                    if self.endpoint_path is not None
                    else base.endpoint_path
                ),
                "headers": {**base.headers, **self.headers},
                "input_cost_per_1k_tokens": (
                    self.input_cost_per_1k_tokens
                    if self.input_cost_per_1k_tokens is not None
                    else base.input_cost_per_1k_tokens
                ),
                "output_cost_per_1k_tokens": (
                    self.output_cost_per_1k_tokens
                    if self.output_cost_per_1k_tokens is not None
                    else base.output_cost_per_1k_tokens
                ),
            }
        )


class ProviderPoolConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    strategy: str = "ordered_failover"
    max_consecutive_failures: int = 2
    quarantine_seconds: int = 300
    members: list[ProviderPoolMemberConfig] = Field(default_factory=list)


class ExecutorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: ExecutorKind = ExecutorKind.DRY_RUN
    command: str = "codex"
    profile: str | None = None
    auto_approve: bool = False
    skip_git_repo_check: bool = True
    sandbox: str | None = None
    color: str = "never"
    extra_args: list[str] = Field(default_factory=list)
    timeout_seconds: int = 600


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
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
    provider: ProviderConfig = Field(default_factory=ProviderConfig)
    provider_pools: dict[str, ProviderPoolConfig] = Field(default_factory=dict)
    execution_policy: ExecutionPolicy = Field(default_factory=lambda: ExecutionPolicy())


class WorkspaceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    run: RunConfig
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
    provider: ProviderConfig = Field(default_factory=ProviderConfig)
    provider_pools: dict[str, ProviderPoolConfig] = Field(default_factory=dict)
    execution_policy: ExecutionPolicy = Field(default_factory=lambda: ExecutionPolicy())
    projects: list[WorkspaceProjectConfig] = Field(default_factory=list)


class ExecutionCapability(BaseModel):
    model_config = ConfigDict(extra="forbid")

    executor_kind: ExecutorKind
    provider_kind: ProviderKind
    model: str | None = None
    cost_tier: str | None = None
    endpoint_class: str | None = None

    @classmethod
    def from_configs(
        cls,
        *,
        executor: ExecutorConfig,
        provider: ProviderConfig,
    ) -> ExecutionCapability:
        return cls(
            executor_kind=executor.kind,
            provider_kind=provider.kind,
            model=provider.model,
            cost_tier=provider.cost_tier,
            endpoint_class=provider.endpoint_class,
        )

    @property
    def capability_id(self) -> str:
        return "__".join(
            [
                f"executor={self.executor_kind.value}",
                f"provider={self.provider_kind.value}",
                f"model={self.model or 'any'}",
                f"cost={self.cost_tier or 'any'}",
                f"endpoint={self.endpoint_class or 'any'}",
            ]
        )

    def as_requirement(self) -> ExecutionRequirement:
        return ExecutionRequirement(
            executor_kinds=[self.executor_kind],
            provider_kinds=[self.provider_kind],
            models=[self.model] if self.model is not None else [],
            cost_tiers=[self.cost_tier] if self.cost_tier is not None else [],
            endpoint_classes=(
                [self.endpoint_class] if self.endpoint_class is not None else []
            ),
        )


class ExecutionRequirement(BaseModel):
    model_config = ConfigDict(extra="forbid")

    executor_kinds: list[ExecutorKind] = Field(default_factory=list)
    provider_kinds: list[ProviderKind] = Field(default_factory=list)
    models: list[str] = Field(default_factory=list)
    cost_tiers: list[str] = Field(default_factory=list)
    endpoint_classes: list[str] = Field(default_factory=list)

    @property
    def profile_key(self) -> tuple[tuple[str, ...], ...]:
        return (
            tuple(sorted(kind.value for kind in self.executor_kinds)),
            tuple(sorted(kind.value for kind in self.provider_kinds)),
            tuple(sorted(self.models)),
            tuple(sorted(self.cost_tiers)),
            tuple(sorted(self.endpoint_classes)),
        )

    @property
    def profile_id(self) -> str:
        return "__".join(
            [
                self._dimension(
                    "executor",
                    [kind.value for kind in self.executor_kinds],
                ),
                self._dimension(
                    "provider",
                    [kind.value for kind in self.provider_kinds],
                ),
                self._dimension("model", self.models),
                self._dimension("cost", self.cost_tiers),
                self._dimension("endpoint", self.endpoint_classes),
            ]
        )

    def matches_axes(
        self,
        *,
        executor_kinds: list[ExecutorKind],
        provider_kinds: list[ProviderKind],
        models: list[str],
        cost_tiers: list[str],
        endpoint_classes: list[str],
    ) -> bool:
        if self.executor_kinds and not set(self.executor_kinds).issubset(set(executor_kinds)):
            return False
        if self.provider_kinds and not set(self.provider_kinds).issubset(
            set(provider_kinds)
        ):
            return False
        if self.models and models and not set(self.models).issubset(set(models)):
            return False
        if self.cost_tiers and cost_tiers and not set(self.cost_tiers).issubset(
            set(cost_tiers)
        ):
            return False
        return not (
            self.endpoint_classes
            and endpoint_classes
            and not set(self.endpoint_classes).issubset(set(endpoint_classes))
        )

    def matches_capability(
        self,
        capability: ExecutionCapability,
        *,
        allow_capability_wildcards: bool = True,
    ) -> bool:
        return self.matches_axes(
            executor_kinds=[capability.executor_kind],
            provider_kinds=[capability.provider_kind],
            models=(
                []
                if allow_capability_wildcards and capability.model is None
                else ([capability.model] if capability.model is not None else [])
            ),
            cost_tiers=(
                []
                if allow_capability_wildcards and capability.cost_tier is None
                else ([capability.cost_tier] if capability.cost_tier is not None else [])
            ),
            endpoint_classes=(
                []
                if allow_capability_wildcards and capability.endpoint_class is None
                else (
                    [capability.endpoint_class]
                    if capability.endpoint_class is not None
                    else []
                )
            ),
        )

    @classmethod
    def from_capability(cls, capability: ExecutionCapability) -> ExecutionRequirement:
        return capability.as_requirement()

    @staticmethod
    def _dimension(label: str, values: list[str]) -> str:
        return f"{label}={','.join(sorted(values)) or 'any'}"


class ExecutionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    project_id: str
    phase: str
    prompt: str
    cwd: Path
    artifact_dir: Path
    task_id: str | None = None
    task_title: str | None = None


class ExecutionResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    executor: ExecutorKind
    status: ExecutionStatus
    response_text: str | None = None
    output_path: Path | None = None
    stdout_path: Path | None = None
    stderr_path: Path | None = None
    request_path: Path | None = None
    response_path: Path | None = None
    command: list[str] = Field(default_factory=list)
    error_message: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    telemetry: ExecutionTelemetry | None = None

    @property
    def text(self) -> str:
        return self.response_text or ""

    @property
    def provider(self) -> str:
        return str(self.metadata.get("provider", self.executor.value))


class AdapterAction(BaseModel):
    phase: str | ActionPhase
    reason: str
    stage: str
    prompt_preview: str | None = None
    task_id: str | None = None
    task_title: str | None = None
    theorem_name: str | None = None
    file_path: Path | None = None
    task_status: TaskStatus | None = None
    task_sources: list[TaskSource] = Field(default_factory=list)
    task_priority: int | None = None
    task_blockers: list[str] = Field(default_factory=list)
    objective_relevant: bool | None = None
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
    execution: ExecutionResult | None = None


class RunLoopResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str
    workspace_id: str
    project_id: str
    status: SessionStatus
    dry_run: bool
    max_iterations: int
    completed_iterations: int
    run_ids: list[str] = Field(default_factory=list)
    stop_reason: str


class SessionQuantumResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str
    workspace_id: str
    project_id: str
    status: SessionStatus
    dry_run: bool
    max_iterations: int
    completed_iterations: int
    run_id: str | None = None
    action_phase: ActionPhase | None = None
    action_reason: str | None = None
    stop_reason: str


class RunPreview(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workflow: WorkflowMode
    workflow_spec_path: Path | None = None
    progress: ProgressSnapshot
    snapshot: ProjectSnapshot
    control: ControlState
    workflow_spec: WorkflowSpec | None = None
    task_graph: TaskGraph
    supervisor: SupervisorDecision
    action: AdapterAction
    resolved_capability: ExecutionCapability | None = None
    resolved_executor: ExecutorConfig | None = None
    resolved_provider: ProviderConfig | None = None


class QueueJobPreview(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phase: ActionPhase | None = None
    reason: str | None = None
    stage: str | None = None
    supervisor_action: SupervisorAction | None = None
    supervisor_reason: SupervisorReason | None = None
    supervisor_summary: str | None = None
    task_id: str | None = None
    task_title: str | None = None
    theorem_name: str | None = None
    file_path: Path | None = None
    task_status: TaskStatus | None = None
    task_priority: int | None = None
    objective_relevant: bool | None = None
    base_priority: int = 0
    task_priority_bonus: int = 0
    objective_relevance_bonus: int = 0
    final_priority: int = 0
    executor_kind: ExecutorKind | None = None
    provider_kind: ProviderKind | None = None
    model: str | None = None
    cost_tier: str | None = None
    endpoint_class: str | None = None
    capability_id: str | None = None


class BenchmarkConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str = ""
    artifact_root: Path
    worker_slots: int = 1


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
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
    provider: ProviderConfig = Field(default_factory=ProviderConfig)
    execution_policy: ExecutionPolicy = Field(default_factory=lambda: ExecutionPolicy())


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


class LeanDeclaration(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    file_path: Path
    declaration_kind: str
    dependencies: list[str] = Field(default_factory=list)
    blocked_by_sorry: bool = False
    uses_axiom: bool = False


class LeanAnalysisSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    project_path: Path
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    declarations: list[LeanDeclaration] = Field(default_factory=list)
    lean_file_count: int = 0
    theorem_count: int = 0
    sorry_count: int = 0
    axiom_count: int = 0


class TheoremState(StrEnum):
    PROVED = "proved"
    CONTAINS_SORRY = "contains_sorry"
    USES_AXIOM = "uses_axiom"
    MISSING = "missing"


class TheoremOutcomeKind(StrEnum):
    UNCHANGED = "unchanged"
    IMPROVED = "improved"
    REGRESSED = "regressed"
    NEW = "new"
    REMOVED = "removed"


class FailureCategory(StrEnum):
    RUN_ERROR = "run_error"
    CONTAINS_SORRY = "contains_sorry"
    USES_AXIOM = "uses_axiom"
    REMOVED_DECLARATION = "removed_declaration"


class TheoremOutcome(BaseModel):
    model_config = ConfigDict(extra="forbid")

    theorem_name: str
    file_path: Path | None = None
    declaration_kind: str | None = None
    before_state: TheoremState
    after_state: TheoremState
    outcome: TheoremOutcomeKind
    failure_categories: list[FailureCategory] = Field(default_factory=list)


class FailureCategorySummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: FailureCategory
    count: int
    samples: list[str] = Field(default_factory=list)


class ExperimentLedgerSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_projects: int
    total_theorems: int
    unchanged: int = 0
    improved: int = 0
    regressed: int = 0
    new: int = 0
    removed: int = 0
    failure_taxonomy: list[FailureCategorySummary] = Field(default_factory=list)


class ExperimentProjectLedger(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    run_id: str | None = None
    run_status: RunStatus
    theorem_outcomes: list[TheoremOutcome] = Field(default_factory=list)
    failure_taxonomy: list[FailureCategorySummary] = Field(default_factory=list)


class ExperimentLedger(BaseModel):
    model_config = ConfigDict(extra="forbid")

    benchmark_name: str
    benchmark_run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    summary: ExperimentLedgerSummary
    outcomes: list[ExperimentProjectLedger] = Field(default_factory=list)


class ExperimentLedgerChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    theorem_name: str
    file_path: Path | None = None
    baseline_state: TheoremState
    candidate_state: TheoremState
    change: TheoremOutcomeKind


class ExperimentLedgerComparisonSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_theorems: int
    unchanged: int = 0
    improved: int = 0
    regressed: int = 0
    new: int = 0
    removed: int = 0


class ExperimentLedgerComparison(BaseModel):
    model_config = ConfigDict(extra="forbid")

    baseline_benchmark: str
    candidate_benchmark: str
    summary: ExperimentLedgerComparisonSummary
    changes: list[ExperimentLedgerChange] = Field(default_factory=list)


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
    theorem_outcomes: list[TheoremOutcome] = Field(default_factory=list)
    failure_taxonomy: list[FailureCategorySummary] = Field(default_factory=list)


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
    ledger_path: Path | None = None
    ledger_summary: ExperimentLedgerSummary | None = None
    projects: list[BenchmarkProjectResult]


class BenchmarkLedgerRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    run_id: str | None = None
    theorem_name: str | None = None
    file_path: Path | None = None
    workflow: WorkflowMode
    run_status: RunStatus
    mode: str
    action_phase: str | None = None
    action_reason: str | None = None
    outcome: BenchmarkLedgerOutcomeKind
    failure_kind: BenchmarkLedgerFailureKind = BenchmarkLedgerFailureKind.NONE
    error_message: str | None = None
    score_after: float
    score_delta: float
    sorry_delta: int
    task_results_delta: int
    review_session_delta: int
    executor: ExecutorKind | None = None
    provider: str | None = None
    provider_pool: str | None = None
    provider_member: str | None = None
    latency_ms: int | None = None
    cost_estimate: float | None = None
    artifact_dir: Path | None = None


class BenchmarkLedgerSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_projects: int
    targeted_projects: int
    outcome_counts: dict[str, int] = Field(default_factory=dict)
    failure_counts: dict[str, int] = Field(default_factory=dict)
    total_estimated_cost: float = 0.0


class BenchmarkLedger(BaseModel):
    model_config = ConfigDict(extra="forbid")

    benchmark_name: str
    benchmark_run_id: str
    dry_run: bool
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    summary: BenchmarkLedgerSummary
    outcomes: list[BenchmarkLedgerRecord] = Field(default_factory=list)


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
    workflow_override: WorkflowMode | None = None
    workflow_spec_override: Path | None = None
    clear_workflow_spec: bool = False
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ProjectSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str
    workspace_id: str
    project_id: str
    status: SessionStatus = SessionStatus.PENDING
    workflow: WorkflowMode = WorkflowMode.ADAPTIVE_LOOP
    dry_run: bool = True
    max_iterations: int = 10
    completed_iterations: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_run_id: str | None = None
    error_message: str | None = None
    note: str | None = None

    @property
    def id(self) -> str:
        return self.session_id


class SessionIteration(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str
    iteration_index: int
    project_id: str
    run_id: str | None = None
    status: RunStatus = RunStatus.STARTED
    action_phase: ActionPhase | None = None
    action_reason: str | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    error_message: str | None = None


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


class QueueSessionPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workspace_config_path: Path
    workspace_id: str
    project_id: str
    session_id: str
    quantum: int = 1


class QueueJob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_id: str
    batch_id: str | None = None
    kind: QueueJobKind
    project_id: str
    workspace_id: str | None = None
    session_id: str | None = None
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
    worker_id: str | None = None
    required_executor_kinds: list[ExecutorKind] = Field(default_factory=list)
    required_provider_kinds: list[ProviderKind] = Field(default_factory=list)
    required_models: list[str] = Field(default_factory=list)
    required_cost_tiers: list[str] = Field(default_factory=list)
    required_endpoint_classes: list[str] = Field(default_factory=list)
    preview: QueueJobPreview | None = None

    @property
    def id(self) -> str:
        return self.job_id

    @property
    def execution_requirement(self) -> ExecutionRequirement:
        return ExecutionRequirement(
            executor_kinds=self.required_executor_kinds,
            provider_kinds=self.required_provider_kinds,
            models=self.required_models,
            cost_tiers=self.required_cost_tiers,
            endpoint_classes=self.required_endpoint_classes,
        )


class QueueWorkerLease(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str
    slot_index: int
    status: WorkerStatus
    current_job_id: str | None = None
    last_job_id: str | None = None
    thread_name: str | None = None
    note: str | None = None
    worktree_root: Path | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    heartbeat_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    processed_jobs: int = 0
    failed_jobs: int = 0
    heartbeat_age_seconds: float | None = None
    stale: bool = False
    executor_kinds: list[ExecutorKind] = Field(default_factory=list)
    provider_kinds: list[ProviderKind] = Field(default_factory=list)
    models: list[str] = Field(default_factory=list)
    cost_tiers: list[str] = Field(default_factory=list)
    endpoint_classes: list[str] = Field(default_factory=list)

    @property
    def heartbeat(self) -> datetime:
        return self.heartbeat_at

    @property
    def updated_at(self) -> datetime:
        return self.heartbeat_at

    @property
    def execution_requirement(self) -> ExecutionRequirement:
        return ExecutionRequirement(
            executor_kinds=self.executor_kinds,
            provider_kinds=self.provider_kinds,
            models=self.models,
            cost_tiers=self.cost_tiers,
            endpoint_classes=self.endpoint_classes,
        )


class QueueFleetProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    profile_id: str
    required_executor_kinds: list[ExecutorKind] = Field(default_factory=list)
    required_provider_kinds: list[ProviderKind] = Field(default_factory=list)
    required_models: list[str] = Field(default_factory=list)
    required_cost_tiers: list[str] = Field(default_factory=list)
    required_endpoint_classes: list[str] = Field(default_factory=list)
    queued_jobs: int = 0
    pending_jobs: int = 0
    running_jobs: int = 0
    active_jobs: int = 0
    dedicated_workers: int = 0
    recommended_total_workers: int = 0
    recommended_additional_workers: int = 0
    dominant_phase: ActionPhase | None = None
    phase_counts: dict[str, int] = Field(default_factory=dict)
    stage_counts: dict[str, int] = Field(default_factory=dict)
    max_priority: int = 0
    avg_priority: float = 0.0
    project_ids: list[str] = Field(default_factory=list)
    focus_examples: list[str] = Field(default_factory=list)


class QueueFleetPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    target_jobs_per_worker: int = 2
    total_profiles: int = 0
    queued_jobs: int = 0
    pending_jobs: int = 0
    running_jobs: int = 0
    active_jobs: int = 0
    active_workers: int = 0
    dedicated_workers: int = 0
    generic_workers: int = 0
    recommended_total_workers: int = 0
    recommended_additional_workers: int = 0
    profiles: list[QueueFleetProfile] = Field(default_factory=list)


class BatchRunReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    processed_job_ids: list[str] = Field(default_factory=list)
    paused_job_ids: list[str] = Field(default_factory=list)
    failed_job_ids: list[str] = Field(default_factory=list)
    worker_ids: list[str] = Field(default_factory=list)


class ExecutionUsage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None


class ExecutionTelemetry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    started_at: datetime | None = None
    finished_at: datetime | None = None
    latency_ms: int | None = None
    retry_count: int = 0
    provider_pool: str | None = None
    provider_member: str | None = None
    attempted_members: list[str] = Field(default_factory=list)
    usage: ExecutionUsage | None = None
    cost_estimate: float | None = None
    status_code: int | None = None
    health_status: str | None = None


class FleetControllerCycle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cycle_index: int
    plan: QueueFleetPlan
    report: BatchRunReport


class FleetControllerResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    cycles_completed: int = 0
    stop_reason: str = "unknown"
    total_processed_jobs: int = 0
    total_paused_jobs: int = 0
    total_failed_jobs: int = 0
    total_workers_launched: int = 0
    cycles: list[FleetControllerCycle] = Field(default_factory=list)
    final_plan: QueueFleetPlan = Field(default_factory=QueueFleetPlan)


class ExecutionPhaseOverride(BaseModel):
    model_config = ConfigDict(extra="forbid")

    executor: ExecutorConfig | None = None
    provider: ProviderConfig | None = None


class ExecutionTaskMatcher(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    phase: ActionPhase | None = None
    task_id: str | None = None
    task_title: str | None = None
    theorem_name: str | None = None
    file_path: Path | None = None
    task_status: TaskStatus | None = None
    task_sources: list[TaskSource] = Field(default_factory=list)
    min_priority: int | None = None
    max_priority: int | None = None
    blockers: list[str] = Field(default_factory=list)
    blocker_pattern: str | None = None
    objective_relevant: bool | None = None
    task_id_pattern: str | None = None
    task_title_pattern: str | None = None
    theorem_pattern: str | None = None
    file_path_pattern: str | None = None


class ExecutionTaskOverride(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    matcher: ExecutionTaskMatcher
    executor: ExecutorConfig | None = None
    provider: ProviderConfig | None = None


class ExecutionPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    phases: dict[ActionPhase, ExecutionPhaseOverride] = Field(default_factory=dict)
    task_rules: list[ExecutionTaskOverride] = Field(default_factory=list)
