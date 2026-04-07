from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient  # noqa: E402

from archonlab.dashboard import create_dashboard_app  # noqa: E402
from archonlab.events import EventStore  # noqa: E402
from archonlab.models import (  # noqa: E402
    ActionPhase,
    BatchRunReport,
    EventRecord,
    ExecutorKind,
    ProjectSession,
    ProviderKind,
    ProviderPoolHealthReport,
    ProviderPoolHealthStatus,
    ProviderPoolMemberHealth,
    ProviderPoolMemberHealthStatus,
    QueueJobPreview,
    RunStatus,
    RunSummary,
    SessionStatus,
    SupervisorAction,
    SupervisorReason,
    WorkflowMode,
)
from archonlab.queue import QueueStore  # noqa: E402


def _make_project(tmp_path: Path, name: str = "DemoProject") -> Path:
    project_path = tmp_path / name
    state_dir = project_path / ".archon"
    prompts_dir = state_dir / "prompts"
    prompts_dir.mkdir(parents=True)
    (state_dir / "CLAUDE.md").write_text("# demo\n", encoding="utf-8")
    (prompts_dir / "plan.md").write_text("# plan\n", encoding="utf-8")
    (prompts_dir / "prover-prover.md").write_text("# prover\n", encoding="utf-8")
    (state_dir / "PROGRESS.md").write_text(
        "# Project Progress\n\n"
        "## Current Stage\n"
        "prover\n\n"
        "## Current Objectives\n\n"
        "1. Understand the current proving bottleneck.\n",
        encoding="utf-8",
    )
    (project_path / "Core.lean").write_text(
        "theorem helper : True := by\n"
        "  trivial\n",
        encoding="utf-8",
    )
    return project_path


def _make_archon(tmp_path: Path) -> Path:
    archon_path = tmp_path / "Archon"
    archon_path.mkdir()
    (archon_path / "archon-loop.sh").write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    return archon_path


def _seed_run(artifact_root: Path) -> None:
    store = EventStore(artifact_root / "archonlab.db")
    summary = RunSummary(
        run_id="run-1",
        project_id="DemoProject",
        workflow=WorkflowMode.ADAPTIVE_LOOP,
        status=RunStatus.STARTED,
        stage="prover",
        dry_run=True,
        started_at=datetime.now(UTC),
        artifact_dir=artifact_root / "runs" / "run-1",
    )
    store.register_run(summary)
    store.append(
        EventRecord(
            run_id="run-1",
            kind="run.started",
            project_id="DemoProject",
            payload={"stage": "prover"},
        )
    )


def test_dashboard_api_lists_runs_and_supports_control_actions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_path = _make_project(tmp_path)
    archon_path = _make_archon(tmp_path)
    artifact_root = tmp_path / "artifacts"
    _seed_run(artifact_root)

    original_connect = sqlite3.connect

    def connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        options = dict(kwargs)
        options.setdefault("check_same_thread", False)
        return original_connect(*args, **options)

    monkeypatch.setattr(sqlite3, "connect", connect)

    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "DemoProject"\n'
        f'project_path = "{project_path}"\n'
        f'archon_path = "{archon_path}"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n",
        encoding="utf-8",
    )

    app = create_dashboard_app(config_path)
    client = TestClient(app)

    index_response = client.get("/")
    assert index_response.status_code == 200
    assert 'id="project-preview-overview"' in index_response.text
    assert 'id="project-preview-rules"' in index_response.text
    assert 'id="fleet-plan-summary"' in index_response.text
    assert 'id="fleet-plan-list"' in index_response.text
    assert 'id="workspace-runtime-summary"' in index_response.text
    assert 'id="workspace-provider-runtime"' in index_response.text
    assert 'id="workspace-provider-health"' in index_response.text
    assert 'id="workspace-enqueue-button"' in index_response.text
    assert 'id="workspace-resume-button"' in index_response.text
    assert 'id="workspace-tag-input"' in index_response.text

    runs_response = client.get("/api/runs")
    assert runs_response.status_code == 200
    runs = runs_response.json()
    assert runs[0]["run_id"] == "run-1"
    assert runs[0]["status"] == "started"

    detail_response = client.get("/api/runs/run-1")
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["run"]["run_id"] == "run-1"
    assert detail["events"][0]["kind"] == "run.started"

    control_response = client.get("/api/projects/DemoProject/control")
    assert control_response.status_code == 200
    control = control_response.json()
    assert control["paused"] is False

    preview_response = client.get("/api/projects/DemoProject/preview")
    assert preview_response.status_code == 200
    preview_payload = preview_response.json()
    assert preview_payload["workflow"] == "adaptive_loop"
    assert preview_payload["configured_workflow"] == "adaptive_loop"
    assert preview_payload["preview"]["action"]["phase"] == "plan"
    assert preview_payload["preview"]["action"]["reason"] == "bootstrap_first_iteration"
    assert preview_payload["task_graph_summary"]["total_nodes"] >= 1
    assert preview_payload["task_graph_summary"]["objective_nodes"] >= 1
    assert preview_payload["focus_task"] is not None
    assert preview_payload["workflow_rules"] == []
    assert isinstance(preview_payload["supervisor_evidence"], dict)

    workflow_override_response = client.post(
        "/api/projects/DemoProject/workflow",
        json={"workflow": "fixed_loop", "clear_workflow_spec": True},
    )
    assert workflow_override_response.status_code == 200
    workflow_override = workflow_override_response.json()
    assert workflow_override["workflow_override"] == "fixed_loop"
    assert workflow_override["clear_workflow_spec"] is True

    fixed_preview_response = client.get("/api/projects/DemoProject/preview")
    assert fixed_preview_response.status_code == 200
    fixed_preview = fixed_preview_response.json()
    assert fixed_preview["workflow"] == "fixed_loop"
    assert fixed_preview["workflow_spec_path"] is None
    assert fixed_preview["preview"]["action"]["reason"] == "fixed_loop_baseline"

    workflow_spec = tmp_path / "workflow-override.toml"
    workflow_spec.write_text(
        "[workflow]\n"
        'name = "dashboard-override"\n'
        'description = "Override plan reason."\n\n'
        "[[rules]]\n"
        'name = "rewrite_plan_reason"\n'
        'when_phase = "plan"\n'
        "when_has_review_sessions = false\n"
        'phase = "plan"\n'
        'reason = "from_dashboard_spec"\n',
        encoding="utf-8",
    )
    workflow_spec_response = client.post(
        "/api/projects/DemoProject/workflow",
        json={"workflow_spec_path": "workflow-override.toml"},
    )
    assert workflow_spec_response.status_code == 200
    workflow_spec_override = workflow_spec_response.json()
    assert workflow_spec_override["workflow_spec_override"] == str(workflow_spec.resolve())

    spec_preview_response = client.get("/api/projects/DemoProject/preview")
    assert spec_preview_response.status_code == 200
    spec_preview = spec_preview_response.json()
    assert spec_preview["workflow"] == "adaptive_loop"
    assert spec_preview["workflow_spec_path"] == str(workflow_spec.resolve())
    assert spec_preview["preview"]["action"]["reason"] == "from_dashboard_spec"
    assert spec_preview["workflow_rules"][0]["name"] == "rewrite_plan_reason"
    assert "phase=plan" in spec_preview["workflow_rules"][0]["conditions"]

    workflow_reset_response = client.post("/api/projects/DemoProject/workflow/reset")
    assert workflow_reset_response.status_code == 200
    workflow_reset = workflow_reset_response.json()
    assert workflow_reset["workflow_override"] is None
    assert workflow_reset["workflow_spec_override"] is None
    assert workflow_reset["clear_workflow_spec"] is False

    pause_response = client.post(
        "/api/projects/DemoProject/pause",
        json={"reason": "manual_hold"},
    )
    assert pause_response.status_code == 200
    paused = pause_response.json()
    assert paused["paused"] is True
    assert paused["pause_reason"] == "manual_hold"

    resume_response = client.post("/api/projects/DemoProject/resume")
    assert resume_response.status_code == 200
    resumed = resume_response.json()
    assert resumed["paused"] is False
    assert resumed["pause_reason"] is None

    hint_response = client.post(
        "/api/projects/DemoProject/hint",
        json={"text": "Try `simp` after `rw`.", "author": "mentor"},
    )
    assert hint_response.status_code == 200
    hint_payload = hint_response.json()
    assert hint_payload["hints"][-1]["author"] == "mentor"
    assert (project_path / ".archon" / "USER_HINTS.md").exists()

    queue_store = QueueStore(artifact_root / "archonlab.db")
    queued_job = queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": str(tmp_path / "demo.toml")},
        project_id="demo-project",
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
            supervisor_summary="Current state looks healthy enough to continue the planned loop.",
            theorem_name="foo",
            base_priority=3,
            task_priority_bonus=2,
            objective_relevance_bonus=5,
            final_priority=10,
            executor_kind=ExecutorKind.DRY_RUN,
            provider_kind=ProviderKind.OPENAI_COMPATIBLE,
            model="gpt-5.4",
            cost_tier="premium",
            endpoint_class="lab",
        ),
    )
    queue_store.register_worker(slot_index=1, worker_id="worker-test")
    queue_store.register_worker(
        slot_index=2,
        worker_id="worker-premium",
        executor_kinds=[ExecutorKind.DRY_RUN],
        provider_kinds=[ProviderKind.OPENAI_COMPATIBLE],
        models=["gpt-5.4"],
        cost_tiers=["premium"],
        endpoint_classes=["lab"],
    )
    stale_heartbeat = (datetime.now(UTC) - timedelta(seconds=300)).isoformat()
    queue_store._conn.execute(
        "UPDATE queue_workers SET heartbeat_at = ? WHERE worker_id = ?",
        (stale_heartbeat, "worker-test"),
    )
    queue_store._conn.commit()
    workers_response = client.get("/api/queue/workers")
    assert workers_response.status_code == 200
    workers = workers_response.json()
    assert workers[0]["worker_id"] == "worker-test"
    assert workers[0]["stale"] is True

    jobs_response = client.get("/api/queue/jobs")
    assert jobs_response.status_code == 200
    jobs = jobs_response.json()
    assert jobs[0]["preview"]["phase"] == "prover"
    assert jobs[0]["preview"]["reason"] == "task_graph_focus"
    assert jobs[0]["preview"]["stage"] == "prover"
    assert jobs[0]["preview"]["final_priority"] == 10
    assert jobs[0]["preview"]["model"] == "gpt-5.4"
    assert jobs[0]["preview"]["supervisor_summary"].startswith("Current state looks healthy")

    fleet_plan_response = client.get("/api/queue/fleet-plan")
    assert fleet_plan_response.status_code == 200
    fleet_plan = fleet_plan_response.json()
    assert fleet_plan["active_jobs"] == 1
    assert fleet_plan["active_workers"] == 1
    assert fleet_plan["dedicated_workers"] == 1
    assert fleet_plan["recommended_additional_workers"] == 0
    assert fleet_plan["profiles"][0]["required_models"] == ["gpt-5.4"]
    assert fleet_plan["profiles"][0]["dominant_phase"] == "prover"
    assert fleet_plan["profiles"][0]["dedicated_workers"] == 1
    assert fleet_plan["profiles"][0]["recommended_total_workers"] == 1

    job_detail_response = client.get(f"/api/queue/jobs/{queued_job.id}")
    assert job_detail_response.status_code == 200
    job_detail = job_detail_response.json()
    assert job_detail["job_id"] == queued_job.id
    assert job_detail["preview"]["theorem_name"] == "foo"

    cancel_job_response = client.post(
        f"/api/queue/jobs/{queued_job.id}/cancel",
        json={"reason": "dashboard_cancel"},
    )
    assert cancel_job_response.status_code == 200
    canceled_job = cancel_job_response.json()
    assert canceled_job["status"] == "canceled"
    assert canceled_job["cancel_reason"] == "dashboard_cancel"

    requeue_job_response = client.post(f"/api/queue/jobs/{queued_job.id}/requeue")
    assert requeue_job_response.status_code == 200
    requeued_job = requeue_job_response.json()
    assert requeued_job["status"] == "queued"
    assert requeued_job["cancel_reason"] is None
    assert requeued_job["preview"]["final_priority"] == 10

    sweep_response = client.post(
        "/api/queue/workers/sweep",
        json={"stale_after_seconds": 60, "requeue_running_jobs": True},
    )
    assert sweep_response.status_code == 200
    swept = sweep_response.json()
    assert swept[0]["worker_id"] == "worker-test"
    assert swept[0]["status"] == "failed"

    captured_fleet: dict[str, object] = {}

    def fake_run_fleet(self, **kwargs) -> BatchRunReport:
        captured_fleet.update(kwargs)
        return BatchRunReport(
            processed_job_ids=["job-1"],
            worker_ids=["worker-auto-1"],
        )

    monkeypatch.setattr("archonlab.dashboard.BatchRunner.run_fleet", fake_run_fleet)
    fleet_response = client.post(
        "/api/queue/fleet",
        json={"plan_driven": True, "target_jobs_per_worker": 3},
    )
    assert fleet_response.status_code == 200
    fleet_payload = fleet_response.json()
    assert fleet_payload["processed_job_ids"] == ["job-1"]
    assert fleet_payload["worker_ids"] == ["worker-auto-1"]
    assert captured_fleet["plan_driven"] is True
    assert captured_fleet["target_jobs_per_worker"] == 3


def test_dashboard_workspace_overview_and_project_switching(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    alpha_project = _make_project(tmp_path, "AlphaProject")
    beta_project = _make_project(tmp_path, "BetaProject")
    archon_path = _make_archon(tmp_path)
    artifact_root = tmp_path / "artifacts"

    original_connect = sqlite3.connect

    def connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        options = dict(kwargs)
        options.setdefault("check_same_thread", False)
        return original_connect(*args, **options)

    monkeypatch.setattr(sqlite3, "connect", connect)

    workspace_config_path = tmp_path / "workspace.toml"
    workspace_config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        f'artifact_root = "{artifact_root}"\n'
        "dry_run = true\n"
        "max_iterations = 6\n\n"
        "[provider]\n"
        'pool = "lab"\n\n'
        "[provider_pool.lab]\n"
        "max_consecutive_failures = 1\n"
        "quarantine_seconds = 60\n\n"
        "[[provider_pool.lab.members]]\n"
        'name = "member-a"\n'
        'model = "gpt-5.4-mini"\n\n'
        "[[provider_pool.lab.members]]\n"
        'name = "member-b"\n'
        'model = "gpt-5.4"\n\n'
        "[[projects]]\n"
        'id = "alpha"\n'
        'tags = ["core", "batch"]\n'
        f'project_path = "{alpha_project}"\n'
        f'archon_path = "{archon_path}"\n\n'
        "[[projects]]\n"
        'id = "beta"\n'
        'tags = ["geometry"]\n'
        f'project_path = "{beta_project}"\n'
        f'archon_path = "{archon_path}"\n'
        'workflow = "fixed_loop"\n'
        "dry_run = false\n"
        "max_iterations = 4\n",
        encoding="utf-8",
    )

    store = EventStore(artifact_root / "archonlab.db")
    store.register_session(
        ProjectSession(
            session_id="session-alpha-1",
            workspace_id="demo-workspace",
            project_id="alpha",
            status=SessionStatus.RUNNING,
            workflow=WorkflowMode.ADAPTIVE_LOOP,
            dry_run=True,
            max_iterations=6,
            completed_iterations=2,
            last_run_id="run-alpha-1",
        )
    )
    store.register_session(
        ProjectSession(
            session_id="session-beta-1",
            workspace_id="demo-workspace",
            project_id="beta",
            status=SessionStatus.PENDING,
            workflow=WorkflowMode.FIXED_LOOP,
            dry_run=False,
            max_iterations=4,
            completed_iterations=1,
        )
    )

    queue_store = QueueStore(artifact_root / "archonlab.db")
    queue_store.enqueue(
        "session_quantum",
        {
            "workspace_config_path": str(workspace_config_path),
            "workspace_id": "demo-workspace",
            "project_id": "alpha",
            "session_id": "session-alpha-1",
            "quantum": 1,
        },
        project_id="alpha",
        workspace_id="demo-workspace",
        session_id="session-alpha-1",
        priority=5,
    )
    queue_store.enqueue(
        "session_quantum",
        {
            "workspace_config_path": str(workspace_config_path),
            "workspace_id": "demo-workspace",
            "project_id": "beta",
            "session_id": "session-beta-1",
            "quantum": 1,
        },
        project_id="beta",
        workspace_id="demo-workspace",
        session_id="session-beta-1",
        priority=6,
    )
    queue_store.register_worker(slot_index=1, worker_id="worker-beta")
    queue_store.claim_next_job(worker_id="worker-beta")
    queue_store.enqueue(
        "benchmark_project",
        {"manifest_path": str(tmp_path / "other.toml")},
        project_id="external",
        workspace_id="other-workspace",
        priority=99,
    )
    store.append(
        EventRecord(
            run_id="run-alpha-1",
            kind="executor.completed",
            project_id="alpha",
            payload={
                "telemetry": {
                    "provider_pool": "lab",
                    "provider_member": "member-a",
                    "retry_count": 1,
                    "cost_estimate": 0.2,
                    "health_status": "degraded",
                }
            },
        )
    )

    monkeypatch.setattr(
        "archonlab.dashboard.snapshot_provider_pool_health",
        lambda provider_pools, *, db_path=None: [
            ProviderPoolHealthReport(
                pool_name="lab",
                status=ProviderPoolHealthStatus.DEGRADED,
                strategy="ordered_failover",
                total_members=2,
                available_members=1,
                quarantined_members=1,
                members=[
                    ProviderPoolMemberHealth(
                        pool_name="lab",
                        member_name="member-a",
                        status=ProviderPoolMemberHealthStatus.HEALTHY,
                        model="gpt-5.4-mini",
                    ),
                    ProviderPoolMemberHealth(
                        pool_name="lab",
                        member_name="member-b",
                        status=ProviderPoolMemberHealthStatus.QUARANTINED,
                        model="gpt-5.4",
                        consecutive_failures=2,
                    ),
                ],
            )
        ],
    )

    app = create_dashboard_app(workspace_config_path)
    client = TestClient(app)

    index_response = client.get("/")
    assert index_response.status_code == 200
    assert 'id="workspace-overview-summary"' in index_response.text
    assert 'id="workspace-session-table"' in index_response.text
    assert 'id="workspace-runtime-summary"' in index_response.text
    assert 'id="workspace-provider-runtime"' in index_response.text
    assert 'id="workspace-provider-health"' in index_response.text
    assert 'id="workspace-enqueue-button"' in index_response.text
    assert 'id="workspace-resume-button"' in index_response.text

    overview_response = client.get("/api/workspace/overview")
    assert overview_response.status_code == 200
    overview = overview_response.json()
    assert overview["workspace"] == "demo-workspace"
    assert overview["mode"] == "workspace"
    assert overview["default_project_id"] == "alpha"
    assert overview["project_count"] == 2
    assert overview["session_count"] == 2
    assert overview["running_sessions"] == 1
    assert overview["queued_jobs"] == 1
    assert overview["running_jobs"] == 1
    assert overview["active_workers"] == 1
    assert overview["budget"]["max_iterations"] == 10
    assert overview["budget"]["completed_iterations"] == 3
    assert overview["budget"]["remaining_iterations"] == 7
    assert {item["project_id"] for item in overview["projects"]} == {"alpha", "beta"}
    beta_summary = next(item for item in overview["projects"] if item["project_id"] == "beta")
    assert beta_summary["workflow"] == "fixed_loop"
    assert beta_summary["dry_run"] is False
    assert beta_summary["max_iterations"] == 4
    assert beta_summary["session_count"] == 1
    assert beta_summary["queued_jobs"] == 0
    assert beta_summary["tags"] == ["geometry"]
    alpha_summary = next(item for item in overview["projects"] if item["project_id"] == "alpha")
    assert alpha_summary["tags"] == ["core", "batch"]
    assert overview["provider_runtime"][0]["pool_name"] == "lab"
    assert overview["provider_runtime"][0]["success_count"] == 1
    assert overview["provider_runtime"][0]["members"][0]["member_name"] == "member-a"
    assert overview["provider_health"][0]["pool_name"] == "lab"
    assert overview["provider_health"][0]["status"] == "degraded"
    assert overview["provider_health"][0]["members"][1]["status"] == "quarantined"
    alpha_session = next(item for item in overview["sessions"] if item["project_id"] == "alpha")
    assert alpha_session["remaining_iterations"] == 4
    assert alpha_session["tags"] == ["core", "batch"]

    tagged_enqueue_response = client.post(
        "/api/workspace/enqueue",
        json={"tags": ["geometry"]},
    )
    assert tagged_enqueue_response.status_code == 200
    tagged_enqueue_payload = tagged_enqueue_response.json()
    assert len(tagged_enqueue_payload) == 1
    assert tagged_enqueue_payload[0]["project_id"] == "beta"

    tagged_resume_response = client.post(
        "/api/workspace/resume",
        json={"tags": ["geometry"]},
    )
    assert tagged_resume_response.status_code == 200
    tagged_resume_payload = tagged_resume_response.json()
    assert len(tagged_resume_payload["resumed"]) == 1
    assert tagged_resume_payload["resumed"][0]["session"]["project_id"] == "beta"

    enqueue_response = client.post(
        "/api/workspace/enqueue",
        json={"project_id": "alpha", "priority": 9, "note": "from_dashboard"},
    )
    assert enqueue_response.status_code == 200
    enqueue_payload = enqueue_response.json()
    assert len(enqueue_payload) == 1
    assert enqueue_payload[0]["project_id"] == "alpha"
    assert enqueue_payload[0]["session_id"] == "session-alpha-1"

    resume_response = client.post(
        "/api/workspace/resume",
        json={"project_id": "beta", "resume_reason": "dashboard_resume"},
    )
    assert resume_response.status_code == 200
    resume_payload = resume_response.json()
    assert len(resume_payload["resumed"]) == 1
    assert resume_payload["resumed"][0]["session"]["project_id"] == "beta"
    assert resume_payload["resumed"][0]["job"]["session_id"] == "session-beta-1"
    resumed_beta = store.get_session("session-beta-1")
    assert resumed_beta is not None
    assert resumed_beta.last_resume_reason == "dashboard_resume"

    beta_preview_response = client.get("/api/projects/beta/preview")
    assert beta_preview_response.status_code == 200
    beta_preview = beta_preview_response.json()
    assert beta_preview["project_id"] == "beta"
    assert beta_preview["workflow"] == "fixed_loop"
    assert beta_preview["configured_workflow"] == "fixed_loop"
    assert beta_preview["preview"]["action"]["reason"] == "fixed_loop_baseline"
