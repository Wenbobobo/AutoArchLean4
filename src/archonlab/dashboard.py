from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .batch import BatchRunner
from .config import load_config
from .control import ControlService
from .events import EventStore
from .models import ExecutorKind, ProviderKind, QueueJob, TaskGraph, TaskStatus
from .queue import QueueStore
from .services import RunService


class PauseRequest(BaseModel):
    reason: str | None = None


class HintRequest(BaseModel):
    text: str
    author: str = "user"


class QueueEnqueueRequest(BaseModel):
    manifest_path: Path
    dry_run: bool = True
    use_worktrees: bool = False


class QueueCancelRequest(BaseModel):
    reason: str | None = None


class QueueFleetRequest(BaseModel):
    workers: int | None = None
    max_jobs_per_worker: int | None = None
    poll_seconds: float = 2.0
    idle_timeout_seconds: float = 30.0
    stale_after_seconds: float | None = 120.0
    executor_kinds: list[ExecutorKind] | None = None
    provider_kinds: list[ProviderKind] | None = None
    models: list[str] | None = None
    cost_tiers: list[str] | None = None
    endpoint_classes: list[str] | None = None


class QueueSweepWorkersRequest(BaseModel):
    stale_after_seconds: float = 120.0
    requeue_running_jobs: bool = True


def create_dashboard_app(config_path: Path) -> FastAPI:
    config = load_config(config_path)
    store = EventStore(config.run.artifact_root / "archonlab.db")
    control = ControlService(config.run.artifact_root)
    queue = QueueStore(config.run.artifact_root / "archonlab.db")
    app = FastAPI(title="ArchonLab Dashboard")

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return render_dashboard_html(config.project.name)

    @app.get("/api/runs")
    def list_runs(limit: int = 20) -> list[dict[str, Any]]:
        runs = store.list_runs(limit=limit)
        return [run.model_dump(mode="json") for run in runs]

    @app.get("/api/runs/{run_id}")
    def run_detail(run_id: str) -> dict[str, Any]:
        run = store.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="Run not found")
        events = store.get_run_events(run_id)
        summary_path = run.artifact_dir / "run-summary.json"
        summary = None
        if summary_path.exists():
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        return {
            "run": run.model_dump(mode="json"),
            "events": [event.model_dump(mode="json") for event in events],
            "summary": summary,
        }

    @app.get("/api/projects/{project_id}/control")
    def get_control(project_id: str) -> dict[str, Any]:
        _ensure_project(config.project.name, project_id)
        return control.read(config.project).model_dump(mode="json")

    @app.get("/api/projects/{project_id}/preview")
    def get_project_preview(project_id: str) -> dict[str, Any]:
        _ensure_project(config.project.name, project_id)
        preview = RunService(config).preview()
        workflow_spec = preview.workflow_spec
        return {
            "project_id": config.project.name,
            "workflow": config.run.workflow.value,
            "workflow_spec_path": (
                str(config.run.workflow_spec)
                if config.run.workflow_spec is not None
                else None
            ),
            "workflow_spec": (
                {
                    "name": workflow_spec.name,
                    "description": workflow_spec.description,
                    "rule_count": len(workflow_spec.rules),
                }
                if workflow_spec is not None
                else None
            ),
            "task_graph_summary": _summarize_task_graph(preview.task_graph),
            "preview": preview.model_dump(mode="json"),
        }

    @app.post("/api/projects/{project_id}/pause")
    def pause_project(project_id: str, body: PauseRequest) -> dict[str, Any]:
        _ensure_project(config.project.name, project_id)
        return control.pause(config.project, reason=body.reason).model_dump(mode="json")

    @app.post("/api/projects/{project_id}/resume")
    def resume_project(project_id: str) -> dict[str, Any]:
        _ensure_project(config.project.name, project_id)
        return control.resume(config.project).model_dump(mode="json")

    @app.post("/api/projects/{project_id}/hint")
    def add_hint(project_id: str, body: HintRequest) -> dict[str, Any]:
        _ensure_project(config.project.name, project_id)
        return control.add_hint(
            config.project,
            text=body.text,
            author=body.author,
        ).model_dump(mode="json")

    @app.get("/api/queue/jobs")
    def list_queue_jobs(limit: int = 50) -> list[dict[str, Any]]:
        return [job.model_dump(mode="json") for job in queue.list_jobs(limit=limit)]

    @app.get("/api/queue/jobs/{job_id}")
    def queue_job_detail(job_id: str) -> dict[str, Any]:
        return _queue_job_or_404(queue, job_id).model_dump(mode="json")

    @app.get("/api/queue/workers")
    def list_queue_workers(stale_after_seconds: float | None = 120.0) -> list[dict[str, Any]]:
        return [
            worker.model_dump(mode="json")
            for worker in queue.list_workers(stale_after_seconds=stale_after_seconds)
        ]

    @app.post("/api/queue/enqueue")
    def enqueue_queue_job(body: QueueEnqueueRequest) -> list[dict[str, Any]]:
        jobs = queue.enqueue_benchmark_manifest(
            body.manifest_path,
            dry_run=body.dry_run,
            use_worktrees=body.use_worktrees,
        )
        return [job.model_dump(mode="json") for job in jobs]

    @app.post("/api/queue/run")
    def run_queue(max_jobs: int | None = None) -> dict[str, Any]:
        runner = BatchRunner(
            queue_store=queue,
            control_service=control,
            artifact_root=config.run.artifact_root,
            slot_limit=config.run.max_parallel,
        )
        return runner.run_pending(max_jobs=max_jobs).model_dump(mode="json")

    @app.post("/api/queue/fleet")
    def run_queue_fleet(body: QueueFleetRequest) -> dict[str, Any]:
        runner = BatchRunner(
            queue_store=queue,
            control_service=control,
            artifact_root=config.run.artifact_root,
            slot_limit=body.workers or config.run.max_parallel,
        )
        return runner.run_fleet(
            worker_count=body.workers or config.run.max_parallel,
            max_jobs_per_worker=body.max_jobs_per_worker,
            poll_seconds=body.poll_seconds,
            idle_timeout_seconds=body.idle_timeout_seconds,
            stale_after_seconds=body.stale_after_seconds,
            executor_kinds=body.executor_kinds,
            provider_kinds=body.provider_kinds,
            models=body.models,
            cost_tiers=body.cost_tiers,
            endpoint_classes=body.endpoint_classes,
        ).model_dump(mode="json")

    @app.post("/api/queue/jobs/{job_id}/cancel")
    def cancel_queue_job(job_id: str, body: QueueCancelRequest) -> dict[str, Any]:
        _queue_job_or_404(queue, job_id)
        job = queue.cancel(job_id, reason=body.reason)
        return job.model_dump(mode="json")

    @app.post("/api/queue/jobs/{job_id}/requeue")
    def requeue_queue_job(job_id: str) -> dict[str, Any]:
        _queue_job_or_404(queue, job_id)
        try:
            job = queue.requeue(job_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return job.model_dump(mode="json")

    @app.post("/api/queue/workers/sweep")
    def sweep_queue_workers(body: QueueSweepWorkersRequest) -> list[dict[str, Any]]:
        workers = queue.reap_stale_workers(
            stale_after_seconds=body.stale_after_seconds,
            requeue_running_jobs=body.requeue_running_jobs,
        )
        return [worker.model_dump(mode="json") for worker in workers]

    return app


def _ensure_project(expected_project_id: str, actual_project_id: str) -> None:
    if expected_project_id != actual_project_id:
        raise HTTPException(status_code=404, detail="Project not found")


def _queue_job_or_404(queue: QueueStore, job_id: str) -> QueueJob:
    job = queue.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Queue job not found")
    return job


def _summarize_task_graph(task_graph: TaskGraph) -> dict[str, int]:
    return {
        "total_nodes": len(task_graph.nodes),
        "blocked_nodes": sum(
            1 for node in task_graph.nodes if node.status is TaskStatus.BLOCKED
        ),
        "pending_nodes": sum(
            1 for node in task_graph.nodes if node.status is TaskStatus.PENDING
        ),
        "completed_nodes": sum(
            1 for node in task_graph.nodes if node.status is TaskStatus.COMPLETED
        ),
    }


def render_dashboard_html(project_id: str) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>ArchonLab Control Deck</title>
    <style>
      :root {{
        --bg: #f5efe1;
        --panel: rgba(255, 252, 245, 0.84);
        --ink: #1f2833;
        --muted: #5d6a72;
        --accent: #c4472d;
        --accent-soft: #f0c4b7;
        --line: rgba(31, 40, 51, 0.14);
        --shadow: 0 22px 50px rgba(31, 40, 51, 0.12);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        min-height: 100vh;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(196, 71, 45, 0.16), transparent 32%),
          radial-gradient(circle at right 20%, rgba(67, 119, 140, 0.14), transparent 28%),
          linear-gradient(180deg, #f6f1e5 0%, #efe6d2 100%);
        font-family: "Iosevka Aile", "IBM Plex Sans", "Avenir Next", sans-serif;
      }}
      .shell {{
        max-width: 1280px;
        margin: 0 auto;
        padding: 24px;
      }}
      .hero {{
        display: grid;
        gap: 18px;
        padding: 28px;
        border: 1px solid var(--line);
        border-radius: 28px;
        background: linear-gradient(135deg, rgba(255,255,255,0.72), rgba(255,248,238,0.84));
        box-shadow: var(--shadow);
      }}
      .eyebrow {{
        letter-spacing: 0.18em;
        text-transform: uppercase;
        font-size: 12px;
        color: var(--accent);
      }}
      h1 {{
        margin: 0;
        font-family: "Iowan Old Style", "Palatino Linotype", serif;
        font-size: clamp(32px, 5vw, 56px);
        line-height: 0.95;
      }}
      .subtitle {{
        max-width: 760px;
        color: var(--muted);
        font-size: 16px;
        line-height: 1.55;
      }}
      .grid {{
        display: grid;
        grid-template-columns: 340px minmax(0, 1fr) 320px;
        gap: 18px;
        margin-top: 18px;
      }}
      .panel {{
        border: 1px solid var(--line);
        border-radius: 24px;
        background: var(--panel);
        box-shadow: var(--shadow);
        padding: 18px;
        backdrop-filter: blur(16px);
      }}
      .panel h2 {{
        margin: 0 0 12px;
        font-size: 15px;
        text-transform: uppercase;
        letter-spacing: 0.12em;
      }}
      .status {{
        display: grid;
        gap: 10px;
      }}
      .pill {{
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 8px 12px;
        border-radius: 999px;
        background: rgba(255,255,255,0.72);
        border: 1px solid var(--line);
        font-size: 14px;
      }}
      .controls {{
        display: grid;
        gap: 10px;
        margin-top: 16px;
      }}
      button {{
        border: none;
        border-radius: 14px;
        padding: 12px 14px;
        font: inherit;
        cursor: pointer;
        color: white;
        background: linear-gradient(135deg, #c4472d, #8f2f1f);
        transition: transform 140ms ease, box-shadow 140ms ease;
        box-shadow: 0 10px 24px rgba(196, 71, 45, 0.28);
      }}
      button.secondary {{
        color: var(--ink);
        background: linear-gradient(135deg, #f4d8c9, #f6f1e6);
      }}
      button:hover {{ transform: translateY(-1px); }}
      button:disabled {{
        opacity: 0.55;
        cursor: not-allowed;
        transform: none;
        box-shadow: none;
      }}
      textarea {{
        width: 100%;
        min-height: 110px;
        resize: vertical;
        border-radius: 18px;
        border: 1px solid var(--line);
        padding: 14px;
        font: inherit;
        background: rgba(255,255,255,0.72);
      }}
      .list {{
        display: grid;
        gap: 10px;
      }}
      .run {{
        padding: 14px;
        border-radius: 18px;
        border: 1px solid var(--line);
        background: rgba(255,255,255,0.64);
        cursor: pointer;
      }}
      .run:hover {{
        border-color: rgba(196, 71, 45, 0.42);
      }}
      .run strong {{
        display: block;
        margin-bottom: 6px;
      }}
      pre {{
        margin: 0;
        padding: 16px;
        border-radius: 18px;
        overflow: auto;
        background: rgba(25, 31, 38, 0.94);
        color: #f7f3e8;
        font-family: "Iosevka", "SFMono-Regular", monospace;
        font-size: 12px;
        line-height: 1.5;
      }}
      .meta {{
        color: var(--muted);
        font-size: 13px;
      }}
      .summary-grid {{
        display: grid;
        gap: 10px;
      }}
      .board-grid {{
        display: grid;
        grid-template-columns: minmax(0, 1.45fr) minmax(320px, 0.75fr);
        gap: 18px;
        margin-top: 18px;
      }}
      .section-head {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        margin-bottom: 14px;
      }}
      .section-head h2 {{
        margin: 0;
      }}
      .chip-row {{
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }}
      .chip {{
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 6px 10px;
        border-radius: 999px;
        border: 1px solid var(--line);
        background: rgba(255,255,255,0.64);
        font-size: 12px;
        color: var(--muted);
      }}
      .board {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 12px;
      }}
      .column {{
        display: grid;
        gap: 10px;
        align-content: start;
        padding: 12px;
        min-height: 260px;
        border-radius: 20px;
        border: 1px solid var(--line);
        background: rgba(255,255,255,0.42);
      }}
      .column-head {{
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
        font-size: 12px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--muted);
      }}
      .stack {{
        display: grid;
        gap: 10px;
      }}
      .card {{
        width: 100%;
        text-align: left;
        padding: 12px;
        border-radius: 16px;
        border: 1px solid transparent;
        background: linear-gradient(135deg, rgba(255,255,255,0.88), rgba(245,236,223,0.92));
        color: var(--ink);
        box-shadow: 0 8px 18px rgba(31, 40, 51, 0.08);
      }}
      .card.active {{
        border-color: rgba(196, 71, 45, 0.55);
        box-shadow: 0 14px 28px rgba(196, 71, 45, 0.18);
      }}
      .card strong {{
        display: block;
        margin-bottom: 6px;
      }}
      .compact-controls {{
        display: grid;
        gap: 10px;
        margin: 12px 0;
      }}
      @media (max-width: 920px) {{
        .grid {{ grid-template-columns: 1fr; }}
        .board-grid {{ grid-template-columns: 1fr; }}
      }}
    </style>
  </head>
  <body>
    <div class="shell">
      <section class="hero">
        <div class="eyebrow">ArchonLab Control Deck</div>
        <h1>{project_id}</h1>
        <div class="subtitle">
          Watch runs, inspect structured artifacts, pause the project, resume it, or inject a hint
          without dropping back to raw files. This dashboard sits directly on top of the existing
          control plane and uses the same event store.
        </div>
      </section>

      <div class="grid">
        <aside class="panel">
          <h2>Project Control</h2>
          <div class="status" id="control-status"></div>
          <div class="controls">
            <button id="pause-button">Pause Project</button>
            <button class="secondary" id="resume-button">Resume Project</button>
            <textarea
              id="hint-input"
              placeholder="Write a hint for the next planning/proving cycle."
            ></textarea>
            <button id="hint-button">Inject Hint</button>
          </div>
        </aside>

        <section class="panel">
          <h2>Runs</h2>
          <div class="list" id="runs-list"></div>
          <div style="height: 16px"></div>
          <h2>Run Detail</h2>
          <div class="meta" id="detail-meta">Select a run to inspect its summary and events.</div>
          <div style="height: 10px"></div>
          <pre id="detail-json">{{}}</pre>
        </section>

        <aside class="panel">
          <h2>Queue Ops</h2>
          <div class="controls">
            <button class="secondary" id="queue-run-button">Run Pending Queue</button>
            <button class="secondary" id="queue-fleet-button">Run Auto-Slot Fleet</button>
            <button class="secondary" id="worker-sweep-button">Sweep Stale Workers</button>
          </div>
          <div style="height: 12px"></div>
          <div class="summary-grid" id="queue-summary"></div>
          <div style="height: 16px"></div>
          <h2>Workers</h2>
          <div class="list" id="workers-list"></div>
        </aside>
      </div>

      <div class="board-grid">
        <section class="panel">
          <div class="section-head">
            <h2>Queue Board</h2>
            <div class="chip-row" id="queue-counts"></div>
          </div>
          <div class="board" id="queue-board"></div>
        </section>

        <aside class="panel">
          <h2>Job Detail</h2>
          <div class="meta" id="queue-detail-meta">
            Select a queue card to inspect and operate on it.
          </div>
          <div class="compact-controls">
            <button class="secondary" id="job-requeue-button" disabled>Requeue Selected Job</button>
            <button class="secondary" id="job-cancel-button" disabled>Cancel Selected Job</button>
          </div>
          <pre id="queue-detail-json">{{}}</pre>
        </aside>
      </div>

      <section class="panel" style="margin-top: 18px;">
        <div class="section-head">
          <h2>Current Preview</h2>
          <div class="meta" id="project-preview-meta">
            Inspect the live supervisor/workflow prediction before launching the next run.
          </div>
        </div>
        <div class="chip-row" id="project-preview-chips"></div>
        <div style="height: 12px"></div>
        <pre id="project-preview-json">{{}}</pre>
      </section>
    </div>

    <script>
      const projectId = {json.dumps(project_id)};
      const controlStatus = document.getElementById("control-status");
      const runsList = document.getElementById("runs-list");
      const detailMeta = document.getElementById("detail-meta");
      const detailJson = document.getElementById("detail-json");
      const hintInput = document.getElementById("hint-input");
      const queueSummary = document.getElementById("queue-summary");
      const queueCounts = document.getElementById("queue-counts");
      const queueBoard = document.getElementById("queue-board");
      const queueDetailMeta = document.getElementById("queue-detail-meta");
      const queueDetailJson = document.getElementById("queue-detail-json");
      const projectPreviewMeta = document.getElementById("project-preview-meta");
      const projectPreviewChips = document.getElementById("project-preview-chips");
      const projectPreviewJson = document.getElementById("project-preview-json");
      const jobRequeueButton = document.getElementById("job-requeue-button");
      const jobCancelButton = document.getElementById("job-cancel-button");
      const workersList = document.getElementById("workers-list");
      let latestJobs = [];
      let selectedQueueJobId = null;

      async function fetchJson(url, options) {{
        const response = await fetch(url, options);
        if (!response.ok) {{
          const detail = await response.text();
          throw new Error(detail || response.statusText);
        }}
        return response.json();
      }}

      function renderControl(state) {{
        const hints = state.hints || [];
        const latestHint = hints.length ? hints[hints.length - 1].text : "No hints yet.";
        controlStatus.innerHTML = `
          <div class="pill">Paused: <strong>${{state.paused ? "yes" : "no"}}</strong></div>
          <div class="pill">Hints: <strong>${{hints.length}}</strong></div>
          <div class="pill">Reason: <strong>${{state.pause_reason || "none"}}</strong></div>
          <div class="meta">Latest hint: ${{latestHint}}</div>
        `;
      }}

      function renderRuns(runs) {{
        if (!runs.length) {{
          runsList.innerHTML = '<div class="meta">No runs recorded yet.</div>';
          return;
        }}
        runsList.innerHTML = "";
        for (const run of runs) {{
          const item = document.createElement("button");
          item.className = "run";
          item.innerHTML = `
            <strong>${{run.run_id}}</strong>
            <div class="meta">${{run.status}} · ${{run.workflow}} · stage=${{run.stage}}</div>
          `;
          item.addEventListener("click", async () => {{
            const detail = await fetchJson(`/api/runs/${{run.run_id}}`);
            detailMeta.textContent = `${{detail.run.run_id}} · ${{detail.events.length}} events`;
            detailJson.textContent = JSON.stringify(detail, null, 2);
          }});
          runsList.appendChild(item);
        }}
      }}

      function queueBuckets() {{
        return [
          {{ key: "queued", label: "Queued", statuses: ["queued", "pending"] }},
          {{ key: "running", label: "Running", statuses: ["running"] }},
          {{ key: "paused", label: "Paused", statuses: ["paused"] }},
          {{ key: "failed", label: "Failed", statuses: ["failed"] }},
          {{ key: "done", label: "Done", statuses: ["completed", "canceled"] }},
        ];
      }}

      function queueCountsByStatus(jobs) {{
        const counts = {{}};
        for (const job of jobs) {{
          counts[job.status] = (counts[job.status] || 0) + 1;
        }}
        return counts;
      }}

      function summarizeQueueJob(job) {{
        const preview = job.preview || {{}};
        const focus = preview.theorem_name || preview.task_title || preview.task_id || "-";
        const phase = preview.phase || "-";
        const stage = preview.stage || "-";
        const reason = preview.reason || "-";
        const priority = preview.final_priority ?? job.priority;
        const workerId = job.worker_id || "-";
        return {{
          focus,
          phase,
          stage,
          reason,
          priority,
          workerId,
          executors: (job.required_executor_kinds || []).join(",") || "-",
          providers: (job.required_provider_kinds || []).join(",") || "-",
          models: (job.required_models || []).join(",") || "-",
          costTiers: (job.required_cost_tiers || []).join(",") || "-",
          endpointClasses: (job.required_endpoint_classes || []).join(",") || "-",
        }};
      }}

      function renderQueueSummary(jobs) {{
        const counts = queueCountsByStatus(jobs);
        queueSummary.innerHTML = `
          <div class="pill">Total jobs: <strong>${{jobs.length}}</strong></div>
          <div class="pill">
            Queued: <strong>${{(counts.queued || 0) + (counts.pending || 0)}}</strong>
          </div>
          <div class="pill">Running: <strong>${{counts.running || 0}}</strong></div>
          <div class="pill">
            Blocked: <strong>${{(counts.paused || 0) + (counts.failed || 0)}}</strong>
          </div>
        `;
        const bucketCounts = queueBuckets().map((bucket) => {{
          const total = bucket.statuses.reduce((sum, status) => sum + (counts[status] || 0), 0);
          return `<div class="chip">${{bucket.label}} <strong>${{total}}</strong></div>`;
        }});
        queueCounts.innerHTML = bucketCounts.join("");
      }}

      function renderQueueBoard(jobs) {{
        if (!jobs.length) {{
          queueBoard.innerHTML = '<div class="meta">No queue jobs.</div>';
          return;
        }}
        queueBoard.innerHTML = "";
        for (const bucket of queueBuckets()) {{
          const column = document.createElement("section");
          column.className = "column";
          const cards = jobs.filter((job) => bucket.statuses.includes(job.status));
          column.innerHTML = `
            <div class="column-head">
              <span>${{bucket.label}}</span>
              <strong>${{cards.length}}</strong>
            </div>
            <div class="stack"></div>
          `;
          const stack = column.querySelector(".stack");
          if (!cards.length) {{
            stack.innerHTML = '<div class="meta">No jobs.</div>';
          }} else {{
            for (const job of cards) {{
              const summary = summarizeQueueJob(job);
              const item = document.createElement("button");
              item.className = selectedQueueJobId === job.job_id ? "card active" : "card";
              item.innerHTML = `
                <strong>${{job.project_id}}</strong>
                <div class="meta">${{job.job_id}}</div>
                <div class="meta">
                  ${{summary.phase}} · ${{summary.stage}} · p=${{summary.priority}}
                </div>
                <div class="meta">${{summary.focus}}</div>
                <div class="meta">worker=${{summary.workerId}}</div>
              `;
              item.addEventListener("click", async () => {{
                await selectQueueJob(job.job_id);
              }});
              stack.appendChild(item);
            }}
          }}
          queueBoard.appendChild(column);
        }}
      }}

      function renderQueueDetail(job) {{
        if (!job) {{
          queueDetailMeta.textContent = "Select a queue card to inspect and operate on it.";
          queueDetailJson.textContent = JSON.stringify({{}}, null, 2);
          jobRequeueButton.disabled = true;
          jobCancelButton.disabled = true;
          return;
        }}
        const summary = summarizeQueueJob(job);
        queueDetailMeta.textContent =
          `${{job.job_id}} · ${{job.status}} · phase=${{summary.phase}} · focus=${{summary.focus}}`;
        queueDetailJson.textContent = JSON.stringify(job, null, 2);
        jobRequeueButton.disabled = ["queued", "pending", "running"].includes(job.status);
        jobCancelButton.disabled = ["completed", "canceled"].includes(job.status);
      }}

      async function selectQueueJob(jobId) {{
        selectedQueueJobId = jobId;
        const detail = await fetchJson(`/api/queue/jobs/${{jobId}}`);
        latestJobs = latestJobs.map((job) => (job.job_id === jobId ? detail : job));
        renderQueue(latestJobs);
      }}

      function renderQueue(jobs) {{
        latestJobs = jobs;
        if (!selectedQueueJobId || !jobs.some((job) => job.job_id === selectedQueueJobId)) {{
          selectedQueueJobId = jobs.length ? jobs[0].job_id : null;
        }}
        renderQueueSummary(jobs);
        renderQueueBoard(jobs);
        renderQueueDetail(
          jobs.find((job) => job.job_id === selectedQueueJobId) || null,
        );
      }}

      function renderProjectPreview(payload) {{
        const preview = payload.preview || {{}};
        const action = preview.action || {{}};
        const supervisor = preview.supervisor || {{}};
        const executor = preview.resolved_executor || {{}};
        const provider = preview.resolved_provider || {{}};
        const graph = payload.task_graph_summary || {{}};
        const workflowSpec = payload.workflow_spec;
        const chips = [
          ["workflow", payload.workflow || "-"],
          ["phase", action.phase || "-"],
          ["reason", action.reason || "-"],
          ["stage", action.stage || preview.progress?.stage || "-"],
          ["supervisor", `${{supervisor.action || "-"}}/${{supervisor.reason || "-"}}`],
          ["executor", executor.kind || "-"],
          ["model", provider.model || "-"],
          ["cost_tier", provider.cost_tier || "-"],
          ["task_graph", `${{graph.total_nodes || 0}} nodes / ${{graph.blocked_nodes || 0}} blocked`],
        ];
        if (workflowSpec) {{
          chips.push(["workflow_spec", `${{workflowSpec.name}} (${{workflowSpec.rule_count}} rules)`]);
        }}
        projectPreviewMeta.textContent =
          `${{payload.project_id}} · ${{action.phase || "-"}} · ${{action.reason || "-"}}`;
        projectPreviewChips.innerHTML = chips
          .map(([label, value]) => `<div class="chip">${{label}} <strong>${{value}}</strong></div>`)
          .join("");
        projectPreviewJson.textContent = JSON.stringify(payload, null, 2);
      }}

      function renderProjectPreviewError(message) {{
        projectPreviewMeta.textContent = "Current preview is unavailable.";
        projectPreviewChips.innerHTML = '<div class="chip">error <strong>preview_failed</strong></div>';
        projectPreviewJson.textContent = message;
      }}

      function renderWorkers(workers) {{
        if (!workers.length) {{
          workersList.innerHTML = '<div class="meta">No worker telemetry yet.</div>';
          return;
        }}
        workersList.innerHTML = "";
        for (const worker of workers) {{
          const item = document.createElement("div");
          item.className = "run";
          const currentJob = worker.current_job_id || "-";
          const heartbeatAge = worker.heartbeat_age_seconds == null
            ? "-"
            : `${{worker.heartbeat_age_seconds.toFixed(1)}}s`;
          const stale = worker.stale ? " · stale" : "";
          const executors = (worker.executor_kinds || []).join(",") || "-";
          const models = (worker.models || []).join(",") || "-";
          const costTiers = (worker.cost_tiers || []).join(",") || "-";
          item.innerHTML = `
            <strong>${{worker.worker_id}}</strong>
            <div class="meta">slot=${{worker.slot_index}} · ${{worker.status}}${{stale}}</div>
            <div class="meta">current=${{currentJob}}</div>
            <div class="meta">processed=${{worker.processed_jobs}}</div>
            <div class="meta">failed=${{worker.failed_jobs}}</div>
            <div class="meta">executors=${{executors}}</div>
            <div class="meta">models=${{models}} · cost_tiers=${{costTiers}}</div>
            <div class="meta">heartbeat_age=${{heartbeatAge}}</div>
          `;
          workersList.appendChild(item);
        }}
      }}

      async function refresh() {{
        const previewPromise = fetchJson(`/api/projects/${{projectId}}/preview`)
          .catch((error) => ({{ error: error.message }}));
        const [control, runs, jobs, workers, projectPreview] = await Promise.all([
          fetchJson(`/api/projects/${{projectId}}/control`),
          fetchJson(`/api/runs?limit=20`),
          fetchJson(`/api/queue/jobs?limit=20`),
          fetchJson(`/api/queue/workers`),
          previewPromise,
        ]);
        renderControl(control);
        renderRuns(runs);
        renderQueue(jobs);
        renderWorkers(workers);
        if (projectPreview.error) {{
          renderProjectPreviewError(projectPreview.error);
        }} else {{
          renderProjectPreview(projectPreview);
        }}
      }}

      document.getElementById("pause-button").addEventListener("click", async () => {{
        await fetchJson(`/api/projects/${{projectId}}/pause`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ reason: "Paused from dashboard" }}),
        }});
        await refresh();
      }});

      document.getElementById("resume-button").addEventListener("click", async () => {{
        await fetchJson(`/api/projects/${{projectId}}/resume`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{}}),
        }});
        await refresh();
      }});

      document.getElementById("hint-button").addEventListener("click", async () => {{
        const text = hintInput.value.trim();
        if (!text) {{
          return;
        }}
        await fetchJson(`/api/projects/${{projectId}}/hint`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ text, author: "dashboard" }}),
        }});
        hintInput.value = "";
        await refresh();
      }});

      document.getElementById("queue-run-button").addEventListener("click", async () => {{
        await fetchJson(`/api/queue/run`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{}}),
        }});
        await refresh();
      }});

      document.getElementById("queue-fleet-button").addEventListener("click", async () => {{
        await fetchJson(`/api/queue/fleet`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{}}),
        }});
        await refresh();
      }});

      jobRequeueButton.addEventListener("click", async () => {{
        if (!selectedQueueJobId) {{
          return;
        }}
        await fetchJson(`/api/queue/jobs/${{selectedQueueJobId}}/requeue`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{}}),
        }});
        await refresh();
      }});

      jobCancelButton.addEventListener("click", async () => {{
        if (!selectedQueueJobId) {{
          return;
        }}
        await fetchJson(`/api/queue/jobs/${{selectedQueueJobId}}/cancel`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ reason: "Canceled from dashboard" }}),
        }});
        await refresh();
      }});

      document.getElementById("worker-sweep-button").addEventListener("click", async () => {{
        await fetchJson(`/api/queue/workers/sweep`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify({{ stale_after_seconds: 120, requeue_running_jobs: true }}),
        }});
        await refresh();
      }});

      refresh().catch((error) => {{
        detailMeta.textContent = "Dashboard failed to load.";
        detailJson.textContent = error.message;
      }});
    </script>
  </body>
</html>"""
