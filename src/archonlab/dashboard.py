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
from .queue import QueueStore


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

    @app.get("/api/queue/workers")
    def list_queue_workers() -> list[dict[str, Any]]:
        return [worker.model_dump(mode="json") for worker in queue.list_workers()]

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

    @app.post("/api/queue/jobs/{job_id}/cancel")
    def cancel_queue_job(job_id: str, body: QueueCancelRequest) -> dict[str, Any]:
        job = queue.cancel(job_id, reason=body.reason)
        return job.model_dump(mode="json")

    return app


def _ensure_project(expected_project_id: str, actual_project_id: str) -> None:
    if expected_project_id != actual_project_id:
        raise HTTPException(status_code=404, detail="Project not found")


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
      @media (max-width: 920px) {{
        .grid {{ grid-template-columns: 1fr; }}
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
          <h2>Queue</h2>
          <div class="controls">
            <button class="secondary" id="queue-run-button">Run Pending Queue</button>
          </div>
          <div style="height: 12px"></div>
          <div class="list" id="queue-list"></div>
          <div style="height: 16px"></div>
          <h2>Workers</h2>
          <div class="list" id="workers-list"></div>
        </aside>
      </div>
    </div>

    <script>
      const projectId = {json.dumps(project_id)};
      const controlStatus = document.getElementById("control-status");
      const runsList = document.getElementById("runs-list");
      const detailMeta = document.getElementById("detail-meta");
      const detailJson = document.getElementById("detail-json");
      const hintInput = document.getElementById("hint-input");
      const queueList = document.getElementById("queue-list");
      const workersList = document.getElementById("workers-list");

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

      function renderQueue(jobs) {{
        if (!jobs.length) {{
          queueList.innerHTML = '<div class="meta">No queued jobs.</div>';
          return;
        }}
        queueList.innerHTML = "";
        for (const job of jobs) {{
          const item = document.createElement("div");
          item.className = "run";
          const workerId = job.worker_id || "-";
          item.innerHTML = `
            <strong>${{job.job_id}}</strong>
            <div class="meta">${{job.status}} · ${{job.project_id}} · worker=${{workerId}}</div>
          `;
          queueList.appendChild(item);
        }}
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
          item.innerHTML = `
            <strong>${{worker.worker_id}}</strong>
            <div class="meta">slot=${{worker.slot_index}} · ${{worker.status}}</div>
            <div class="meta">current=${{currentJob}}</div>
            <div class="meta">processed=${{worker.processed_jobs}}</div>
            <div class="meta">failed=${{worker.failed_jobs}}</div>
          `;
          workersList.appendChild(item);
        }}
      }}

      async function refresh() {{
        const [control, runs, jobs, workers] = await Promise.all([
          fetchJson(`/api/projects/${{projectId}}/control`),
          fetchJson(`/api/runs?limit=20`),
          fetchJson(`/api/queue/jobs?limit=20`),
          fetchJson(`/api/queue/workers`),
        ]);
        renderControl(control);
        renderRuns(runs);
        renderQueue(jobs);
        renderWorkers(workers);
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

      refresh().catch((error) => {{
        detailMeta.textContent = "Dashboard failed to load.";
        detailJson.textContent = error.message;
      }});
    </script>
  </body>
</html>"""
