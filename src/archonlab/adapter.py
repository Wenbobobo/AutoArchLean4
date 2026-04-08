from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

from .models import (
    AdapterAction,
    ChecklistItem,
    ExecutionIngestionResult,
    ProgressSnapshot,
    ProjectConfig,
    WorkflowMode,
)


def bootstrap_archon_project_state(project_path: Path) -> Path:
    resolved_project_path = project_path.resolve()
    state_dir = resolved_project_path / ".archon"
    prompts_dir = state_dir / "prompts"
    task_results_dir = state_dir / "task_results"
    proof_journal_dir = state_dir / "proof-journal" / "sessions"
    resolved_project_path.mkdir(parents=True, exist_ok=True)
    prompts_dir.mkdir(parents=True, exist_ok=True)
    task_results_dir.mkdir(parents=True, exist_ok=True)
    proof_journal_dir.mkdir(parents=True, exist_ok=True)

    lean_files = sorted(resolved_project_path.glob("*.lean"))
    objective_target = lean_files[0].name if lean_files else "Core.lean"

    default_files: dict[Path, str] = {
        state_dir / "CLAUDE.md": (
            "# Archon Project Role\n\n"
            "Follow the phase-specific prompt files under `.archon/prompts/`.\n"
            "Keep project state in `.archon/` and treat `PROGRESS.md` as the source of truth.\n"
        ),
        state_dir / "PROGRESS.md": (
            "# Project Progress\n\n"
            "## Current Stage\n"
            "prover\n\n"
            "## Stages\n"
            "- [x] init\n"
            "- [ ] prover\n"
            "- [ ] review\n\n"
            "## Current Objectives\n\n"
            f"1. Inspect `{objective_target}` and choose the next theorem bottleneck.\n"
        ),
        state_dir / "USER_HINTS.md": (
            "# User Hints\n\n"
            "Record operator hints here when they are injected from the control plane.\n"
        ),
        prompts_dir / "plan.md": (
            "# Planning Prompt\n\n"
            "Review the current Lean declarations, blockers, and objectives.\n"
            "Choose the next focused proving step and record why it is highest leverage.\n"
        ),
        prompts_dir / "prover-prover.md": (
            "# Prover Prompt\n\n"
            "Focus on the selected theorem or declaration.\n"
            "Prefer sound proof steps and avoid introducing axioms unless explicitly requested.\n"
        ),
        prompts_dir / "review.md": (
            "# Review Prompt\n\n"
            "Review the latest task result, proof changes, and failure modes.\n"
            "Summarize regressions, remaining blockers, and next corrective action.\n"
        ),
    }
    for path, content in default_files.items():
        if not path.exists():
            path.write_text(content, encoding="utf-8")
    return state_dir


class ArchonAdapter:
    def __init__(self, project: ProjectConfig) -> None:
        self.project = project
        self.state_dir = self.project.project_path / ".archon"
        self.progress_file = self.state_dir / "PROGRESS.md"

    def validate(self) -> list[str]:
        issues: list[str] = []
        if not self.project.archon_path.exists():
            issues.append(f"Archon path does not exist: {self.project.archon_path}")
        if not (self.project.archon_path / "archon-loop.sh").exists():
            issues.append(f"archon-loop.sh not found under: {self.project.archon_path}")
        if not self.project.project_path.exists():
            issues.append(f"Lean project does not exist: {self.project.project_path}")
        if not self.progress_file.exists():
            issues.append(f"Missing .archon/PROGRESS.md: {self.progress_file}")
        return issues

    def ensure_valid(self) -> None:
        bootstrap_archon_project_state(self.project.project_path)
        issues = self.validate()
        if issues:
            raise FileNotFoundError("; ".join(issues))

    def read_progress(self) -> ProgressSnapshot:
        content = self.progress_file.read_text(encoding="utf-8")
        stage_match = re.search(r"## Current Stage\s*\n\s*(\S+)", content)
        stage = stage_match.group(1) if stage_match else "init"

        objectives: list[str] = []
        obj_match = re.search(r"## Current Objectives\s*\n([\s\S]*?)(?=\n## |\n# |$)", content)
        if obj_match:
            for line in obj_match.group(1).splitlines():
                item_match = re.match(r"\s*\d+\.\s+(.+)", line)
                if item_match:
                    objectives.append(item_match.group(1).strip())

        checklist: list[ChecklistItem] = []
        stages_match = re.search(r"## Stages\s*\n([\s\S]*?)(?=\n## |\n# |$)", content)
        if stages_match:
            for line in stages_match.group(1).splitlines():
                checklist_match = re.match(r"\s*-\s*\[([ xX])\]\s*(.+)", line)
                if checklist_match:
                    checklist.append(
                        ChecklistItem(
                            label=checklist_match.group(2).strip(),
                            done=checklist_match.group(1) != " ",
                        )
                    )
        return ProgressSnapshot(stage=stage, objectives=objectives, checklist=checklist)

    def list_task_results(self) -> list[Path]:
        task_results_dir = self.state_dir / "task_results"
        if not task_results_dir.exists():
            return []
        return sorted(task_results_dir.glob("*.md"))

    def list_review_sessions(self) -> list[Path]:
        sessions_dir = self.state_dir / "proof-journal" / "sessions"
        if not sessions_dir.exists():
            return []
        return sorted(path for path in sessions_dir.iterdir() if path.is_dir())

    def choose_next_action(
        self, workflow: WorkflowMode, progress: ProgressSnapshot
    ) -> AdapterAction:
        if progress.stage == "COMPLETE":
            return AdapterAction(phase="stop", reason="project_complete", stage=progress.stage)

        if self.list_task_results():
            phase = "plan"
            reason = "unprocessed_task_results"
        elif workflow is WorkflowMode.FIXED_LOOP:
            phase = "plan"
            reason = "fixed_loop_baseline"
        elif not self.list_review_sessions():
            phase = "plan"
            reason = "bootstrap_first_iteration"
        else:
            phase = "prover"
            reason = "no_pending_results"

        prompt_preview = self.build_prompt(phase=phase, stage=progress.stage)
        return AdapterAction(
            phase=phase,
            reason=reason,
            stage=progress.stage,
            prompt_preview=prompt_preview,
        )

    def build_prompt(self, *, phase: str, stage: str) -> str:
        if phase == "plan":
            return (
                "You are the plan agent for project "
                f"'{self.project.name}'. Current stage: {stage}.\n"
                f"Project directory: {self.project.project_path}\n"
                f"Project state directory: {self.state_dir}\n"
                f"Read {self.state_dir / 'CLAUDE.md'} for your role, then read "
                f"{self.state_dir / 'prompts' / 'plan.md'} and {self.progress_file}.\n"
                "All state files (PROGRESS.md, task_pending.md, task_done.md, "
                f"USER_HINTS.md, task_results/) are in {self.state_dir}.\n"
                f"The .lean files are in {self.project.project_path}.\n"
            )
        if phase == "prover":
            return (
                "You are the prover agent for project "
                f"'{self.project.name}'. Current stage: {stage}.\n"
                f"Project directory: {self.project.project_path}\n"
                f"Project state directory: {self.state_dir}\n"
                f"Read {self.state_dir / 'CLAUDE.md'} for your role, then read "
                f"{self.state_dir / 'prompts' / f'prover-{stage}.md'} and {self.progress_file}.\n"
                f"All state files are in {self.state_dir}, including "
                f"{self.state_dir / 'USER_HINTS.md'} when present. "
                f"The .lean files are in {self.project.project_path}.\n"
            )
        if phase == "review":
            return (
                "You are the review agent for project "
                f"'{self.project.name}'. Current stage: {stage}.\n"
                f"Project directory: {self.project.project_path}\n"
                f"Project state directory: {self.state_dir}\n"
                f"Read {self.state_dir / 'CLAUDE.md'} for your role, then read "
                f"{self.state_dir / 'prompts' / 'review.md'} and "
                f"{self.state_dir / 'USER_HINTS.md'} when present.\n"
            )
        return "No prompt is required because the project is complete.\n"

    def ingest_execution_output(
        self,
        *,
        run_id: str,
        phase: str,
        response_text: str,
        task_id: str | None = None,
        task_title: str | None = None,
    ) -> ExecutionIngestionResult | None:
        body = response_text.strip()
        if not body:
            return None
        self.ensure_valid()
        normalized_phase = phase.strip().lower()
        if normalized_phase == "prover":
            task_result_path = self._write_task_result(
                run_id=run_id,
                phase=normalized_phase,
                response_text=body,
                task_id=task_id,
                task_title=task_title,
            )
            return ExecutionIngestionResult(
                phase=normalized_phase,
                task_result_path=task_result_path.resolve(),
            )
        if normalized_phase in {"plan", "review"}:
            session_dir = self._write_proof_journal_session(
                run_id=run_id,
                phase=normalized_phase,
                response_text=body,
                task_id=task_id,
                task_title=task_title,
            )
            archived_task_results = self._archive_task_results(session_dir)
            return ExecutionIngestionResult(
                phase=normalized_phase,
                proof_journal_session_path=session_dir.resolve(),
                archived_task_results=[path.resolve() for path in archived_task_results],
            )
        return None

    def _write_task_result(
        self,
        *,
        run_id: str,
        phase: str,
        response_text: str,
        task_id: str | None,
        task_title: str | None,
    ) -> Path:
        task_results_dir = self.state_dir / "task_results"
        task_results_dir.mkdir(parents=True, exist_ok=True)
        path = task_results_dir / f"{run_id}.md"
        path.write_text(
            self._format_markdown_output(
                title="Task Result",
                run_id=run_id,
                phase=phase,
                response_text=response_text,
                task_id=task_id,
                task_title=task_title,
            ),
            encoding="utf-8",
        )
        return path

    def _write_proof_journal_session(
        self,
        *,
        run_id: str,
        phase: str,
        response_text: str,
        task_id: str | None,
        task_title: str | None,
    ) -> Path:
        session_dir = self.state_dir / "proof-journal" / "sessions" / run_id
        session_dir.mkdir(parents=True, exist_ok=True)
        (session_dir / f"{phase}.md").write_text(
            self._format_markdown_output(
                title=f"{phase.title()} Output",
                run_id=run_id,
                phase=phase,
                response_text=response_text,
                task_id=task_id,
                task_title=task_title,
            ),
            encoding="utf-8",
        )
        return session_dir

    def _archive_task_results(self, session_dir: Path) -> list[Path]:
        task_results_dir = self.state_dir / "task_results"
        if not task_results_dir.exists():
            return []
        pending = sorted(task_results_dir.glob("*.md"))
        if not pending:
            return []
        archive_dir = session_dir / "task-results"
        archive_dir.mkdir(parents=True, exist_ok=True)
        archived_paths: list[Path] = []
        for path in pending:
            archived_path = archive_dir / path.name
            path.replace(archived_path)
            archived_paths.append(archived_path)
        return archived_paths

    @staticmethod
    def _format_markdown_output(
        *,
        title: str,
        run_id: str,
        phase: str,
        response_text: str,
        task_id: str | None,
        task_title: str | None,
    ) -> str:
        generated_at = datetime.now(UTC).isoformat()
        lines = [
            f"# {title}",
            "",
            f"- run_id: {run_id}",
            f"- phase: {phase}",
            f"- generated_at: {generated_at}",
        ]
        if task_id is not None:
            lines.append(f"- task_id: {task_id}")
        if task_title is not None:
            lines.append(f"- task_title: {task_title}")
        lines.extend(["", response_text, ""])
        return "\n".join(lines)
