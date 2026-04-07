from __future__ import annotations

import re
from pathlib import Path

from .models import AdapterAction, ChecklistItem, ProgressSnapshot, ProjectConfig, WorkflowMode


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
                f"All state files are in {self.state_dir}. "
                f"The .lean files are in {self.project.project_path}.\n"
            )
        if phase == "review":
            return (
                "You are the review agent for project "
                f"'{self.project.name}'. Current stage: {stage}.\n"
                f"Project directory: {self.project.project_path}\n"
                f"Project state directory: {self.state_dir}\n"
                f"Read {self.state_dir / 'CLAUDE.md'} for your role, then read "
                f"{self.state_dir / 'prompts' / 'review.md'}.\n"
            )
        return "No prompt is required because the project is complete.\n"
