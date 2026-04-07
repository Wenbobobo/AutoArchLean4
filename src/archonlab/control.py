from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from .models import ControlState, HintRecord, ProjectConfig


class ControlService:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.state_dir = self.root / "control"
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def read(self, project: ProjectConfig) -> ControlState:
        path = self._state_path(project)
        if not path.exists():
            return ControlState(project_id=project.name)
        return ControlState.model_validate_json(path.read_text(encoding="utf-8"))

    def pause(self, project: ProjectConfig, *, reason: str | None = None) -> ControlState:
        state = self.read(project).model_copy(
            update={
                "paused": True,
                "pause_reason": reason,
                "updated_at": datetime.now(UTC),
            }
        )
        self._write(project, state)
        return state

    def resume(self, project: ProjectConfig) -> ControlState:
        state = self.read(project).model_copy(
            update={
                "paused": False,
                "pause_reason": None,
                "updated_at": datetime.now(UTC),
            }
        )
        self._write(project, state)
        return state

    def add_hint(
        self,
        project: ProjectConfig,
        *,
        text: str,
        author: str = "user",
    ) -> ControlState:
        hint = HintRecord(text=text, author=author)
        state = self.read(project)
        updated_state = state.model_copy(
            update={
                "hints": [*state.hints, hint],
                "updated_at": datetime.now(UTC),
            }
        )
        self._write(project, updated_state)
        hints_path = project.project_path / ".archon" / "USER_HINTS.md"
        hints_path.parent.mkdir(parents=True, exist_ok=True)
        with hints_path.open("a", encoding="utf-8") as handle:
            handle.write(
                f"- [{hint.ts.isoformat()}] ({hint.author}) {hint.text.strip()}\n"
            )
        return updated_state

    def _write(self, project: ProjectConfig, state: ControlState) -> None:
        self._state_path(project).write_text(
            json.dumps(state.model_dump(mode="json"), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _state_path(self, project: ProjectConfig) -> Path:
        return self.state_dir / f"{project.name}.json"
