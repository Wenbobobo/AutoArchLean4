from __future__ import annotations

import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

from .models import WorktreeLease


class WorktreeManager:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root

    def create(self, repo_path: Path, *, name: str | None = None) -> WorktreeLease:
        resolved_repo = repo_path.resolve()
        top_level = self._git_output(resolved_repo, "rev-parse", "--show-toplevel")
        head_sha = self._git_output(resolved_repo, "rev-parse", "HEAD")
        lease_id = name or f"wt-{uuid.uuid4().hex[:8]}"
        parent = (
            self.root.resolve()
            if self.root is not None
            else resolved_repo / ".archonlab-worktrees"
        )
        worktree_path = parent / lease_id
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        self._git_run(
            resolved_repo,
            "worktree",
            "add",
            "--detach",
            str(worktree_path),
            head_sha,
        )
        return WorktreeLease(
            lease_id=lease_id,
            repo_path=Path(top_level),
            worktree_path=worktree_path,
            head_sha=head_sha,
            created_at=datetime.now(UTC),
        )

    def release(self, lease: WorktreeLease, *, force: bool = False) -> None:
        command = ["worktree", "remove"]
        if force:
            command.append("--force")
        command.append(str(lease.worktree_path))
        self._git_run(lease.repo_path, *command)

    @staticmethod
    def _git_output(repo_path: Path, *args: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(repo_path), *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    @classmethod
    def _git_run(cls, repo_path: Path, *args: str) -> None:
        cls._git_output(repo_path, *args)
