from __future__ import annotations

import shutil
import subprocess

from pydantic import BaseModel

from .models import ProjectConfig


class ToolStatus(BaseModel):
    name: str
    required: bool
    available: bool
    path: str | None = None
    version: str | None = None


class PathStatus(BaseModel):
    name: str
    ok: bool
    detail: str


class DoctorReport(BaseModel):
    python_version: str
    tools: list[ToolStatus]
    paths: list[PathStatus]


def _read_version(executable: str) -> str | None:
    try:
        proc = subprocess.run(
            [executable, "--version"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    text = proc.stdout.strip() or proc.stderr.strip()
    return text.splitlines()[0] if text else None


def _tool_status(name: str, *, required: bool) -> ToolStatus:
    path = shutil.which(name)
    if not path:
        return ToolStatus(name=name, required=required, available=False)
    return ToolStatus(
        name=name,
        required=required,
        available=True,
        path=path,
        version=_read_version(path),
    )


def gather_doctor_report(project: ProjectConfig | None = None) -> DoctorReport:
    tools = [
        _tool_status("uv", required=True),
        _tool_status("python3.12", required=True),
        _tool_status("git", required=True),
        _tool_status("elan", required=True),
        _tool_status("lean", required=True),
        _tool_status("lake", required=True),
        _tool_status("claude", required=False),
    ]

    paths: list[PathStatus] = []
    if project is not None:
        archon_root = project.archon_path
        paths.extend(
            [
                PathStatus(
                    name="archon_root",
                    ok=archon_root.exists(),
                    detail=str(archon_root),
                ),
                PathStatus(
                    name="archon_loop",
                    ok=(archon_root / "archon-loop.sh").exists(),
                    detail=str(archon_root / "archon-loop.sh"),
                ),
                PathStatus(
                    name="project_root",
                    ok=project.project_path.exists(),
                    detail=str(project.project_path),
                ),
                PathStatus(
                    name="archon_state",
                    ok=(project.project_path / ".archon" / "PROGRESS.md").exists(),
                    detail=str(project.project_path / ".archon" / "PROGRESS.md"),
                ),
            ]
        )

    return DoctorReport(
        python_version=_read_version("python3.12") or "unavailable",
        tools=tools,
        paths=paths,
    )
