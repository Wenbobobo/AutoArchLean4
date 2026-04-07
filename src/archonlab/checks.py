from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from pydantic import BaseModel

from .lean_analyzer import (
    CommandLeanAnalyzer,
    RegexLeanAnalyzer,
    build_lean_analyzer,
    collect_lean_analysis,
)
from .models import LeanAnalyzerConfig, LeanAnalyzerKind, ProjectConfig


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


def _lean_analyzer_detail(config: LeanAnalyzerConfig | None) -> str:
    if config is None or config.kind is LeanAnalyzerKind.REGEX:
        return RegexLeanAnalyzer.__name__
    command = " ".join(config.command) if config.command else "<missing command>"
    return f"{CommandLeanAnalyzer.__name__} ({command})"


def _resolve_command_path(command: list[str]) -> str | None:
    if not command:
        return None
    executable = command[0]
    command_path = Path(executable).expanduser()
    if command_path.is_absolute() or "/" in executable:
        return str(command_path.resolve()) if command_path.exists() else None
    return shutil.which(executable)


def _smoke_check_lean_analyzer(
    project: ProjectConfig,
    config: LeanAnalyzerConfig | None,
) -> PathStatus:
    detail = _lean_analyzer_detail(config)
    if config is None or config.kind is LeanAnalyzerKind.REGEX:
        return PathStatus(name="lean_analyzer", ok=True, detail=detail)
    if not config.command:
        return PathStatus(name="lean_analyzer", ok=False, detail=detail)

    command_path = _resolve_command_path(config.command)
    if not project.project_path.exists() or not project.archon_path.exists():
        if command_path is None:
            detail = f"{detail} | executable=missing"
            return PathStatus(name="lean_analyzer", ok=False, detail=detail)
        detail = f"{detail} | executable={command_path} | smoke=skipped"
        return PathStatus(name="lean_analyzer", ok=True, detail=detail)

    snapshot = collect_lean_analysis(
        project_path=project.project_path,
        archon_path=project.archon_path,
        analyzer=build_lean_analyzer(config),
    )
    detail = (
        f"{detail} | backend={snapshot.backend} | theorem_count={snapshot.theorem_count} | "
        f"proof_gaps={len(snapshot.proof_gaps)} | diagnostics={len(snapshot.diagnostics)} | "
        f"fallback={'yes' if snapshot.fallback_used else 'no'}"
    )
    if snapshot.fallback_reason:
        detail = f"{detail} | fallback_reason={snapshot.fallback_reason}"
    return PathStatus(
        name="lean_analyzer",
        ok=not snapshot.fallback_used,
        detail=detail,
    )


def gather_doctor_report(
    project: ProjectConfig | None = None,
    *,
    lean_analyzer: LeanAnalyzerConfig | None = None,
) -> DoctorReport:
    tools = [
        _tool_status("uv", required=True),
        _tool_status("python3.12", required=True),
        _tool_status("git", required=True),
        _tool_status("elan", required=True),
        _tool_status("lean", required=True),
        _tool_status("lake", required=True),
        _tool_status("codex", required=False),
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
        paths.append(_smoke_check_lean_analyzer(project, lean_analyzer))

    return DoctorReport(
        python_version=_read_version("python3.12") or "unavailable",
        tools=tools,
        paths=paths,
    )
