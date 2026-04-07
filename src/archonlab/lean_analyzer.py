from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Protocol

from .models import (
    LeanAnalysisSnapshot,
    LeanAnalyzerConfig,
    LeanAnalyzerKind,
    LeanDeclaration,
)

DECL_PATTERN = re.compile(r"^\s*(theorem|lemma|example)\s+([A-Za-z0-9_'.]+)", re.MULTILINE)
THEOREM_PATTERN = re.compile(r"\b(?:theorem|lemma|example)\b")
SORRY_PATTERN = re.compile(r"\bsorry\b")
AXIOM_PATTERN = re.compile(r"\baxiom\b")


class LeanAnalyzer(Protocol):
    def analyze(self, *, project_path: Path, archon_path: Path) -> LeanAnalysisSnapshot: ...


class RegexLeanAnalyzer:
    def analyze(self, *, project_path: Path, archon_path: Path) -> LeanAnalysisSnapshot:
        del archon_path
        resolved_project_path = project_path.resolve()
        declarations: list[LeanDeclaration] = []
        declaration_blocks: dict[str, str] = {}
        lean_file_count = 0
        theorem_count = 0
        sorry_count = 0
        axiom_count = 0

        for lean_file in sorted(resolved_project_path.rglob("*.lean")):
            content = lean_file.read_text(encoding="utf-8")
            relative_file = lean_file.relative_to(resolved_project_path)
            lean_file_count += 1
            theorem_count += len(THEOREM_PATTERN.findall(content))
            sorry_count += len(SORRY_PATTERN.findall(content))
            axiom_count += len(AXIOM_PATTERN.findall(content))
            matches = list(DECL_PATTERN.finditer(content))
            for index, match in enumerate(matches):
                start = match.start()
                end = matches[index + 1].start() if index + 1 < len(matches) else len(content)
                block = content[start:end]
                name = match.group(2)
                declarations.append(
                    LeanDeclaration(
                        name=name,
                        file_path=relative_file,
                        declaration_kind=match.group(1),
                        blocked_by_sorry="sorry" in block,
                        uses_axiom="axiom" in block,
                    )
                )
                declaration_blocks[name] = block

        known_names = {declaration.name for declaration in declarations}
        declarations = [
            declaration.model_copy(
                update={
                    "dependencies": sorted(
                        dependency
                        for dependency in known_names
                        if dependency != declaration.name
                        and re.search(
                            rf"\b{re.escape(dependency)}\b",
                            declaration_blocks[declaration.name],
                        )
                    )
                }
            )
            for declaration in declarations
        ]
        return LeanAnalysisSnapshot(
            project_id=resolved_project_path.name,
            project_path=resolved_project_path,
            backend="regex",
            declarations=declarations,
            lean_file_count=lean_file_count,
            theorem_count=theorem_count,
            sorry_count=sorry_count,
            axiom_count=axiom_count,
        )


class CommandLeanAnalyzer:
    def __init__(
        self,
        *,
        command: list[str],
        timeout_seconds: int = 60,
    ) -> None:
        if not command:
            raise ValueError("lean_analyzer.command is required for command analyzers")
        self.command = command
        self.timeout_seconds = timeout_seconds

    def analyze(self, *, project_path: Path, archon_path: Path) -> LeanAnalysisSnapshot:
        completed = subprocess.run(
            [
                *self.command,
                "--project-path",
                str(project_path),
                "--archon-path",
                str(archon_path),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=self.timeout_seconds,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip() or "unknown error"
            raise RuntimeError(f"Lean analyzer command failed: {detail}")
        payload = completed.stdout.strip()
        if not payload:
            raise RuntimeError("Lean analyzer command returned no output")
        raw_snapshot = json.loads(payload)
        snapshot = LeanAnalysisSnapshot.model_validate(raw_snapshot)
        return snapshot.model_copy(
            update={
                "backend": str(raw_snapshot.get("backend", "command")),
                "fallback_used": bool(raw_snapshot.get("fallback_used", False)),
                "fallback_reason": raw_snapshot.get("fallback_reason"),
            }
        )


def build_lean_analyzer(config: LeanAnalyzerConfig | None = None) -> LeanAnalyzer:
    if config is None or config.kind is LeanAnalyzerKind.REGEX:
        return RegexLeanAnalyzer()
    if config.kind is LeanAnalyzerKind.COMMAND:
        return CommandLeanAnalyzer(
            command=config.command,
            timeout_seconds=config.timeout_seconds,
        )
    raise ValueError(f"Unsupported lean analyzer kind: {config.kind.value}")


def collect_lean_analysis(
    *,
    project_path: Path,
    archon_path: Path,
    analyzer: LeanAnalyzer | None = None,
) -> LeanAnalysisSnapshot:
    primary_analyzer = analyzer or RegexLeanAnalyzer()
    try:
        return primary_analyzer.analyze(
            project_path=project_path,
            archon_path=archon_path,
        )
    except Exception as exc:  # noqa: BLE001
        if isinstance(primary_analyzer, RegexLeanAnalyzer):
            raise
        fallback_snapshot = RegexLeanAnalyzer().analyze(
            project_path=project_path,
            archon_path=archon_path,
        )
        return fallback_snapshot.model_copy(
            update={
                "fallback_used": True,
                "fallback_reason": str(exc),
            }
        )
