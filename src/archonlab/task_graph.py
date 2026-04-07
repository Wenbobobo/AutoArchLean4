from __future__ import annotations

import re
from pathlib import Path

from .adapter import ArchonAdapter
from .lean_analyzer import LeanAnalyzer, collect_lean_analysis
from .models import ProjectConfig, TaskEdge, TaskGraph, TaskNode, TaskSource, TaskStatus

OBJECTIVE_PATTERN = re.compile(
    r"\*\*(?P<file>[^*]+)\*\*\s*[-—]\s*(?:fill\s+theorem\s+)?`(?P<theorem>[^`]+)`"
)


def build_task_graph(
    *,
    project_path: Path,
    archon_path: Path,
    analyzer: LeanAnalyzer | None = None,
) -> TaskGraph:
    resolved_project_path = project_path.resolve()
    resolved_archon_path = archon_path.resolve()
    project = ProjectConfig(
        name=resolved_project_path.name,
        project_path=resolved_project_path,
        archon_path=resolved_archon_path,
    )
    adapter = ArchonAdapter(project)
    adapter.ensure_valid()
    progress = adapter.read_progress()
    analysis = collect_lean_analysis(
        project_path=resolved_project_path,
        archon_path=resolved_archon_path,
        analyzer=analyzer,
    )

    nodes: dict[str, TaskNode] = {}
    edges: list[TaskEdge] = []
    theorem_to_id: dict[str, str] = {}

    for index, objective in enumerate(progress.objectives, start=1):
        objective_node = _objective_to_node(objective, index=index)
        nodes[objective_node.id] = objective_node

    for declaration in analysis.declarations:
        status = TaskStatus.BLOCKED if declaration.blocked_by_sorry else TaskStatus.COMPLETED
        node_id = f"lean:{declaration.file_path}:{declaration.name}"
        nodes[node_id] = TaskNode(
            id=node_id,
            title=declaration.name,
            status=status,
            sources=[TaskSource.LEAN_DECLARATION],
            file_path=declaration.file_path,
            theorem_name=declaration.name,
            priority=0,
            blockers=["contains_sorry"] if status is TaskStatus.BLOCKED else [],
            metadata={"declaration_kind": declaration.declaration_kind},
        )
        theorem_to_id[declaration.name] = node_id

    for declaration in analysis.declarations:
        source_id = theorem_to_id.get(declaration.name)
        if source_id is None:
            continue
        for dependency in declaration.dependencies:
            dependency_id = theorem_to_id.get(dependency)
            if dependency_id is None or dependency_id == source_id:
                continue
            edges.append(
                TaskEdge(
                    source_id=source_id,
                    target_id=dependency_id,
                    kind="depends_on",
                )
            )

    for objective_node in list(nodes.values()):
        if TaskSource.OBJECTIVE not in objective_node.sources:
            continue
        if objective_node.file_path is None or objective_node.theorem_name is None:
            continue
        target_id = f"lean:{objective_node.file_path}:{objective_node.theorem_name}"
        target_node = nodes.get(target_id)
        if target_node is None:
            continue
        target_node.priority += 1
        target_node.sources = sorted(
            set(target_node.sources + [TaskSource.OBJECTIVE]),
            key=lambda source: source.value,
        )
        edges.append(
            TaskEdge(
                source_id=objective_node.id,
                target_id=target_id,
                kind="objective_targets_theorem",
            )
        )

    return TaskGraph(
        project_id=project.name,
        nodes=sorted(nodes.values(), key=lambda node: node.id),
        edges=edges,
    )


def _objective_to_node(objective: str, *, index: int) -> TaskNode:
    match = OBJECTIVE_PATTERN.search(objective.replace("—", "-"))
    if not match:
        return TaskNode(
            id=f"objective:{index}",
            title=objective,
            status=TaskStatus.PENDING,
            sources=[TaskSource.OBJECTIVE],
            priority=1,
        )
    file_path = Path(match.group("file"))
    theorem_name = match.group("theorem")
    return TaskNode(
        id=f"objective:{index}",
        title=theorem_name,
        status=TaskStatus.PENDING,
        sources=[TaskSource.OBJECTIVE],
        file_path=file_path,
        theorem_name=theorem_name,
        priority=1,
        metadata={"raw_objective": objective},
    )
