from __future__ import annotations

import re
from pathlib import Path

from .adapter import ArchonAdapter
from .lean_analyzer import LeanAnalyzer, collect_lean_analysis
from .models import (
    LeanDeclaration,
    ProjectConfig,
    TaskEdge,
    TaskGraph,
    TaskNode,
    TaskSource,
    TaskStatus,
)

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
        node_id = f"lean:{declaration.file_path}:{declaration.name}"
        nodes[node_id] = TaskNode(
            id=node_id,
            title=declaration.name,
            status=TaskStatus.COMPLETED,
            sources=[TaskSource.LEAN_DECLARATION],
            file_path=declaration.file_path,
            theorem_name=declaration.name,
            priority=0,
            blockers=_direct_declaration_blockers(declaration),
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

    dependency_map = {
        node_id: {
            edge.target_id
            for edge in edges
            if edge.kind == "depends_on" and edge.source_id == node_id
        }
        for node_id in theorem_to_id.values()
    }
    declaration_nodes = {
        node_id: nodes[node_id]
        for node_id in theorem_to_id.values()
        if node_id in nodes
    }
    blocker_cache: dict[str, list[str]] = {}
    for node_id, node in declaration_nodes.items():
        blockers = _resolve_declaration_blockers(
            node_id=node_id,
            declaration_nodes=declaration_nodes,
            dependency_map=dependency_map,
            blocker_cache=blocker_cache,
        )
        nodes[node_id] = node.model_copy(
            update={
                "status": TaskStatus.BLOCKED if blockers else TaskStatus.COMPLETED,
                "blockers": blockers,
            }
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


def _direct_declaration_blockers(declaration: LeanDeclaration) -> list[str]:
    blockers: list[str] = []
    if declaration.blocked_by_sorry:
        blockers.append("contains_sorry")
    if declaration.uses_axiom:
        blockers.append("uses_axiom")
    return blockers


def _resolve_declaration_blockers(
    *,
    node_id: str,
    declaration_nodes: dict[str, TaskNode],
    dependency_map: dict[str, set[str]],
    blocker_cache: dict[str, list[str]],
    visiting: set[str] | None = None,
) -> list[str]:
    if node_id in blocker_cache:
        return blocker_cache[node_id]
    active_visiting = visiting or set()
    if node_id in active_visiting:
        return ["cyclic_dependency"]

    active_visiting.add(node_id)
    node = declaration_nodes[node_id]
    blockers = list(node.blockers)
    for dependency_id in sorted(dependency_map.get(node_id, set())):
        dependency_node = declaration_nodes.get(dependency_id)
        if dependency_node is None:
            continue
        dependency_blockers = _resolve_declaration_blockers(
            node_id=dependency_id,
            declaration_nodes=declaration_nodes,
            dependency_map=dependency_map,
            blocker_cache=blocker_cache,
            visiting=active_visiting,
        )
        if not dependency_blockers:
            continue
        blockers.append(f"depends_on:{dependency_node.theorem_name or dependency_node.title}")
        blockers.extend(dependency_blockers)
    active_visiting.remove(node_id)
    resolved = _dedupe_preserve_order(blockers)
    blocker_cache[node_id] = resolved
    return resolved


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered
