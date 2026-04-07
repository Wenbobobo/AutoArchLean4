from __future__ import annotations

from pathlib import Path

from archonlab.config import (
    build_workspace_project_app_config,
    init_config,
    init_workspace_config,
    load_config,
    load_workspace_config,
)
from archonlab.models import WorkflowMode


def test_load_config_resolves_relative_paths(tmp_path: Path) -> None:
    project_path = tmp_path / "LeanProject"
    archon_path = tmp_path / "Archon"
    project_path.mkdir()
    archon_path.mkdir()
    config_path = tmp_path / "archonlab.toml"
    config_path.write_text(
        "[project]\n"
        'name = "demo"\n'
        'project_path = "./LeanProject"\n'
        'archon_path = "./Archon"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'workflow_spec = "./workflow.toml"\n'
        'artifact_root = "./artifacts"\n',
        encoding="utf-8",
    )
    (tmp_path / "workflow.toml").write_text("[workflow]\nname = \"demo\"\n", encoding="utf-8")

    config = load_config(config_path)

    assert config.project.project_path == project_path.resolve()
    assert config.project.archon_path == archon_path.resolve()
    assert config.run.artifact_root == (tmp_path / "artifacts").resolve()
    assert config.run.workflow_spec == (tmp_path / "workflow.toml").resolve()


def test_init_config_writes_expected_template(tmp_path: Path) -> None:
    config_path = tmp_path / "archonlab.toml"
    init_config(
        config_path=config_path,
        project_path=tmp_path / "LeanProject",
        archon_path=tmp_path / "Archon",
        artifact_root=Path("artifacts"),
        workflow=WorkflowMode.FIXED_LOOP,
        dry_run=True,
    )

    content = config_path.read_text(encoding="utf-8")
    assert 'workflow = "fixed_loop"' in content
    assert "dry_run = true" in content


def test_load_workspace_config_resolves_relative_paths(tmp_path: Path) -> None:
    alpha_project = tmp_path / "AlphaProject"
    beta_project = tmp_path / "BetaProject"
    archon_path = tmp_path / "Archon"
    alpha_project.mkdir()
    beta_project.mkdir()
    archon_path.mkdir()
    workflow_spec = tmp_path / "workspace-workflow.toml"
    workflow_spec.write_text("[workflow]\nname = \"workspace\"\n", encoding="utf-8")
    beta_workflow_spec = tmp_path / "beta-workflow.toml"
    beta_workflow_spec.write_text("[workflow]\nname = \"beta\"\n", encoding="utf-8")
    config_path = tmp_path / "workspace.toml"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'workflow_spec = "./workspace-workflow.toml"\n'
        'artifact_root = "./artifacts"\n'
        "max_iterations = 12\n"
        "dry_run = true\n\n"
        "[[projects]]\n"
        'id = "alpha"\n'
        'project_path = "./AlphaProject"\n'
        'archon_path = "./Archon"\n\n'
        "[[projects]]\n"
        'id = "beta"\n'
        'project_path = "./BetaProject"\n'
        'archon_path = "./Archon"\n'
        'workflow = "fixed_loop"\n'
        'workflow_spec = "./beta-workflow.toml"\n'
        "max_iterations = 5\n"
        "dry_run = false\n",
        encoding="utf-8",
    )

    workspace = load_workspace_config(config_path)

    assert workspace.name == "demo-workspace"
    assert workspace.run.artifact_root == (tmp_path / "artifacts").resolve()
    assert workspace.run.workflow_spec == workflow_spec.resolve()
    assert workspace.projects[0].project_path == alpha_project.resolve()
    assert workspace.projects[1].workflow_spec == beta_workflow_spec.resolve()


def test_build_workspace_project_app_config_applies_project_overrides(tmp_path: Path) -> None:
    alpha_project = tmp_path / "AlphaProject"
    archon_path = tmp_path / "Archon"
    alpha_project.mkdir()
    archon_path.mkdir()
    workflow_spec = tmp_path / "workspace-workflow.toml"
    workflow_spec.write_text("[workflow]\nname = \"workspace\"\n", encoding="utf-8")
    project_workflow_spec = tmp_path / "alpha-workflow.toml"
    project_workflow_spec.write_text("[workflow]\nname = \"alpha\"\n", encoding="utf-8")
    config_path = tmp_path / "workspace.toml"
    config_path.write_text(
        "[workspace]\n"
        'name = "demo-workspace"\n\n'
        "[run]\n"
        'workflow = "adaptive_loop"\n'
        'workflow_spec = "./workspace-workflow.toml"\n'
        'artifact_root = "./artifacts"\n'
        "max_iterations = 12\n"
        "dry_run = true\n\n"
        "[[projects]]\n"
        'id = "alpha"\n'
        'project_path = "./AlphaProject"\n'
        'archon_path = "./Archon"\n'
        'workflow = "fixed_loop"\n'
        'workflow_spec = "./alpha-workflow.toml"\n'
        "max_iterations = 5\n"
        "dry_run = false\n",
        encoding="utf-8",
    )

    workspace = load_workspace_config(config_path)
    app_config = build_workspace_project_app_config(workspace, project_id="alpha")

    assert app_config.project.name == "alpha"
    assert app_config.run.workflow is WorkflowMode.FIXED_LOOP
    assert app_config.run.workflow_spec == project_workflow_spec.resolve()
    assert app_config.run.max_iterations == 5
    assert app_config.run.dry_run is False


def test_init_workspace_config_writes_expected_template(tmp_path: Path) -> None:
    config_path = tmp_path / "workspace.toml"
    init_workspace_config(
        config_path=config_path,
        workspace_name="demo-workspace",
        project_id="alpha",
        project_path=tmp_path / "AlphaProject",
        archon_path=tmp_path / "Archon",
        artifact_root=Path("artifacts"),
        workflow=WorkflowMode.FIXED_LOOP,
        dry_run=True,
    )

    content = config_path.read_text(encoding="utf-8")
    assert '[workspace]\nname = "demo-workspace"' in content
    assert '[[projects]]\nid = "alpha"' in content
    assert 'workflow = "fixed_loop"' in content
