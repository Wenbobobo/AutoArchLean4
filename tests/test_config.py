from __future__ import annotations

from pathlib import Path

from archonlab.config import init_config, load_config
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
