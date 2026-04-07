# ArchonLab

ArchonLab 是一个面向大规模形式化证明的外置编排器项目。它把 `Archon` 当作后端 proving engine，自己负责更高一层的研究控制平面、任务调度、评测、监督和回放。

默认技术栈：
- `uv`
- `Python 3.12`
- TDD
- `git worktree`
- `subagents`

## 你会得到什么

- 一套适合门外汉的 Lean/Archon 入门文档
- 一个可迭代的外置 orchestrator
- 一个可重复的 benchmark 和回放体系
- 一个面向 theorem/task DAG 的控制平面

## 先读什么

1. [Phase Roadmap](docs/roadmap/phase1-5.md)
2. [环境安装](docs/onboarding/environment.md)
3. [Lean 心智模型](docs/onboarding/lean-mental-model.md)
4. [Archon 原理](docs/onboarding/archon-principles.md)
5. [第一次运行](docs/onboarding/first-run.md)
6. [常见问题](docs/onboarding/troubleshooting.md)

## 建议工作方式

- 先跑通环境，再谈架构升级
- 所有功能先写测试，再写实现
- 控制平面优先于看板
- 先做小规模 benchmark，再扩展到多项目并行
- 充分使用 `subagents` 和 `git worktree` 做隔离并行

## 当前命令

```bash
uv run archonlab doctor
uv run archonlab project init --project-path /path/to/lean-project --archon-path /path/to/Archon
uv run archonlab run start --config archonlab.toml --dry-run
uv run archonlab benchmark run --manifest benchmarks/smoke.example.toml --dry-run
```

当前 `run start` 和 `benchmark run` 都支持可回放的 dry-run 基线。
真实执行仍然依赖可用的 `claude` CLI。
