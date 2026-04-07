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
uv run archonlab project init --project-path /path/to/lean-project --archon-path /path/to/Archon --executor-kind codex_exec --model gpt-5.4-mini --codex-auto-approve
uv run archonlab run start --config archonlab.toml --dry-run
uv run archonlab run start --config archonlab.toml --execute
uv run archonlab benchmark run --manifest benchmarks/smoke.example.toml --dry-run
uv run archonlab benchmark run --manifest benchmarks/smoke.example.toml --use-worktrees --worker-slots 4
uv run archonlab queue enqueue-benchmark --config archonlab.toml --manifest benchmarks/smoke.example.toml
uv run archonlab queue run --config archonlab.toml --slots 4
uv run archonlab queue fleet --config archonlab.toml --workers 4
uv run archonlab queue fleet --config archonlab.toml --workers 2 --executor-kinds dry_run,codex_exec
uv run archonlab queue fleet --config archonlab.toml --workers 2 --models gpt-5.4-mini --cost-tiers cheap --endpoint-classes fast
uv run archonlab queue worker --config archonlab.toml --slot-index 1 --max-jobs 10
uv run archonlab queue worker --config archonlab.toml --auto-slot --max-jobs 10
uv run archonlab queue sweep-workers --config archonlab.toml --stale-after-seconds 120
uv run archonlab queue status --config archonlab.toml
uv run archonlab queue workers --config archonlab.toml
uv run archonlab control pause --config archonlab.toml --reason "manual_hold"
uv run archonlab control hint --config archonlab.toml --text "Try `rw` before `simp`."
uv run archonlab dashboard serve --config archonlab.toml --port 8000
uv run archonlab worktree create --repo-path /path/to/repo --name phase4-run
```

当前 `run start` 和 `benchmark run` 都支持可回放的 dry-run 基线，也支持真实执行。
真实执行现在有两条主路径：
- `codex exec`
- OpenAI-compatible HTTP endpoint

## 当前结构化产物

- `run-summary.json`: 项目配置、进度、下一步动作
- `task-graph.json`: 从 objectives 和 Lean 声明提取的 task graph
- `supervisor.json`: stuck/健康状态判断与建议动作
- `summary.json`: benchmark 级别的统一回放摘要

`run start` 现在会用 `task-graph.json` 和 `supervisor.json` 共同选择下一步动作，
而不再只是复用最早的固定启发式。
supervisor 也会读取同一项目的近期历史事件，识别重复无进展的 loop。
benchmark 则已经支持在隔离 `git worktree` 中运行。
queue/batch 层已经支持 benchmark 作业排队、slot-aware 并发处理、pause-aware 跳过和 job 级 artifacts。
benchmark project 入队前现在会先做一次 `preview()`，按当前预测的 next action 派生 executor/provider 约束和 job priority。
queue job 现在还会持久化一份精简的 preview 摘要，dashboard/API 能直接解释当前 phase、reason、focus task 和 priority 组成。
queue worker 现在会留下可查询的 lease / heartbeat / 当前 job telemetry。
你现在还可以单独启动 `queue worker` 进程，让多个外部 worker 共享同一个 sqlite 队列。
独立 worker 现在支持 `--auto-slot`，可以自动抢占当前空闲 slot，而不必人工分配编号。
stale worker 现在也可以被显式 sweep，并把遗留的运行中 job 重新入队。

## 执行器配置

`archonlab.toml` 现在支持独立的 `[executor]` 和 `[provider]` 段。

```toml
[executor]
kind = "codex_exec" # dry_run | codex_exec | openai_compatible
command = "codex"
auto_approve = true
timeout_seconds = 600

[provider]
model = "gpt-5.4-mini"
cost_tier = "cheap"
endpoint_class = "lab"
base_url = "http://127.0.0.1:8000/v1"
api_key_env = "OPENAI_API_KEY"
endpoint_path = "/v1/responses"
```

如果你想走 OpenAI-compatible HTTP，通常把 `kind = "openai_compatible"` 并填好 `[provider]` 即可。
如果你想让本地 agent 真正接管工作树，优先用 `codex_exec`。

## Phase Policy

你现在还可以按 `plan / prover / review` 分别选 executor 或 model。

```toml
[phase_executor.plan]
kind = "dry_run"

[phase_provider.plan]
model = "gpt-5.4-mini"

[phase_executor.prover]
kind = "openai_compatible"

[phase_provider.prover]
model = "gpt-5.4"

[phase_executor.review]
kind = "codex_exec"

[phase_provider.review]
model = "gpt-5.4-mini"
```

常见用法是：
- `plan` 走便宜模型
- `prover` 走更强模型或 OpenAI-compatible endpoint
- `review` 走 `codex_exec`

## Task Policy

如果 phase 级策略还不够细，可以继续按 theorem/task 匹配 executor/provider。

```toml
[task_matcher.core_focus]
phase = "prover"
file_path_pattern = "Core\\.lean$"
theorem_pattern = "^foo$"
task_status = "blocked"
task_sources = ["lean_declaration"]
min_priority = 2
blocker_pattern = "contains_sorry"
objective_relevant = true

[task_executor.core_focus]
kind = "codex_exec"

[task_provider.core_focus]
model = "gpt-5.4"
```

常见用法是：
- 只让关键 theorem 走更强模型
- 对某个文件或 theorem family 单独切换 provider
- 保持 `plan/review` 便宜，把预算集中到最难的 prover task
- 对 objective 关键路径、带特定 blocker 的 theorem 单独升级 executor

## Worker Pool

有两种跑法：

- `queue run --slots 4`
  适合单机内快速起一个本地线程池。
- `queue fleet --workers 4`
  适合启动一组 auto-slot worker，行为上更接近真实 worker farm。
- `queue fleet --workers 2 --executor-kinds dry_run,codex_exec`
  适合按 executor 能力声明启动 resource-aware worker pool。
- `queue fleet --workers 2 --models gpt-5.4-mini --cost-tiers cheap`
  适合把低成本模型 worker 和高成本模型 worker 分开。
- `queue worker --slot-index N`
  适合起多个独立 worker 进程，共享同一个队列数据库。
- `queue worker --auto-slot`
  适合 worker 数量不固定时自动认领空闲 slot。
- `queue sweep-workers --stale-after-seconds 120`
  适合清理崩溃或失联 worker，并把它们占住的运行中 job 重新入队。

`queue worker` 模式下，每个 worker 都会：
- 注册自己的 lease
- 周期性 heartbeat
- 记录 `current_job_id` / `last_job_id`
- 使用自己的 `queue-worktrees/<worker_id>` 根目录
- 暴露自己的 executor/provider capabilities，并只 claim 自己能跑的 job
- 进一步暴露 `model / cost_tier / endpoint_class`，支持更细粒度的资源调度
- dashboard / API 可直接显示每个 job 的调度解释，而不只是资源标签

## Workflow DSL

- 示例文件见 [review-on-stuck.example.toml](/home/niracler/Gary/Math/archonlab/workflows/review-on-stuck.example.toml)
- 可以在 `archonlab.toml` 的 `[run]` 段里加 `workflow_spec = "./workflows/review-on-stuck.example.toml"`
- 当前规则支持按 `supervisor reason`、`focus task status`、当前 phase、是否有 task results、是否有 review sessions 覆盖下一步动作
