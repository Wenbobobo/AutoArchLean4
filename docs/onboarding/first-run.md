# 第一次运行

这份流程的目标是让你第一次就能看到完整闭环，而不是一上来就被复杂性淹没。

## 你要先完成什么

1. 安装环境
2. 确认 Lean 工具链可用
3. 确认 Archon 能初始化一个 Lean 项目
4. 跑一次最小 smoke test
5. 再启动 orchestrator

## 推荐流程

```bash
# 1. 检查环境
uv run archonlab doctor

# 2. 初始化 Archon 项目
./init.sh /path/to/lean-project

# 3. 跑 Lean 侧诊断
/lean4:doctor

# 4. 初始化 ArchonLab 配置
uv run archonlab project init --project-path /path/to/lean-project --archon-path /path/to/Archon

# 5. 启动最小 dry-run
uv run archonlab run start --config archonlab.toml --dry-run

# 6. 如果要无人值守真实执行，优先走 codex exec
uv run archonlab project init --project-path /path/to/lean-project --archon-path /path/to/Archon --config-path archonlab.exec.toml --executor-kind codex_exec --model gpt-5.4-mini --codex-auto-approve
uv run archonlab run start --config archonlab.exec.toml --execute

# 7. 跑一个 benchmark smoke test
uv run archonlab benchmark run --manifest benchmarks/smoke.example.toml --dry-run --worker-slots 2

# 8. 看当前 queue worker telemetry
uv run archonlab queue workers --config archonlab.toml

# 9. 如果你想分离成独立 worker 进程
uv run archonlab queue worker --config archonlab.toml --slot-index 1 --max-jobs 1

# 9b. 如果你不想手工分配 worker slot
uv run archonlab queue worker --config archonlab.toml --auto-slot --max-jobs 1

# 9c. 如果你想一次拉起一组 auto-slot worker
uv run archonlab queue fleet --config archonlab.toml --workers 2

# 9c-b. 如果你只想让 fleet 接特定 executor 类型的 job
uv run archonlab queue fleet --config archonlab.toml --workers 2 --executor-kinds dry_run,codex_exec

# 9c-c. 如果你想把 cheap 和 premium 模型 worker 分开
uv run archonlab queue fleet --config archonlab.toml --workers 2 --models gpt-5.4-mini --cost-tiers cheap --endpoint-classes fast

# 9d. 如果 worker 异常退出，清理 stale lease
uv run archonlab queue sweep-workers --config archonlab.toml --stale-after-seconds 120

# 10. 启动控制面 dashboard
uv run archonlab dashboard serve --config archonlab.toml --port 8000

# 10b. 如果你想临时切 workflow，而不改配置文件
uv run archonlab control workflow --config archonlab.toml --workflow fixed_loop --clear-workflow-spec
uv run archonlab control clear-workflow --config archonlab.toml

# 11. 如果要直接走 Archon 固定 loop，再手动启动
./archon-loop.sh /path/to/lean-project
```

## 你第一次应该观察什么

- `PROGRESS.md` 有没有变化
- `task_results/` 有没有产出
- `proof-journal/` 有没有 session
- Lean 文件里的 `sorry` 有没有减少
- `artifacts/` 里有没有产生可回放的 run / benchmark 结果
- dashboard 里能不能暂停、恢复、注入 hint
- dashboard / CLI 里能不能临时切 workflow override，再恢复默认
- `execution.json` 里有没有真实 executor 输出
- `queue workers` 能不能看到 slot / current job / processed 统计
- 独立 `queue worker` 跑完后，worker 是否进入 `stopped`
- `--auto-slot` 启动的 worker 能不能自动拿到空闲 slot
- `queue fleet` 能不能拉起多个 auto-slot worker 并吃掉队列
- capability 受限的 worker/fleet 会不会只 claim 自己能执行的 job
- model/cost-tier 受限的 worker/fleet 会不会把 cheap/premium 任务分流
- stale worker sweep 之后，遗留 job 是否重新进入队列
- 日志里是正常推进还是反复 stuck

## 你第一次不用做什么

- 不用理解所有 prompt
- 不用一开始就改 workflow
- 不用追求多 agent 并发
- 不用先做大规模 benchmark

## 判断是否“跑通”

满足下面任意一条，通常说明流程已经开始工作：
- `doctor` 通过
- `archonlab run start --dry-run` 产出 `run-summary.json`
- `archonlab benchmark run --dry-run` 产出 `summary.json`
- `archon-loop.sh` 能启动并写日志
- `task_results/` 出现新的结果文件
- dashboard 能看到运行状态
