# Operator Console Guide

这份手册面向第一次真正使用 ArchonLab control plane 的人。
目标不是解释所有内部实现，而是让你知道：

- 先看哪里
- 什么时候该点 `enqueue/resume`
- 什么时候应该停下来查 blocked reason
- 高级 benchmark / replay 应该怎么接到日常自治 loop

如果你已经完成环境配置，建议和 [第一次运行](./first-run.md) 配合阅读。

## 1. 心智模型

先记住一句话：

- `Archon` 是证明后端引擎
- `ArchonLab` 是压在它上面的 control plane

你日常真正操作的是 ArchonLab 的 Mission Console，也就是 dashboard 的三段主视图：

1. `Plan`
   看当前项目为什么会走到下一步，必要时暂停、恢复、注入 hint、切 workflow。
2. `Loop`
   看 workspace/session/queue/worker/provider/fleet 的整体健康度。
3. `Finish`
   看最近运行结果、loop outcome，以及高级 experiment / replay 抽屉。

最重要的原则：

- daemon 负责“持续跑”
- queue/fleet 负责“怎么调度”
- Mission Console 负责“为什么现在这样跑、现在卡在哪、最近产出了什么”

## 2. 推荐操作顺序

### 一套可直接复制的启动命令

下面假设你已经在本机装好了 `uv`、Python 3.12、Lean 和 Archon。
如果当前目录下还没有 `workspace.toml`，先生成它；
不要直接运行 `workspace daemon run --config workspace.toml`，否则会报
`Path 'workspace.toml' does not exist`。

```bash
cd /home/niracler/Gary/Math/archonlab

export PROJECT_PATH=/absolute/path/to/your/lean-project
export ARCHON_PATH=/absolute/path/to/your/Archon

uv run archonlab workspace init \
  --project-path "$PROJECT_PATH" \
  --archon-path "$ARCHON_PATH" \
  --config-path workspace.toml

uv run archonlab workspace status --config workspace.toml
```

启动 dashboard。

- 本机访问：`--host 127.0.0.1`
- 远程访问：`--host 0.0.0.0`

```bash
cd /home/niracler/Gary/Math/archonlab
uv run archonlab dashboard serve --config workspace.toml --host 0.0.0.0 --port 8000
```

新开一个终端，启动 daemon：

```bash
cd /home/niracler/Gary/Math/archonlab
uv run archonlab workspace daemon run --config workspace.toml
```

可选：如果你想更快看到 queue / worker 变化，再开一个终端启动 fleet：

```bash
cd /home/niracler/Gary/Math/archonlab
uv run archonlab queue fleet --config workspace.toml --workers 2
```

打开浏览器：

```text
http://127.0.0.1:8000
```

远程访问时改成：

```text
http://<your-host-or-tailscale-ip>:8000
```

### 默认会跑什么

如果你只运行上面这套命令，系统默认跑的是 workspace 自治 session：

- job kind: `session_quantum`
- workflow: `adaptive_loop`
- mode: `dry_run = true`
- first likely phase: `plan`
- first likely reason: `bootstrap_first_iteration`

它不会自动开始 benchmark。
只有你显式运行 `benchmark run` 或把 benchmark 入队，才会产生 `benchmark_project` 作业。

### 第一次接管一个 workspace

1. 先检查环境和配置

```bash
uv run archonlab doctor
uv run archonlab workspace status --config workspace.toml
```

2. 启动 dashboard

```bash
uv run archonlab dashboard serve --config workspace.toml --port 8000
```

3. 启动 daemon，让系统持续自治

```bash
uv run archonlab workspace daemon run --config workspace.toml
```

4. 打开 dashboard，按这个顺序看：

- `Plan`
- `Loop`
- `Finish`

### 只想人工推进一轮

不用 daemon，直接：

```bash
uv run archonlab workspace enqueue --config workspace.toml
uv run archonlab workspace resume --config workspace.toml
uv run archonlab queue fleet --config workspace.toml --workers 2
```

这适合调试某个项目或验证新的 workflow / provider policy。

## 3. 打开 Dashboard 后先看什么

### 先看 `Plan`

这里回答的是：“系统下一步为什么要这么做”。

优先关注：

- `Current Preview`
- `Focus Task`
- `Supervisor`
- `Workflow Rules`
- `Lean Analysis`

如果 `Current Preview` 和你的预期差很多，先改 workflow / task policy，再继续扩 worker。

### 再看 `Loop`

这里回答的是：“自治循环现在健康不健康”。

优先关注：

- `blocked_sessions`
- `blocked_detail`
- `provider health`
- `daemon status`
- `latest loop / fleet stop reason`
- `queue board` 里的 phase / reason / focus / priority

常见 blocked reason：

- `failure_cooldown_active`
  说明这个 session 最近失败，系统正在等 cooldown 结束。
- `failure_budget_exhausted`
  说明连续失败次数达到上限，不应继续自动重试。
- `control_paused`
  说明是操作员或控制面主动暂停。
- `budget_exhausted`
  说明 iteration 预算耗尽。

如果你看到 blocked session，先判断它属于哪一种，再决定是否人工 resume。

如果 queue 很满但没什么 progress，通常问题不是“没在跑”，而是：

- worker capability 不匹配
- provider 不健康
- session 被 cooldown / budget 挡住

### 最后看 `Finish`

这里回答的是：“最近到底产出了什么，有没有真实进展”。

默认先看：

- `Recent Runs`
- `Loop Outcomes`

只有当你已经确认 loop 本身健康，才进入高级 experiment / replay 抽屉做 theorem-level 对比。

## 4. 什么时候用 Enqueue / Resume

### 用 `Enqueue Workspace`

适合：

- 还没有 session 或 queue 很空
- 想让符合 tag 的项目进入排队

### 用 `Resume Sessions`

适合：

- session 已存在，但处于 `paused/failed` 后可恢复状态
- 你确认这不是 `failure_budget_exhausted`

如果 session 处于 `failure_cooldown_active`，一般不需要立刻手动 resume；
让 daemon 等 cooldown 到期再继续，通常更稳。

## 5. Workspace Daemon 该怎么理解

daemon 不是 worker。

它做的是：

1. 定期触发一次 workspace loop
2. 让 loop 负责 admission
3. 让 fleet 负责起 worker
4. 把 stop reason 持久化到状态文件和 event store

常见 stop reason：

- `queue_drained`
  目前没有待处理 queue job
- `capacity_unavailable`
  provider pool 没有可用 member
- `failure_cooldown_active`
  session 因冷却时间暂时不能继续
- `operator_stop_requested`
  操作员主动停止

当前版本里，daemon 遇到 `failure_cooldown_active` 时会按最早 cooldown 结束时间回退，而不是固定频率空转。

## 6. Finish 里的 Advanced Experiment And Replay 怎么用

建议顺序：

1. 在 `Run Index` 里选一轮 benchmark
2. 看 `Run Detail`
3. 自动填充 ledger / summary
4. 用 `Compare` 看 theorem-level `improved/regressed`
5. 用 `Replay` 回放具体 theorem

不要只看总 score。

对形式化证明平台来说，更关键的是：

- 哪些 theorem 变成 `proved`
- 哪些 theorem 退化成 `contains_sorry`
- 有没有引入 `uses_axiom`
- 哪类失败在反复出现

## 7. 推荐的日常值守动作

### 正常巡检

看这四个信号就够了：

- blocked session 数量有没有上升
- provider health 是否 degraded / quarantined
- queue 是否长期堆积但 worker 没处理
- recent runs / loop outcomes 是否开始反复无进展

### 出现停滞时

优先顺序：

1. 看 `Loop` 里的 blocked reason 和 provider health
2. 看 `Plan` 里的 supervisor reason / focus task
4. 再决定是否：
   - 调 workflow
   - 调 provider / execution policy
   - 手工 resume
   - 做 experiment replay

## 8. 当前最适合的使用方式

如果你是研究平台 operator，而不是单次跑 demo，推荐：

1. `workspace daemon run`
2. dashboard 常开，并按 `Plan -> Loop -> Finish` 巡检
3. 先用 `Loop` 保证自治主链路健康
4. 再用 `Finish` 里的 experiment / replay 做 theorem-level 对比

一句话总结：

先看 `Plan` 判断“下一步为什么会这么做”，
再看 `Loop` 判断“系统为什么停/慢/卡”，
最后才去 `Finish` 判断“这套策略到底有没有带来 theorem 级进展”。
