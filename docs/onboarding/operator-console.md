# Operator Console Guide

这份手册面向第一次真正使用 ArchonLab control plane 的人。
目标不是解释所有内部实现，而是让你知道：

- 先看哪里
- 什么时候该点 `enqueue/resume`
- 什么时候应该停下来查 blocked reason
- benchmark / replay 应该怎么接到日常自治 loop

如果你已经完成环境配置，建议和 [第一次运行](./first-run.md) 配合阅读。

## 1. 心智模型

ArchonLab 现在有三层操作面：

1. `workspace daemon`
   负责无人值守地反复触发 workspace loop。
2. `dashboard`
   负责实时观察 session、queue、worker、provider、benchmark 结果。
3. `benchmark lab`
   负责比较不同策略的 theorem-level 结果，并回放单个 theorem。

最重要的原则：

- daemon 负责“持续跑”
- queue/fleet 负责“怎么调度”
- dashboard 负责“为什么现在这样跑”

## 2. 推荐操作顺序

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

4. 打开 dashboard，优先看：

- `Workspace Overview`
- `Provider Health`
- `Queue Board`
- `Current Preview`

### 只想人工推进一轮

不用 daemon，直接：

```bash
uv run archonlab workspace enqueue --config workspace.toml
uv run archonlab workspace resume --config workspace.toml
uv run archonlab queue fleet --config workspace.toml --workers 2
```

这适合调试某个项目或验证新的 workflow / provider policy。

## 3. 打开 Dashboard 后先看什么

### Workspace Overview

这是最重要的总览。

优先关注：

- `blocked_sessions`
- `blocked_detail`
- `provider health`
- `daemon status`
- `latest loop / fleet stop reason`

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

### Queue Board

这里回答两个问题：

- 现在系统到底在排什么
- 为什么某个 job 会被这些 worker 处理

重点看：

- `phase`
- `reason`
- `focus task / theorem`
- `priority`
- `executor/provider/model/cost`

如果 queue 很满但没什么 progress，通常问题不是“没在跑”，而是：

- worker capability 不匹配
- provider 不健康
- session 被 cooldown / budget 挡住

### Current Preview

这里用来确认“下一步为什么会做这件事”。

重点看：

- `Focus Task`
- `Supervisor`
- `Workflow Rules`
- `Lean Analysis`

如果 `Current Preview` 和你的预期差很多，先改 workflow / task policy，再继续扩 worker。

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

## 6. Benchmark Lab 怎么用

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
- benchmark compare 是否出现 theorem regression

### 出现停滞时

优先顺序：

1. 看 `Workspace Overview` 的 blocked reason
2. 看 `Provider Health`
3. 看 `Current Preview` 的 supervisor reason
4. 再决定是否：
   - 调 workflow
   - 调 provider / execution policy
   - 手工 resume
   - 做 benchmark replay

## 8. 当前最适合的使用方式

如果你是研究平台 operator，而不是单次跑 demo，推荐：

1. `workspace daemon run`
2. dashboard 常开
3. 用 benchmark lab 比较策略版本
4. 用 replay 研究 theorem regression

一句话总结：

先看 workspace 总览判断“为什么系统现在停/慢/卡”，
再看 preview 判断“下一步为什么会这么做”，
最后才去 benchmark lab 判断“这套策略到底有没有带来 theorem 级进展”。
