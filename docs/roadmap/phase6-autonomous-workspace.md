# Phase 6+: Workspace, Session Loop, and Autonomous Scale

这份路线图承接 [Phase 1-5](./phase1-5.md)。
前一阶段的目标是把外置 orchestrator 的最小控制平面跑通；
这一阶段的目标是把 ArchonLab 从“能跑一次”推进到“能持续自治扩展”。

## 当前定位

Phase 1-5 已经把这些基线打通：
- Lean onboarding、`uv`、Python 3.12、基本文档
- `run start` / `benchmark run` / `queue worker` / dashboard
- task graph、supervisor、workflow DSL、execution policy
- worker lease / heartbeat / stale recovery / fleet planning
- capability-aware worker matching 和 plan-driven heterogeneous fleet

当前阶段新增基线：
- `workspace.toml` 配置与 workspace CLI
- `ProjectSession` / `SessionIteration` 持久化模型
- `RunService.run_loop()` 最小自治 loop
- `run loop` 与 `workspace run-project` 命令

这意味着系统已经开始从“单次 run”过渡到“session 驱动的多 iteration 执行”。

## 总目标

1. 建立 workspace 级控制面，而不再把系统绑定到单项目配置。
2. 让项目以 session/loop 方式持续运行，而不是一次性 `run start`。
3. 把 queue/fleet 从单次 run job 升级为 session-aware 执行闭环。
4. 用更 Lean-native 的结构化分析替换当前主要依赖 regex 的状态模型。
5. 把 executor/provider/runtime、operator console、experiment ledger 补齐到研究平台级。

## 执行原则

- 每完成一个 ticket 并通过验证后，不停下，直接读取本计划进入下一张未阻塞 ticket。
- 默认优先实现主链路，不因 dashboard 或外围工具打断自治闭环主线。
- 每个工作块都遵循 TDD：先补 failing tests，再补实现，再做全量验证。
- 每个稳定里程碑都允许 commit/push，但不要等到整个阶段结束。
- 并行时优先按“文件所有权”切分 subagents / worktrees，避免同文件冲突。

## 主工作流

### Workstream A: Workspace + Session Substrate

目标：
- 让 workspace 成为多项目主控制面。
- 让 session 成为项目自治执行的基本单位。

当前已完成：
- `WorkspaceConfig`
- `ProjectSession` / `SessionIteration`
- workspace init/status/start-session/run-project
- `RunService.run_loop()`

下一步：
- 增加 session claim/release/recover 状态机
- 给 session 引入 stop reason、resume reason、budget fields
- 支持从 workspace 批量启动 / 恢复 session

验收：
- workspace 能稳定加载多个项目
- session 可创建、运行、暂停、失败恢复
- 单项目 CLI 不回归

### Workstream B: Session-Quantum Queue and Autoscaling

目标：
- 把 queue 的 job 单位从“单次 project run”升级到“session quantum”
- 把 fleet plan 变成自动执行闭环

实施顺序：
1. 给 queue 增加 session-oriented job kind
2. `BatchRunner` 支持执行固定 quantum 的 session tick
3. `queue enqueue-workspace` / `queue session-status` / `queue resume-session`
4. 引入 `FleetController` 和 `WorkerLauncher`
5. 基于 `plan_fleet()` 自动起停 worker

验收：
- 多项目 session 在共享队列下公平推进
- 单个 session 不会长期占满全部 worker
- stale worker recovery 后 session 状态不损坏

### Workstream C: Lean-native Analysis

目标：
- 用结构化 Lean 分析替换当前 task graph / snapshot 的主要启发式来源

实施顺序：
1. 定义 `LeanAnalyzer` 接口和 `LeanAnalysisSnapshot`
2. 接入 doctor/smoke checks
3. 让 `task_graph.py` / `project_state.py` analyzer-first、heuristic-fallback
4. 再考虑 persistent sidecar / 更深集成

验收：
- analyzer 不可用时系统可降级
- analyzer 可提供 theorem index、dependency、diagnostics、proof-gap 信号
- planner/supervisor 可消费结构化产物

### Workstream D: Execution Runtime and Provider Pools

目标：
- 把 executor/provider 抽象从“能发请求”升级到“能承载规模化自治”

实施顺序：
1. 增加 runtime telemetry：latency、retry、usage、cost estimate
2. 引入 named provider pools / endpoint health
3. 加 circuit break、quarantine、fallback
4. 让 execution policy 能路由到 pool/member，而不只是裸配置

验收：
- HTTP / codex exec 都有统一 telemetry
- provider 异常时不会导致整体 session loop 失控

### Workstream E: Operator Console and Experiment Ledger

目标：
- 在已落地的 workspace operator console 之上继续强化 Mission Console
- 把 benchmark 从 snapshot 分数升级成 theorem-level 实验账本

实施顺序：
1. workspace overview API
2. session table / worker pool / budget panel
3. failure taxonomy 和 theorem outcome ledger
4. compare/replay/report surfaces

验收：
- 能查看多项目 session backlog、worker 健康和失败趋势
- 能比较两次策略运行的 theorem-level 差异
- `Plan / Loop / Finish` 能承载日常 operator 值守，而 experiment/replay 退居高级工具层

## 推荐执行顺序

建议按下列顺序持续推进，不要在每个点之间停顿：

1. Workspace + Session Substrate 收口
2. Session-Quantum Queue
3. FleetController / autoscaling 闭环
4. LeanAnalyzer contract
5. Analyzer-first task graph / snapshot
6. Execution runtime telemetry / provider pool
7. Operator console
8. Experiment ledger

## 推荐并行拆分

当允许 subagents / worktrees 并行时，优先按下列分工：

- Worker A: `queue.py` / `batch.py` / autoscaler / session quantum
- Worker B: `services.py` / `events.py` / session state machine / CLI glue
- Worker C: `task_graph.py` / `project_state.py` / analyzer contract / checks
- Worker D: `dashboard.py` / workspace API / operator console
- Worker E: `executors.py` / runtime telemetry / provider pools

规则：
- 每个 worker 必须有明确文件所有权
- 不要改动他人正在持有的文件
- 主 agent 负责集成、验证、冲突解决和最终 commit

## 当前默认下一张票

如果没有更高优先级插入，下一张默认实现票是：

`Session-Quantum Queue v1`

内容：
- 定义 session-oriented queue payload
- 增加 enqueue/list/status/resume CLI
- 让 worker 执行单个 session quantum
- 补公平调度与最小恢复测试
