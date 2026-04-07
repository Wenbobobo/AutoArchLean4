# Phase Roadmap 1-5

这份路线图是 ArchonLab 的主计划。默认假设你是 Lean 和 Archon 的新手，所以顺序是：
先让环境能跑，再让你看懂，再让系统可控，最后才做规模化。

## Phase 1: 环境与入门

目标：
- 装好 `uv`、`Python 3.12`、`elan`、`Lean`、`lake`
- 跑通最小的 Archon 后端 smoke test
- 看懂最基本的 Lean/Archon 术语

交付物：
- 可运行环境
- 新手文档
- 一次成功的 `doctor`/`first-run`

## Phase 2: 外置控制平面骨架

目标：
- 做一个独立于 `Archon` 的 orchestrator
- 定义项目、运行、任务、事件、预算等核心数据结构
- 支持 dry-run 和回放

交付物：
- CLI 骨架
- 配置加载
- 结构化事件存储
- Archon adapter 的最小实现

## Phase 3: 小规模 benchmark

目标：
- 先选 3-5 个 Lean 项目
- 定义统一评分与回放格式
- 跑出稳定、可比较的基线

交付物：
- benchmark manifest
- scorer
- 可重复运行脚本
- 回归测试

## Phase 4: theorem/DAG 调度与 supervisor

目标：
- 从“按文件调度”升级到“按 theorem/task node 调度”
- 引入 supervisor 识别 stuck、错误构造、缺失基础设施
- 用 `git worktree` 隔离并行实验

交付物：
- task graph
- supervisor policy
- worktree manager
- subagent 分工规范

当前基线已实现：
- `run start` 自动生成 `task-graph.json` 和 `supervisor.json`
- `run start` 会基于 task graph + supervisor 做 task-aware next-action 选择
- supervisor 会参考近期运行历史识别重复无进展
- `worktree create/remove` 可管理隔离工作树
- benchmark 可选用隔离 worktree 运行
- benchmark 和 queue 都支持 slot-aware 并发 worker
- benchmark queue 的单个 job 现在对应单个 benchmark project，而不是整份 manifest
- Phase 4 的最小契约测试已经覆盖 task graph、supervisor、worktree

## Phase 5: workflow 与最小 UI

目标：
- 提供可操作的监督界面
- 支持暂停、恢复、注入 hint、切换 workflow
- 为未来 100+ agent 并发预留接口

交付物：
- 最小 dashboard
- workflow DSL
- 人工干预入口
- 可扩展的运行看板

当前基线已实现：
- FastAPI dashboard 与本地 control API
- `control pause/resume/hint` CLI
- workflow DSL 及规则加载器
- dashboard 中的 pause/resume/hint 操作
- provider/executor 抽象，可切换 `dry_run` / `codex_exec` / OpenAI-compatible HTTP
- `run start --execute` 已接通 executor
- benchmark queue / batch runner 已支持 slot-aware 并发

## 优先级原则

- 先控制平面，后看板
- 先小 benchmark，后规模化
- 先稳定回放，后复杂自动化
- 先文档和环境，后性能和花哨功能
