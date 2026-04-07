# Continuous Execution Playbook

这份文档约束无人值守或半无人值守的实现节奏。
目标是避免“做完一个小块就停住”，并把 subagents / worktrees 用在最有价值的地方。

## 核心规则

1. 开工前先读：
   - [Phase 1-5](./phase1-5.md)
   - [Phase 6+](./phase6-autonomous-workspace.md)
2. 每次只做一个明确 ticket，但做完以后不要停，直接切到下一张未阻塞 ticket。
3. 只有在真正的 blocker 出现时才停下汇报：
   - 需求冲突
   - 外部环境缺失
   - 无法安全推断的架构分歧
4. 每张票都必须：
   - 先补测试
   - 再做实现
   - 最后跑验证
5. 每个稳定节点都应 commit；在用户允许时可直接 push。

## 单张票执行节奏

1. 读取当前 phase 计划与相关模块
2. 写或补 failing tests
3. 实现最小闭环
4. 跑局部验证
5. 跑全量验证：
   - `uv run pytest -q`
   - `uv run ruff check .`
   - `uv run mypy`
6. commit / push
7. 重新读取计划，进入下一张未阻塞票

## Subagents / Worktree 约定

- 只有在任务可明确切文件边界时才并行。
- 默认优先创建独立 worktree，避免主工作树冲突。
- 推荐拆分：
  - queue / autoscaler
  - services / events / session state
  - analyzer / planner / snapshot
  - dashboard / workspace API
  - runtime / provider pools

并行规则：
- worker 必须声明自己负责哪些文件
- 不允许回滚他人改动
- 主 agent 负责最终集成与统一验证

## 默认优先级

- 先自治闭环，后操作面
- 先状态模型，后 UI
- 先可验证执行，后复杂优化
- 先 session / queue / autoscaler，后实验比较和前端增强

## Prompt 模板

```text
你在 /home/niracler/Gary/Math/archonlab 工作。
开始前先阅读：
- docs/roadmap/phase1-5.md
- docs/roadmap/phase6-autonomous-workspace.md
- docs/roadmap/continuous-execution.md

当前原则：
- uv + Python 3.12
- TDD、小步提交、最佳实践
- 用 subagents + git worktree 并行
- 完成一张票并通过验证后，不要停下，继续进入下一张未阻塞票
- 不修改无关文件

本轮优先 ticket：
[填写 ticket]

完成后输出：
- 实现内容
- 验证结果
- 当前完成到计划中的哪一段
- 下一张默认继续的 ticket
```
