# Archon 原理

Archon 是 Lean 项目级的自动化编排器，不是单个证明器。

## 核心思想

- 把一个 Lean 项目当成长期运行的研究任务
- 把证明工作拆成 plan / prove / review 三段
- 把上下文、失败记录和进展写进状态文件
- 让 agent 在有限上下文里持续推进

## 你会频繁看到的东西

- `.archon/PROGRESS.md`：当前阶段和目标
- `.archon/task_results/`：prover 的结果
- `.archon/proof-journal/`：review 产生的日志和总结
- `.claude/skills/`：本地技能
- `.archon/prompts/`：本地提示词

## 为什么要这样设计

- 形式化证明经常需要长链条上下文
- 单个 LLM 上下文很容易爆
- plan/prover/review 分离后，更容易定位失败原因
- review 可以把零散尝试整理成可复用知识

## ArchonLab 为什么还要再包一层

Archon 本身已经能跑，但它更像“后端引擎”。
ArchonLab 要做的是：
- 把多个项目和 benchmark 管起来
- 统一调度策略
- 统一评分和回放
- 统一监督与人工介入

## 你可以先记住的一句话

Archon 负责“怎么证明”，ArchonLab 负责“证明什么、先证明哪个、证明到什么程度、失败后怎么办”。

