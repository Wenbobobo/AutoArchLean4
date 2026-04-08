# Benchmark And Open-Problem Ladder

这份文档不是泛泛的“题单推荐”，而是面向当前 ArchonLab 基线的执行建议。
目标是把系统从：

1. 能稳定跑 Lean benchmark
2. 能识别自己的失败类型
3. 能在研究级 formalization 任务上持续迭代
4. 最后再接触真正的 open conjectures

## 1. 先澄清 FATE 的定位

`FATE-M` 的 `M` 是 `Medium`，不是 `Master`。

按照 FATE 官方仓库：

- `FATE-M`: 150 道基础抽象代数题，主要来自教材
- `FATE-H`: 100 道更难的抽象代数 / 交换代数题，来源扩展到期末、资格考和研究文献
- `FATE-X`: 100 道极难题，通常还需要额外 formalize 少量依赖定义

这说明：

- `FATE-M` 很适合做代数专项起点
- `FATE-H/X` 更接近研究级 formalization benchmark
- 但它们本质上仍然主要是 benchmark / unsolved-formalization，不是真正的 open math

## 2. ArchonLab 当前最适合的递增路线

### Layer 0: Lean 基本功

目标：

- 降低“系统不会 Lean，而不是不会证明”的噪声

推荐题源：

- `lean4game`
- `Mathematics in Lean`

为什么要有这一层：

- 当前 ArchonLab 已经有 workspace loop、queue、dashboard、workflow DSL
- 但在真正接 open problem 之前，先用小题把 tactic、rewrite、induction、library lookup 的失败类型打透，性价比最高

### Layer 1: 通用本科 benchmark

目标：

- 先评估系统的“广谱”能力，而不是一开始就局限在代数

推荐题源：

- `ProofNet`
- `miniF2F`
- `PutnamBench`

如何使用：

- `ProofNet` 更适合 autoformalization + proving 混合评估
- `miniF2F` 更适合竞赛型、短证明型 stress test
- `PutnamBench` 更适合作为高难通用 benchmark

### Layer 2: 代数专项起点

目标：

- 开始进入和你目标更一致的代数 / 交换代数场景

推荐题源：

- `FATE-M`

为什么现在做：

- 文件粒度清晰
- 每题单 `sorry`
- Lean 项目结构干净
- 非常适合作为 ArchonLab 的 theorem-level ledger、queue 调度、失败分类的第一批真实数据

建议执行方式：

1. 先跑 `1-25`
2. 再跑 `1-75`
3. 最后跑完整 `150`

出层条件建议：

- 单题成功率、平均迭代次数、blocked ratio、review rollback rate 稳定
- 能复现至少两轮结果，不靠偶然命中

### Layer 3: 研究前夜 benchmark

目标：

- 从“标准教材题”升级到“研究级 formalization 压力”

推荐题源：

- `FATE-H`
- `FATE-X`

为什么重要：

- `FATE-H` 会更频繁暴露 lemma discovery 和检索规划问题
- `FATE-X` 会暴露“mathlib 没有现成定义 / 需要先建 API”的问题

这两层非常适合检验：

- Lean-native task ingestion
- project-level autonomous loop
- provider / executor 路由是否真的有价值

### Layer 4: True Open Conjectures

目标：

- 进入“陈述已 formalize，但数学证明本身未解决”的任务

推荐题源：

- `formal-conjectures`

为什么它是当前最适合的真 open 层：

- Lean 原生
- 明确区分 `research open` / `research solved`
- 目录结构和标签系统较成熟
- 允许从一个清晰 statement 出发，而不是先做大规模 informal-to-formal

建议策略：

- 不要一上来就追求“解题”
- 先把它当作 statement-ingestion、dependency-gap、counterexample search 和 partial-progress ledger 的实验场

### Layer 5: 外部开放问题挑战

目标：

- 在平台成熟之后，接入非 Lean-native 的公开开放问题来源

候选：

- `FrontierMath Open Problems`

为什么放最后：

- 不是 Lean-native
- statement / verifier / library gap 更大
- 更适合做长期挑战，而不是当前主线

## 3. 哪些算 true open，哪些不算

### true open

- `formal-conjectures`
- `FrontierMath Open Problems`

### benchmark 或 unsolved-formalization

- `ProofNet`
- `miniF2F`
- `PutnamBench`
- `FATE-M`
- `FATE-H`
- `FATE-X`

`FATE-X` 虽然已经足够前沿，且常常需要补定义，但官方题源描述仍然是教材、考试和研究文献，不是“专门的未解猜想库”。

## 4. 当前推荐主线

如果目标是“Agent 自主进行大规模 Lean 形式化证明，并逐步逼近 open problem”，当前最推荐的主线是：

1. `Lean onboarding`
2. `ProofNet / miniF2F / PutnamBench`
3. `FATE-M`
4. `FATE-H`
5. `FATE-X`
6. `formal-conjectures`

其中：

- `miniF2F / PutnamBench` 更像横向压力测试
- `FATE-M/H/X` 是更贴近“代数研究型 formalization”的主线
- `formal-conjectures` 才是第一层真正的 open conjecture

## 5. 当前最值得直接执行的第一批实验

建议按这个顺序推进：

1. 用 `benchmarks/fate-m-codex-deepseek.example.toml` 跑 `FATE-M`
2. 记录 theorem-level 失败类型，尤其是：
   - library retrieval failure
   - missing helper lemma
   - proof search loops
   - review regression
3. 基于 `FATE-M` 调整 workflow DSL、provider routing、review policy
4. 成功率和稳定性达标后，迁移到 `FATE-H`
5. 等 `FATE-H` 稳定后，再挑选 `formal-conjectures` 中依赖较浅、陈述清晰的条目做第一批 open-task ingestion
