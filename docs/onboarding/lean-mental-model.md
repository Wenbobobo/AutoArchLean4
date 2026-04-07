# Lean 心智模型

Lean 可以先理解成“可执行的数学证明语言”。

## 你最需要先懂的几个概念

- `theorem` / `lemma`：要证明的命题
- `by`：进入证明模式
- `sorry`：临时占位，不是最终证明
- `simp`：自动化化简
- `rw`：按等式重写
- `exact`：直接给出目标所需的项

## 你可以怎么读一个 Lean 文件

1. 先看声明
2. 再看假设
3. 找到目标
4. 看当前 proof 是不是在做重写、化简或分情况
5. 如果有 `sorry`，把它看成“这里还缺一个证明策略”

## 常见误解

- Lean 不是普通的数学笔记
- `sorry` 不是证明完成
- LLM 写出的 proof 也必须被 Lean 编译器验证
- 能“看起来对”不等于“真的对”

## 新手建议

- 先学会看目标，再学会写证明
- 先学会 `simp`、`rw`、`exact`
- 再学 `apply`、`cases`、`induction`
- 遇到报错时，优先理解错误信息，而不是盲改

## 和 Archon 的关系

Archon 不是替代 Lean 的系统，它只是帮你组织 Lean 证明工作：
- plan agent 决定先做什么
- prover agent 写证明
- review agent 总结失败模式

真正的正确性，仍然由 Lean 编译器保证。

