# 常见问题

## Lean 安装失败

先看这几个命令：

```bash
python3.12 --version
elan --version
lean --version
lake --version
```

如果 `lean` 或 `lake` 不在 PATH，通常是 `elan` 没装好，或者 shell 没重新加载。

如果你已经安装了 `elan`，但 `zsh -lc 'lean --version'` 还是找不到命令，
优先检查 `~/.zshenv`、`~/.zprofile` 里有没有把 `~/.elan/bin` 加进 PATH。

## Claude Code 安装失败

如果执行：

```bash
curl -fsSL https://claude.ai/install.sh | bash
```

结果不是 shell 脚本，而是返回一个 HTML 页面，尤其包含
`App unavailable in region`，那问题通常不在本机，而在地区或访问链路。

这时不要继续折腾 `uv`、`elan`、`lean`。
Lean 环境可以先单独配好，再切到 `codex exec` 或 OpenAI-compatible endpoint。
`claude` 现在不是唯一执行器。

## `codex exec` 不可用

先看：

```bash
codex --version
codex exec --help
```

如果 `codex` 不在 PATH，优先修 PATH。
如果 `codex exec` 能跑但无人值守时卡住，检查 `archonlab.toml` 里的：

- `[executor].kind = "codex_exec"`
- `[executor].auto_approve = true`
- `[provider].model`

## OpenAI-compatible endpoint 不工作

先确认三件事：

1. `base_url` 对应的服务真的在运行
2. `endpoint_path` 对得上服务暴露的接口
3. `api_key_env` 指向的环境变量已经导出

最常见错误是把 `base_url` 写成已经带 `/v1/responses` 的完整路径，又额外给了同样的 `endpoint_path`。

## `lake build` 失败

先不要急着改 agent。
先做三件事：

1. 看完整报错
2. 确认是不是依赖没装全
3. 先让最小 Lean 项目编译通过

## Agent 没动静

检查：

- `PROGRESS.md` 是否有当前阶段
- `task_results/` 是否有输出
- `.archon/logs/` 是否在增长
- `doctor` 是否能识别到 Lean 环境

## `sorry` 很多但没减少

这通常有三种原因：

- 任务目标太大
- prompt 信息不足
- 需要先补 lemma 或 blueprint

处理顺序建议：

1. 缩小任务
2. 加入更具体的 hint
3. 把 theorem 拆成更小的 sub-lemma

## 想提高成功率

- 先用小项目练流程
- 先做单文件任务
- 先给清晰目标，再给 proofs
- 让 review 记录 dead ends
- 让 supervisor 维护失败签名

## 什么时候该暂停

如果你连续遇到同一类错误，先暂停并修正流程，不要硬堆 agent 数量。
在形式化证明里，盲目扩并发通常只会更快放大错误。
