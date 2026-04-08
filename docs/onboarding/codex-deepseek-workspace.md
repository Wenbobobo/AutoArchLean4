# Codex + DeepSeek Workspace Setup

这份手册对应当前推荐的真实运行模式：

- `Codex` 负责大部分 `prover/review`
- `DeepSeek` 只负责 `plan`
- `workspace.toml` 已按这个路由方式配置

当前系统还没有单独的 `hint` 执行 phase，所以 `DeepSeek only for hint`
还不能纯靠配置实现。现阶段最接近的做法就是 `DeepSeek only for plan`。

## 1. 前提

先确认两件事：

1. `codex` CLI 已安装并完成登录
2. 当前 shell 已提供下面两个环境变量

```bash
export OPENAI_API_KEY="<your-openai-key>"
export DEEPSEEK_API_KEY="<your-deepseek-key>"
```

如果你想检查当前 shell 是否已经配置：

```bash
cd /home/niracler/Gary/Math/archonlab
uv run python - <<'PY'
import os
for key in ["OPENAI_API_KEY", "DEEPSEEK_API_KEY"]:
    print(f"{key}={'set' if os.getenv(key) else 'unset'}")
PY
```

## 2. 当前路由

`workspace.toml` 当前的 phase 路由是：

- `plan -> openai_compatible -> https://api.deepseek.com/chat/completions -> deepseek-chat`
- `prover -> codex_exec -> gpt-5.2-codex`
- `review -> codex_exec -> gpt-5.2-codex`

这要求 `openai_compatible` 执行器支持两种 payload：

- OpenAI `Responses API`
- OpenAI-compatible `chat/completions`

仓库已补上 `chat/completions` 兼容层，适用于 DeepSeek 这类端点。

## 3. 可直接复制的命令

先看工作区状态：

```bash
cd /home/niracler/Gary/Math/archonlab
uv run archonlab workspace status --config workspace.toml
```

启动 dashboard：

```bash
cd /home/niracler/Gary/Math/archonlab
uv run archonlab dashboard serve --config workspace.toml --host 0.0.0.0 --port 8000
```

新开一个终端，启动 daemon：

```bash
cd /home/niracler/Gary/Math/archonlab
export OPENAI_API_KEY="<your-openai-key>"
export DEEPSEEK_API_KEY="<your-deepseek-key>"
uv run archonlab workspace daemon run --config workspace.toml
```

可选：再开一个终端，用 fleet 方式拉起多个 worker：

```bash
cd /home/niracler/Gary/Math/archonlab
export OPENAI_API_KEY="<your-openai-key>"
export DEEPSEEK_API_KEY="<your-deepseek-key>"
uv run archonlab queue fleet --config workspace.toml --workers 2
```

查看 provider pool 健康：

```bash
cd /home/niracler/Gary/Math/archonlab
uv run archonlab queue provider-health --config workspace.toml --json
```

## 4. 跑 FATE-M

仓库里已经新增了示例 manifest：

- `benchmarks/fate-m-codex-deepseek.example.toml`

第一次使用时先把其中的 `path` 和 `archon_path` 改成你的真实路径。

然后运行：

```bash
cd /home/niracler/Gary/Math/archonlab
export OPENAI_API_KEY="<your-openai-key>"
export DEEPSEEK_API_KEY="<your-deepseek-key>"
uv run archonlab benchmark run \
  --manifest benchmarks/fate-m-codex-deepseek.example.toml \
  --use-worktrees \
  --worker-slots 4
```

查看 benchmark 结果：

```bash
cd /home/niracler/Gary/Math/archonlab
uv run archonlab benchmark runs --manifest benchmarks/fate-m-codex-deepseek.example.toml
```

## 5. 运行边界

当前建议：

- 把 `DeepSeek` 限制在 `plan`
- 不要让 `Codex` 和 `DeepSeek` 在同一个 phase 里做自动 failover
- 先拿 `FATE-M` 做真实基线，再决定是否上 `FATE-H`

原因是：

- `plan` 更适合便宜模型给方向和检索线索
- `prover/review` 更需要 worktree 操作和 Lean 验证闭环
- `FATE-M` 更适合作为代数专项的第一层，而不是最终 open-problem 目标
