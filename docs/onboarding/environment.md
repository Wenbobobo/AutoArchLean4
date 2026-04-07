# 环境安装

这一步的目标很简单：让你在本机上稳定跑起 Lean 和 ArchonLab。

## 推荐环境

- 操作系统：Linux 或 macOS
- Python：`3.12`
- 包管理：`uv`
- Lean 工具链：`elan`
- 项目构建：`lake`
- 后端执行器：`codex exec` 或 OpenAI-compatible HTTP endpoint

## 安装顺序

1. 安装 `uv`
2. 安装 `Python 3.12`
3. 安装 `elan`
4. 拉起 Lean 工具链
5. 检查 `lake` 和 `lean`
6. 再安装 Archon / ArchonLab 依赖

## 最小检查命令

```bash
python3.12 --version
uv --version
elan --version
lean --version
lake --version
codex --version
claude --version
```

如果其中任何一步失败，先修环境，不要急着跑 agent。

## 当前已知外部 blocker

如果你能用 `codex exec` 或 OpenAI-compatible endpoint，就不必被 `Claude Code` 阻塞。
`claude` 现在只是可选执行器，不再是唯一入口。

如果你在安装 `Claude Code` 时看到 `App unavailable in region` 之类的网页返回，
这不是本地 Python/Lean 环境问题，而是地域或分发链路限制。

判断方法：

```bash
curl -fsSL https://claude.ai/install.sh | bash
```

如果返回的是 HTML 页面而不是安装脚本，说明链路被挡住了。
这时应该先解决网络/地区可达性，再继续跑 Archon。

## 推荐心法

- 先确保命令行里能看到版本号
- 再确保能编译一个最小 Lean 项目
- 最后再让自动化编排器接管

## 不要一开始就做的事

- 不要先改 prompt
- 不要先调 workflow
- 不要先追求并发
- 不要先做大规模 benchmark

环境没通时，优化编排只会放大噪音。
