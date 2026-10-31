# mcp2skill

常驻 MCP 客户端 Skill。连接已有的 MCP 服务端并在多次调用之间复用连接，不启动服务端，不使用配置文件。

## 安装

把 `src/mcp2skill` 复制到目标 Agent 的 Skills 目录，例如：

```bash
cp -r src/mcp2skill ~/.claude/skills/
```

## 依赖

```bash
pip install "mcp>=2.3"
```

脚本必须使用已安装 `mcp` 的 Python 解释器运行。

## 使用

```bash
python scripts/mcp_client.py connect <url> --header "Authorization: Bearer <token>"
python scripts/mcp_client.py tools
python scripts/mcp_client.py call <tool> '<json参数>'
python scripts/mcp_client.py list
python scripts/mcp_client.py disconnect <name|all>
```

连接信息只在 `connect` 时设置一次，后续调用直接复用。支持同时连接多个服务端、工具列表、工具调用、30 分钟空闲自动断开，并在连接时展示服务端 `instructions`。

详细用法见 `src/mcp2skill/SKILL.md`。

## 目录

- `src/mcp2skill/SKILL.md`：技能说明
- `src/mcp2skill/scripts/mcp_client.py`：常驻客户端实现
