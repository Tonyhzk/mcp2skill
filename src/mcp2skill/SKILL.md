---
name: mcp2skill
description: 常驻 MCP 客户端，连接已有的 MCP 服务端并在多次调用之间复用连接。当用户给出 MCP 服务端地址（HTTP/SSE）、需要查看某个 MCP 服务端暴露的工具，或需要调用 MCP 工具时使用；只连接已存在的服务端，不启动服务端，不使用配置文件。
---

# mcp2skill

常驻 MCP 客户端。连接信息只在 `connect` 时设置一次，之后每次调用都内嵌成一条命令，不写脚本，不建配置文件。

## 依赖

需要 Python 3.10+ 和 `mcp>=2.3`：

```bash
pip install "mcp>=2.3"
```

脚本路径为 `scripts/mcp_client.py`。必须使用已安装 `mcp` 的 Python 解释器运行，后台进程会用同一个解释器启动。

## 连接

密钥、请求头、传输方式只在 `connect` 这一条命令里设置一次：

```bash
python scripts/mcp_client.py connect <url> \
  --header "Authorization: Bearer <token>"
```

- 连接保存在后台进程中，后续所有命令直接复用，不再传密钥。
- `--name` 可选，默认使用 `host:port`。同时连接多个服务端时用它区分。
- `--token-env VAR` 从环境变量读取 Bearer token。
- `--transport auto|http|sse`，默认 auto，先试 Streamable HTTP 再回退 SSE。
- 同名连接重复执行时，地址和请求头一致则直接复用。

同时连接多个服务端：

```bash
python scripts/mcp_client.py connect https://a.example.com/mcp --name a --header "..."
python scripts/mcp_client.py connect https://b.example.com/mcp --name b --header "..."
```

## 调用

所有操作都用单条命令内嵌完成：

```bash
python scripts/mcp_client.py list
python scripts/mcp_client.py tools [--name <name>] [--json]
python scripts/mcp_client.py call <tool> '<json参数>' [--name <name>] [--json]
python scripts/mcp_client.py status [--json]
python scripts/mcp_client.py disconnect <name|all>
python scripts/mcp_client.py stop
```

只有一个连接时 `--name` 可以省略；存在多个连接时必须指定。

## 必须遵守

能用命令内嵌的调用一律内嵌，不写 Python 或 bash 脚本：

- 调用工具直接用 `call <tool> '<json>'`，参数写在命令里。
- 需要多步完成的工具，把上一条命令返回的值直接填进下一条命令。例如 `Terminal` 的启动、读取、关闭：

```bash
python scripts/mcp_client.py call Terminal '{"action":"start","cwd":"/tmp"}'
# 从返回结果中取 terminal_id 和 owner_key，直接填入下一条命令
python scripts/mcp_client.py call Terminal '{"action":"read","terminal_id":"...","owner_key":"...","cursor":0}'
python scripts/mcp_client.py call Terminal '{"action":"close","terminal_id":"...","owner_key":"..."}'
```

- 需要解析返回值时直接阅读命令输出，把值手工填入下一条命令，不写解析脚本。
- `--json` 输出 `is_error`、`content`、`structured`，结构化字段优先看 `structured`。
- 工具返回错误时退出码为 1。

## 生命周期

- 连接保存在独立的后台进程中，`connect` 命令结束后继续存活。
- 某个连接 30 分钟没有被 `tools` 或 `call` 使用会自动断开，可用 `MCP2SKILL_IDLE`（秒）调整。
- 所有连接断开后，后台进程在 5 分钟没有新请求时退出，可用 `MCP2SKILL_DAEMON_IDLE`（秒）调整。
- 主动释放连接使用 `disconnect`；断开只影响客户端连接，不停止或影响服务端。
- 任务结束或确认不再使用时，先执行 `disconnect all` 释放连接，再执行 `stop` 停止技能自身的后台进程；自动退出只是兜底。
- 后台进程异常退出后，下一次 `connect` 会重新启动并建立连接。

## 排查

- 连接失败时查看 `~/.local/state/mcp2skill/daemon.log`，或 `MCP2SKILL_STATE` 指定目录下的 `daemon.log`。
- 同名连接地址或请求头变化时，会断开旧连接并按新参数重连。
