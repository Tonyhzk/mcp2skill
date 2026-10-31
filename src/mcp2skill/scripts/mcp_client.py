#!/usr/bin/env python3
"""mcp2skill：常驻 MCP 客户端。

connect 建立到已存在 MCP 服务端的连接并保持，后续 tools/call 通过连接名复用。
不启动任何服务端，不读取配置文件，所有连接参数只在 connect 时传入。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import time
from contextlib import AsyncExitStack
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse

STATE_DIR = Path(
    os.environ.get("MCP2SKILL_STATE") or Path.home() / ".local" / "state" / "mcp2skill"
)
SOCKET_PATH = STATE_DIR / "daemon.sock"
PID_PATH = STATE_DIR / "daemon.pid"
LOG_PATH = STATE_DIR / "daemon.log"

IDLE_TIMEOUT = float(os.environ.get("MCP2SKILL_IDLE", "1800"))
DAEMON_IDLE_TIMEOUT = float(os.environ.get("MCP2SKILL_DAEMON_IDLE", "300"))
REAPER_INTERVAL = 30.0
RPC_TIMEOUT = float(os.environ.get("MCP2SKILL_RPC_TIMEOUT", "300"))


def _load_mcp() -> SimpleNamespace:
    try:
        from mcp import Client, ClientSession
        from mcp.client.sse import sse_client
        from mcp.client.streamable_http import streamable_http_client
        from mcp.shared._httpx_utils import create_mcp_http_client
    except ImportError as exc:
        raise RuntimeError("缺少 mcp 依赖，请安装：pip install 'mcp>=2.3'") from exc
    return SimpleNamespace(
        Client=Client,
        ClientSession=ClientSession,
        sse_client=sse_client,
        streamable_http_client=streamable_http_client,
        create_mcp_http_client=create_mcp_http_client,
    )


def _ensure_state_dir() -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(STATE_DIR, 0o700)
    return STATE_DIR


def _rpc(payload: dict, timeout: float = RPC_TIMEOUT) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.connect(str(SOCKET_PATH))
        sock.sendall(json.dumps(payload).encode() + b"\n")
        buffer = b""
        while b"\n" not in buffer:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buffer += chunk
    if not buffer:
        raise ConnectionError("daemon 没有返回数据")
    return json.loads(buffer.split(b"\n", 1)[0].decode())


def _daemon_alive() -> bool:
    if not SOCKET_PATH.exists():
        return False
    try:
        return bool(_rpc({"cmd": "ping"}, timeout=1.0).get("ok"))
    except Exception:
        return False


def ensure_daemon() -> None:
    if _daemon_alive():
        return
    _ensure_state_dir()
    for path in (SOCKET_PATH, PID_PATH):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    with open(LOG_PATH, "ab") as log:
        proc = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "_serve"],
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=log,
            start_new_session=True,
            close_fds=True,
        )
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if _daemon_alive():
            return
        if proc.poll() is not None:
            raise RuntimeError(f"daemon 启动失败，查看日志 {LOG_PATH}")
        time.sleep(0.1)
    raise RuntimeError(f"daemon 启动超时，查看日志 {LOG_PATH}")


def _default_name(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc or url


def _format_result(result: Any) -> dict:
    content: list[Any] = []
    for item in getattr(result, "content", None) or []:
        text = getattr(item, "text", None)
        if text is not None:
            content.append(text)
            continue
        data = getattr(item, "data", None)
        if data is not None:
            content.append({"type": getattr(item, "type", "data"), "data": data})
            continue
        content.append(str(item))
    payload: dict[str, Any] = {
        "is_error": bool(getattr(result, "is_error", False)),
        "content": content,
    }
    structured = getattr(result, "structured_content", None)
    if structured is None:
        structured = getattr(result, "structuredContent", None)
    if structured is not None:
        payload["structured"] = structured
    return payload


class Connection:
    def __init__(
        self,
        name: str,
        url: str,
        headers: dict[str, str],
        transport: str,
        timeout: float,
    ) -> None:
        self.name = name
        self.url = url
        self.headers = headers
        self.transport = transport
        self.resolved_transport = transport
        self.timeout = timeout
        self.stack: AsyncExitStack | None = None
        self.client: Any = None
        self.tools: list[dict] = []
        self.instructions = ""
        self.lock = asyncio.Lock()
        self.connected_at = 0.0
        self.last_active = time.monotonic()

    async def open(self) -> None:
        mcp = _load_mcp()
        if self.transport in ("auto", "http"):
            try:
                await self._open_streamable(mcp)
                return
            except Exception:
                await self.close()
                if self.transport == "http":
                    raise
        await self._open_sse(mcp)

    async def _open_streamable(self, mcp: SimpleNamespace) -> None:
        self.stack = AsyncExitStack()
        http_client = mcp.create_mcp_http_client(headers=self.headers or None)
        transport = mcp.streamable_http_client(self.url, http_client=http_client)
        self.client = await self.stack.enter_async_context(
            mcp.Client(transport, read_timeout_seconds=self.timeout)
        )
        self.resolved_transport = "http"
        self.instructions = getattr(self.client, "instructions", None) or ""
        await self.refresh_tools()

    async def _open_sse(self, mcp: SimpleNamespace) -> None:
        self.stack = AsyncExitStack()
        read, write = await self.stack.enter_async_context(
            mcp.sse_client(self.url, headers=self.headers or None, timeout=self.timeout)
        )
        session = await self.stack.enter_async_context(mcp.ClientSession(read, write))
        await session.initialize()
        self.client = session
        self.resolved_transport = "sse"
        self.instructions = getattr(self.client, "instructions", None) or ""
        await self.refresh_tools()

    async def refresh_tools(self) -> list[dict]:
        result = await self.client.list_tools()
        tools: list[dict] = []
        for tool in result.tools:
            schema = getattr(tool, "inputSchema", None)
            if schema is None:
                schema = getattr(tool, "input_schema", None)
            tools.append(
                {
                    "name": tool.name,
                    "description": getattr(tool, "description", None) or "",
                    "input_schema": schema,
                }
            )
        self.tools = tools
        if not self.connected_at:
            self.connected_at = time.monotonic()
        return tools

    async def call(self, tool: str, arguments: dict) -> dict:
        result = await self.client.call_tool(tool, arguments)
        return _format_result(result)

    def info(self) -> dict:
        now = time.monotonic()
        return {
            "name": self.name,
            "url": self.url,
            "transport": self.resolved_transport,
            "connected_seconds": round(now - self.connected_at, 1) if self.connected_at else 0,
            "idle_seconds": round(now - self.last_active, 1),
            "tools": len(self.tools),
        }

    async def close(self) -> None:
        stack, self.stack = self.stack, None
        self.client = None
        if stack is not None:
            try:
                await stack.aclose()
            except Exception:
                pass


class Daemon:
    def __init__(self) -> None:
        self.connections: dict[str, Connection] = {}
        self.last_activity = time.monotonic()
        self.stop_event = asyncio.Event()

    async def handle(self, request: dict) -> dict:
        self.last_activity = time.monotonic()
        command = request.get("cmd")
        try:
            if command == "ping":
                return {"ok": True, "data": {"pid": os.getpid()}}
            if command == "connect":
                return await self._connect(request)
            if command == "list":
                return {"ok": True, "data": [c.info() for c in self.connections.values()]}
            if command == "status":
                return {
                    "ok": True,
                    "data": {
                        "pid": os.getpid(),
                        "socket": str(SOCKET_PATH),
                        "idle_timeout": IDLE_TIMEOUT,
                        "connections": [c.info() for c in self.connections.values()],
                    },
                }
            if command == "tools":
                return await self._tools(request)
            if command == "call":
                return await self._call(request)
            if command == "disconnect":
                return await self._disconnect(request)
            return {"ok": False, "error": f"未知命令：{command}", "type": "usage"}
        except Exception as exc:
            return {"ok": False, "error": str(exc), "type": "error"}

    async def _connect(self, request: dict) -> dict:
        url = request["url"]
        name = request.get("name") or _default_name(url)
        headers = request.get("headers") or {}
        transport = request.get("transport") or "auto"
        timeout = float(request.get("timeout") or 30.0)
        existing = self.connections.get(name)
        if existing is not None:
            same_target = (
                existing.url == url
                and existing.headers == headers
                and existing.transport == transport
            )
            if same_target and existing.client is not None:
                existing.last_active = time.monotonic()
                return {
                    "ok": True,
                    "data": {
                        **existing.info(),
                        "status": "already_connected",
                        "instructions": existing.instructions,
                    },
                }
            await existing.close()
            self.connections.pop(name, None)
        connection = Connection(name, url, headers, transport, timeout)
        try:
            await connection.open()
        except Exception as exc:
            await connection.close()
            return {
                "ok": False,
                "error": f"连接 {url} 失败：{exc}",
                "type": "connection",
            }
        self.connections[name] = connection
        return {
            "ok": True,
            "data": {
                **connection.info(),
                "status": "connected",
                "instructions": connection.instructions,
            },
        }

    def _resolve(self, name: str | None) -> Connection:
        if name:
            connection = self.connections.get(name)
            if connection is None or connection.client is None:
                available = ", ".join(self.connections) or "无"
                raise ValueError(f"没有名为 {name} 的连接，当前连接：{available}")
            return connection
        active = [c for c in self.connections.values() if c.client is not None]
        if len(active) == 1:
            return active[0]
        if not active:
            raise ValueError("没有活跃连接，先执行 connect")
        names = ", ".join(c.name for c in active)
        raise ValueError(f"存在多个连接，使用 --name 指定：{names}")

    async def _tools(self, request: dict) -> dict:
        connection = self._resolve(request.get("name"))
        async with connection.lock:
            tools = await connection.refresh_tools()
        connection.last_active = time.monotonic()
        return {"ok": True, "data": tools}

    async def _call(self, request: dict) -> dict:
        connection = self._resolve(request.get("name"))
        async with connection.lock:
            result = await connection.call(
                request["tool"], request.get("arguments") or {}
            )
        connection.last_active = time.monotonic()
        return {"ok": True, "data": result}

    async def _disconnect(self, request: dict) -> dict:
        name = request["name"]
        if name == "all":
            names = list(self.connections)
            for item in names:
                connection = self.connections.pop(item)
                await connection.close()
            return {"ok": True, "data": {"disconnected": names}}
        connection = self.connections.pop(name, None)
        if connection is None:
            return {
                "ok": False,
                "error": f"没有名为 {name} 的连接",
                "type": "not_connected",
            }
        await connection.close()
        return {"ok": True, "data": {"disconnected": [name]}}

    async def reaper(self) -> None:
        while True:
            await asyncio.sleep(REAPER_INTERVAL)
            now = time.monotonic()
            for name in list(self.connections):
                connection = self.connections[name]
                if now - connection.last_active >= IDLE_TIMEOUT:
                    await connection.close()
                    self.connections.pop(name, None)
            if not self.connections and now - self.last_activity >= DAEMON_IDLE_TIMEOUT:
                self.stop_event.set()
                return


async def _handle_client(
    daemon: Daemon, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    try:
        line = await reader.readline()
        request = json.loads(line)
        response = await daemon.handle(request)
    except Exception as exc:
        response = {"ok": False, "error": str(exc), "type": "internal"}
    writer.write(json.dumps(response, ensure_ascii=False).encode() + b"\n")
    try:
        await writer.drain()
    except Exception:
        pass
    writer.close()


async def serve() -> None:
    _ensure_state_dir()
    for path in (SOCKET_PATH, PID_PATH):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
    PID_PATH.write_text(str(os.getpid()))
    os.chmod(PID_PATH, 0o600)
    daemon = Daemon()
    server = await asyncio.start_unix_server(
        lambda reader, writer: _handle_client(daemon, reader, writer),
        path=str(SOCKET_PATH),
    )
    os.chmod(SOCKET_PATH, 0o600)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, daemon.stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass
    reaper = asyncio.create_task(daemon.reaper())
    async with server:
        await daemon.stop_event.wait()
    reaper.cancel()
    for connection in list(daemon.connections.values()):
        await connection.close()
    daemon.connections.clear()
    for path in (SOCKET_PATH, PID_PATH):
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def _human(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    return f"{seconds // 3600}h"


def _print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str))


def _print_connections(rows: list[dict]) -> None:
    if not rows:
        print("没有活跃连接。")
        return
    print(f"{'TRANSPORT':<10}{'TOOLS':<8}{'IDLE':<8}URL")
    for row in rows:
        print(
            f"{row['transport']:<10}{row['tools']:<8}"
            f"{_human(row['idle_seconds']):<8}{row['url']}"
        )


def _print_tools(tools: list[dict], as_json: bool) -> None:
    if as_json:
        _print_json(tools)
        return
    if not tools:
        print("该连接没有暴露工具。")
        return
    for tool in tools:
        description = (tool.get("description") or "").strip().replace("\n", " ")
        print(f"{tool['name']}\t{description}".rstrip())
    print("\n完整参数 schema 使用 --json 查看。")


def _print_call(data: dict, as_json: bool) -> None:
    if as_json:
        _print_json(data)
        return
    content = data.get("content") or []
    if content:
        for item in content:
            if isinstance(item, str):
                print(item)
            else:
                _print_json(item)
    elif data.get("structured") is not None:
        _print_json(data["structured"])
    else:
        print("工具没有返回内容。")
    if data.get("is_error"):
        print("工具返回了错误结果。", file=sys.stderr)


def parse_headers(values: list[str], token_env: str | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    for item in values:
        if ":" not in item:
            raise ValueError(f"--header 格式应为 'Key: Value'：{item}")
        key, value = item.split(":", 1)
        headers[key.strip()] = value.strip()
    if token_env:
        token = os.environ.get(token_env)
        if not token:
            raise ValueError(f"环境变量 {token_env} 为空")
        headers.setdefault("Authorization", f"Bearer {token}")
    return headers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mcp_client.py",
        description="常驻 MCP 客户端：连接信息只在 connect 时设置一次，之后直接调用",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    connect = sub.add_parser("connect", help="连接 MCP 服务端，密钥只在这里设置一次")
    connect.add_argument("url", help="MCP 服务端地址，例如 https://host/mcp")
    connect.add_argument("--name", help="连接名，默认使用 host:port")
    connect.add_argument(
        "--header",
        action="append",
        default=[],
        metavar="KEY: VALUE",
        help="附加请求头，可重复",
    )
    connect.add_argument(
        "--token-env", metavar="VAR", help="从环境变量读取 Bearer token"
    )
    connect.add_argument(
        "--transport", choices=["auto", "http", "sse"], default="auto"
    )
    connect.add_argument("--timeout", type=float, default=30.0)

    sub.add_parser("list", help="列出已保持的连接")

    status = sub.add_parser("status", help="显示 daemon 和连接的详细状态")
    status.add_argument("--json", action="store_true")

    tools = sub.add_parser("tools", help="列出工具")
    tools.add_argument("--name", help="连接名，只有一个连接时可省略")
    tools.add_argument("--json", action="store_true")

    call = sub.add_parser("call", help="调用工具")
    call.add_argument("tool")
    call.add_argument("arguments", nargs="?", default="{}", help="JSON 参数，默认 {}")
    call.add_argument("--name", help="连接名，只有一个连接时可省略")
    call.add_argument("--json", action="store_true")

    disconnect = sub.add_parser("disconnect", help="断开连接")
    disconnect.add_argument("name", help="连接名，或 all")

    return parser


def _fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "_serve":
        asyncio.run(serve())
        return
    args = build_parser().parse_args()
    try:
        if args.command == "connect":
            headers = parse_headers(args.header, args.token_env)
            ensure_daemon()
            response = _rpc(
                {
                    "cmd": "connect",
                    "name": args.name,
                    "url": args.url,
                    "headers": headers,
                    "transport": args.transport,
                    "timeout": args.timeout,
                }
            )
            if not response.get("ok"):
                _fail(response.get("error", "连接失败"))
            data = response["data"]
            if data["status"] == "already_connected":
                print(f"连接 {data['name']} 已存在，直接复用。")
            else:
                print(f"已连接 {data['name']}（{args.url}），{data['tools']} 个工具。")
            instructions = (data.get("instructions") or "").strip()
            if instructions:
                print("\n服务端说明：")
                print(instructions)
            return

        if args.command == "list":
            if not _daemon_alive():
                print("没有活跃连接。")
                return
            response = _rpc({"cmd": "list"})
            if not response.get("ok"):
                _fail(response.get("error", "读取连接失败"))
            _print_connections(response["data"])
            return

        if args.command == "status":
            if not _daemon_alive():
                print("daemon 未运行，没有活跃连接。")
                return
            response = _rpc({"cmd": "status"})
            if not response.get("ok"):
                _fail(response.get("error", "读取状态失败"))
            data = response["data"]
            if args.json:
                _print_json(data)
                return
            print(f"pid: {data['pid']}")
            print(f"socket: {data['socket']}")
            print(f"连接空闲超时: {_human(data['idle_timeout'])}")
            _print_connections(data["connections"])
            return

        if args.command == "disconnect":
            if not _daemon_alive():
                print("没有活跃连接。")
                return
            response = _rpc({"cmd": "disconnect", "name": args.name})
            if not response.get("ok"):
                _fail(response.get("error", "断开失败"))
            names = response["data"]["disconnected"]
            print("已断开：" + (", ".join(names) if names else "无"))
            return

        if args.command == "tools":
            ensure_daemon()
            response = _rpc({"cmd": "tools", "name": args.name})
            if not response.get("ok"):
                _fail(response.get("error", "读取工具失败"))
            _print_tools(response["data"], args.json)
            return

        if args.command == "call":
            try:
                arguments = json.loads(args.arguments)
            except json.JSONDecodeError as exc:
                _fail(f"参数不是合法 JSON：{exc}")
            if not isinstance(arguments, dict):
                _fail("参数必须是 JSON 对象")
            ensure_daemon()
            response = _rpc(
                {
                    "cmd": "call",
                    "name": args.name,
                    "tool": args.tool,
                    "arguments": arguments,
                }
            )
            if not response.get("ok"):
                _fail(response.get("error", "调用失败"))
            _print_call(response["data"], args.json)
            if response["data"].get("is_error"):
                raise SystemExit(1)
            return

        _fail(f"未知命令：{args.command}")
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    except Exception as exc:
        _fail(str(exc))


if __name__ == "__main__":
    main()


