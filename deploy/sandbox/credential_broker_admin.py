"""Bounded, stdlib-only broker Unix-socket admin client; credentials enter stdin."""

from __future__ import annotations

import asyncio
import json
import struct
import sys
from pathlib import Path
from typing import Any

ADMIN_SOCKET_PATH = Path("/run/soveren/credential-broker.sock")
MAX_ADMIN_PAYLOAD_BYTES = 1024 * 1024
ADMIN_TIMEOUT_S = 10.0


async def _send_admin_payload(path: Path, payload: bytes) -> dict[str, Any]:
    if not payload or len(payload) > MAX_ADMIN_PAYLOAD_BYTES:
        raise ValueError("credential registry payload size is invalid")
    async with asyncio.timeout(ADMIN_TIMEOUT_S):
        reader, writer = await asyncio.open_unix_connection(str(path))
        try:
            writer.write(struct.pack("!I", len(payload)) + payload)
            await writer.drain()
            size = struct.unpack("!I", await reader.readexactly(4))[0]
            if size < 1 or size > 4096:
                raise RuntimeError("credential broker admin response is invalid")
            response = json.loads(await reader.readexactly(size))
            if not isinstance(response, dict):
                raise RuntimeError("credential broker admin response is invalid")
            return response
        finally:
            writer.close()
            await writer.wait_closed()


async def main() -> None:
    payload = await asyncio.to_thread(sys.stdin.buffer.read, MAX_ADMIN_PAYLOAD_BYTES + 1)
    response = await _send_admin_payload(ADMIN_SOCKET_PATH, payload)
    if response.get("ok") is not True:
        raise RuntimeError("credential registry update failed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        raise SystemExit("credential registry update failed") from None
