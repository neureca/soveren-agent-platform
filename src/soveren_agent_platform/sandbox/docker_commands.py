"""Typed Docker CLI command boundary shared by sandbox infrastructure managers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class DockerCommandRunner(Protocol):
    async def run(self, args: list[str], *, input_data: bytes | None = None) -> CommandResult: ...


class SubprocessDockerCommandRunner:
    async def run(self, args: list[str], *, input_data: bytes | None = None) -> CommandResult:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE if input_data is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate(input_data)
        returncode = proc.returncode if proc.returncode is not None else -1
        return CommandResult(
            returncode=returncode,
            stdout=stdout.decode("utf-8", "replace"),
            stderr=stderr.decode("utf-8", "replace"),
        )
