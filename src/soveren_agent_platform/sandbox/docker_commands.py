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
    def __init__(self, *, timeout_s: float = 300.0, terminate_grace_s: float = 3.0) -> None:
        if timeout_s <= 0 or terminate_grace_s <= 0:
            raise ValueError("Docker command timeouts must be positive")
        self.timeout_s = timeout_s
        self.terminate_grace_s = terminate_grace_s

    async def run(self, args: list[str], *, input_data: bytes | None = None) -> CommandResult:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE if input_data is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(self.timeout_s):
                stdout, stderr = await proc.communicate(input_data)
        except TimeoutError as exc:
            await self._stop_process(proc)
            raise TimeoutError(f"Docker command timed out after {self.timeout_s:g} seconds") from exc
        except asyncio.CancelledError:
            await self._stop_process(proc)
            raise
        returncode = proc.returncode if proc.returncode is not None else -1
        return CommandResult(
            returncode=returncode,
            stdout=stdout.decode("utf-8", "replace"),
            stderr=stderr.decode("utf-8", "replace"),
        )

    async def _stop_process(self, proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is not None:
            return
        try:
            proc.terminate()
        except ProcessLookupError:
            return
        cleanup_task = asyncio.create_task(self._wait_then_kill(proc))
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                continue
        cleanup_task.result()

    async def _wait_then_kill(self, proc: asyncio.subprocess.Process) -> None:
        try:
            await asyncio.wait_for(proc.wait(), timeout=self.terminate_grace_s)
        except TimeoutError:
            if proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    return
            await proc.wait()
