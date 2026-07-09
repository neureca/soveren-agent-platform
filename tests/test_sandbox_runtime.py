import asyncio
import hashlib
import json
from types import SimpleNamespace

import pytest

from soveren_agent_platform.sandbox import CommandResult, DockerSandboxRuntime, SandboxHandle, SandboxSpec
from soveren_agent_platform.sessions import OpenSpec, SandboxedCodexAppServerBackend


class FakeDockerRunner:
    def __init__(self, results: list[CommandResult]) -> None:
        self.results = results
        self.calls: list[list[str]] = []

    async def run(self, args: list[str]) -> CommandResult:
        self.calls.append(args)
        if not self.results:
            return CommandResult(returncode=0)
        return self.results.pop(0)


def test_docker_sandbox_runtime_creates_container_with_hard_limits_without_raw_tenant_label():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="container-123\n"),
        ]
    )
    runtime = DockerSandboxRuntime(runner=runner)

    handle = asyncio.run(
        runtime.acquire(
            SandboxSpec(
                tenant_id="telegram-chat-123",
                image="soveren-codex-sandbox:latest",
                memory="384m",
                cpus="0.5",
                pids_limit=96,
                network="bridge",
            )
        )
    )

    assert handle.id == "container-123"
    run = runner.calls[1]
    assert run[:3] == ["docker", "run", "-d"]
    assert "--memory" in run
    assert run[run.index("--memory") + 1] == "384m"
    assert "--cpus" in run
    assert run[run.index("--cpus") + 1] == "0.5"
    assert "--pids-limit" in run
    assert run[run.index("--pids-limit") + 1] == "96"
    assert "--network" in run
    assert run[run.index("--network") + 1] == "bridge"
    assert "telegram-chat-123" not in " ".join(run)
    assert "soveren.tenant_key=" in " ".join(run)
    assert "soveren.spec_hash=" in " ".join(run)


def test_docker_sandbox_runtime_reuses_existing_stopped_container():
    spec = SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest")
    spec_hash = _expected_spec_hash(spec)
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="container-123\n"),
            CommandResult(returncode=0, stdout=f"{spec_hash}\n"),
            CommandResult(returncode=0, stdout="false\n"),
            CommandResult(returncode=0, stdout="container-123\n"),
        ]
    )
    runtime = DockerSandboxRuntime(runner=runner)

    handle = asyncio.run(runtime.acquire(spec))

    assert handle.id == "container-123"
    assert runner.calls[3] == ["docker", "start", "container-123"]


def test_docker_sandbox_runtime_rejects_existing_container_with_different_spec():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="container-123\n"),
            CommandResult(returncode=0, stdout="old-spec-hash\n"),
        ]
    )
    runtime = DockerSandboxRuntime(runner=runner)

    with pytest.raises(RuntimeError, match="config changed"):
        asyncio.run(
            runtime.acquire(SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest"))
        )

    assert all("start" not in call for call in runner.calls)


def test_docker_sandbox_runtime_rejects_host_network():
    runtime = DockerSandboxRuntime(runner=FakeDockerRunner([]))

    with pytest.raises(ValueError, match="network"):
        asyncio.run(
            runtime.acquire(
                SandboxSpec(
                    tenant_id="tenant-a",
                    image="soveren-codex-sandbox:latest",
                    network="host",
                )
            )
        )


def test_docker_sandbox_runtime_builds_interactive_exec_command():
    runtime = DockerSandboxRuntime()
    handle = SandboxHandle(
        id="container-123",
        name="soveren-sandbox",
        tenant_id="tenant-a",
        workspace_root="/workspace",
        codex_home="/codex-home",
    )

    command = runtime.exec_command(
        handle,
        ["codex", "app-server", "--listen", "stdio://"],
        env={"CODEX_HOME": "/codex-home"},
        workdir="/workspace",
    )

    assert command == [
        "docker",
        "exec",
        "-i",
        "-w",
        "/workspace",
        "-e",
        "CODEX_HOME=/codex-home",
        "container-123",
        "codex",
        "app-server",
        "--listen",
        "stdio://",
    ]


class FakeSandboxRuntime:
    def __init__(self) -> None:
        self.handle = SandboxHandle(
            id="container-123",
            name="soveren-sandbox-abc",
            tenant_id="tenant-a",
            workspace_root="/workspace",
            codex_home="/codex-home",
            metadata={"runtime": "docker", "tenant_key": "abc"},
        )
        self.acquired: list[SandboxSpec] = []
        self.directories: list[str] = []
        self.commands: list[list[str]] = []

    async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
        self.acquired.append(spec)
        return self.handle

    async def destroy(self, handle: SandboxHandle) -> None:
        return None

    async def ensure_directory(self, handle: SandboxHandle, path: str) -> None:
        self.directories.append(path)

    def exec_command(
        self,
        handle: SandboxHandle,
        command: list[str],
        *,
        env: dict[str, str] | None = None,
        workdir: str | None = None,
        interactive: bool = True,
    ) -> list[str]:
        built = ["docker", "exec", "-i", handle.id, *command]
        self.commands.append(built)
        return built


class FakeCodexClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.last_turns: dict[str, object] = {}

    async def request(self, method: str, params: dict):
        self.calls.append((method, params))
        if method == "thread/start":
            return {"thread": {"id": "thread-1"}, "modelProvider": "openai", "cwd": params["cwd"]}
        if method == "thread/archive":
            return {}
        return {}

    async def close(self) -> None:
        return None

    def set_last_turn(self, thread_id: str, turn_id: str):
        state = SimpleNamespace(turn_id=turn_id)
        self.last_turns[thread_id] = state
        return state

    def last_turn(self, thread_id: str):
        return self.last_turns.get(thread_id)


def test_sandboxed_codex_backend_opens_thread_inside_sandbox():
    runtime = FakeSandboxRuntime()
    client = FakeCodexClient()
    backend = SandboxedCodexAppServerBackend(
        sandbox_runtime=runtime,
        sandbox_spec=SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest"),
        client=client,
    )

    opened = asyncio.run(
        backend.open(
            OpenSpec(
                kind="codex_cli",
                cwd="/host/path/ignored",
                metadata={"sandbox_cwd": "/workspace/chat-a"},
            )
        )
    )

    assert opened.backend_session_id == "thread-1"
    assert runtime.directories == ["/workspace", "/codex-home", "/workspace/chat-a"]
    assert runtime.commands == [["docker", "exec", "-i", "container-123", "codex", "app-server", "--listen", "stdio://"]]
    assert client.calls[0][0] == "thread/start"
    assert client.calls[0][1]["cwd"] == "/workspace/chat-a"
    assert opened.metadata["runtime"] == "sandboxed_codex_app_server"
    assert opened.metadata["sandbox_runtime"] == "docker"


def _expected_spec_hash(spec: SandboxSpec) -> str:
    payload = {
        "image": spec.image,
        "memory": spec.memory,
        "cpus": spec.cpus,
        "pids_limit": spec.pids_limit,
        "network": spec.network,
        "workspace_root": spec.workspace_root,
        "codex_home": spec.codex_home,
        "command": list(spec.command),
        "env": dict(sorted(spec.env.items())),
        "labels": dict(sorted(spec.labels.items())),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@pytest.mark.parametrize("sandbox_cwd", ["/", "/codex-home", "/workspace/../codex-home"])
def test_sandboxed_codex_backend_rejects_cwd_outside_workspace(sandbox_cwd):
    runtime = FakeSandboxRuntime()
    backend = SandboxedCodexAppServerBackend(
        sandbox_runtime=runtime,
        sandbox_spec=SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest"),
        client=FakeCodexClient(),
    )

    with pytest.raises(ValueError, match="workspace root"):
        asyncio.run(
            backend.open(
                OpenSpec(
                    kind="codex_cli",
                    cwd="/host/path/ignored",
                    metadata={"sandbox_cwd": sandbox_cwd},
                )
            )
        )
