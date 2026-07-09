import asyncio
import hashlib
import json
from types import SimpleNamespace

import pytest

from soveren_agent_platform.sandbox import (
    CommandResult,
    DockerEgressSpec,
    DockerSandboxRuntime,
    SandboxHandle,
    SandboxSpec,
)
from soveren_agent_platform.sessions import (
    CodexApiKeyCredentials,
    CodexAuthFileCredentials,
    ExistingCodexCredentials,
    OpenSpec,
    SandboxedCodexAppServerBackend,
    SessionBackendRegistry,
    create_sandbox_pool,
    create_sandboxed_codex_backend,
)


class FakeDockerRunner:
    def __init__(self, results: list[CommandResult]) -> None:
        self.results = results
        self.calls: list[list[str]] = []

    async def run(self, args: list[str], *, input_data: bytes | None = None) -> CommandResult:
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
                network="soveren-sandbox-egress",
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
    assert run[run.index("--network") + 1] == "soveren-sandbox-egress"
    assert run[run.index("--storage-opt") + 1] == "size=1g"
    assert run[run.index("--user") + 1] == "10001:10001"
    assert run[run.index("--cap-drop") + 1] == "ALL"
    assert "no-new-privileges:true" in run
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


def test_docker_sandbox_runtime_reports_missing_disk_quota_support():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=1, stderr="overlay2: storage-opt size is not supported"),
            CommandResult(returncode=0, stdout=""),
        ]
    )
    runtime = DockerSandboxRuntime(runner=runner)

    with pytest.raises(RuntimeError, match="XFS with pquota"):
        asyncio.run(runtime.acquire(
            SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest")
        ))


def test_docker_sandbox_runtime_recovers_from_cross_process_create_race():
    spec = SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest")
    spec_hash = _expected_spec_hash(spec)
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=1, stderr="container name is already in use"),
            CommandResult(returncode=0, stdout="container-winner\n"),
            CommandResult(returncode=0, stdout=f"{spec_hash}\n"),
            CommandResult(returncode=0, stdout="true\n"),
        ]
    )
    runtime = DockerSandboxRuntime(runner=runner)

    handle = asyncio.run(runtime.acquire(spec))

    assert handle.id == "container-winner"


def test_docker_sandbox_runtime_provisions_shared_egress_before_tenant_container():
    egress_image = "soveren-sandbox-egress:test"
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=1, stderr="not found"),
            CommandResult(returncode=0, stdout="internal-network\n"),
            CommandResult(returncode=1, stderr="not found"),
            CommandResult(returncode=0, stdout="public-network\n"),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="egress-123\n"),
            CommandResult(returncode=0, stdout=f"{egress_image}\n"),
            CommandResult(returncode=0, stdout="true\n"),
            CommandResult(returncode=0, stdout='{"soveren-sandbox-public-egress": {}}\n'),
            CommandResult(returncode=0),
            CommandResult(returncode=0, stdout="healthy\n"),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="tenant-123\n"),
        ]
    )
    runtime = DockerSandboxRuntime(
        runner=runner,
        egress=DockerEgressSpec(image=egress_image),
    )

    handle = asyncio.run(runtime.acquire(SandboxSpec(
        tenant_id="tenant-a",
        image="soveren-codex-sandbox:test",
        network="soveren-sandbox-egress",
    )))

    assert handle.id == "tenant-123"
    assert runner.calls[1] == ["docker", "network", "create", "--internal", "soveren-sandbox-egress"]
    assert runner.calls[3] == ["docker", "network", "create", "soveren-sandbox-public-egress"]
    egress_run = runner.calls[5]
    assert egress_run[:3] == ["docker", "run", "-d"]
    assert egress_image in egress_run
    assert "--read-only" in egress_run
    assert runner.calls[9][-3:] == [
        "soveren-sandbox-egress",
        "soveren-sandbox-egress",
        "egress-123",
    ]
    assert runner.calls[12][1:3] == ["run", "-d"]


def test_docker_sandbox_runtime_recovers_running_orphans_once_after_process_restart():
    runner = FakeDockerRunner(
        [
            CommandResult(returncode=0, stdout="orphan-a\norphan-b\n"),
            CommandResult(returncode=0, stdout="orphan-a\n"),
            CommandResult(returncode=0, stdout="orphan-b\n"),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="tenant-a\n"),
            CommandResult(returncode=0, stdout=""),
            CommandResult(returncode=0, stdout="tenant-b\n"),
        ]
    )
    runtime = DockerSandboxRuntime(
        runner=runner,
        max_active_sandboxes=2,
        recover_orphaned_sandboxes=True,
    )

    async def run():
        first = await runtime.acquire(
            SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest")
        )
        second = await runtime.acquire(
            SandboxSpec(tenant_id="tenant-b", image="soveren-codex-sandbox:latest")
        )
        return first, second

    first, second = asyncio.run(run())

    assert (first.id, second.id) == ("tenant-a", "tenant-b")
    assert runner.calls[0][1:3] == ["ps", "-q"]
    assert runner.calls[1] == ["docker", "stop", "orphan-a"]
    assert runner.calls[2] == ["docker", "stop", "orphan-b"]
    assert sum(call[1:3] == ["ps", "-q"] for call in runner.calls) == 1


def test_docker_sandbox_runtime_limits_active_tenant_capacity():
    class FastDockerSandboxRuntime(DockerSandboxRuntime):
        async def _acquire_locked(self, spec, *, tenant_key):
            return SandboxHandle(
                id=f"container-{tenant_key[:8]}",
                name=f"sandbox-{tenant_key[:8]}",
                tenant_id=spec.tenant_id,
                workspace_root=spec.workspace_root,
                codex_home=spec.codex_home,
                metadata={"tenant_key": tenant_key},
            )

    async def run():
        runtime = FastDockerSandboxRuntime(
            runner=FakeDockerRunner([]),
            max_active_sandboxes=1,
        )
        first = await runtime.acquire(
            SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest")
        )
        second_task = asyncio.create_task(runtime.acquire(
            SandboxSpec(tenant_id="tenant-b", image="soveren-codex-sandbox:latest")
        ))
        await asyncio.sleep(0)
        assert not second_task.done()
        await runtime.stop(first)
        second = await asyncio.wait_for(second_task, timeout=1)
        await runtime.destroy(second)
        return second

    second = asyncio.run(run())

    assert second.tenant_id == "tenant-b"


def test_docker_sandbox_runtime_keeps_capacity_reserved_when_stop_fails():
    class FastDockerSandboxRuntime(DockerSandboxRuntime):
        async def _acquire_locked(self, spec, *, tenant_key):
            return SandboxHandle(
                id=f"container-{tenant_key[:8]}",
                name=f"sandbox-{tenant_key[:8]}",
                tenant_id=spec.tenant_id,
                workspace_root=spec.workspace_root,
                codex_home=spec.codex_home,
                metadata={"tenant_key": tenant_key},
            )

    async def run():
        runtime = FastDockerSandboxRuntime(
            runner=FakeDockerRunner([CommandResult(returncode=1, stderr="stop failed")]),
            max_active_sandboxes=1,
        )
        first = await runtime.acquire(
            SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest")
        )
        with pytest.raises(RuntimeError, match="stop failed"):
            await runtime.stop(first)
        second_task = asyncio.create_task(runtime.acquire(
            SandboxSpec(tenant_id="tenant-b", image="soveren-codex-sandbox:latest")
        ))
        await asyncio.sleep(0)
        assert not second_task.done()
        second_task.cancel()
        await asyncio.gather(second_task, return_exceptions=True)

    asyncio.run(run())


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
        self.command_inputs: list[bytes | None] = []
        self.stopped: list[SandboxHandle] = []
        self.destroyed: list[SandboxHandle] = []

    async def acquire(self, spec: SandboxSpec) -> SandboxHandle:
        self.acquired.append(spec)
        return self.handle

    async def destroy(self, handle: SandboxHandle) -> None:
        self.destroyed.append(handle)

    async def stop(self, handle: SandboxHandle) -> None:
        self.stopped.append(handle)

    async def ensure_directory(self, handle: SandboxHandle, path: str) -> None:
        self.directories.append(path)

    async def run_command(
        self,
        handle: SandboxHandle,
        command: list[str],
        *,
        input_data: bytes | None = None,
        env: dict[str, str] | None = None,
        workdir: str | None = None,
    ) -> None:
        self.commands.append(["run", handle.id, *command])
        self.command_inputs.append(input_data)

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
        if method == "turn/start":
            return {"turn": {"id": "turn-1"}}
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
    assert opened.metadata["runtime"] == "codex"
    assert opened.metadata["sandbox_runtime"] == "docker"


def test_sandboxed_codex_backend_single_flights_concurrent_open_and_stops_on_shutdown():
    async def run():
        runtime = FakeSandboxRuntime()
        backend = SandboxedCodexAppServerBackend(
            sandbox_runtime=runtime,
            sandbox_spec=SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest"),
            client=FakeCodexClient(),
        )
        opened = await asyncio.gather(*(
            backend.open(OpenSpec(kind="codex_cli", cwd="/ignored"))
            for _ in range(10)
        ))
        await backend.shutdown()
        return runtime, opened

    runtime, opened = asyncio.run(run())

    assert len(runtime.acquired) == 1
    assert len(runtime.commands) == 1
    assert len(opened) == 10
    assert runtime.stopped == [runtime.handle]
    assert runtime.destroyed == []


def test_sandboxed_codex_backend_stops_container_when_app_server_shutdown_fails():
    class FailingCloseCodexClient(FakeCodexClient):
        async def close(self) -> None:
            raise RuntimeError("app-server close failed")

    async def run():
        runtime = FakeSandboxRuntime()
        backend = SandboxedCodexAppServerBackend(
            sandbox_runtime=runtime,
            sandbox_spec=SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest"),
            client=FailingCloseCodexClient(),
        )
        await backend.open(OpenSpec(kind="codex_cli", cwd="/ignored"))
        with pytest.raises(ExceptionGroup, match="sandboxed Codex shutdown failed"):
            await backend.shutdown()
        return runtime

    runtime = asyncio.run(run())

    assert runtime.stopped == [runtime.handle]


def test_sandboxed_codex_backend_resumes_persisted_thread_after_process_restart():
    async def run():
        runtime = FakeSandboxRuntime()
        client = FakeCodexClient()
        backend = SandboxedCodexAppServerBackend(
            sandbox_runtime=runtime,
            sandbox_spec=SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest"),
            client=client,
        )
        await backend.send("thread-existing", "continue")
        return runtime, client

    runtime, client = asyncio.run(run())

    assert len(runtime.acquired) == 1
    assert client.calls == [
        ("thread/resume", {"threadId": "thread-existing"}),
        (
            "turn/start",
            {"threadId": "thread-existing", "input": [{"type": "text", "text": "continue"}]},
        ),
    ]


def test_sandboxed_codex_backend_stops_sandbox_when_credential_provisioning_fails():
    class FailingCredentials:
        async def provision(self, runtime, handle):
            raise RuntimeError("credentials unavailable")

    async def run():
        runtime = FakeSandboxRuntime()
        backend = SandboxedCodexAppServerBackend(
            sandbox_runtime=runtime,
            sandbox_spec=SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest"),
            credentials=FailingCredentials(),
            client=FakeCodexClient(),
        )
        with pytest.raises(RuntimeError, match="credentials unavailable"):
            await backend.send("thread-existing", "continue")
        return runtime

    runtime = asyncio.run(run())

    assert runtime.stopped == [runtime.handle]


def test_sandboxed_codex_backend_stops_after_failed_thread_start():
    class FailingThreadStartClient(FakeCodexClient):
        async def request(self, method: str, params: dict):
            if method == "thread/start":
                raise RuntimeError("thread start failed")
            return await super().request(method, params)

    async def run():
        runtime = FakeSandboxRuntime()
        backend = SandboxedCodexAppServerBackend(
            sandbox_runtime=runtime,
            sandbox_spec=SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest"),
            client=FailingThreadStartClient(),
            idle_stop_after_s=0,
        )
        with pytest.raises(RuntimeError, match="thread start failed"):
            await backend.open(OpenSpec(kind="codex_cli", cwd="/ignored"))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return runtime

    runtime = asyncio.run(run())

    assert runtime.stopped == [runtime.handle]


def test_sandboxed_codex_backend_stops_after_last_thread_becomes_idle():
    async def run():
        runtime = FakeSandboxRuntime()
        backend = SandboxedCodexAppServerBackend(
            sandbox_runtime=runtime,
            sandbox_spec=SandboxSpec(tenant_id="tenant-a", image="soveren-codex-sandbox:latest"),
            client=FakeCodexClient(),
            idle_stop_after_s=0,
        )
        opened = await backend.open(OpenSpec(kind="codex_cli", cwd="/ignored"))
        await backend.close(opened.backend_session_id)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return runtime

    runtime = asyncio.run(run())

    assert runtime.stopped == [runtime.handle]


def test_codex_credentials_are_streamed_without_docker_metadata(tmp_path):
    auth_path = tmp_path / "auth.json"
    auth_path.write_text('{"tokens":{"access_token":"secret"}}')

    async def run():
        runtime = FakeSandboxRuntime()
        await CodexAuthFileCredentials(auth_path).provision(runtime, runtime.handle)
        api_credentials = CodexApiKeyCredentials("sk-secret")
        await api_credentials.provision(runtime, runtime.handle)
        return runtime, api_credentials

    runtime, api_credentials = asyncio.run(run())

    assert runtime.command_inputs == [auth_path.read_bytes(), b"sk-secret\n"]
    assert "secret" not in repr(api_credentials)
    assert all("secret" not in " ".join(command) for command in runtime.commands)
    assert 'test -s "$CODEX_HOME/auth.json"' in " ".join(runtime.commands[0])


def test_create_sandboxed_codex_backend_uses_profile_and_registers_backend():
    runtime = FakeSandboxRuntime()
    registry = SessionBackendRegistry()

    backend = create_sandboxed_codex_backend(
        tenant_id="tenant-a",
        credentials=ExistingCodexCredentials(),
        resources="small",
        session_backends=registry,
        sandbox_runtime=runtime,
    )

    assert registry.require(backend.name) is backend
    assert backend.sandbox_spec.memory == "512m"
    assert backend.sandbox_spec.disk_limit == "1g"
    assert backend.sandbox_spec.network == "soveren-sandbox-egress"
    assert backend.sandbox_spec.env["HTTPS_PROXY"] == "http://soveren-sandbox-egress:3128"


def test_create_sandbox_pool_owns_shared_capacity_and_managed_egress():
    runtime = create_sandbox_pool(max_active_sandboxes=2)

    assert runtime.max_active_sandboxes == 2
    assert runtime.recover_orphaned_sandboxes is True
    assert runtime.egress is not None
    assert runtime.egress.image == "ghcr.io/neureca/soveren-sandbox-egress:0.2.8"


def _expected_spec_hash(spec: SandboxSpec) -> str:
    payload = {
        "policy_version": "1",
        "image": spec.image,
        "memory": spec.memory,
        "cpus": spec.cpus,
        "pids_limit": spec.pids_limit,
        "disk_limit": spec.disk_limit,
        "tmpfs_size": spec.tmpfs_size,
        "network": spec.network,
        "user": spec.user,
        "workspace_root": spec.workspace_root,
        "codex_home": spec.codex_home,
        "command": list(spec.command),
        "env": dict(sorted(spec.env.items())),
        "labels": dict(sorted(spec.labels.items())),
        "name": spec.name,
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
