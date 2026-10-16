"""Select a trusted deployment image through the supported public composition API."""

import asyncio

import pytest

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.sessions import ExistingCodexCredentials
from soveren_agent_platform.sessions.sandboxing import (
    DEFAULT_CREDENTIAL_BROKER_IMAGE,
    DEFAULT_EGRESS_IMAGE,
    DEFAULT_SANDBOX_IMAGE,
)


@pytest.mark.parametrize("image", [None, "soveren-credential-broker:pilot-candidate"])
def test_public_composition_forwards_only_broker_image_without_changing_runtime_limits(tmp_path, image):
    async def run():
        async def credentials(tenant):
            return ExistingCodexCredentials()

        app = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)
        runtime = app.configure_sandboxed_codex(
            credentials_for_tenant=credentials,
            model="fixture",
            credential_broker_image=image,
        )
        try:
            manager = runtime._sandbox_manager
            assert manager.credential_broker.image == (DEFAULT_CREDENTIAL_BROKER_IMAGE if image is None else image)
            assert manager.credential_broker.cpus == "0.25"
            assert manager.credential_broker.memory == "128m"
            assert manager.credential_broker.port == 8080
            assert manager.egress.image == DEFAULT_EGRESS_IMAGE
            assert DEFAULT_SANDBOX_IMAGE.endswith(":0.10.0")
            assert DEFAULT_CREDENTIAL_BROKER_IMAGE == "ghcr.io/neureca/soveren-credential-broker:0.10.0"
            assert manager.recover_orphaned_sandboxes
        finally:
            await runtime.shutdown()

    asyncio.run(run())


@pytest.mark.parametrize("image", ["", " ", 123, False])
def test_invalid_broker_image_is_rejected_before_runtime_activation(tmp_path, image):
    async def credentials(tenant):
        return ExistingCodexCredentials()

    app = AgentPlatformApp(db_path=tmp_path / "app.db", bootstrap_storage=False)
    with pytest.raises((TypeError, ValueError), match="credential_broker_image"):
        app.configure_sandboxed_codex(
            credentials_for_tenant=credentials, model="fixture", credential_broker_image=image
        )
    assert app._sandboxed_codex_runtime is None
