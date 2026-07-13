import tomllib
from pathlib import Path

from soveren_agent_platform import __version__
from soveren_agent_platform.sessions import (
    DEFAULT_CREDENTIAL_BROKER_IMAGE,
    DEFAULT_EGRESS_IMAGE,
    DEFAULT_SANDBOX_IMAGE,
)


def test_package_and_runtime_image_versions_are_coupled():
    root = Path(__file__).parents[1]
    with (root / "pyproject.toml").open("rb") as project_file:
        project_version = tomllib.load(project_file)["project"]["version"]

    assert __version__ == project_version
    assert DEFAULT_SANDBOX_IMAGE.endswith(f":{project_version}")
    assert DEFAULT_EGRESS_IMAGE.endswith(f":{project_version}")
    assert DEFAULT_CREDENTIAL_BROKER_IMAGE.endswith(f":{project_version}")
    assert DEFAULT_EGRESS_IMAGE in (root / "deploy" / "sandbox" / "compose.yaml").read_text()
    assert DEFAULT_CREDENTIAL_BROKER_IMAGE in (root / "docs" / "API.md").read_text()
