#!/usr/bin/env python3
"""Validate version coupling between package and published runtime images."""
from __future__ import annotations

import tomllib
from pathlib import Path

from soveren_agent_platform import __version__
from soveren_agent_platform.sessions import (
    DEFAULT_CREDENTIAL_BROKER_IMAGE,
    DEFAULT_EGRESS_IMAGE,
    DEFAULT_SANDBOX_IMAGE,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    with (ROOT / "pyproject.toml").open("rb") as project_file:
        project_version = str(tomllib.load(project_file)["project"]["version"])

    expected_sandbox = f"ghcr.io/neureca/soveren-codex-sandbox:{project_version}"
    expected_egress = f"ghcr.io/neureca/soveren-sandbox-egress:{project_version}"
    expected_broker = f"ghcr.io/neureca/soveren-credential-broker:{project_version}"
    compose = (ROOT / "deploy" / "sandbox" / "compose.yaml").read_text()
    readme = (ROOT / "README.md").read_text()
    api_docs = (ROOT / "docs" / "API.md").read_text()
    consuming_docs = (ROOT / "docs" / "CONSUMING_APP.md").read_text()
    major, minor, *_ = (int(part) for part in project_version.split("."))
    expected_range = f"soveren-agent-platform>={major}.{minor},<{major}.{minor + 1}"
    errors: list[str] = []
    if __version__ != project_version:
        errors.append(f"package metadata version {__version__!r} != {project_version!r}")
    if DEFAULT_SANDBOX_IMAGE != expected_sandbox:
        errors.append(f"sandbox image {DEFAULT_SANDBOX_IMAGE!r} != {expected_sandbox!r}")
    if DEFAULT_EGRESS_IMAGE != expected_egress:
        errors.append(f"egress image {DEFAULT_EGRESS_IMAGE!r} != {expected_egress!r}")
    if DEFAULT_CREDENTIAL_BROKER_IMAGE != expected_broker:
        errors.append(f"credential broker image {DEFAULT_CREDENTIAL_BROKER_IMAGE!r} != {expected_broker!r}")
    if expected_egress not in compose:
        errors.append(f"compose default does not contain {expected_egress!r}")
    if expected_sandbox not in api_docs or expected_egress not in api_docs or expected_broker not in api_docs:
        errors.append("API docs do not reference all current runtime image tags")
    if (
        expected_range not in readme
        or expected_range not in api_docs
        or expected_range not in consuming_docs
    ):
        errors.append(f"consumer docs do not contain current dependency range {expected_range!r}")
    if errors:
        raise SystemExit("release validation failed:\n- " + "\n- ".join(errors))
    print(f"release {project_version} is internally consistent")


if __name__ == "__main__":
    main()
