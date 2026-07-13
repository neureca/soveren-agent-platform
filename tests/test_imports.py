from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

PACKAGE_ROOT = Path(__file__).parents[1] / "src" / "soveren_agent_platform"
PACKAGE_MODULES = tuple(
    sorted(
        {
            ".".join(
                (
                    "soveren_agent_platform",
                    *(
                        path.relative_to(PACKAGE_ROOT).parent.parts
                        if path.name == "__init__.py"
                        else path.relative_to(PACKAGE_ROOT).with_suffix("").parts
                    ),
                )
            ).rstrip(".")
            for path in PACKAGE_ROOT.rglob("*.py")
        }
    )
)


@pytest.mark.parametrize("module_name", PACKAGE_MODULES)
def test_package_module_imports_in_clean_interpreter(module_name: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", f"import {module_name}"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
