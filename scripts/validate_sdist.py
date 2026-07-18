#!/usr/bin/env python3
"""Fail when a source distribution contains files outside the release allowlist."""

from __future__ import annotations

import argparse
import tarfile
from pathlib import Path, PurePosixPath

ALLOWED_ROOT_FILES = frozenset({".gitignore", "LICENSE", "PKG-INFO", "README.md", "pyproject.toml"})
PACKAGE_PREFIX = PurePosixPath("src/soveren_agent_platform")


def validate_sdist(path: Path) -> None:
    errors: list[str] = []
    roots: set[str] = set()
    packaged_files: set[PurePosixPath] = set()
    with tarfile.open(path, mode="r:*") as archive:
        for member in archive.getmembers():
            member_path = PurePosixPath(member.name)
            if member_path.is_absolute() or ".." in member_path.parts or not member_path.parts:
                errors.append(f"unsafe archive path: {member.name!r}")
                continue
            roots.add(member_path.parts[0])
            if member.issym() or member.islnk():
                errors.append(f"archive links are not allowed: {member.name!r}")
                continue
            if len(member_path.parts) == 1:
                continue
            relative = PurePosixPath(*member_path.parts[1:])
            if member.isfile():
                packaged_files.add(relative)
                if relative.name in ALLOWED_ROOT_FILES and len(relative.parts) == 1:
                    continue
                if relative.is_relative_to(PACKAGE_PREFIX):
                    continue
                errors.append(f"unexpected source-distribution file: {relative}")
                continue
            if member.isdir() and (
                relative in {PurePosixPath("src"), PACKAGE_PREFIX}
                or relative.is_relative_to(PACKAGE_PREFIX)
            ):
                continue
            if member.isdir():
                errors.append(f"unexpected source-distribution directory: {relative}")
                continue
            errors.append(f"unsupported source-distribution member: {relative}")

    if len(roots) != 1:
        errors.append(f"source distribution must have one archive root, found {sorted(roots)!r}")
    required = {PurePosixPath(name) for name in ALLOWED_ROOT_FILES}
    required.add(PACKAGE_PREFIX / "py.typed")
    missing = sorted(str(value) for value in required - packaged_files)
    if missing:
        errors.append(f"source distribution is missing required files: {missing!r}")
    if errors:
        raise SystemExit("source distribution validation failed:\n- " + "\n- ".join(errors))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archives", nargs="+", type=Path)
    args = parser.parse_args()
    for archive in args.archives:
        validate_sdist(archive)
        print(f"source distribution inventory is safe: {archive}")


if __name__ == "__main__":
    main()
