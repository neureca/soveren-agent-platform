"""Reusable runtime primitives for durable agent applications."""

from importlib.metadata import PackageNotFoundError, version

__all__ = ["__version__"]

try:
    __version__ = version("soveren-agent-platform")
except PackageNotFoundError:
    __version__ = "0+unknown"
