"""Compatibility shim – actual code lives in bot/"""

from bot import setup, teardown  # noqa: F401

__all__ = ["setup", "teardown"]
