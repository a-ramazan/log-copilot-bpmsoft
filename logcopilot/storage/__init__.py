from __future__ import annotations

"""SQLite persistence layer for pipeline runs and artifacts."""

from .sqlite import StorageRepository

__all__ = ["StorageRepository"]
