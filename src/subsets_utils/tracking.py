"""Lightweight tracking for DAG execution.

This module provides a shared context between orchestrator and io modules
without creating circular imports. The orchestrator sets the current task,
and io functions record what files they write.
"""

from contextvars import ContextVar
from dataclasses import dataclass, field

# Current executing task (set by orchestrator)
_current_task_id: ContextVar[str | None] = ContextVar('current_task_id', default=None)

# Track which task wrote which assets: {asset_path: task_id}
_asset_writers: dict[str, str] = {}


def set_current_task(task_id: str | None):
    """Set the currently executing task ID. Called by orchestrator."""
    _current_task_id.set(task_id)


def get_current_task() -> str | None:
    """Get the currently executing task ID."""
    return _current_task_id.get()


def record_write(asset_path: str):
    """Record that the current task wrote an asset. Called by io functions."""
    task_id = _current_task_id.get()
    if task_id:
        _asset_writers[asset_path] = task_id


def get_writer(asset_path: str) -> str | None:
    """Get the task ID that wrote an asset."""
    return _asset_writers.get(asset_path)


def get_assets_by_writer(task_id: str) -> list[str]:
    """Get all assets written by a specific task."""
    return [asset for asset, writer in _asset_writers.items() if writer == task_id]


def clear_tracking():
    """Clear all tracking data. Called at start of DAG run."""
    _asset_writers.clear()
