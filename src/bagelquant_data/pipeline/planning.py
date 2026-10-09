"""Read-only plans for resumable initialization and subsequent updates."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any


def initialization_actions(*, start: date, end: date, definition_hash: str,
                           initialization: Mapping[str, Any] | None,
                           has_history: bool) -> list[dict[str, str]]:
    """Preserve an unfinished baseline's bounds before admitting new work."""
    if end < start:
        raise ValueError("end precedes start")
    if initialization is not None and initialization["status"] == "running":
        first = date.fromisoformat(str(initialization["start"]))
        last = date.fromisoformat(str(initialization["end"]))
        if initialization["definition_hash"] != definition_hash or first != start:
            raise ValueError("Unfinished initialization requires its original definition and start")
        if end < last:
            raise ValueError("Requested end precedes the frozen initialization end")
        actions = [{"mode": "initialize", "start": first.isoformat(), "end": last.isoformat()}]
        if end > last:
            actions.append({"mode": "incremental", "start": start.isoformat(), "end": end.isoformat()})
        return actions
    mode = "incremental" if initialization is not None or has_history else "initialize"
    return [{"mode": mode, "start": start.isoformat(), "end": end.isoformat()}]
