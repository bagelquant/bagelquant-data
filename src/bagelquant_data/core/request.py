"""Request planning models."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.types import DateLike


@dataclass(frozen=True, slots=True)
class RequestContext:
    """Context passed to request planners and source adapters."""

    source: str
    dataset: str
    start: DateLike | None = None
    end: DateLike | None = None
    assets: Sequence[str] | None = None
    force: bool = False
    repair: bool = False
    options: Mapping[str, Any] = field(default_factory=dict)


class RequestPlanner(Protocol):
    """Plan source requests for one dataset update."""

    def plan(self, context: RequestContext) -> Iterable[dict[str, object]]:
        """Yield provider-neutral request mappings."""
        ...


class SnapshotPlanner:
    """A single request containing the supplied context filters."""

    def plan(self, context: RequestContext) -> Iterable[dict[str, object]]:
        request: dict[str, object] = dict(context.options)
        if context.start is not None:
            request["start"] = context.start
        if context.end is not None:
            request["end"] = context.end
        if context.assets is not None:
            request["assets"] = list(context.assets)
        yield request


class AssetPlanner:
    """One request per asset."""

    def plan(self, context: RequestContext) -> Iterable[dict[str, object]]:
        if not context.assets:
            yield from SnapshotPlanner().plan(context)
            return
        for asset in context.assets:
            request = dict(context.options)
            request["asset_id"] = asset
            if context.start is not None:
                request["start"] = context.start
            if context.end is not None:
                request["end"] = context.end
            yield request
