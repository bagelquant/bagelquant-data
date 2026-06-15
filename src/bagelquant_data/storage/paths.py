"""Data lake path conventions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class LakePaths:
    """Filesystem layout for a local data lake root."""

    root: Path

    @classmethod
    def open(cls, root: str | Path) -> "LakePaths":
        return cls(Path(root))

    @property
    def lake(self) -> Path:
        return self.root / "lake"

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    @property
    def rejected(self) -> Path:
        return self.root / "rejected"

    @property
    def metadata(self) -> Path:
        return self.root / "metadata"

    @property
    def tmp(self) -> Path:
        return self.root / "tmp"

    @property
    def database(self) -> Path:
        return self.metadata / "lake.db"

    def ensure(self) -> None:
        for path in (self.lake, self.staging, self.rejected, self.metadata, self.tmp):
            path.mkdir(parents=True, exist_ok=True)

    def dataset_root(self, source: str, dataset: str) -> Path:
        return self.lake / source / dataset
