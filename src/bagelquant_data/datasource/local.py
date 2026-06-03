"""Local file data source."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from bagelquant_data.datasource.base import DataRequest
from bagelquant_data.utils.exceptions import DatasetNotFoundError


class LocalFileDataSource:
    """Read local tabular files by dataset name."""

    name = "local"

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def read(self, request: DataRequest) -> pd.DataFrame:
        """Read a local dataset as a DataFrame."""

        path = self._path_for(request.dataset)
        if path.suffix == ".csv":
            return pd.read_csv(path)
        if path.suffix == ".json":
            return pd.read_json(path)
        if path.suffix == ".parquet":
            return pd.read_parquet(path)
        raise DatasetNotFoundError(f"Unsupported local dataset format: {path.suffix}")

    def exists(self, dataset: str) -> bool:
        """Return whether a matching local file exists."""

        return any((self.root / f"{dataset}{suffix}").exists() for suffix in _SUFFIXES)

    def describe(self, dataset: str) -> Mapping[str, Any]:
        """Return simple file metadata."""

        path = self._path_for(dataset)
        return {"provider": self.name, "dataset": dataset, "path": str(path)}

    def _path_for(self, dataset: str) -> Path:
        for suffix in _SUFFIXES:
            path = self.root / f"{dataset}{suffix}"
            if path.exists():
                return path
        raise DatasetNotFoundError(f"Local dataset not found: {dataset}")


_SUFFIXES = (".csv", ".json", ".parquet")
