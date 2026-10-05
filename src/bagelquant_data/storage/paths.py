"""Data lake path conventions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath
import re


def validate_generation(partition_path: str, generation_path: str) -> PurePosixPath:
    """Validate the relative immutable file against its declared monthly partition."""
    if not re.fullmatch(
        r"year=\d{4}/month=(0[1-9]|1[0-2])/data\.parquet", partition_path
    ):
        raise ValueError("Invalid monthly partition reference")
    generation = PurePosixPath(generation_path)
    if (
        generation.is_absolute()
        or "\\" in generation_path
        or ":" in generation_path
        or ".." in generation.parts
        or generation.as_posix() != generation_path
        or generation.parent != PurePosixPath(partition_path).parent
        or not re.fullmatch(r"data-[0-9a-f]{32}\.parquet", generation.name)
    ):
        raise ValueError("Invalid generation reference for the declared partition")
    return generation


@dataclass(frozen=True, slots=True)
class LakePaths:
    """Caller-owned data files and dedicated Data metadata locations."""

    data_meta_path: Path
    lake_path: Path
    read_only: bool = False

    @classmethod
    def open(
        cls,
        *,
        data_meta_path: str | Path,
        lake_path: str | Path,
        read_only: bool = False,
    ) -> "LakePaths":
        return cls(Path(data_meta_path).resolve(), Path(lake_path).resolve(), read_only)

    @property
    def lake(self) -> Path:
        return self.lake_path

    @property
    def rejected(self) -> Path:
        return self.lake / ".rejected"

    def ensure(self) -> None:
        from bagelquant_data.storage.atomic import _filesystem_path

        if self.read_only:
            if (
                not Path(_filesystem_path(self.lake)).is_dir()
                or not self.data_meta_path.is_file()
            ):
                raise FileNotFoundError("Read-only Data paths must already exist")
            return
        for path in (self.lake, self.rejected, self.data_meta_path.parent):
            Path(_filesystem_path(path)).mkdir(parents=True, exist_ok=True)

    def dataset_root(self, source: str, dataset: str) -> Path:
        source_path = Path(source)
        if (
            source_path.is_absolute()
            or ".." in source_path.parts
            or source in {"", "."}
        ):
            raise ValueError("Source path escapes the raw directory")
        base = self.lake / "items" if source == "items" else self.lake / "raw" / source
        path = (base / dataset).resolve()
        if not path.is_relative_to(base.resolve()) or path == base.resolve():
            raise ValueError("Dataset path escapes its owner directory")
        return path

    def generation_path(
        self, source: str, dataset: str, partition_path: str, generation_path: str
    ) -> Path:
        relative = validate_generation(partition_path, generation_path)
        base = self.dataset_root(source, dataset)
        path = (base / relative).resolve()
        if not path.is_relative_to(base) or path == base:
            raise ValueError("Generation reference escapes its dataset")
        return path
