"""Metadata catalog interfaces."""

from __future__ import annotations

from dataclasses import dataclass, field

from bagelquant_data.metadata.contract import DataContract, DatasetIdentity
from bagelquant_data.utils.exceptions import DatasetNotFoundError


@dataclass(slots=True)
class InMemoryMetadataCatalog:
    """Simple metadata catalog for tests and local use."""

    _contracts: dict[str, DataContract] = field(default_factory=dict)

    def put(self, contract: DataContract) -> None:
        """Store a contract by logical dataset name."""

        self._contracts[contract.identity.name] = contract

    def get(self, name: str) -> DataContract:
        """Return a contract by name."""

        try:
            return self._contracts[name]
        except KeyError as exc:
            raise DatasetNotFoundError(f"Unknown dataset: {name}") from exc

    def identity(self, name: str) -> DatasetIdentity:
        """Return a dataset identity by name."""

        return self.get(name).identity
