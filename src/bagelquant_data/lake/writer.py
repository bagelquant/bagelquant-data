"""Lake writer extension point."""

from bagelquant_data.lake.base import LakeStore


class LakeWriter:
    """Thin writer wrapper around a lake store."""

    def __init__(self, store: LakeStore) -> None:
        self.store = store
