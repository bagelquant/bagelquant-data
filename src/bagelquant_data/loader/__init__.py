"""Loader interfaces."""

from bagelquant_data.loader.custom import CustomLoader
from bagelquant_data.loader.fundamentals import FundamentalsLoader
from bagelquant_data.loader.loader import LoadedDataset, Loader, PanelInputAgreement
from bagelquant_data.loader.market import MarketLoader
from bagelquant_data.loader.universe import UniverseLoader

__all__ = [
    "CustomLoader",
    "FundamentalsLoader",
    "LoadedDataset",
    "Loader",
    "MarketLoader",
    "PanelInputAgreement",
    "UniverseLoader",
]
