from pathlib import Path

from bagelquant_data.datasource import DataSourceRegistry, LocalFileDataSource
from bagelquant_data.loader import Loader

registry = DataSourceRegistry()
registry.register(LocalFileDataSource(Path("data")))

dataset = Loader(registry=registry).source("local").load("prices")
dataset.data.head()
