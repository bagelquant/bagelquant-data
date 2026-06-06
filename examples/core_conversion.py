# pyright: reportMissingImports=false
from bagelquant_core import Domain, Panel


def to_core_panel(retrieved):
    domain = Domain(calendar=retrieved.calendar, universe=retrieved.universe)
    return Panel.from_domain(
        retrieved.data,
        domain,
        name=retrieved.dataset_name,
        metadata=retrieved.metadata,
    )
