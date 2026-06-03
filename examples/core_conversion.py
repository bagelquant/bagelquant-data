# pyright: reportMissingImports=false
from bagelquant_core import Domain, Panel


def to_core_panel(agreement):
    domain = Domain(**agreement.domain_spec.to_core_kwargs())
    return Panel.from_domain(
        agreement.frame,
        domain,
        name=agreement.dataset_name,
        metadata=agreement.metadata,
    )
