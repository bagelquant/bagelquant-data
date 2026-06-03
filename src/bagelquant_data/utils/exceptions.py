"""Project exceptions."""


class BagelQuantDataError(Exception):
    """Base exception for bagelquant-data."""


class DataSourceError(BagelQuantDataError):
    """Raised when a data source fails."""


class DataSourceAuthError(DataSourceError):
    """Raised when a data source cannot resolve credentials."""


class DatasetNotFoundError(DataSourceError):
    """Raised when a dataset cannot be resolved."""


class ContractValidationError(BagelQuantDataError):
    """Raised when a dataset violates its declared contract."""


class LakeError(BagelQuantDataError):
    """Raised for lake storage failures."""
