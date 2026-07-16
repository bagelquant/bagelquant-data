"""Framework exceptions."""

from __future__ import annotations


class BagelQuantDataError(Exception):
    """Base package error."""


class ConfigurationError(BagelQuantDataError):
    """Configuration is invalid or incomplete."""


class DatasetSpecError(ConfigurationError):
    """Dataset specification is invalid."""


class DatasetNotFoundError(BagelQuantDataError):
    """Requested dataset is not registered or has no canonical data."""


class SourceNotFoundError(BagelQuantDataError):
    """Requested source is not registered."""


class DataSourceError(BagelQuantDataError):
    """Source adapter failed."""


class ValidationError(BagelQuantDataError):
    """Data failed validation."""


class DuplicateResolutionError(BagelQuantDataError):
    """A single-value panel cannot be produced without resolving duplicates."""


class DestructiveOperationError(BagelQuantDataError):
    """A destructive operation was requested without explicit confirmation."""


class StaleUpdatePlanError(BagelQuantDataError):
    """The lake changed after an update plan was previewed."""
