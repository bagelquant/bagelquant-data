"""Logging helpers."""

from __future__ import annotations

import logging


def get_logger(name: str) -> logging.Logger:
    """Return a package logger."""

    return logging.getLogger(name)
