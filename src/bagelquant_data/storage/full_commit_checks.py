"""Bounded exact proofs for large unchanged full-baseline attestations.

Original headers, witnesses and batches remain authoritative and immutable.
The content-addressed seal is a checked representation of their exact equality.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

COMPACT_MIN_ROWS = 100_000


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      default=lambda value: dict(value) if isinstance(value, Mapping) else
                      list(value) if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else str(value))


def validate_seal(seal: Mapping[str, Any]) -> None:
    body = {key: value for key, value in seal.items() if key != "digest"}
    if hashlib.sha256(_json(body).encode()).hexdigest() != seal["digest"]:
        raise RuntimeError("Full-commit check seal checksum mismatch")
    if sum(batch["row_count"] for batch in seal["binding"]["batches"]) != seal["row_count"]:
        raise RuntimeError("Full-commit check seal row count mismatch")
