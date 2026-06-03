from __future__ import annotations

from pathlib import Path


def test_package_code_does_not_import_bagelquant_core() -> None:
    package_root = Path("src/bagelquant_data")

    forbidden_imports = ("import bagelquant_core", "from bagelquant_core")
    offenders = [
        path
        for path in package_root.rglob("*.py")
        if any(
            forbidden in path.read_text(encoding="utf-8")
            for forbidden in forbidden_imports
        )
    ]

    assert offenders == []
