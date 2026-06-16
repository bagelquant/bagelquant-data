from __future__ import annotations

import getpass
import platform
from pathlib import Path


def default_lake_root() -> Path:
    username = getpass.getuser()
    system = platform.system()
    if system == "Windows":
        return Path(f"C:/Users/{username}/data")
    if system == "Darwin":
        return Path(f"/Users/{username}/data")
    return Path.home() / "data"
