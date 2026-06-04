"""Generate the GUI Tushare API catalog from tushare.pro docs."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

BASE_URL = "https://tushare.pro"
NAV_URL = f"{BASE_URL}/document/2?doc_id=108"
OUTPUT = Path("src/bagelquant_data/gui/tushare_tables.json")

API_RE = re.compile(r"\u63a5\u53e3[:\uff1a]\s*([A-Za-z][A-Za-z0-9_]*)")
DESC_RE = re.compile(r"\u63cf\u8ff0[:\uff1a]\s*([^\n\r]+)")
EXTRA_ENTRIES = (
    {
        "api": "income_vip",
        "name_zh": "利润表VIP",
        "description_zh": "VIP利润表，按报告期更新",  # noqa: RUF001
        "category_path": ["股票数据", "财务数据"],
        "category_zh": "股票数据 / 财务数据",
        "default_kind": "fundamental_vip",
        "source_url": "https://tushare.pro/document/2?doc_id=33",
    },
    {
        "api": "balancesheet_vip",
        "name_zh": "资产负债表VIP",
        "description_zh": "VIP资产负债表，按报告期更新",  # noqa: RUF001
        "category_path": ["股票数据", "财务数据"],
        "category_zh": "股票数据 / 财务数据",
        "default_kind": "fundamental_vip",
        "source_url": "https://tushare.pro/document/2?doc_id=36",
    },
    {
        "api": "cashflow_vip",
        "name_zh": "现金流量表VIP",
        "description_zh": "VIP现金流量表，按报告期更新",  # noqa: RUF001
        "category_path": ["股票数据", "财务数据"],
        "category_zh": "股票数据 / 财务数据",
        "default_kind": "fundamental_vip",
        "source_url": "https://tushare.pro/document/2?doc_id=44",
    },
)
SESSION = requests.Session()
SESSION.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
        )
    ),
)


def main() -> None:
    """Fetch Tushare docs and write the local GUI catalog JSON."""

    previous = _previous_entries()
    response = SESSION.get(NAV_URL, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    root = soup.find("ul", class_="components")
    if root is None:
        raise RuntimeError("Could not find Tushare document navigation")

    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    for link in root.find_all("a"):
        href = str(link.get("href") or "")
        if not href.startswith("/document/2?doc_id="):
            continue
        li = link.find_parent("li")
        if li is not None and "in" in (li.get("class") or []):
            continue
        category_path = _category_path(li)
        if not category_path:
            continue
        source_url = urljoin(BASE_URL, href)
        detail = _fetch_api_detail(source_url)
        if detail is None or detail["api"] in seen:
            continue
        api = str(detail["api"])
        seen.add(api)
        entries.append(
            {
                "api": api,
                "name_zh": str(link.get_text(strip=True)),
                "description_zh": str(
                    detail.get("description")
                    or previous.get(api, {}).get("description_zh")
                    or ""
                ),
                "category_path": category_path,
                "category_zh": " / ".join(category_path),
                "default_kind": _default_kind(api, previous),
                "source_url": source_url,
                "doc_order": len(entries),
            }
        )

    for extra in EXTRA_ENTRIES:
        if extra["api"] in seen:
            continue
        entries.append({**extra, "doc_order": len(entries)})
        seen.add(str(extra["api"]))

    OUTPUT.write_text(
        json.dumps(entries, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote {len(entries)} Tushare API entries to {OUTPUT}")


def _previous_entries() -> dict[str, dict[str, Any]]:
    if not OUTPUT.exists():
        return {}
    payload = json.loads(OUTPUT.read_text(encoding="utf-8"))
    return {str(item["api"]): dict(item) for item in payload}


def _category_path(li) -> list[str]:
    categories: list[str] = []
    parent = li.parent if li is not None else None
    while parent is not None:
        parent_li = parent.find_parent("li")
        if parent_li is None:
            break
        if "in" in (parent_li.get("class") or []):
            link = parent_li.find("a", recursive=False)
            if link is not None:
                categories.append(str(link.get_text(strip=True)))
        parent = parent_li.parent
    return list(reversed(categories))


def _fetch_api_detail(source_url: str) -> dict[str, str] | None:
    response = SESSION.get(source_url, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    text = soup.get_text("\n")
    api_match = API_RE.search(text)
    if api_match is None:
        return None
    desc_match = DESC_RE.search(text)
    return {
        "api": api_match.group(1),
        "description": desc_match.group(1).strip() if desc_match else "",
    }


def _default_kind(api: str, previous: dict[str, dict[str, Any]]) -> str:
    previous_kind = previous.get(api, {}).get("default_kind")
    if previous_kind in {"general", "price", "fundamental", "fundamental_vip"}:
        return str(previous_kind)
    if api.endswith("_vip"):
        return "fundamental_vip"
    return "general"


if __name__ == "__main__":
    main()
