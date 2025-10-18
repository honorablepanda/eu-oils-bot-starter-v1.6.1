from __future__ import annotations
import json, time
from typing import Dict, Optional, Tuple
from playwright.sync_api import Page

DDG_HTML = "https://duckduckgo.com/html/?q={q}"

def resolve_retailer(page: Page, retailer_name: str, country: str, category_hint: str, cache_path: str) -> Tuple[str, str]:
    """Return (home_url, category_url). Uses DDG HTML endpoint in-page to keep referrer realistic.
    If a cache exists at cache_path, re-use it.
    """
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            cached = json.load(f)
            if "home_url" in cached and "category_url" in cached:
                return cached["home_url"], cached["category_url"]
    except Exception:
        pass

    # Home query
    q_home = f"{retailer_name} {country} official site"
    page.goto(DDG_HTML.format(q=q_home), wait_until="domcontentloaded")
    home_url = _first_result_url(page)

    # Category query (site-scoped)
    host = _host_from_url(home_url) if home_url else retailer_name
    q_cat = f"site:{host} (olijfolie OR huile d'olive OR olive oil)"
    page.goto(DDG_HTML.format(q=q_cat), wait_until="domcontentloaded")
    category_url = _first_result_url(page)

    data = {"home_query": q_home, "category_query": q_cat, "home_url": home_url, "category_url": category_url, "source": "ddg"}
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return home_url or "", category_url or ""

def _first_result_url(page: Page) -> Optional[str]:
    for sel in ["a.result__a", "a.result__url", "a[href]"]:
        links = page.query_selector_all(sel)
        for a in links:
            href = a.get_attribute("href") or ""
            if href.startswith("http"):
                return href
    return None

def _host_from_url(u: str) -> str:
    try:
        from urllib.parse import urlparse
        return urlparse(u).netloc
    except Exception:
        return u
