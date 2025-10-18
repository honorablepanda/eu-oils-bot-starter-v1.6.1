from __future__ import annotations

import sys
import datetime as _dt
from typing import Dict, List, Tuple
from urllib.parse import urlsplit, urlunsplit

UA = "URL-Finder/1.1 (+https://example.local)"
TIMEOUT = 15


def _requests():
    """Lazily import requests so the package can load without it."""
    try:
        import requests  # type: ignore
        return requests
    except Exception:
        # Helpful warning so you immediately know why stability is 0
        print("[WARN] 'requests' not installed; archive metrics will be 0.", file=sys.stderr)
        return None


def _group_by_month(stamps: List[str]) -> int:
    """Count unique YYYY-MM months in a list of Wayback/Arquivo timestamps."""
    seen = set()
    for ts in stamps:
        if len(ts) >= 8:
            y, m = ts[:4], ts[4:6]
            seen.add(f"{y}-{m}")
    return len(seen)


def _strip_query_and_fragment(url: str) -> str:
    """Normalize URL for archive lookups by removing query + fragment."""
    sp = urlsplit(url)
    return urlunsplit((sp.scheme, sp.netloc.lower(), sp.path, "", ""))


def _parent_urls(url: str):
    """
    Yield URL → each parent path → site root.
    Example: /a/b/c → /a/b → /a → /
    """
    sp = urlsplit(_strip_query_and_fragment(url))
    segs = [s for s in sp.path.split("/") if s]
    for i in range(len(segs), -1, -1):
        p = "/" + "/".join(segs[:i]) if i else "/"
        yield urlunsplit((sp.scheme or "https", sp.netloc, p, "", ""))


def wayback_cdx(url: str, start_year: int = 2022, end_year: int | None = None) -> Tuple[int, int]:
    """Return (total_hits, months_with_snapshots) for the exact URL from Wayback CDX."""
    rq = _requests()
    if not rq:
        return 0, 0
    if end_year is None:
        end_year = _dt.datetime.utcnow().year
    api = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url": url,
        "output": "json",
        "filter": "statuscode:200",
        "from": str(start_year),
        "to": str(end_year),
        "collapse": "digest",  # collapse identical content
    }
    try:
        r = rq.get(api, params=params, headers={"User-Agent": UA}, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if not data or len(data) <= 1:
            return 0, 0
        stamps = [row[1] for row in data[1:] if len(row) > 1]
        return len(stamps), _group_by_month(stamps)
    except Exception:
        return 0, 0


def arquivo_cdx(url: str, start_year: int = 2022, end_year: int | None = None) -> Tuple[int, int]:
    """Return (total_hits, months_with_snapshots) for the exact URL from Arquivo.pt CDX."""
    rq = _requests()
    if not rq:
        return 0, 0
    api = "https://arquivo.pt/wayback/cdx"
    params = {
        "url": url,
        "output": "json",
        "filter": "status:200",
    }
    try:
        r = rq.get(api, params=params, headers={"User-Agent": UA}, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if not data or len(data) <= 1:
            return 0, 0
        stamps = [row[1] for row in data[1:] if len(row) > 1]
        return len(stamps), _group_by_month(stamps)
    except Exception:
        return 0, 0


def summarize_archives(url: str, start_year: int = 2022) -> Dict[str, int]:
    """
    Archive-first stability summary with parent-path fallback.
    Tries exact URL; if no snapshots, climbs up to parent paths and site root.

    Returns dict with:
      - wayback_hits / wayback_months
      - arquivo_hits / arquivo_months
      - months_with_snapshots (max of the two)
      - total_hits
    """
    best = {
        "wayback_hits": 0,
        "wayback_months": 0,
        "arquivo_hits": 0,
        "arquivo_months": 0,
        "months_with_snapshots": 0,
        "total_hits": 0,
    }

    for candidate in _parent_urls(url):
        w_hits, w_months = wayback_cdx(candidate, start_year=start_year)
        a_hits, a_months = arquivo_cdx(candidate, start_year=start_year)
        months = max(w_months, a_months)
        hits = w_hits + a_hits
        if months > 0 or hits > 0:
            return {
                "wayback_hits": w_hits,
                "wayback_months": w_months,
                "arquivo_hits": a_hits,
                "arquivo_months": a_months,
                "months_with_snapshots": months,
                "total_hits": hits,
            }

    return best
