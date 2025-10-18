from __future__ import annotations
import json, time, random, urllib.parse, urllib.request
from dataclasses import dataclass
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

from tools.phase1.price_parser import parse_price_to_eur
from tools.phase1.quantity_parser import parse_quantity

@dataclass
class ArchiveRow:
    name: str
    price_eur: float
    source_url: str
    snapshot_url: str

def _cdx_query_urls(host: str, path_hints: List[str], limit: int) -> List[Dict[str, str]]:
    """Query Wayback CDX API for PDP-like paths and return [{'original','timestamp'}...]"""
    urls: List[Dict[str, str]] = []
    seen = set()
    for hint in path_hints:
        # Example: http://web.archive.org/cdx/search/cdx?url=www.vomar.nl*/producten/*&output=json&filter=statuscode:200&limit=200
        qurl = (
            "http://web.archive.org/cdx/search/cdx?"
            + urllib.parse.urlencode({
                "url": f"{host}*/{hint}/*",
                "output": "json",
                "filter": "statuscode:200",
                "collapse": "digest",
                "limit": str(limit * 4)  # overfetch; we'll dedupe and cap below
            })
        )
        with urllib.request.urlopen(qurl, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        # First row is header
        for row in data[1:]:
            original = row[2]
            ts = row[1]
            key = (original, ts)
            if key in seen: 
                continue
            seen.add(key)
            urls.append({"original": original, "timestamp": ts})
            if len(urls) >= limit * 2:
                break
        if len(urls) >= limit * 2:
            break
        time.sleep(random.uniform(0.15, 0.3))  # polite jitter
    # Deduplicate by original, keep the newest snapshot
    by_orig: Dict[str, Dict[str,str]] = {}
    for it in urls:
        o = it["original"]
        if (o not in by_orig) or (it["timestamp"] > by_orig[o]["timestamp"]):
            by_orig[o] = it
    uniq = list(by_orig.values())
    uniq.sort(key=lambda x: x["timestamp"], reverse=True)
    return uniq[:limit]

def _fetch_snapshot_html(original: str, timestamp: str) -> str:
    snap = f"https://web.archive.org/web/{timestamp}/{original}"
    with urllib.request.urlopen(snap, timeout=20) as resp:
        html = resp.read().decode("utf-8", errors="replace")
    return html

def archive_pdp_rescue(
    category_url: str,
    max_items: int,
    positive_terms: List[str],
    brand_terms: List[str],
    negative_terms: List[str],
) -> List[ArchiveRow]:
    """Pull PDP snapshots via CDX and extract name + price (best-effort)."""
    from urllib.parse import urlparse
    parsed = urlparse(category_url)
    host = parsed.netloc or category_url

    pdp_hints = ["producten", "product", "p"]
    cdx = _cdx_query_urls(host, pdp_hints, limit=max_items)
    rows: List[ArchiveRow] = []

    for it in cdx:
        original, ts = it["original"], it["timestamp"]
        try:
            html = _fetch_snapshot_html(original, ts)
        except Exception:
            continue

        soup = BeautifulSoup(html, "lxml")
        # name: prefer h1, fallback title
        name = ""
        h1 = soup.find("h1")
        if h1 and h1.get_text(strip=True):
            name = h1.get_text(strip=True)
        elif soup.title and soup.title.get_text(strip=True):
            name = soup.title.get_text(strip=True)
        if not name:
            continue

        price = parse_price_to_eur(html)
        if price is None:
            continue

        low = (name + " " + html).lower()
        if not any(t in low for t in positive_terms) and not any(b in low for b in brand_terms):
            continue
        if any(n in low for n in negative_terms):
            continue

        qty = parse_quantity(name)  # may be None; handled later by caller if needed
        rows.append(ArchiveRow(name=name, price_eur=price, source_url=original,
                               snapshot_url=f"https://web.archive.org/web/{ts}/{original}"))
        time.sleep(random.uniform(0.15, 0.3))
        if len(rows) >= max_items:
            break
    return rows
