
from __future__ import annotations
from urllib.parse import quote_plus
from typing import Dict, List

from finder.core.canonicalize import clean_url

SEARCH_PATTERNS = [
    "/search?q={q}", "/recherche?q={q}", "/zoek?q={q}", "/zoeken?text={q}", "/catalogsearch/result/?q={q}"
]

CATEGORY_HINTS = [
    "", "category/", "shop/", "c/", "cat/", "rayon/", "epicerie/"
]

def _slug_variants(slugs: List[str]) -> List[str]:
    uniq = []
    for s in slugs:
        s = s.strip("/")
        if s and s not in uniq:
            uniq.append(s)
    return uniq

def generate_candidates_for_retailer_oil(ret: Dict[str,str], oil: str, keywords: Dict) -> List[Dict]:
    domain = ret.get("domain","").strip()
    website_id = ret.get("website_id","").strip()
    out = []

    oil_slugs = _slug_variants(keywords.get(oil, []))
    # A) category slugs
    for slug in oil_slugs:
        for pref in CATEGORY_HINTS:
            url = f"https://{domain}/{pref}{slug}".replace("//", "/").replace("https:/","https://")
            out.append({
                "website_id": website_id,
                "domain": domain,
                "retailer_name": ret.get("retailer_name",""),
                "country": ret.get("country_iso2",""),
                "oil": oil,
                "class": "category",
                "source": "sluggen",
                "original_url": clean_url(url),
                "archive_hits": {"wayback": 0, "arquivo": 0, "memento": 0},
                "months_with_snapshots": 0,
                "signals": {},
                "notes": ""
            })
    # B) site-search
    for q in oil_slugs:
        for pat in SEARCH_PATTERNS:
            url = f"https://{domain}{pat.format(q=quote_plus(q))}"
            out.append({
                "website_id": website_id, "domain": domain,
                "retailer_name": ret.get("retailer_name",""),
                "country": ret.get("country_iso2",""),
                "oil": oil, "class":"search", "source":"search",
                "original_url": clean_url(url),
                "archive_hits": {"wayback": 0, "arquivo": 0, "memento": 0},
                "months_with_snapshots": 0,
                "signals": {}, "notes": ""
            })
    return out
