
from __future__ import annotations
from typing import Dict, List, Any
from collections import defaultdict

# Minimal scoring using the blueprint sketch
def score_candidate(signals: Dict[str,Any], row: Dict[str,Any]) -> int:
    s = 0
    if signals.get("jsonld"): s += 15
    if signals.get("oil_in_url") and signals.get("oil_in_title"): s += 10
    if signals.get("unit_tokens"): s += 10
    if signals.get("qualifier_match"): s += 10
    if signals.get("per_unit_price"): s += 10
    if signals.get("gtin_valid"): s += 10
    if signals.get("from_sitemap"): s += 8
    if row.get("class") in ("category","pdp"): s += 5
    if signals.get("locale_ok"): s += 5
    if signals.get("cosmetic_ambiguous") and not signals.get("tagged"): s -= 12
    if signals.get("url_churn"): s -= 10
    if signals.get("search_no_tiles"): s -= 8
    if signals.get("mixed_oils"): s -= 6
    if signals.get("currency_mismatch"): s -= 6
    # snapshot density (placeholder)
    months = int(row.get("months_with_snapshots") or 0)
    if months >= 6: s += 10
    return int(s)

def select_per_group(rows: List[Dict[str,Any]], group_keys: List[str], k: int = 2) -> List[Dict[str,Any]]:
    buckets = defaultdict(list)
    for r in rows:
        key = tuple(r.get(k) for k in group_keys)
        buckets[key].append(r)
    out = []
    for key, items in buckets.items():
        items_sorted = sorted(items, key=lambda r: int(r.get("score",0)), reverse=True)
        out.extend(items_sorted[:k])
    return out
