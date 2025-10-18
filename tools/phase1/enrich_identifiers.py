# tools/phase1/enrich_identifiers.py
from __future__ import annotations
import asyncio
import json
import random
from typing import List, Dict

from .parsers_jsonld import extract_ldjson_products

async def _sleep_jitter(a_ms=150, b_ms=300):
    await asyncio.sleep(random.uniform(a_ms/1000.0, b_ms/1000.0))

async def enrich_identifiers(context, rows: List[Dict], max_pdp: int = 40) -> int:
    """
    Visit up to max_pdp product URLs lacking identifiers and fill ean/sku
    from JSON-LD on the PDP. Reuses the same persistent browser context.
    """
    todo = [r for r in rows if (not r.get("ean") and not r.get("sku")) and r.get("source_url")]
    todo = todo[:max_pdp]
    if not todo:
        return 0
    page = await context.new_page()
    enriched = 0
    try:
        for r in todo:
            url = r["source_url"]
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except Exception:
                continue
            try:
                html = await page.content()
                prods = extract_ldjson_products(html)
                # heuristic: pick item with closest name if multiple
                best = None
                if prods:
                    # prefer gtin present
                    for p in prods:
                        if p.get("gtin") or p.get("sku"):
                            best = p
                            break
                    best = best or prods[0]
                if best:
                    if best.get("gtin"):
                        r["ean"] = best["gtin"]
                    if best.get("sku"):
                        r["sku"] = best["sku"]
                    enriched += 1
            except Exception:
                pass
            await _sleep_jitter()
    finally:
        await page.close()
    return enriched
