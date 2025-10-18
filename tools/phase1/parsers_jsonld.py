from __future__ import annotations
import json
import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup


def _first_offer(offers: Any) -> Dict[str, Any]:
    if isinstance(offers, list):
        return offers[0] if offers else {}
    return offers or {}


def _qty_hint_from_text(*texts: str) -> Optional[str]:
    blob = " ".join([t for t in texts if t]) or ""
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(ml|l|L)\b", blob, re.I)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    # packs like 2x1 L, 4 × 250 ml
    m2 = re.search(r"(\d+)\s*[x×]\s*(\d+(?:[.,]\d+)?)\s*(ml|l|L)\b", blob, re.I)
    if m2:
        return f"{m2.group(1)}x{m2.group(2)} {m2.group(3)}"
    return None


def extract_products_from_jsonld(html: str) -> List[Dict[str, Any]]:
    """
    Parse schema.org Product blocks on a listing page.
    Returns dicts with keys: name, price, currency, url, gtin, sku, quantity_hint.
    """
    out: List[Dict[str, Any]] = []
    if not html:
        return out
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = tag.string or tag.get_text() or ""
        try:
            data = json.loads(text)
        except Exception:
            continue
        blocks = data if isinstance(data, list) else [data]
        for d in blocks:
            try:
                t = d.get("@type") or d.get("type")
                if isinstance(t, list):
                    t = ",".join(t)
                if not (t and "Product" in str(t)):
                    continue

                name = (d.get("name") or d.get("title") or "").strip()
                offers = _first_offer(d.get("offers"))
                price = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")
                currency = offers.get("priceCurrency") or (offers.get("priceSpecification") or {}).get("priceCurrency")
                url = d.get("url") or offers.get("url")
                gtin = d.get("gtin13") or d.get("gtin14") or d.get("gtin12") or d.get("gtin8") or d.get("gtin")
                sku = d.get("sku") or d.get("productID")
                qhint = _qty_hint_from_text(name, d.get("description"))

                out.append({
                    "name": name,
                    "price": None if price in (None, "") else str(price),
                    "currency": currency or "EUR",
                    "url": url,
                    "gtin": (None if gtin in (None, "") else str(gtin)),
                    "sku": (None if sku in (None, "") else str(sku)),
                    "quantity_hint": qhint,
                })
            except Exception:
                continue
    return out


def extract_next_products(html: str) -> List[Dict[str, Any]]:
    """
    Parse simple Next.js __NEXT_DATA__ payloads for product cards.
    Returns dicts with keys: name, price, currency, url, gtin, sku.
    """
    out: List[Dict[str, Any]] = []
    if not html:
        return out
    soup = BeautifulSoup(html, "lxml")
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag:
        return out
    try:
        data = json.loads(tag.text)
    except Exception:
        return out

    found: List[Dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if "products" in node and isinstance(node["products"], list):
                for p in node["products"]:
                    if isinstance(p, dict) and ("name" in p) and ("price" in p or "sellingPrice" in p):
                        found.append(p)
            # product-like dicts scattered around
            if ("name" in node) and ("price" in node or "sellingPrice" in node):
                found.append(node)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(data)
    for p in found:
        try:
            name = p.get("name")
            price = p.get("price") or p.get("sellingPrice")
            currency = p.get("currency") or "EUR"
            url = p.get("url") or p.get("link") or p.get("canonicalUrl")
            gtin = p.get("gtin") or p.get("gtin13") or p.get("ean")
            sku = p.get("sku") or p.get("id")
            out.append({
                "name": name,
                "price": None if price in (None, "") else str(price),
                "currency": currency,
                "url": url,
                "gtin": (None if gtin in (None, "") else str(gtin)),
                "sku": (None if sku in (None, "") else str(sku)),
            })
        except Exception:
            continue
    return out


def extract_datalayer_products(html: str):
    out = []
    try:
        import re, json
        m = re.search(r"dataLayer\s*=\s*(\[[\s\S]*?\]);", html)
        if not m:
            return out
        dl = json.loads(m.group(1))
        # try various shapes where products may live
        candidates = []
        for obj in dl:
            if isinstance(obj, dict):
                if "product" in obj:
                    candidates.append(obj["product"])
                ecommerce = obj.get("ecommerce") or {}
                detail = ecommerce.get("detail") or {}
                if "products" in detail:
                    candidates.append(detail["products"])
        for c in candidates:
            if isinstance(c, dict):
                c = [c]
            if not isinstance(c, list):
                continue
            for p in c:
                if not isinstance(p, dict):
                    continue
                name = p.get("name")
                price = p.get("price") or p.get("unit_price") or p.get("value")
                if name and price:
                    out.append({"name": name, "price": price, "currency": "EUR", "url": None})
    except Exception:
        pass
    return out
