# --- import shim so "tools.*" works when run directly ---
import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[2]  # project root
if str(root) not in sys.path:
    sys.path.insert(0, str(root))
# -------------------------------------------------------

import argparse
import csv
import json
import os
import random
import re
import time
import hashlib
from dataclasses import dataclass, fields as dc_fields
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable
from urllib.parse import urljoin, urlparse, urlunparse, urlencode, parse_qs, parse_qsl

import requests
import yaml
from bs4 import BeautifulSoup
from playwright.sync_api import BrowserContext, Page
from playwright.sync_api import TimeoutError as PWTimeoutError

from tools.phase1.utils_playwright import open_persistent_context, operator_unlock
from tools.phase1.ddg_search import resolve_retailer
from tools.phase1.scroller import (
    try_accept_cookies,
    aggressive_accept_cookies,
    bounded_scroll,
    collect_card_count,
    click_load_more,
)
from tools.phase1.parsers_jsonld import (
    extract_products_from_jsonld,
    extract_next_products,
)
from tools.phase1.price_parser import parse_price_to_eur
from tools.phase1.quantity_parser import parse_quantity
from tools.phase1 import detectors
from tools.phase1.exporters import Row, write_rows_csv, write_run_health, merge_final_export
from tools.phase1.archive_backends import archive_pdp_rescue

# ---- optional archive HTML orchestrator
try:
    from eopt.archives import archive_fetch_html
except Exception:
    archive_fetch_html = None

# ---- optional website_id helpers
try:
    from eopt.ids import make_website_id, _root_domain  # noqa: F401
except Exception:
    def _root_domain(u: str) -> str:
        try:
            netloc = urlparse(u).netloc or u
            parts = netloc.split(".")
            return ".".join(parts[-2:]) if len(parts) >= 2 else netloc
        except Exception:
            return ""
    def make_website_id(iso2: str, site_domain: str) -> str:  # noqa: F401
        return f"{(iso2 or '').upper()}:{site_domain}"

# ---- optional request guard & pager tracker (with safe fallbacks)
try:
    from tools.phase1.request_guard import guard_or_raise, guard_url
except Exception:
    def _allow_or_review(url: str) -> tuple[bool, str]:
        dom = _root_domain(url or "")
        if not dom:
            return False, "error: empty url"
        # AH strict pattern
        if dom == "ah.nl":
            qs = dict(parse_qsl(urlparse(url).query))
            if "page" in qs and qs.get("withOffset") != "true":
                return False, "deny: ah.nl requires withOffset=true with page"
            if "/zoeken" in url:
                return False, "deny: ah.nl zoek is not allowed"
        # Jumbo strict pattern
        if dom == "jumbo.com":
            qs = dict(parse_qsl(urlparse(url).query))
            if qs and set(qs.keys()) - {"offSet"}:
                return False, f"deny: jumbo.com only allows offSet (got {sorted(qs.keys())})"
        # Colruyt PDP disallow
        if dom == "colruyt.be" and "/producten/product-detail" in url:
            return False, "deny: colruyt PDP path"
        return True, "allowed"
    def guard_or_raise(url: str, retailer_code: str|None = None):
        ok, reason = _allow_or_review(url)
        if not ok:
            raise RuntimeError(f"[ROBOTS_GUARD] {reason} :: {url}")
        return reason
    def guard_url(url: str, retailer_code: str|None = None) -> tuple[bool, str]:
        return _allow_or_review(url)

try:
    from tools.phase1.paging import PagerTracker
except Exception:
    class PagerTracker:
        def __init__(self, min_growth: int = 1, max_stalls: int = 1):
            self.seen = set()
            self.stalls = 0
            self.min_growth = min_growth
            self.max_stalls = max_stalls
        def add_batch(self, keys: Iterable[str]) -> bool:
            before = len(self.seen)
            for k in keys:
                if k:
                    self.seen.add(k)
            growth = len(self.seen) - before
            if growth < self.min_growth:
                self.stalls += 1
                return False
            self.stalls = 0
            return True
        def should_stop(self) -> bool:
            return self.stalls >= self.max_stalls

# -------------------------
# Constants / Defaults
# -------------------------
CARD_CANDIDATES = [
    "[data-product]",
    "article[class*='product']",
    "li[class*='product']",
    "div[class*='product-card']",
]

DEFAULT_NEGATIVE_TERMS = [
    "pesto","cracker","crackers","chips","koek","koekjes","snack",
    "nootjes","oregano","toast",
]

SNAPSHOT_PATH = Path("policy")
PREWARM_STAMP = "{code}_last_prewarm.txt"
UNLOCK_STAMP = "{code}_last_unlock.txt"
STORE_STAMP = "{code}_store_selected.txt"

# -------------------------
# Retailer model + loader
# -------------------------
@dataclass
class Retailer:
    code: str
    name: str
    base_url: str
    category_url: str
    country: str
    locale: str
    oil_type: str
    prefer_wayback: str
    archive_providers: str
    scroll_strategy: str
    max_pages: int
    load_more_selector: str
    preferred_store_name: str
    store_open_selector: str
    store_confirm_selector: str
    website_id: str | None = None
    max_archive_lookback_days: int | None = None

def load_retailers(csv_path: Path, targets: List[str]) -> List[Retailer]:
    """Load retailers from CSV, mapping known aliases and ignoring unknown columns safely."""
    rs: List[Retailer] = []
    valid_fields = {f.name for f in dc_fields(Retailer)}
    with csv_path.open("r", encoding="utf-8") as fh:
        rdr = csv.DictReader(fh)
        for row in rdr:
            # Normalize code and target filtering
            code = (row.get("code") or row.get("retailer") or "").strip()
            if targets and code not in targets:
                continue
            row["code"] = code

            # Map legacy/alternate CSV column names
            # prefer_archive -> prefer_wayback
            if not row.get("prefer_wayback"):
                row["prefer_wayback"] = row.get("prefer_archive") or ""

            # Ensure required fields exist with defaults
            row.setdefault("max_pages", "6")
            row.setdefault("archive_providers", "")
            row.setdefault("scroll_strategy", "")
            row.setdefault("load_more_selector", "")
            row.setdefault("preferred_store_name", "")
            row.setdefault("store_open_selector", "")
            row.setdefault("store_confirm_selector", "")

            # Keep only dataclass fields; coerce types
            clean: Dict[str, Any] = {}
            for k in valid_fields:
                clean[k] = row.get(k)

            # Type conversions / normalizations
            try:
                clean["max_pages"] = int(clean.get("max_pages") or 6)
            except Exception:
                clean["max_pages"] = 6

            # Finalize instance
            rs.append(Retailer(**clean))
    return rs

# -------------------------
# Terms / Filters
# -------------------------
def _load_oil_terms(path: Path) -> Dict[str, List[str]]:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        pos = [x.lower() for x in data.get("positive", [])]
        brands = [x.lower() for x in data.get("brands_hint", [])]
        if "virgin" not in pos:
            pos.append("virgin")
        return {"positive": pos, "brands": brands}
    except Exception:
        return {
            "positive": ["olijfolie","huile d'olive","olive oil","extravierge","extra virgin","virgin"],
            "brands": [],
        }

def _is_oil_candidate(name: str, html: str, terms: Dict[str, List[str]]) -> bool:
    tname = (name or "").lower()
    thtml = (html or "").lower()
    pos = terms["positive"]
    if any(term in tname for term in pos) or any(term in thtml for term in pos):
        return True
    if any(b in tname for b in terms["brands"]) or any(b in thtml for b in terms["brands"]):
        return True
    return False

def _negative_hit(text: str) -> bool:
    t = (text or "").lower()
    return any(bad in t for bad in DEFAULT_NEGATIVE_TERMS)

# -------------------------
# Render settling
# -------------------------
def wait_for_listing_render(page: Page, timeout_ms: int = 5000) -> None:
    try:
        page.wait_for_selector("script[type='application/ld+json']", timeout=timeout_ms)
    except Exception:
        pass
    try:
        page.wait_for_selector(
            "li[class*='product'], article[class*='product'], div[class*='product'], [data-product]",
            timeout=timeout_ms,
        )
    except Exception:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception:
        pass
    page.wait_for_timeout(400)

# -------------------------
# Pre-warm / Unlock Throttles
# -------------------------
def _should_prewarm(retailer_code: str, hours: int = 6) -> bool:
    SNAPSHOT_PATH.mkdir(parents=True, exist_ok=True)
    stamp = SNAPSHOT_PATH / PREWARM_STAMP.format(code=retailer_code)
    if not stamp.exists():
        return True
    try:
        last = stamp.stat().st_mtime
    except Exception:
        return True
    return (time.time() - last) > (hours * 3600)

def _record_prewarm(retailer_code: str) -> None:
    SNAPSHOT_PATH.mkdir(parents=True, exist_ok=True)
    stamp = SNAPSHOT_PATH / PREWARM_STAMP.format(code=retailer_code)
    try:
        stamp.write_text(str(int(time.time())), encoding="utf-8")
    except Exception:
        pass

def prewarm_session(ctx: BrowserContext, url: str, wait_ms: int = 2500) -> None:
    try:
        p = ctx.new_page()
        guard_or_raise(url)  # guard
        p.goto(url, wait_until="domcontentloaded", timeout=45000)
        try_accept_cookies(p)
        p.wait_for_timeout(wait_ms)
    except Exception:
        pass
    finally:
        try:
            p.close()
        except Exception:
            pass

def _unlock_recent(retailer_code: str, hours: int = 12) -> bool:
    SNAPSHOT_PATH.mkdir(parents=True, exist_ok=True)
    stamp = SNAPSHOT_PATH / UNLOCK_STAMP.format(code=retailer_code)
    if not stamp.exists():
        return False
    try:
        last = stamp.stat().st_mtime
    except Exception:
        return False
    return (time.time() - last) <= (hours * 3600)

def _record_unlock(retailer_code: str) -> None:
    SNAPSHOT_PATH.mkdir(parents=True, exist_ok=True)
    stamp = SNAPSHOT_PATH / UNLOCK_STAMP.format(code=retailer_code)
    try:
        stamp.write_text(str(int(time.time())), encoding="utf-8")
    except Exception:
        pass

# -------------------------
# Helpers: stable product key
# -------------------------
def _product_key(name: str, qty: str, url: str, price: float|None) -> str:
    base = f"{name.strip().lower()}|{(qty or '').strip().lower()}|{(url or '').strip().lower()}|{'' if price is None else price}"
    return hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()

# -------------------------
# URL QS helpers
# -------------------------
def _set_qs(url: str, **kv) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    q.update({k: [str(v)] for k, v in kv.items() if v is not None})
    new_q = urlencode({k: v[0] for k, v in q.items() if v and v[0] is not None})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

# -------------------------
# AH pagination helper (page=&withOffset=true)
# -------------------------
def _ah_paginate_collect(page: Page, category_url: str, max_pages: int = 6) -> List[Row]:
    rows: List[Row] = []
    terms = _load_oil_terms(Path("configs/oil_terms.yaml"))
    # page 1
    guard_or_raise(category_url)
    page.goto(category_url, wait_until="domcontentloaded", timeout=45000)
    try_accept_cookies(page)
    bounded_scroll(page, max_steps=6)
    # collect JSON-LD for page 1
    for p in extract_products_from_jsonld(page.content()):
        name = (p.get("name") or "").strip()
        price = parse_price_to_eur(str(p.get("price") or ""))
        if not name or price is None:
            continue
        qty = parse_quantity(name) or (p.get("quantity_hint") or "")
        src = p.get("url") or category_url
        if not _is_oil_candidate(name, "", terms) or _negative_hit(name):
            continue
        rows.append(Row(retailer="", product_name=name, quantity=qty, price_eur=price, source_url=src, selector_used="jsonld", mode="live", stale=False))

    tracker = PagerTracker(min_growth=1, max_stalls=1)
    tracker.add_batch([_product_key(r.product_name, r.quantity, r.source_url, r.price_eur) for r in rows])

    for pn in range(2, int(max_pages) + 1):
        u = _set_qs(category_url, page=pn, withOffset="true")
        ok, _reason = guard_url(u)
        if not ok:
            break
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(350)
        except Exception:
            break
        batch: List[Row] = []
        for p in extract_products_from_jsonld(page.content()):
            name = (p.get("name") or "").strip()
            price = parse_price_to_eur(str(p.get("price") or ""))
            if not name or price is None:
                continue
            qty = parse_quantity(name) or (p.get("quantity_hint") or "")
            src = p.get("url") or u
            if not _is_oil_candidate(name, "", terms) or _negative_hit(name):
                continue
            batch.append(Row(retailer="", product_name=name, quantity=qty, price_eur=price, source_url=src, selector_used="jsonld", mode="live", stale=False))
        keys = [_product_key(r.product_name, r.quantity, r.source_url, r.price_eur) for r in batch]  # fixed: price_eur
        grew = tracker.add_batch(keys)
        rows.extend(batch)
        if not grew and tracker.should_stop():
            break
    return rows

# -------------------------
# Jumbo pagination helper (?offSet=)
# -------------------------
def _jumbo_paginate_collect(page: Page, category_url: str, max_pages: int = 6, page_size: int = 24) -> List[Row]:
    rows: List[Row] = []
    terms = _load_oil_terms(Path("configs/oil_terms.yaml"))

    def _offset_url(base: str, off: int) -> str:
        u = _set_qs(base, offSet=off)
        return u

    tracker = PagerTracker(min_growth=1, max_stalls=1)

    off = 0
    for _ in range(int(max_pages)):
        u = _offset_url(category_url, off)
        ok, _reason = guard_url(u)
        if not ok:
            break
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=45000)
            try_accept_cookies(page)
            page.wait_for_timeout(350)
            bounded_scroll(page, max_steps=4)
        except Exception:
            break

        batch: List[Row] = []
        # JSON-LD first
        for p in extract_products_from_jsonld(page.content()):
            name = (p.get("name") or "").strip()
            price = parse_price_to_eur(str(p.get("price") or ""))
            if not name or price is None:
                continue
            qty = parse_quantity(name) or (p.get("quantity_hint") or "")
            src = p.get("url") or u
            if not _is_oil_candidate(name, "", terms) or _negative_hit(name):
                continue
            batch.append(Row(retailer="", product_name=name, quantity=qty, price_eur=price, source_url=src, selector_used="jsonld", mode="live", stale=False))

        keys = [_product_key(r.product_name, r.quantity, r.source_url, r.price_eur) for r in batch]
        grew = tracker.add_batch(keys)
        rows.extend(batch)

        if not grew and tracker.should_stop():
            break
        off += page_size
    return rows

# -------------------------
# Snapshot helpers
# -------------------------
def _persist_last_good_listing(retailer_code: str, html: str) -> None:
    SNAPSHOT_PATH.mkdir(parents=True, exist_ok=True)
    (SNAPSHOT_PATH / f"{retailer_code}_last_good_listing.html").write_text(html, encoding="utf-8")

def _reparse_snapshot(retailer_code: str, retailer_name: str, url_hint: str) -> List[Row]:
    fp = SNAPSHOT_PATH / f"{retailer_code}_last_good_listing.html"
    if not fp.exists():
        return []
    html = fp.read_text(encoding="utf-8")
    return parse_listing_html_static(html, retailer_name, url_hint, selector_used="snapshot_listing", stale=True)

# -------------------------
# Static HTML listing parser (archives)
# -------------------------
def parse_listing_html_static(
    html: str,
    retailer_name: str,
    url_hint: str,
    *,
    max_cards: int = 80,
    selector_used: str = "archive_listing",
    stale: bool = True,
) -> List[Row]:
    rows: List[Row] = []
    terms = _load_oil_terms(Path("configs/oil_terms.yaml"))
    soup = BeautifulSoup(html or "", "lxml")

    # 1) JSON-LD
    prods = extract_products_from_jsonld(html or "")
    for p in prods:
        name = (p.get("name") or "").strip()
        price = parse_price_to_eur(str(p.get("price") or ""))
        if not name or price is None:
            continue
        qty = parse_quantity(name) or (p.get("quantity_hint") or "")
        src = p.get("url") or url_hint
        rows.append(
            Row(
                retailer=retailer_name,
                product_name=name,
                quantity=qty,
                price_eur=price,
                source_url=src,
                selector_used="jsonld",
                mode="archive" if stale else "live",
                stale=stale,
            )
        )
        if len(rows) >= max_cards:
            return rows

    # 2) CSS card scrape from static HTML
    seen = set()
    for card in soup.select("li[class*='product'],article[class*='product'],div[class*='product'],[data-product]"):
        ch = str(card)
        price = parse_price_to_eur(ch)
        if price is None:
            m = re.search(r"(?:€\s*|\b)(\d{1,4}(?:[.,]\d{2}))\b", ch)
            if m:
                val = m.group(1).replace(".", "").replace(",", ".")
                try:
                    price = float(val)
                except Exception:
                    price = None
        # Title
        name = ""
        for tsel in ["h3", "h2", "a", "[class*='title']", "[class*='name']"]:
            tnode = card.select_one(tsel)
            if tnode and (tnode.get_text(strip=True) or tnode.get("title")):
                name = (tnode.get_text(strip=True) or tnode.get("title") or "").strip()
                if name:
                    break
        if not name or price is None:
            continue
        if not _is_oil_candidate(name, ch, terms):
            continue
        if _negative_hit(name):
            continue
        qty = parse_quantity(name) or ""
        k = (name.lower(), qty.lower(), price)
        if k in seen:
            continue
        seen.add(k)
        rows.append(
            Row(
                retailer=retailer_name,
                product_name=name,
                quantity=qty,
                price_eur=price,
                source_url=url_hint,
                selector_used=selector_used,
                mode="archive" if stale else "live",
                stale=stale,
            )
        )
        if len(rows) >= max_cards:
            break
    return rows

# -------------------------
# CSS listing fallback (live DOM)
# -------------------------
def css_listing_fallback(page: Page, retailer_name: str, max_cards: int = 80) -> List[Row]:
    rows: List[Row] = []
    terms = _load_oil_terms(Path("configs/oil_terms.yaml"))
    selectors = [
        "li[class*='product']","article[class*='product']","div[class*='product']","[data-product]",
    ]
    seen = set()
    for sel in selectors:
        cards = page.query_selector_all(sel)
        for card in cards:
            try:
                card_html = card.inner_html()
            except Exception:
                continue
            price = parse_price_to_eur(card_html)
            if price is None:
                m = re.search(r"(?:€\s*|\b)(\d{1,4}(?:[.,]\d{2}))\b", card_html)
                if m:
                    val = m.group(1).replace(".", "").replace(",", ".")
                    try:
                        price = float(val)
                    except Exception:
                        price = None
            name = ""
            for tsel in ["h3", "h2", "a", "[class*='title']", "[class*='name']"]:
                tnode = card.query_selector(tsel)
                if tnode:
                    txt = (tnode.text_content() or "").strip()
                    if txt:
                        name = txt
                        break
            if not name or price is None:
                continue
            if not _is_oil_candidate(name, card_html, terms):
                continue
            if _negative_hit(name):
                continue
            qty = parse_quantity(name) or ""
            k = (name.lower(), qty.lower(), price)
            if k in seen:
                continue
            seen.add(k)

            rows.append(
                Row(
                    retailer=retailer_name,
                    product_name=name,
                    quantity=qty,
                    price_eur=price,
                    source_url=page.url,
                    selector_used="css_card",
                    mode="live",
                    stale=False,
                )
            )
            if len(rows) >= max_cards:
                return rows
    return rows

# -------------------------
# PDP rescue (live; guarded)
# -------------------------
def _collect_pdp_links_quick(page: Page, max_links: int = 80) -> List[str]:
    urls: List[str] = []
    base = page.url
    for a in page.query_selector_all("a[href]"):
        try:
            href = a.get_attribute("href") or ""
        except Exception:
            continue
        if not href or href.startswith("#"):
            continue
        if any(seg in href.lower() for seg in ["/product", "/producten/", "/p/"]):
            u = urljoin(base, href)
            if u.startswith("http") and (u not in urls):
                urls.append(u)
        if len(urls) >= max_links:
            break
    return urls

_EAN_RX = re.compile(r"\b(?:\d[-\s]?){12,14}\b")

def _jsonld_ean_sku(html: str) -> Dict[str, Optional[str]]:
    out = {"ean": None, "sku": None}
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        return out
    for s in soup.select("script[type='application/ld+json']"):
        txt = s.text or ""
        try:
            data = json.loads(txt)
        except Exception:
            continue
        block = data if isinstance(data, list) else [data]
        for it in block:
            if not isinstance(it, dict):
                continue
            gt = it.get("gtin13") or it.get("gtin") or it.get("gtin8") or it.get("gtin12")
            sku = it.get("sku") or (it.get("productID") if isinstance(it.get("productID"), str) else None)
            if gt and not out["ean"]:
                out["ean"] = re.sub(r"\D+", "", str(gt))
            if sku and not out["sku"]:
                out["sku"] = str(sku)
    if not out["ean"]:
        m = _EAN_RX.search(html or "")
        if m:
            out["ean"] = re.sub(r"\D+", "", m.group(0))
    return out

def _probe_identifiers_http(urls: List[str], cap: int = 40, ua: str = "EOPT-Phase1/1.0") -> Dict[str, Dict[str, Optional[str]]]:
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for u in urls[:cap]:
        ok, _reason = guard_url(u)  # guard PDP probes
        if not ok:
            continue
        try:
            r = requests.get(u, headers={"User-Agent": ua}, timeout=10)
            if r.status_code >= 400:
                continue
            j = _jsonld_ean_sku(r.text)
            out[u] = j
        except Exception:
            continue
    return out

# -------------------------
# Blocker diagnostics
# -------------------------
_JSONLD_PRODUCT_RX = re.compile(r'["@]\s*type"\s*:\s*"Product"', re.IGNORECASE)

def _count_jsonld_products(html: str) -> int:
    if not html:
        return 0
    return len(_JSONLD_PRODUCT_RX.findall(html))

def _print_blocker_reason(tag: str, health: Dict[str, Any], card_count: int, jsonld_count: int, recent_unlock: bool) -> None:
    msg = (
        f"[{tag}] auth={health.get('auth_redirect')} cf={health.get('cf_detected')} "
        f"cookie_wall={health.get('why_flip') == 'cookie_wall'} cards={card_count} "
        f"jsonld={jsonld_count} recent_unlock={recent_unlock} robots={health.get('robots')}"
    )
    print(msg)

# -------------------------
# robots quick guard (weak heuristic, we still enforce guard_url)
# -------------------------
def robots_allowed_status(url: str, ua: str = "EOPT-Phase1/1.0") -> str:
    try:
        dom = _root_domain(url or "")
        if not dom:
            return "review"
        r = requests.get(f"https://{dom}/robots.txt", headers={"User-Agent": ua}, timeout=5)
        if r.status_code >= 500:
            return "review"
        return "allowed" if r.text else "review"
    except Exception:
        return "review"

# -------------------------
# Store-picker helpers (Colruyt)
# -------------------------
def _profile_store_stamp_path(retailer_code: str) -> Path:
    p = Path("_pw_profile") / retailer_code / ".store_selected"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _ensure_store_selected(
    page: Page,
    store_name: str,
    open_selector: str,
    confirm_selector: str,
    timeout_ms: int = 15000,
) -> bool:
    try:
        if open_selector:
            page.wait_for_selector(open_selector, timeout=timeout_ms)
            page.click(open_selector)
    except Exception:
        pass
    try:
        inp = page.query_selector('input[role="combobox"], input[type="search"], input[type="text"]')
        if inp:
            inp.fill(store_name)
            page.wait_for_timeout(500)
            opt = (
                page.query_selector(f'//li[contains(normalize-space(.), "{store_name}")]')
                or page.query_selector(f'//div[contains(normalize-space(.), "{store_name}")]')
                or page.query_selector(f'//button[contains(normalize-space(.), "{store_name}")]')
                or page.query_selector(f'//a[contains(normalize-space(.), "{store_name}")]')
            )
            if opt:
                opt.click()
        else:
            opt = (
                page.query_selector(f'//button[contains(normalize-space(.), "{store_name}")]')
                or page.query_selector(f'//a[contains(normalize-space(.), "{store_name}")]')
                or page.query_selector(f'//div[contains(normalize-space(.), "{store_name}")]')
            )
            if opt:
                opt.click()

        if confirm_selector:
            page.click(confirm_selector, timeout=timeout_ms)

        page.wait_for_timeout(1200)
        return True
    except Exception:
        return False

# -------------------------
# Early flip decision
# -------------------------
def should_flip_to_archive(health: dict, card_count: int, jsonld_count: int) -> Optional[str]:
    if health.get("auth_redirect"):
        return "auth_redirect"
    if health.get("cf_detected"):
        return "cf_challenge"
    if (health.get("why_flip") == "cookie_wall") and (card_count == 0) and (jsonld_count == 0):
        return "cookie_wall"
    if health.get("why_flip") == "store_context_failed":
        return "store_context_failed"
    if (card_count < 5) and (jsonld_count == 0):
        return "too_few_cards"
    return None

# -------------------------
# Archive listing helper
# -------------------------
def _archive_listing_attempt(ret: Retailer, target_url: str, run_dir: Path, max_cards: int = 80) -> List[Row]:
    if not archive_fetch_html:
        return []
    arch = archive_fetch_html(ret.code, target_url, timeout_ms=5000)
    try:
        (run_dir / "listing_archive_meta.json").write_text(
            json.dumps(
                {"ok": getattr(arch, "ok", False), "source": getattr(arch, "source", "?"), "status": getattr(arch, "status", None), "url": getattr(arch, "url", target_url), "reason": getattr(arch, "reason", None)},
                indent=2,
            ),
            encoding="utf-8",
        )
        if getattr(arch, "html", None):
            (run_dir / "listing_archive.html").write_text(arch.html, encoding="utf-8")
    except Exception:
        pass

    if arch and arch.ok and arch.html:
        rows = parse_listing_html_static(arch.html, ret.name, target_url, max_cards=max_cards, selector_used="archive_listing", stale=True)
        return rows
    return []

# -------------------------
# Main runner for one retailer
# -------------------------
def run_one(ret: Retailer, run_id: str, root: Path, *, archive_first: bool = False, live_only: bool = False) -> Dict[str, Any]:
    run_dir = root / f"logs/run_{run_id}/{ret.code}"
    run_dir.mkdir(parents=True, exist_ok=True)

    health: Dict[str, Any] = {
        "code": ret.code,
        "mode": "live" if not archive_first else "archive-first",
        "rows": 0,
        "why_flip": "",
        "robots": False,
        "cf_detected": False,
        "auth_redirect": False,
        "budget_ms": 0,
        "ddg": {},
    }

    all_rows: List[Row] = []
    start = time.time()

    # Resolve URLs up-front
    home_url, category_url = None, None
    browser, ctx, shutdown = open_persistent_context(ret.code, headless=True)
    try:
        page = ctx.new_page()

        home_url, category_url = resolve_retailer(
            page, ret.name, ret.country, "olijfolie", str(run_dir / "ddg.json")
        )
        target_url = category_url or ret.category_url or home_url or ret.base_url
        home_prewarm_url = home_url or ret.base_url or target_url

        health["ddg"] = {"home": home_url, "category": category_url}
        health["robots"] = robots_allowed_status(target_url)

        # ARCHIVE-FIRST
        if archive_first and not live_only:
            ok, _reason = guard_url(target_url)
            if not ok:
                health["why_flip"] = "robots_guard"
            arch_rows = _archive_listing_attempt(ret, target_url, run_dir, max_cards=80)
            if arch_rows:
                # dedupe
                seen = set()
                uniq: List[Row] = []
                for r in arch_rows:
                    k = _product_key(r.product_name, r.quantity or "", r.source_url, r.price_eur)
                    if k in seen:
                        continue
                    seen.add(k)
                    uniq.append(r)
                all_rows = uniq
                write_rows_csv(all_rows, run_dir)
                health["rows"] = len(all_rows)
                health["mode"] = "archive-first"
                health["budget_ms"] = int((time.time() - start) * 1000)
                write_run_health(health, root / f"logs/run_{run_id}")
                return {"rows": all_rows, "health": health, "extras": {"target_url": target_url, "base_url": ret.base_url}}

        # Pre-warm
        if _should_prewarm(ret.code, hours=6) and home_prewarm_url:
            prewarm_session(ctx, home_prewarm_url, wait_ms=2500)
            _record_prewarm(ret.code)

        # ===== Site entry + pagination strategy
        if ret.code == "ah_nl":
            # AH strict pager: page=&withOffset=true
            batch_rows = _ah_paginate_collect(page, target_url, max_pages=int(ret.max_pages or 6))
            # annotate retailer/name later (we only collect raw)
        elif ret.code == "jumbo_nl" or _root_domain(target_url) == "jumbo.com":
            guard_or_raise(target_url)
            batch_rows = _jumbo_paginate_collect(page, target_url, max_pages=int(ret.max_pages or 6), page_size=24)
        else:
            # Carrefour/Colruyt: no query pager; scroll/load-more
            guard_or_raise(target_url)
            page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
            try_accept_cookies(page)
            aggressive_accept_cookies(page)
            wait_for_listing_render(page)
            bounded_scroll(page, max_steps=10)
            if ret.load_more_selector:
                click_load_more(page, ret.load_more_selector, max_pages=int(ret.max_pages or 6))
                bounded_scroll(page, max_steps=3)
            batch_rows = []
            # listing JSON-LD
            for p in extract_products_from_jsonld(page.content()):
                name = (p.get("name") or "").strip()
                price = parse_price_to_eur(str(p.get("price") or ""))
                if not name or price is None:
                    continue
                qty = parse_quantity(name) or (p.get("quantity_hint") or "")
                src = p.get("url") or target_url
                batch_rows.append(Row(retailer="", product_name=name, quantity=qty, price_eur=price, source_url=src, selector_used="jsonld", mode="live", stale=False))

        # Save initial listing artifacts (for non-AH/Jumbo we already navigated)
        if ret.code in ("ah_nl","jumbo_nl") or _root_domain(target_url) in ("ah.nl","jumbo.com"):
            # after pagers, land back on target for artifacts (best-effort)
            try:
                guard_or_raise(target_url)
                page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
                try_accept_cookies(page)
                wait_for_listing_render(page)
                bounded_scroll(page, max_steps=4)
            except Exception:
                pass

        listing_html = page.content()
        (run_dir / "listing.html").write_text(listing_html, encoding="utf-8")
        page.screenshot(path=str(run_dir / "listing.png"), full_page=True)

        # blockers
        retried = False
        if detectors.detect_auth_redirect(page):
            health["auth_redirect"] = True
        if detectors.detect_cf_challenge(page):
            health["cf_detected"] = True

        card_count = collect_card_count(page, CARD_CANDIDATES)
        if detectors.detect_cookie_wall(page):
            health["why_flip"] = "cookie_wall"
        if detectors.detect_empty_listing(card_count):
            health["why_flip"] = health["why_flip"] or "empty_listing"

        jsonld_count = _count_jsonld_products(listing_html)
        recent_unlock = _unlock_recent(ret.code, hours=12)
        _print_blocker_reason("WHY@pre", health, card_count, jsonld_count, recent_unlock)

        # Colruyt: select store if empty
        if (card_count < 5) and ret.preferred_store_name and ret.code == "colruyt_be":
            marker = _profile_store_stamp_path(ret.code)
            if not marker.exists():
                ok = _ensure_store_selected(
                    page,
                    ret.preferred_store_name or "Halle",
                    ret.store_open_selector or ".store-picker",
                    ret.store_confirm_selector or ".confirm-store",
                )
                if ok:
                    try:
                        marker.touch()
                    except Exception:
                        pass
                    page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
                    try_accept_cookies(page)
                    wait_for_listing_render(page)
                    bounded_scroll(page, max_steps=8)
                    if ret.load_more_selector:
                        click_load_more(page, ret.load_more_selector, max_pages=int(ret.max_pages or 6))
                        bounded_scroll(page, max_steps=3)
                    listing_html = page.content()
                    card_count = collect_card_count(page, CARD_CANDIDATES)
                    jsonld_count = _count_jsonld_products(listing_html)

        # Try to clear cookie wall programmatically
        if (health["why_flip"] == "cookie_wall") and card_count == 0 and jsonld_count == 0:
            if aggressive_accept_cookies(page):
                wait_for_listing_render(page)
                bounded_scroll(page, max_steps=4)
                health["auth_redirect"] = detectors.detect_auth_redirect(page)
                health["cf_detected"] = detectors.detect_cf_challenge(page)
                card_count = collect_card_count(page, CARD_CANDIDATES)
                health["why_flip"] = ""
                if detectors.detect_cookie_wall(page):
                    health["why_flip"] = "cookie_wall"
                if detectors.detect_empty_listing(card_count):
                    health["why_flip"] = health["why_flip"] or "empty_listing"
                jsonld_count = _count_jsonld_products(page.content())
                _print_blocker_reason("COOKIE@cleared?", health, card_count, jsonld_count, recent_unlock)

        # Unlock once, else archive
        try:
            cookie_blocker = (health["why_flip"] == "cookie_wall") and (card_count == 0) and (jsonld_count == 0)
            blocker = health["auth_redirect"] or health["cf_detected"] or cookie_blocker
            should_unlock = blocker and (not recent_unlock)
            if blocker and not should_unlock:
                health["why_flip"] = health["why_flip"] or ("cf_challenge" if health["cf_detected"] else "cookie_wall")
            if blocker and should_unlock and not retried:
                retried = True
                _record_unlock(ret.code)
                shutdown()
                operator_unlock(ret.code, target_url)
                browser, ctx, shutdown = open_persistent_context(ret.code, headless=True)
                page = ctx.new_page()
                guard_or_raise(target_url)
                page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
                try_accept_cookies(page)
                aggressive_accept_cookies(page)
                wait_for_listing_render(page)
                bounded_scroll(page, max_steps=10)
                if ret.load_more_selector:
                    click_load_more(page, ret.load_more_selector, max_pages=int(ret.max_pages or 6))
                    bounded_scroll(page, max_steps=3)
                (run_dir / "listing_after_unlock.html").write_text(page.content(), encoding="utf-8")
                page.screenshot(path=str(run_dir / "listing_after_unlock.png"), full_page=True)
                health["auth_redirect"] = detectors.detect_auth_redirect(page)
                health["cf_detected"] = detectors.detect_cf_challenge(page)
                card_count = collect_card_count(page, CARD_CANDIDATES)
                health["why_flip"] = ""
                if detectors.detect_cookie_wall(page):
                    health["why_flip"] = "cookie_wall"
                if detectors.detect_empty_listing(card_count):
                    health["why_flip"] = health["why_flip"] or "empty_listing"
                jsonld_count = _count_jsonld_products(page.content())
                _print_blocker_reason("WHY@post", health, card_count, jsonld_count, True)
        except Exception:
            pass

        # Early flip to archives if still weak
        if not live_only:
            reason = should_flip_to_archive(health, card_count, jsonld_count)
            if reason:
                arch_rows = _archive_listing_attempt(ret, target_url, run_dir, max_cards=80)
                if arch_rows:
                    all_rows.extend(arch_rows)

        # Quick PDP links
        pdp_links_quick = _collect_pdp_links_quick(page, max_links=80)
        try:
            (run_dir / "pdp_links_quick.json").write_text(
                json.dumps(pdp_links_quick, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass

        # Merge batch_rows collected by pagers (AH/Jumbo/Carrefour/Colruyt live listing)
        # Annotate retailer name here
        enriched_batch: List[Row] = []
        for r in (batch_rows or []):
            enriched_batch.append(
                Row(
                    retailer=ret.name,
                    product_name=r.product_name,
                    quantity=r.quantity,
                    price_eur=r.price_eur,
                    source_url=r.source_url or target_url,
                    selector_used=r.selector_used or "jsonld",
                    mode=r.mode or "live",
                    stale=False,
                )
            )
        all_rows.extend(enriched_batch)

        # If still few live rows, try other in-page fallbacks
        if len([x for x in all_rows if not x.stale]) < 5:
            # Next.js payload
            nx = extract_next_products(page.content())
            for p in nx:
                name = (p.get("name") or "").strip()
                price = parse_price_to_eur(str(p.get("price") or ""))
                if not name or price is None:
                    continue
                qty = parse_quantity(name) or ""
                all_rows.append(Row(retailer=ret.name, product_name=name, quantity=qty, price_eur=price, source_url=target_url, selector_used="nextjs", mode="live", stale=False))

        if len([x for x in all_rows if not x.stale]) < 5:
            # dataLayer (best effort)
            try:
                from tools.phase1.parsers_jsonld import extract_datalayer_products
                _dl = extract_datalayer_products(page.content())
                for p in _dl:
                    name = (p.get("name") or "").strip()
                    price = parse_price_to_eur(str(p.get("price") or ""))
                    if not name or price is None:
                        continue
                    all_rows.append(Row(retailer=ret.name, product_name=name, quantity="", price_eur=price, source_url=target_url, selector_used="datalayer", mode="live", stale=False))
            except Exception:
                pass

        if len([x for x in all_rows if not x.stale]) < 5:
            css_rows = css_listing_fallback(page, ret.name, max_cards=80)
            all_rows.extend(css_rows)

        # PDP fallback (skip for Colruyt due to robots)
        if ret.code != "colruyt_be" and len([x for x in all_rows if not x.stale]) < 5:
            # live PDP via Playwright
            pdp_rows_live: List[Row] = []
            anchors = page.query_selector_all("a[href*='/producten/'], a[href*='/product'], a[href*='/p/']")
            urls: List[str] = []
            base = page.url
            for a in anchors:
                href = a.get_attribute("href") or ""
                if not href or href.startswith("#"):
                    continue
                u = urljoin(base, href)
                if u not in urls and u.startswith("http"):
                    ok, _reason = guard_url(u)
                    if not ok:
                        continue
                    urls.append(u)
                if len(urls) >= 80:
                    break

            p2 = ctx.new_page()
            try:
                for u in urls[:100]:
                    try:
                        guard_or_raise(u)
                        p2.goto(u, wait_until="domcontentloaded", timeout=45000)
                    except Exception:
                        continue
                    name = (p2.locator("h1").first.text_content() or "").strip()
                    html = p2.content()
                    if not name:
                        try:
                            title_txt = p2.locator("title").first.text_content() or ""
                            name = title_txt.strip()
                        except Exception:
                            name = ""
                    price = parse_price_to_eur(html)
                    if not name or price is None:
                        continue
                    if _negative_hit(name):
                        continue
                    qty = parse_quantity(name) or ""
                    pdp_rows_live.append(Row(retailer=ret.name, product_name=name, quantity=qty, price_eur=price, source_url=u, selector_used="pdp", mode="live", stale=False))
                    p2.wait_for_timeout(random.randint(150, 300))
            finally:
                try:
                    p2.close()
                except Exception:
                    pass
            all_rows.extend(pdp_rows_live)

        # Archive listing fallback again if still poor
        if not live_only and len([x for x in all_rows if not x.stale]) < 5:
            arch_rows2 = _archive_listing_attempt(ret, target_url, run_dir, max_cards=80)
            all_rows.extend(arch_rows2)

        # Archive PDP rescue (Wayback CDX) as last resort
        if len(all_rows) < 5:
            terms = _load_oil_terms(Path("configs/oil_terms.yaml"))
            arch_rows = archive_pdp_rescue(
                category_url=target_url,
                max_items=40,
                positive_terms=terms["positive"],
                brand_terms=terms["brands"],
                negative_terms=[n.lower() for n in DEFAULT_NEGATIVE_TERMS],
            )
            for r in arch_rows:
                qty = parse_quantity(r.name) or ""
                all_rows.append(
                    Row(
                        retailer=ret.name,
                        product_name=r.name,
                        quantity=qty,
                        price_eur=r.price_eur,
                        source_url=r.source_url,
                        selector_used="archive_pdp",
                        mode="archive",
                        stale=True,
                    )
                )

        # Snapshot sink re-parse
        if len(all_rows) < 5:
            snap_rows = _reparse_snapshot(ret.code, ret.name, target_url)
            all_rows.extend(snap_rows)

        # Deduplicate
        seen = set()
        uniq: List[Row] = []
        for r in all_rows:
            k = _product_key(r.product_name, r.quantity or "", r.source_url, r.price_eur)
            if k in seen:
                continue
            seen.add(k)
            uniq.append(r)
        all_rows = uniq

        # Persist snapshot on green runs
        if len(all_rows) >= 5:
            _persist_last_good_listing(ret.code, page.content())

        # Save phase-1 artifacts
        write_rows_csv(all_rows, run_dir)
        health["rows"] = len(all_rows)
        health["budget_ms"] = int((time.time() - start) * 1000)
        write_run_health(health, root / f"logs/run_{run_id}")

        extras = {
            "pdp_links_quick": pdp_links_quick,
            "target_url": target_url,
            "base_url": ret.base_url,
        }
        return {"rows": all_rows, "health": health, "extras": extras}

    except PWTimeoutError:
        health["why_flip"] = health["why_flip"] or "timeout"
        health["budget_ms"] = int((time.time() - start) * 1000)
        write_run_health(health, root / f"logs/run_{run_id}")
        return {"rows": [], "health": health, "extras": {"pdp_links_quick": [], "target_url": ret.category_url, "base_url": ret.base_url}}
    finally:
        try:
            shutdown()
        except Exception:
            pass

# -------------------------
# Phase-2/3 CLI adapter (rich rows for normalized exports)
# -------------------------
def run_one_for_cli(code: str, run_id: str, root: Path, *, archive_first: bool = False, live_only: bool = False) -> List[Dict[str, Any]]:
    rets = load_retailers(root / "retailers.csv", targets=[code])
    if not rets:
        return []
    ret = rets[0]
    result = run_one(ret, run_id, root, archive_first=archive_first, live_only=live_only)
    rows: List[Row] = result["rows"]
    health = result["health"]
    extras = result.get("extras", {})
    robots_status = health.get("robots") or "review"

    # identifier probing (HTTP only, capped) — skip for Colruyt
    pdp_urls: List[str] = extras.get("pdp_links_quick") or []
    id_map: Dict[str, Dict[str, Optional[str]]] = {}
    if robots_status == "allowed" and pdp_urls and ret.code != "colruyt_be":
        id_map = _probe_identifiers_http(pdp_urls, cap=40)

    out: List[Dict[str, Any]] = []
    site_domain = _root_domain(ret.base_url or extras.get("target_url") or "")
    for r in rows:
        ean, sku = None, None
        src = getattr(r, "source_url", None)
        if src and src in id_map:
            ean = id_map[src].get("ean")
            sku = id_map[src].get("sku")
        out.append(
            {
                "country": ret.country,
                "chain": ret.name,
                "retailer_code": ret.code,
                "product_name": r.product_name,
                "quantity": r.quantity,
                "price_eur": r.price_eur,
                "source_url": src or extras.get("target_url") or ret.category_url,
                "robots_status": robots_status,
                "site_domain": site_domain,
                "mode": getattr(r, "mode", "live"),
                "ean": ean,
                "sku": sku,
            }
        )
    return out

# -------------------------
# Simple Phase-1 CLI (4-col convenience CSV)
# -------------------------
def load_retailers_from_cli_file(path: Path, targets: List[str]) -> List[Retailer]:
    return load_retailers(path, targets)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--retailers", default="retailers.csv")
    ap.add_argument("--targets", default="", help="comma-separated retailer codes (optional)")
    ap.add_argument("--archive-first", action="store_true", help="Prefer archives for listing first, then live as needed")
    ap.add_argument("--live-only", action="store_true", help="Disable archive listing/PDP fallbacks")
    args = ap.parse_args()

    root = Path(".").resolve()
    targets = [t.strip() for t in args.targets.split(",") if t.strip()] if args.targets else []
    rets = load_retailers_from_cli_file(Path(args.retailers), targets)

    all_rows: List[Row] = []
    for ret in rets:
        print(f"[INFO] Running {ret.code}…")
        result = run_one(ret, args.run_id, root, archive_first=args.archive_first, live_only=args.live_only)
        all_rows.extend(result["rows"])

    export_path = root / f"exports/oil_prices_{args.run_id}.csv"
    merge_final_export(all_rows, export_path)
    print(f"[OK] Final export → {export_path}")

if __name__ == "__main__":
    main()
