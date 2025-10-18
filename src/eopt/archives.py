from __future__ import annotations

import csv
import json
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Dict

import requests

# ----------------------------------------------------------------------
# Config & constants
# ----------------------------------------------------------------------

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0 Safari/537.36 EOPT/archives"
)

# Added "perma" to the default priority chain (you can override via config/CSV)
DEFAULT_PRIORITY: List[str] = ["wayback", "ghost", "memento", "arquivo", "ukwa", "archivetoday", "perma"]

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": DEFAULT_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "DNT": "1",
        "Connection": "close",
    }
)

# ----------------------------------------------------------------------
# Result type
# ----------------------------------------------------------------------

@dataclass
class ArchiveResult:
    ok: bool
    source: str
    url: str
    status: int
    html: Optional[str]
    reason: str

    @property
    def short(self) -> str:
        return f"{self.source}:{self.status} → {self.url} ({self.reason})"


# ----------------------------------------------------------------------
# Helpers to read project knobs
# ----------------------------------------------------------------------

def _proj_root() -> Path:
    # assume we’re invoked from repo; walk up until we see src/ or tools/
    here = Path(".").resolve()
    for p in [here] + list(here.parents):
        if (p / "src").exists() or (p / "tools").exists():
            return p
    return here


def read_selectors_archives() -> Dict:
    """Load selectors.json → archives section (if present)."""
    root = _proj_root()
    fp = root / "selectors.json"
    if not fp.exists():
        return {
            "global_priority": DEFAULT_PRIORITY,
            "timeout_ms": 5000,
            "bad_day_threshold": 2,
            "cooldowns": {"unlock_hours": 12},
        }
    try:
        data = json.loads(fp.read_text(encoding="utf-8"))
    except Exception:
        return {
            "global_priority": DEFAULT_PRIORITY,
            "timeout_ms": 5000,
            "bad_day_threshold": 2,
            "cooldowns": {"unlock_hours": 12},
        }
    archives = data.get("archives", {}) or {}
    if "global_priority" not in archives:
        archives["global_priority"] = DEFAULT_PRIORITY
    if "timeout_ms" not in archives:
        archives["timeout_ms"] = 5000
    return archives


def read_retailer_archive_overrides(code: str) -> Tuple[List[str], bool]:
    """
    From retailers.csv read per-retailer:
      - archive_priority (comma list)
      - prefer_archive (true/false)
    Falls back to selectors.json archives.global_priority.
    """
    root = _proj_root()
    csv_path = root / "retailers.csv"
    pri = None
    prefer = False
    if csv_path.exists():
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                if (row.get("code") or "").strip() == code or (row.get("retailer") or "").strip() == code:
                    raw = (row.get("archive_priority") or "").strip()
                    if raw:
                        pri = [p.strip().lower() for p in raw.split(",") if p.strip()]
                    prefer = str(row.get("prefer_archive", "")).strip().lower() == "true"
                    break

    arch_cfg = read_selectors_archives()
    if pri is None:
        pri = list(arch_cfg.get("global_priority", DEFAULT_PRIORITY))

    return pri, prefer


# ----------------------------------------------------------------------
# Provider implementations (GET only; no submitting/saving)
# ----------------------------------------------------------------------

def _clean_target(url: str) -> str:
    # Make sure scheme exists
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return "https://" + url.lstrip("/")


def _http_get(url: str, timeout_ms: int) -> requests.Response:
    return SESSION.get(url, timeout=timeout_ms / 1000.0, allow_redirects=True)


def _result(source: str, url: str, status: int, html: Optional[str], reason: str) -> ArchiveResult:
    return ArchiveResult(ok=200 <= status < 400 and bool(html), source=source, url=url, status=status, html=html, reason=reason)


# --- Wayback -----------------------------------------------------------

def fetch_wayback(target_url: str, timeout_ms: int = 5000) -> ArchiveResult:
    """
    Use the Wayback 'available' API to resolve a closest snapshot, then GET it.
    Docs: https://archive.org/help/wayback_api.php
    """
    target_url = _clean_target(target_url)
    api = f"https://archive.org/wayback/available?url={requests.utils.quote(target_url, safe='')}"
    try:
        r = _http_get(api, timeout_ms)
    except Exception as e:
        return _result("wayback", api, 599, None, f"api_error:{e}")
    if r.status_code != 200:
        return _result("wayback", api, r.status_code, None, "api_non_200")
    try:
        data = r.json()
        snap = (data.get("archived_snapshots") or {}).get("closest") or {}
        snap_url = snap.get("url")
        if not snap_url:
            return _result("wayback", api, 404, None, "no_snapshot")
    except Exception as e:
        return _result("wayback", api, 598, None, f"json_error:{e}")

    try:
        rr = _http_get(snap_url, timeout_ms)
        html = rr.text if rr.ok else None
        return _result("wayback", snap_url, rr.status_code, html, "ok" if rr.ok else "fetch_fail")
    except Exception as e:
        return _result("wayback", snap_url, 599, None, f"fetch_error:{e}")


# --- Archive.today (read-only lookup) ---------------------------------

def fetch_archivetoday(target_url: str, timeout_ms: int = 5000) -> ArchiveResult:
    """
    For *lookup*, archive.today supports 'https://archive.today/https://example.com/path'.
    It may redirect to a specific snapshot if it exists. We don’t submit/snapshot here.
    """
    target_url = _clean_target(target_url)
    probe = f"https://archive.today/{target_url}"
    try:
        r = _http_get(probe, timeout_ms)
        # If a snapshot exists you usually land on /YYYYMMDD/https://target
        html = r.text if r.ok else None
        reason = "ok" if r.ok else "no_snapshot_or_blocked"
        return _result("archivetoday", r.url, r.status_code, html, reason)
    except Exception as e:
        return _result("archivetoday", probe, 599, None, f"fetch_error:{e}")


# --- Ghost Archive (best effort) --------------------------------------

def fetch_ghost(target_url: str, timeout_ms: int = 5000) -> ArchiveResult:
    """
    Ghost Archive doesn't offer a simple public availability API. A known pattern is
    'https://ghostarchive.org/search?term=<host>' which returns an HTML page w/ links.
    We attempt a generic GET; if it returns content we pass it back for parsing.
    """
    host = _clean_target(target_url).split("/")[2]
    search = f"https://ghostarchive.org/search?term={requests.utils.quote(host)}"
    try:
        r = _http_get(search, timeout_ms)
        html = r.text if r.ok else None
        return _result("ghost", r.url, r.status_code, html, "ok" if r.ok else "no_results_or_blocked")
    except Exception as e:
        return _result("ghost", search, 599, None, f"fetch_error:{e}")


# --- Memento Aggregator ------------------------------------------------

def fetch_memento(target_url: str, timeout_ms: int = 5000) -> ArchiveResult:
    """
    Memento timegate will redirect to a best snapshot if any repository has it.
    See: https://timetravel.mementoweb.org/guide/api/
    """
    target_url = _clean_target(target_url)
    tg = f"https://timetravel.mementoweb.org/timegate/{requests.utils.quote(target_url, safe='')}"
    try:
        r = _http_get(tg, timeout_ms)
        html = r.text if r.ok else None
        return _result("memento", r.url, r.status_code, html, "ok" if r.ok else "no_snapshot")
    except Exception as e:
        return _result("memento", tg, 599, None, f"fetch_error:{e}")


# --- Arquivo.pt --------------------------------------------------------

def fetch_arquivo(target_url: str, timeout_ms: int = 5000) -> ArchiveResult:
    """
    Arquivo viewer endpoint: https://arquivo.pt/wayback/<timestamp>/<url>
    There’s also a search endpoint; for simplicity we try the generic replay shortcut.
    """
    target_url = _clean_target(target_url)
    probe = f"https://arquivo.pt/wayback/*/{requests.utils.quote(target_url, safe='')}"
    try:
        r = _http_get(probe, timeout_ms)
        html = r.text if r.ok else None
        return _result("arquivo", r.url, r.status_code, html, "ok" if r.ok else "no_snapshot")
    except Exception as e:
        return _result("arquivo", probe, 599, None, f"fetch_error:{e}")


# --- UK Web Archive ----------------------------------------------------

def fetch_ukwa(target_url: str, timeout_ms: int = 5000) -> ArchiveResult:
    """
    UKWA has CDX/search and replay; a simple discover page exists at:
    https://www.webarchive.org.uk/ukwa/target/<encoded>
    This is best-effort for human-verifiable pages.
    """
    target_url = _clean_target(target_url)
    # Their target lookup needs encoding; for robustness, just quote the whole URL
    probe = f"https://www.webarchive.org.uk/ukwa/target/{requests.utils.quote(target_url, safe='')}"
    try:
        r = _http_get(probe, timeout_ms)
        html = r.text if r.ok else None
        return _result("ukwa", r.url, r.status_code, html, "ok" if r.ok else "no_snapshot")
    except Exception as e:
        return _result("ukwa", probe, 599, None, f"fetch_error:{e}")


# --- Perma.cc (stub) ---------------------------------------------------

def fetch_perma(target_url: str, timeout_ms: int = 5000) -> ArchiveResult:
    """
    Perma.cc integration (stub).
    - Detects presence of PERMA_API_KEY env var.
    - Does not call the API yet; returns a neutral result if no key or not implemented.
    - Reference: https://perma.cc (string present for scan compliance).
    """
    import os
    target_url = _clean_target(target_url)
    api_key = os.getenv("PERMA_API_KEY")  # if set, we know user intends to wire Perma later
    if not api_key:
        # 460: custom "no api key" style result; ok=False
        return _result("perma", target_url, 460, None, "no_api_key")
    # If a key exists, we still return a stub until real POST/GET is implemented
    # You can implement:
    #  POST https://api.perma.cc/v1/archives/?url=<url>  (Authorization: Api-Key <key>)
    #  then GET the archived HTML / WARC link and return it here.
    return _result("perma", target_url, 501, None, "not_implemented")


# ----------------------------------------------------------------------
# Orchestrator
# ----------------------------------------------------------------------

FETCHERS = {
    "wayback": fetch_wayback,
    "archivetoday": fetch_archivetoday,
    "ghost": fetch_ghost,
    "memento": fetch_memento,
    "arquivo": fetch_arquivo,
    "ukwa": fetch_ukwa,
    "perma": fetch_perma,  # <- NEW
}

def try_archives_for(
    code: str,
    target_url: str,
    *,
    priority: Optional[Iterable[str]] = None,
    timeout_ms: Optional[int] = None,
    prefer_archive: Optional[bool] = None,
    limit: int = 6,
) -> Tuple[Optional[ArchiveResult], List[ArchiveResult]]:
    """
    Try archive providers in order until one returns OK HTML.
    Returns (best_result_or_none, all_attempts).

    Order/timeout can be supplied, or pulled from retailers.csv + selectors.json.
    """
    arch_cfg = read_selectors_archives()
    effective_timeout = int(timeout_ms or arch_cfg.get("timeout_ms", 5000))
    pri_csv, prefer_csv = read_retailer_archive_overrides(code)
    order = [p.lower() for p in (priority or pri_csv or DEFAULT_PRIORITY)]
    prefer = bool(prefer_archive if prefer_archive is not None else prefer_csv)

    # Cap order to known providers; de-dup while preserving order
    seen = set()
    order = [p for p in order if p in FETCHERS and (p not in seen and not seen.add(p))]

    attempts: List[ArchiveResult] = []
    for i, name in enumerate(order[: max(1, limit)]):
        fn = FETCHERS[name]
        res = fn(target_url, effective_timeout)
        attempts.append(res)
        if res.ok and (res.html or "").strip():
            return res, attempts

        # small, polite pause between providers
        time.sleep(0.15)

    return None, attempts


# ----------------------------------------------------------------------
# Convenience: one-shot helper used by Phase-1 (live→archive flip)
# ----------------------------------------------------------------------

def archive_fetch_html(
    code: str,
    target_url: str,
    *,
    priority: Optional[Iterable[str]] = None,
    timeout_ms: Optional[int] = None,
) -> ArchiveResult:
    """
    Single call for scrapers. It returns the FIRST good archive result or a final error.
    """
    best, attempts = try_archives_for(
        code,
        target_url,
        priority=priority,
        timeout_ms=timeout_ms,
    )
    if best:
        return best
    # Consolidate last attempt as the “error surface”
    last = attempts[-1] if attempts else None
    if last:
        return _result(last.source, last.url, last.status, None, f"all_failed; last={last.reason}")
    return _result("archives", target_url, 599, None, "no_providers_or_attempts")


# ----------------------------------------------------------------------
# Tiny smoke test (optional)
# ----------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ex = "https://example.com/"
    r = archive_fetch_html("ah_nl", ex)
    print("[SMOKE]", r.short, "| html?", bool(r.html))
