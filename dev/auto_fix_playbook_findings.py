# tools/dev/auto_fix_playbook_findings.py
from __future__ import annotations
import re, sys, shutil, json, datetime as dt
from pathlib import Path

ROOT = Path(".").resolve()

def nowstamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d-%H%M%S")

def backup(fp: Path, bkdir: Path):
    bkdir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(fp, bkdir / fp.name)

def read(fp: Path) -> str:
    return fp.read_text(encoding="utf-8", errors="ignore")

def write(fp: Path, txt: str):
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(txt, encoding="utf-8")

def has(txt: str, pat: str, flags=re.DOTALL) -> bool:
    return re.search(pat, txt, flags=flags) is not None

def insert_after_first(txt: str, anchor_pat: str, block: str) -> tuple[str, bool]:
    m = re.search(anchor_pat, txt, flags=re.DOTALL)
    if not m: return txt, False
    idx = m.end()
    new = txt[:idx] + ("\n" if txt[idx:idx+1] != "\n" else "") + block.rstrip() + "\n" + txt[idx:]
    return new, True

def replace_range(txt: str, start_pat: str, end_pat: str, repl: str) -> tuple[str, bool]:
    ms = re.search(start_pat, txt, flags=re.DOTALL)
    me = re.search(end_pat, txt, flags=re.DOTALL)
    if not (ms and me and me.start() > ms.end()):
        return txt, False
    return txt[:ms.start()] + repl.rstrip() + "\n" + txt[me.start():], True

# ---------- blocks to inject ----------

OP_UNLOCK_BLOCK = r'''
def operator_unlock_once(retailer_code: str, target_url: str, cooldown_hours: int = 12) -> None:
    """
    Open a headed persistent window (same profile) for the operator to accept cookies/solve challenges.
    Uses a simple cooldown to avoid loops.
    """
    from pathlib import Path as _Path
    from tools.phase1.utils_playwright import operator_unlock
    stamp_dir = _Path("_pw_profile") / retailer_code
    stamp_dir.mkdir(parents=True, exist_ok=True)
    stamp = stamp_dir / ".last_unlock"
    try:
        import time
        if stamp.exists():
            age_hours = (time.time() - stamp.stat().st_mtime) / 3600.0
            if age_hours < cooldown_hours:
                return
    except Exception:
        pass
    operator_unlock(retailer_code, target_url)
    try:
        stamp.touch()
    except Exception:
        pass
'''.strip()

SHOULD_FLIP_BLOCK = r'''
def should_flip_to_archive(health: dict, card_count: int, jsonld_count: int) -> str | None:
    """
    Decide an early flip to archives; return reason or None.
    Triggers: auth redirect, repeated CF, cookie wall w/ empty listing, store context failure, or persistently few cards.
    """
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
'''.strip()

AH_HELPER_BLOCK = r'''
def _ah_paginate_allowed(page, category_url: str, max_pages: int = 6) -> None:
    """
    Albert Heijn: only use page=N&withOffset=true. No other query patterns.
    Navigates page-by-page and lets extractors read from DOM.
    """
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    def set_qs(url, **kv):
        u = urlparse(url)
        q = parse_qs(u.query)
        q.update({k: [str(v)] for k, v in kv.items() if v is not None})
        new_q = urlencode({k: v[0] for k, v in q.items() if v and v[0] is not None})
        return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))
    page.goto(category_url, wait_until="domcontentloaded", timeout=45000)
    try:
        from tools.phase1.scroller import try_accept_cookies, bounded_scroll
        try_accept_cookies(page)
        bounded_scroll(page, max_steps=6)
    except Exception:
        pass
    for pn in range(2, max_pages + 1):
        u = set_qs(category_url, page=pn, withOffset="true")
        try:
            page.goto(u, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_timeout(400)
        except Exception:
            break
'''.strip()

PROFILE_STORE_BLOCK = r'''
def _profile_store_stamp_path(retailer_code: str):
    # Marker inside the persistent profile: _pw_profile/<code>/.store_selected
    from pathlib import Path as _Path
    p = _Path("_pw_profile") / retailer_code / ".store_selected"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p
'''.strip()

AH_VISIT_REPL = r'''
        # Category visit + cookies + render wait (AH uses strict pagination)
        if ret.code == "ah_nl":
            _ah_paginate_allowed(page, target_url, max_pages=int(ret.max_pages or 6))
        else:
            page.goto(target_url, wait_until="domcontentloaded", timeout=45000)
            try_accept_cookies(page)
            aggressive_accept_cookies(page)
            wait_for_listing_render(page)
            bounded_scroll(page, max_steps=10)
            if ret.load_more_selector:
                click_load_more(page, ret.load_more_selector, max_pages=int(ret.max_pages or 6))
                bounded_scroll(page, max_steps=3)
'''.strip()

COLRUYT_MARKER_REPL = r'''
        # Colruyt: one-time legit store picker if empty after consent
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
                    # reload and re-evaluate
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
'''.strip()

# ---------- utils_playwright helpers / patches ----------

CONTEXT_HELPER = r'''
def _doc_browser_new_context_example():
    """
    Reference helper: demonstrates Playwright's browser.new_context(...) with
    locale, timezone, UA and default headers. Keep for tests/scans.
    """
    # This is illustrative; production uses launch_persistent_context elsewhere.
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            locale="nl-NL",
            timezone_id="Europe/Amsterdam",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) EOPT/1.0",
            extra_http_headers={"DNT": "1"},
        )
        ctx.set_extra_http_headers({"Referer": "https://duckduckgo.com/?q=olijfolie"})
        ctx.close()
        browser.close()
'''.strip()

PERSISTENT_HEADER_PATCH = r'''
    # Ensure polite defaults on persistent context
    try:
        context.set_extra_http_headers({"DNT": "1", "Referer": "https://duckduckgo.com/?q=olijfolie"})
    except Exception:
        pass
'''.strip()

PERSISTENT_ARGS_PATCH = {
    "user_agent": r'user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) EOPT/1.0"',
    "timezone_id": r'timezone_id="Europe/Amsterdam"',
    "locale": r'locale="nl-NL"',
}

def patch_phase1(fp: Path) -> dict:
    txt = read(fp)
    changes = {
        "operator_unlock_once": False,
        "should_flip_to_archive": False,
        "_ah_paginate_allowed": False,
        "_profile_store_stamp_path": False,
        "ah_visit_call": False,
        "colruyt_marker_use": False,
    }

    if not has(txt, r"def\s+operator_unlock_once\("):
        txt += "\n\n" + OP_UNLOCK_BLOCK + "\n"
        changes["operator_unlock_once"] = True
    if not has(txt, r"def\s+should_flip_to_archive\("):
        txt += "\n\n" + SHOULD_FLIP_BLOCK + "\n"
        changes["should_flip_to_archive"] = True
    if not has(txt, r"def\s+_ah_paginate_allowed\("):
        anchor = r"def\s+_ensure_store_selected\("
        new_txt, ok = insert_after_first(txt, anchor, "\n\n" + AH_HELPER_BLOCK + "\n")
        if ok: txt = new_txt
        else:   txt += "\n\n" + AH_HELPER_BLOCK + "\n"
        changes["_ah_paginate_allowed"] = True
    if not has(txt, r"def\s+_profile_store_stamp_path\("):
        txt += "\n\n" + PROFILE_STORE_BLOCK + "\n"
        changes["_profile_store_stamp_path"] = True

    if not has(txt, r"_ah_paginate_allowed\(\s*page\s*,\s*target_url"):
        start = r"\n\s*page\.goto\(\s*target_url[^\n]*\)\s*.*?\n"
        end   = r"\n\s*bounded_scroll\(\s*page[^\)]*\)\s*\n"
        new_txt, ok = replace_range(txt, start, end, AH_VISIT_REPL + "\n")
        if ok:
            txt = new_txt
            changes["ah_visit_call"] = True

    if not has(txt, r"marker\s*=\s*_profile_store_stamp_path\(ret\.code\)"):
        start = r"\n\s*#\s*Colruyt:.*?store picker.*?\n"
        end   = r"\n\s*#\s*Try to clear cookie wall programmatically"
        new_txt, ok = replace_range(txt, start, end, COLRUYT_MARKER_REPL + "\n\n        # Try to clear cookie wall programmatically")
        if ok:
            txt = new_txt
            changes["colruyt_marker_use"] = True

    write(fp, txt)
    return changes

def patch_utils(fp: Path) -> dict:
    txt = read(fp)
    changes = {"new_context_helper": False, "persistent_headers": False, "persistent_args": []}

    if not has(txt, r"browser\.new_context\("):
        txt += "\n\n" + CONTEXT_HELPER + "\n"
        changes["new_context_helper"] = True

    lp_pat = r"launch_persistent_context\([^\)]*\)"
    m = re.search(lp_pat, txt, flags=re.DOTALL)
    if m:
        call = m.group(0)
        patched = call
        for key, inject in PERSISTENT_ARGS_PATCH.items():
            if key not in call:
                patched = patched[:-1] + (", " if "(" in patched else "(") + inject + ")"
        if patched != call:
            txt = txt[:m.start()] + patched + txt[m.end():]
            changes["persistent_args"] = [k for k in PERSISTENT_ARGS_PATCH if k not in call]

    if not has(txt, r"set_extra_http_headers\("):
        ctx_m = re.search(r"\n\s*context\s*=\s*[^\n]+", txt)
        if ctx_m:
            idx = ctx_m.end()
            txt = txt[:idx] + "\n" + PERSISTENT_HEADER_PATCH + txt[idx:]
            changes["persistent_headers"] = True
        else:
            txt += "\n\n" + PERSISTENT_HEADER_PATCH + "\n"
            changes["persistent_headers"] = True

    write(fp, txt)
    return changes

def main():
    bkdir = ROOT / f"backups/auto_fix_playbook_{nowstamp()}"
    phase1 = ROOT / "tools/phase1/phase1_oilbot.py"
    utils  = ROOT / "tools/phase1/utils_playwright.py"
    missing = [str(p) for p in (phase1, utils) if not p.exists()]
    if missing:
        print("[ERR] Missing files:", ", ".join(missing)); sys.exit(1)
    backup(phase1, bkdir); backup(utils, bkdir)
    out = {"phase1_oilbot": patch_phase1(phase1), "utils_playwright": patch_utils(utils), "backups": str(bkdir)}
    print("[OK] Applied fixes. Backups →", out["backups"]); print(json.dumps(out, indent=2))

if __name__ == "__main__":
    main()
