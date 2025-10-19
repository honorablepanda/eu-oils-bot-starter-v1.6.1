# tools/dev/apply_updates_idempotent.py
from __future__ import annotations
import re, sys, json, shutil, datetime as dt
from pathlib import Path

ROOT = Path(".").resolve()

# ---- helpers ---------------------------------------------------------------

def ts() -> str:
    return dt.datetime.now().strftime("%Y%m%d-%H%M%S")

def backup(fp: Path, bkdir: Path):
    bkdir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(fp, bkdir / fp.name)

def load(fp: Path) -> str:
    return fp.read_text(encoding="utf-8")

def save(fp: Path, text: str):
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(text, encoding="utf-8")

def already(text: str, needle: str) -> bool:
    return needle in text

def insert_after(text: str, anchor_regex: str, block: str) -> tuple[str, bool]:
    """
    Insert block immediately after the first anchor match.
    Returns (new_text, changed)
    """
    m = re.search(anchor_regex, text, flags=re.DOTALL)
    if not m:
        return text, False
    idx = m.end()
    new_text = text[:idx] + ("\n" if not text[idx:idx+1] == "\n" else "") + block.rstrip() + "\n" + text[idx:]
    return new_text, True

def replace_block(text: str, start_regex: str, end_regex: str, replacement: str) -> tuple[str, bool]:
    """
    Replace the first block bounded by start_regex..end_regex (end kept exclusive).
    """
    ms = re.search(start_regex, text, flags=re.DOTALL)
    me = re.search(end_regex, text, flags=re.DOTALL)
    if not (ms and me and me.start() > ms.end()):
        return text, False
    new_text = text[:ms.start()] + replacement.rstrip() + "\n" + text[me.start():]
    return new_text, True

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

# ---- content blocks (exact) -----------------------------------------------

AH_HELPER = r'''
def _ah_paginate_allowed(page, category_url: str, max_pages: int = 6) -> None:
    """
    Albert Heijn: only use page=N&withOffset=true. No other query patterns.
    Navigates page-by-page and lets the rest of the pipeline read from DOM.
    """
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    def set_qs(url, **kv):
        u = urlparse(url)
        q = parse_qs(u.query)
        q.update({k: [str(v)] for k, v in kv.items() if v is not None})
        new_q = urlencode({k: v[0] for k, v in q.items() if v and v[0] is not None})
        return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

    # page 1 (no param), then 2..N with offset param
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

PROFILE_STORE_HELPER = r'''
def _profile_store_stamp_path(retailer_code: str) -> Path:
    # Marker inside the persistent profile: _pw_profile/<code>/.store_selected
    p = Path("_pw_profile") / retailer_code / ".store_selected"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p
'''.strip()

AH_VISIT_REPLACEMENT = r'''
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
'''.rstrip()

COLRUYT_BLOCK_REPLACEMENT = r'''
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
                    # reload category and re-evaluate
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
'''.rstrip()

SELECTOR_WIZARD_CONTENT = r'''
from __future__ import annotations
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Optional

@dataclass
class Candidate:
    id: str
    css: str
    last_score: float = 0.0
    wins_in_a_row: int = 0

@dataclass
class EvalMetrics:
    cards: int
    price_ok_rate: float
    qty_ok_rate: float
    dup_rate: float

PROMOTE_MARGIN = 0.05
REQUIRED_WINS = 2

def _policy_path(code: str) -> Path:
    p = Path("policy") / f"{code}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _load_policy(code: str) -> Dict[str, Any]:
    fp = _policy_path(code)
    if fp.exists():
        return json.loads(fp.read_text(encoding="utf-8"))
    return {"selectors": {"active": None, "candidates": {}}}

def _save_policy(code: str, p: Dict[str, Any]) -> None:
    _policy_path(code).write_text(json.dumps(p, ensure_ascii=False, indent=2), encoding="utf-8")

def _explain_path(run_dir: Path) -> Path:
    return run_dir / "selectors_explained.json"

def shadow_score(m: EvalMetrics) -> float:
    return 0.5 * m.price_ok_rate + 0.5 * m.qty_ok_rate

def _gates_pass(m: EvalMetrics) -> bool:
    return (m.cards >= 5 and m.price_ok_rate >= 0.70 and m.qty_ok_rate >= 0.50 and m.dup_rate <= 0.20)

def maybe_promote(code: str, run_dir: Path, baseline: Candidate, challengers: List[Candidate],
                  baseline_metrics: EvalMetrics, challenger_metrics: Dict[str, EvalMetrics]) -> Candidate:
    """
    Returns the selected candidate (either baseline or a promoted challenger).
    Rules:
      - Challenger must outscore baseline by >= PROMOTE_MARGIN *and* pass gates.
      - Needs 2 consecutive wins to promote.
      - First regression → auto rollback to baseline.
    """
    policy = _load_policy(code)
    active_id = policy["selectors"].get("active") or baseline.id

    # Register candidates
    reg: Dict[str, Dict[str, Any]] = {baseline.id: {"css": baseline.css, "wins": 0}}
    for c in challengers:
        reg[c.id] = {"css": c.css, "wins": 0}
    policy["selectors"]["candidates"] = {**policy["selectors"].get("candidates", {}), **reg}

    base_score = shadow_score(baseline_metrics)
    winner = baseline
    promoted = False

    best: Optional[Candidate] = None
    best_score = base_score
    for c in challengers:
        m = challenger_metrics.get(c.id)
        if not m:
            continue
        s = shadow_score(m)
        if s > best_score and (s - base_score) >= PROMOTE_MARGIN and _gates_pass(m):
            best = c
            best_score = s

    if best is not None:
        wins = policy["selectors"]["candidates"][best.id].get("wins", 0) + 1
        policy["selectors"]["candidates"][best.id]["wins"] = wins
        if wins >= REQUIRED_WINS:
            policy["selectors"]["active"] = best.id
            winner = best
            promoted = True
            # Reset others' counters
            for k in list(policy["selectors"]["candidates"].keys()):
                if k != best.id:
                    policy["selectors"]["candidates"][k]["wins"] = 0
    else:
        # Regression / no winner → reset and keep baseline
        for k in list(policy["selectors"]["candidates"].keys()):
            if k != baseline.id:
                policy["selectors"]["candidates"][k]["wins"] = 0
        policy["selectors"]["active"] = baseline.id
        winner = baseline

    explanation = {
        "active_before": active_id,
        "baseline": {"id": baseline.id, "score": round(base_score, 3)},
        "challengers": {
            cid: {
                "score": round(shadow_score(challenger_metrics[cid]), 3) if cid in challenger_metrics else None,
                "wins": policy["selectors"]["candidates"][cid]["wins"]
            } for cid in policy["selectors"]["candidates"] if cid != baseline.id
        },
        "promoted": promoted,
        "active_after": policy["selectors"]["active"],
        "gates": {
            "cards>=5": baseline_metrics.cards >= 5,
            "price>=70%": baseline_metrics.price_ok_rate >= 0.70,
            "qty>=50%": baseline_metrics.qty_ok_rate >= 0.50,
            "dup<=20%": baseline_metrics.dup_rate <= 0.20,
        }
    }
    _explain_path(run_dir).write_text(json.dumps(explanation, ensure_ascii=False, indent=2), encoding="utf-8")
    _save_policy(code, policy)
    return winner
'''.strip()

EXPORTER_BACKFILL_BLOCK = r'''
    # --- Backfill unit price (EUR/L) when missing but qty is present ---
    try:
        up = df.apply(lambda r: unit_price_eur_per_L(r.get("price_eur"), r.get("net_qty_value"),
                                                     r.get("net_qty_unit"), r.get("pack_count")), axis=1)
        mask_missing = df["unit_price_eur_per_unit"].isna() & up.notna()
        if "unit_basis" not in df.columns:
            df["unit_basis"] = None
        df.loc[mask_missing, "unit_price_eur_per_unit"] = up[mask_missing]
        df.loc[mask_missing & df["unit_basis"].isna(), "unit_basis"] = "L"
    except Exception:
        pass
'''.rstrip()

# ---- patchers -------------------------------------------------------------

def patch_phase1_oilbot(fp: Path) -> dict:
    changed = {"ah_helper": False, "profile_store_helper": False,
               "ah_visit_block": False, "colruyt_block": False}
    text = load(fp)

    # 1) Insert AH helper if missing
    if not already(text, "def _ah_paginate_allowed("):
        # place after _ensure_store_selected or after website_id helpers as fallback
        anchor = r"def _ensure_store_selected\(.*?\)\s*:\s*.*?\n\s*return False\n"
        new_text, ok = insert_after(text, anchor, "\n\n" + AH_HELPER + "\n")
        if not ok:
            # fallback: insert after website_id helpers block
            anchor = r"# ---- website_id helpers .*? ----"
            new_text, ok = insert_after(text, anchor, "\n\n" + AH_HELPER + "\n")
        if ok:
            text = new_text
            changed["ah_helper"] = True

    # 2) Insert profile store marker helper if missing
    if not already(text, "def _profile_store_stamp_path("):
        anchor = r"def _ah_paginate_allowed\(.*?\):.*?\n"
        new_text, ok = insert_after(text, anchor, "\n\n" + PROFILE_STORE_HELPER + "\n")
        if not ok:
            # fallback: after _ensure_store_selected
            anchor = r"def _ensure_store_selected\(.*?\)\s*:\s*.*?\n\s*return False\n"
            new_text, ok = insert_after(text, anchor, "\n\n" + PROFILE_STORE_HELPER + "\n")
        if ok:
            text = new_text
            changed["profile_store_helper"] = True

    # 3) Replace category visit block with AH-aware version
    if not already(text, "_ah_paginate_allowed(page, target_url"):
        start = r"\n\s*# Category visit \+ cookies \+ render wait"
        end   = r"\n\s*# Save listing artifacts"
        new_text, ok = replace_block(text, start, end, AH_VISIT_REPLACEMENT + "\n\n        # Save listing artifacts")
        if ok:
            text = new_text
            changed["ah_visit_block"] = True

    # 4) Replace Colruyt store-picker block to use profile marker
    if not already(text, "marker = _profile_store_stamp_path(ret.code)"):
        start = r"\n\s*# Colruyt: one-time legit store picker if empty after consent"
        # end at last recompute line for jsonld_count
        end = r"\n\s*# Try to clear cookie wall programmatically"
        new_text, ok = replace_block(text, start, end, COLRUYT_BLOCK_REPLACEMENT + "\n\n        # Try to clear cookie wall programmatically")
        if ok:
            text = new_text
            changed["colruyt_block"] = True

    save(fp, text)
    return changed


def ensure_selector_wizard(fp: Path) -> bool:
    if fp.exists():
        txt = load(fp)
        if "def maybe_promote(" in txt and "REQUIRED_WINS = 2" in txt:
            return False  # looks good
    save(fp, SELECTOR_WIZARD_CONTENT + "\n")
    return True


def patch_exporter_backfill(fp: Path) -> bool:
    txt = load(fp)
    if "Backfill unit price (EUR/L)" in txt:
        return False
    # insert right after df creation in write_weekly_and_master
    pattern = r"(def write_weekly_and_master\(.*?\):\s*.*?\n\s*df = _as_dataframe\(rows\).*?\n\s*df\[\"ts_utc\"\].*?\n)"
    m = re.search(pattern, txt, flags=re.DOTALL)
    if not m:
        return False
    idx = m.end()
    new_txt = txt[:idx] + EXPORTER_BACKFILL_BLOCK + "\n" + txt[idx:]
    save(fp, new_txt)
    return True


# ---- main -----------------------------------------------------------------

def main():
    bkdir = ROOT / f"backups/apply_updates_{ts()}"
    ensure_dir(bkdir)

    # targets
    f_phase1 = ROOT / "tools/phase1/phase1_oilbot.py"
    f_wizard = ROOT / "tools/phase1/selector_wizard.py"
    f_export = ROOT / "src/eopt/exporters_normalized.py"

    # sanity existence
    missing = [str(p) for p in [f_phase1, f_export] if not p.exists()]
    if missing:
        print("[ERR] Missing files:", ", ".join(missing))
        sys.exit(1)

    # backups
    backup(f_phase1, bkdir)
    backup(f_export, bkdir)
    if f_wizard.exists():
        backup(f_wizard, bkdir)

    summary = {"phase1_oilbot": {}, "selector_wizard": False, "exporter_backfill": False}

    # patch phase1
    summary["phase1_oilbot"] = patch_phase1_oilbot(f_phase1)

    # ensure selector wizard
    summary["selector_wizard"] = ensure_selector_wizard(f_wizard)

    # exporter backfill
    summary["exporter_backfill"] = patch_exporter_backfill(f_export)

    print("[OK] Updates applied (idempotent). Backups →", str(bkdir))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
