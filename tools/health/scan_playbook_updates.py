# tools/health/scan_playbook_updates.py
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------- utilities

def nowstamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d-%H%M%S")


def read_text_safe(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def exists(p: Path) -> bool:
    try:
        return p.exists()
    except Exception:
        return False


def has(text: str, pattern: str, flags=re.DOTALL) -> bool:
    return re.search(pattern, text, flags=flags) is not None


def csv_rows(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not exists(path):
        return [], []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)
    return headers, rows


# ---------- issue model

@dataclass
class Issue:
    severity: str  # P1 | P2 | P3
    code: str
    message: str
    file: Optional[str] = None
    hint: Optional[str] = None


class Logger:
    def __init__(self):
        self.issues: List[Issue] = []

    def add(self, sev: str, code: str, message: str, file: Optional[str] = None, hint: Optional[str] = None):
        self.issues.append(Issue(sev, code, message, file, hint))

    def counts(self) -> Dict[str, int]:
        c = {"P1": 0, "P2": 0, "P3": 0}
        for i in self.issues:
            c[i.severity] = c.get(i.severity, 0) + 1
        return c

    def dump(self, out_prefix: str = "playbook"):
        reports = Path("reports")
        reports.mkdir(parents=True, exist_ok=True)
        base = reports / f"{out_prefix}_scan_{nowstamp()}"
        # JSON
        j = {
            "summary": self.counts(),
            "issues": [asdict(i) for i in self.issues],
        }
        (base.with_suffix(".json")).write_text(json.dumps(j, ensure_ascii=False, indent=2), encoding="utf-8")
        # TXT
        lines = []
        c = self.counts()
        lines.append(f"[SCAN] P1={c['P1']} P2={c['P2']} P3={c['P3']}\n")
        for i in self.issues:
            lines.append(f"{i.severity} | {i.code} | {i.message}" + (f" | file={i.file}" if i.file else ""))
            if i.hint:
                lines.append(f"  ↳ hint: {i.hint}")
        (base.with_suffix(".txt")).write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
        return base.with_suffix(".txt"), base.with_suffix(".json")


# ---------- individual checks

def check_net_gateway(root: Path, log: Logger):
    fp = root / "src/eopt/net_gateway.py"
    if not exists(fp):
        log.add("P2", "GATEWAY_MISSING", "robots-aware NetGateway not found (src/eopt/net_gateway.py).",
                str(fp), "Create per playbook; switch archive/CDX helpers to use it (httpx + robots).")
        return
    txt = read_text_safe(fp)
    if not has(txt, r"class\s+NetGateway\b"):
        log.add("P2", "GATEWAY_CLASS", "NetGateway class missing.", str(fp), "Define NetGateway with robots + rate limit.")
    if not has(txt, r"urllib\.robotparser"):
        log.add("P2", "GATEWAY_ROBOTS", "Robots parser not referenced.", str(fp), "Use urllib.robotparser.RobotFileParser.")
    if not has(txt, r"httpx\.Client"):
        log.add("P3", "GATEWAY_HTTPX", "httpx client not detected.", str(fp), "Use httpx.Client(..., follow_redirects=True).")


def check_unlock_helper(root: Path, log: Logger):
    fp = root / "tools/phase1/unlock_helper.py"
    txt = read_text_safe(fp) if exists(fp) else ""
    if not txt:
        log.add("P3", "UNLOCK_HELPER_MISSING", "Human unlock helper not found (optional but recommended).",
                str(fp), "Add tools/phase1/unlock_helper.py and call operator_unlock_once(...) with cooldown.")
    fp_bot = root / "tools/phase1/phase1_oilbot.py"
    bot = read_text_safe(fp_bot)
    if not bot:
        log.add("P1", "PHASE1_MISSING", "phase1_oilbot.py not found.", str(fp_bot))
        return
    if not has(bot, r"def\s+operator_unlock_once\("):
        log.add("P3", "UNLOCK_CALL_MISSING", "operator_unlock_once(...) function not defined.", str(fp_bot),
                "Add the cooldowned unlock helper wrap; call only when needed.")


def check_flip_controller(root: Path, log: Logger):
    fp = root / "tools/phase1/phase1_oilbot.py"
    txt = read_text_safe(fp)
    if not has(txt, r"def\s+should_flip_to_archive\("):
        log.add("P3", "FLIP_FN_MISSING", "should_flip_to_archive(...) not found.", str(fp),
                "Add unified early-flip decision (CF/auth/store/empty-after-attempts).")


def check_utils_playwright_headers(root: Path, log: Logger):
    fp = root / "tools/phase1/utils_playwright.py"
    if not exists(fp):
        log.add("P2", "UTILS_MISSING", "utils_playwright.py not found.", str(fp))
        return
    txt = read_text_safe(fp)
    if not has(txt, r"new_context\("):
        log.add("P2", "PW_CONTEXT", "browser.new_context(...) not detected.", str(fp))
    if not has(txt, r"locale\s*=\s*['\"]nl-[NLBE]{2}['\"]") and not has(txt, r"locale\s*=\s*['\"]nl-NL['\"]"):
        log.add("P3", "PW_LOCALE", "Locale nl-NL/nl-BE not detected on context.", str(fp),
                "Set locale='nl-NL' (and nl-BE where applicable).")
    if not has(txt, r"timezone_id\s*=\s*['\"]Europe/Amsterdam['\"]"):
        log.add("P3", "PW_TZ", "Timezone Europe/Amsterdam not detected.", str(fp))
    if not has(txt, r"user_agent\s*=\s*['\"][^'\"]*EOPT/1\.0"):
        log.add("P3", "PW_UA", "User-Agent not set to EOPT/1.0 (+compliance; polite).", str(fp))
    if not has(txt, r"extra_http_headers\s*=\s*\{[^\}]*['\"]DNT['\"]\s*:\s*['\"]1['\"]"):
        log.add("P3", "PW_DNT", "DNT header not set on context.", str(fp))
    if not has(txt, r"set_extra_http_headers\(\s*\{[^\}]*['\"]Referer['\"]"):
        log.add("P3", "PW_REF", "Referer header not set via context.set_extra_http_headers(...).", str(fp),
                "Set Referer to a realistic source (e.g., DuckDuckGo search).")


def _csv_expect(row: Dict[str, str], key: str, expected: Optional[str]) -> bool:
    val = (row.get(key) or "").strip()
    if expected is None:
        return val != ""
    return val.lower() == expected.lower()


def check_retailers_csv(root: Path, log: Logger):
    fp = root / "retailers.csv"
    headers, rows = csv_rows(fp)
    if not headers:
        log.add("P1", "CSV_MISSING", "retailers.csv not found or empty.", str(fp))
        return

    def find(code: str) -> Optional[Dict[str, str]]:
        for r in rows:
            if (r.get("code") or "").strip().lower() == code:
                return r
        return None

    # AH
    ah = find("ah_nl")
    if not ah:
        log.add("P2", "CSV_AH", "Missing row for ah_nl.", str(fp))
    else:
        if not (ah.get("category_url") or "").startswith("https://www.ah.nl/producten/"):
            log.add("P2", "CSV_AH_URL", "ah_nl.category_url should start with https://www.ah.nl/producten/…", str(fp))
        if not ("/olijfolie" in (ah.get("category_url") or "")):
            log.add("P3", "CSV_AH_OLIJF", "ah_nl.category_url should contain /olijfolie.", str(fp))
        if not _csv_expect(ah, "scroll_strategy", "auto"):
            log.add("P3", "CSV_AH_SCROLL", "ah_nl.scroll_strategy should be 'auto'.", str(fp))
    # Jumbo
    jm = find("jumbo_nl")
    if not jm:
        log.add("P2", "CSV_JM", "Missing row for jumbo_nl.", str(fp))
    else:
        if not _csv_expect(jm, "prefer_wayback", "true"):
            log.add("P2", "CSV_JM_WB", "jumbo_nl.prefer_wayback should be true.", str(fp))
        if "meer" not in (jm.get("load_more_selector") or "").lower():
            log.add("P3", "CSV_JM_LOADMORE", "jumbo_nl.load_more_selector should include text button for 'meer'.", str(fp))
    # Carrefour BE
    cf = find("carrefour_be")
    if not cf:
        log.add("P2", "CSV_CF", "Missing row for carrefour_be.", str(fp))
    else:
        if not ("/nl/" in (cf.get("category_url") or "") or "/fr/" in (cf.get("category_url") or "")):
            log.add("P3", "CSV_CF_LANG", "carrefour_be.category_url should include /nl/ or /fr/ path.", str(fp))
    # Colruyt BE
    cr = find("colruyt_be")
    if not cr:
        log.add("P2", "CSV_CR", "Missing row for colruyt_be.", str(fp))
    else:
        if not (cr.get("preferred_store_name") or "").strip():
            log.add("P2", "CSV_CR_STORE", "colruyt_be.preferred_store_name should be set (e.g., 'Halle').", str(fp))
        if not (cr.get("store_open_selector") or "").strip():
            log.add("P2", "CSV_CR_OPENSEL", "colruyt_be.store_open_selector missing.", str(fp))
        if not (cr.get("store_confirm_selector") or "").strip():
            log.add("P2", "CSV_CR_CONFSEL", "colruyt_be.store_confirm_selector missing.", str(fp))
    # Vomar
    vm = find("vomar_nl")
    if not vm:
        log.add("P3", "CSV_VM", "Missing row for vomar_nl (optional).", str(fp))


def check_ah_pagination(root: Path, log: Logger):
    fp = root / "tools/phase1/phase1_oilbot.py"
    txt = read_text_safe(fp)
    if not has(txt, r"def\s+_ah_paginate_allowed\("):
        log.add("P2", "AH_HELPER", "_ah_paginate_allowed(...) helper missing.", str(fp),
                "Insert AH pagination helper using ?page=N&withOffset=true only.")
    if not has(txt, r"_ah_paginate_allowed\(\s*page\s*,\s*target_url"):
        log.add("P2", "AH_CALL", "AH pagination helper not called for ah_nl.", str(fp),
                "Wrap category visit for ah_nl in _ah_paginate_allowed(...).")


def check_colruyt_store_marker(root: Path, log: Logger):
    fp = root / "tools/phase1/phase1_oilbot.py"
    txt = read_text_safe(fp)
    if not has(txt, r"def\s+_profile_store_stamp_path\("):
        log.add("P2", "CR_STAMP_FN", "_profile_store_stamp_path(...) missing.", str(fp),
                "Store-selected marker should live under _pw_profile/<code>/.store_selected")
    if not has(txt, r"marker\s*=\s*_profile_store_stamp_path\(ret\.code\)"):
        log.add("P2", "CR_STAMP_USE", "Colruyt store marker not used when selecting a store.", str(fp))
    if not has(txt, r"_ensure_store_selected\("):
        log.add("P3", "CR_ENSURE_FN", "_ensure_store_selected(...) helper not found.", str(fp))


def check_selector_wizard(root: Path, log: Logger):
    fp = root / "tools/phase1/selector_wizard.py"
    if not exists(fp):
        log.add("P3", "WIZ_MISSING", "selector_wizard.py not found (optional guardrails).", str(fp),
                "Add wizard with REQUIRED_WINS=2 and gate thresholds.")
        return
    txt = read_text_safe(fp)
    if not has(txt, r"REQUIRED_WINS\s*=\s*2"):
        log.add("P3", "WIZ_WINS", "Selector wizard REQUIRED_WINS != 2.", str(fp))
    if not has(txt, r"def\s+maybe_promote\("):
        log.add("P3", "WIZ_PROMOTE", "maybe_promote(...) not found.", str(fp))
    if not has(txt, r"cards\s*>=\s*5") or not has(txt, r"price.*>=\s*0?\.70") or not has(txt, r"qty.*>=\s*0?\.50") or not has(txt, r"dup.*<=\s*0?\.20"):
        log.add("P3", "WIZ_GATES", "Gates not detected (cards>=5, price>=70%, qty>=50%, dup<=20%).", str(fp))


def check_exporter_quarantine_and_backfill(root: Path, log: Logger):
    fp = root / "src/eopt/exporters_normalized.py"
    if not exists(fp):
        log.add("P1", "EXP_MISSING", "exporters_normalized.py not found.", str(fp))
        return
    txt = read_text_safe(fp)
    # Suspect quarantine indicator: writing a "Suspect" sheet
    if not ("sheet_name=\"Suspect\"" in txt or "sheet_name='Suspect'" in txt):
        log.add("P1", "EXP_SUSPECT", "Suspect quarantine sheet not detected in Excel writer.", str(fp),
                "Split out-of-bounds EUR/L rows to 'Suspect' before saving weekly/master.")
    # Backfill block indicator
    if not ("Backfill unit price (EUR/L)" in txt or "unit_price_eur_per_L(" in txt):
        log.add("P2", "EXP_BACKFILL", "Unit-price backfill not detected.", str(fp),
                "Compute unit_price_eur_per_unit + unit_basis='L' when qty present but unit price missing.")


def check_seed_savepagenow(root: Path, log: Logger):
    fp = root / "tools/archives/seed_savepagenow.py"
    if not exists(fp):
        log.add("P3", "SPN_MISSING", "Wayback seeding helper not found (optional).", str(fp),
                "Add tools/archives/seed_savepagenow.py to pre-seed archive snapshots.")


# ---------- main orchestration

def run_checks(root: Path, out_prefix: str) -> Tuple[Path, Path]:
    log = Logger()
    # 1) gateway & unlock & flip
    check_net_gateway(root, log)
    check_unlock_helper(root, log)
    check_flip_controller(root, log)
    # 2) playwright headers
    check_utils_playwright_headers(root, log)
    # 3) retailers.csv deltas
    check_retailers_csv(root, log)
    # 4) AH & Colruyt specifics
    check_ah_pagination(root, log)
    check_colruyt_store_marker(root, log)
    # 5) selector wizard
    check_selector_wizard(root, log)
    # 6) exporter quarantine + backfill
    check_exporter_quarantine_and_backfill(root, log)
    # 7) optional helper
    check_seed_savepagenow(root, log)

    txt_path, json_path = log.dump(out_prefix)
    c = log.counts()
    print(f"[SCAN] Done. P1={c['P1']} P2={c['P2']} P3={c['P3']} | Reports → {txt_path}  &  {json_path}")
    return txt_path, json_path


def main():
    ap = argparse.ArgumentParser(description="Scan repo for legal-playbook updates and flag gaps.")
    ap.add_argument("--root", default=".", help="Repo root (default: current directory).")
    ap.add_argument("--out-prefix", default="playbook", help="Report filename prefix (default: playbook).")
    args = ap.parse_args()
    root = Path(args.root).resolve()
    run_checks(root, args.out_prefix)


if __name__ == "__main__":
    main()
