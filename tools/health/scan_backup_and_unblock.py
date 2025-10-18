#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scan project for:
 a) Backup Implementation (archive fallbacks)
 b) Unblocking Retailers (AH/Jumbo/Carrefour/Colruyt)

Outputs:
 - Colorized console summary
 - Markdown + JSON reports in debug/scan_report/

Stdlib only. Safe to run anywhere.
"""

from __future__ import annotations
import argparse, csv, json, os, re, sys, time
from pathlib import Path
from typing import Dict, List, Tuple, Optional

# -------------------------- helpers --------------------------

RESET = "\033[0m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
BOLD = "\033[1m"

def color(s, c): 
    return f"{c}{s}{RESET}"

def read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            return p.read_text(encoding="latin-1", errors="ignore")
        except Exception:
            return ""

def find_files(root: Path, exts: Tuple[str,...]=(".py",".json",".yaml",".yml",".csv",".md",".ps1",".sh",".ts",".tsx",".cjs",".mjs",".js")) -> List[Path]:
    files = []
    for dp, dn, fn in os.walk(root):
        # skip virtual envs/node_modules/builds for speed
        skip = any(x in dp.replace("\\","/") for x in ["/.venv","/venv","/node_modules","/.git","/.next","/dist","/build","/__pycache__"])
        if skip: 
            continue
        for f in fn:
            if f.lower().endswith(exts):
                files.append(Path(dp)/f)
    return files

def grep(files: List[Path], patterns: List[str]) -> Dict[str, List[str]]:
    rx = [re.compile(p, re.IGNORECASE) for p in patterns]
    hits = {p: [] for p in patterns}
    for f in files:
        try:
            txt = read_text(f)
        except Exception:
            continue
        for i, r in enumerate(rx):
            if r.search(txt):
                hits[patterns[i]].append(str(f))
    return hits

def load_json(p: Path) -> Optional[dict]:
    if not p.exists(): return None
    try:
        return json.loads(read_text(p))
    except Exception:
        return None

def load_csv(p: Path) -> Tuple[List[str], List[Dict[str,str]]]:
    if not p.exists(): return ([], [])
    rows = []
    with p.open("r", encoding="utf-8", errors="ignore", newline="") as fh:
        sniffer = csv.Sniffer()
        data = fh.read()
        fh.seek(0)
        dialect = None
        try:
            dialect = sniffer.sniff(data.splitlines()[0] if data else ",")
        except Exception:
            pass
        fh.seek(0)
        reader = csv.DictReader(fh, dialect=dialect) if dialect else csv.DictReader(fh)
        headers = [h.strip() for h in (reader.fieldnames or [])]
        for r in reader:
            rows.append({k.strip(): (v or "").strip() for k,v in r.items()})
        return (headers, rows)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def pct(n, d): 
    return 0 if d==0 else int(round(100.0*n/d))

# ---------------------- expectations -------------------------

RETAILERS = ["ah_nl","jumbo_nl","carrefour_be","colruyt_be"]

EXPECTED_RETAILER_COLUMNS = [
    "retailer","category_url","prefer_wayback","scroll_strategy","max_pages","preferred_store_name"
]

EXPECTED_SELECTORS = {
    "ah_nl": {
        "consent_accept_selector": "#onetrust-accept-btn-handler",
    },
    "jumbo_nl": {
        "consent_accept_selector": "#onetrust-accept-btn-handler",
    },
    "carrefour_be": {
        "consent_accept_selector_nl": "Alles accepteren",
        "consent_accept_selector_fr": "Accepter tout",
    },
    "colruyt_be": {
        "store_open_selector": "Verander winkel|Zoek je winkel",
        "store_search_selector": "Zoek",
        "store_option_selector": "Halle",
        "store_confirm_selector": "Bevestig|Bevestigen",
    },
}

# Backup implementation expectations (strings we expect somewhere in code/config)
BACKUP_CODE_PATTERNS = [
    r"fetch_with_fallback", 
    r"fetch_from_archive",
    r"archive_priority",
    r"prefer_archive|archive_first",
    r"--archive-first|--no-archive|--live-only|--archive-now|--prefer-archive",
    r"wayback|web\.archive\.org|CDX",
    r"archive\.ph|archive\.today",
    r"ghostarchive\.org",
    r"memento|timemap|timegate|memgator",
    r"arquivo\.pt",
    r"perma\.cc",
]

# Unblocking patterns (presence of logic/strings)
UNBLOCK_PATTERNS = [
    r"#onetrust-accept-btn-handler",
    r"Alles accepteren|Accepter tout",
    r"Verander winkel|Zoek je winkel|preferred_store_name|store_selected",
    r"offSet|offset|withOffset|page=",
    r"scroll|infinite|load more|IntersectionObserver",
    r"Cloudflare|cf-ray|captcha|bot|Access Denied",
    r"Referer|Referrer|headers.*Referer",
    r"1\.5s|1500ms|jitter|random",
    r"listing\.html|dump|debug",
    r"red day|bad day|archive-first mode|ARCHIVE_MODE",
]

# File candidates (common locations in your project)
CANDIDATE_FILES = {
    "retailers_csv": [
        "retailers/retailers.csv",
        "retailers/registry.csv",
        "retailers.csv",
    ],
    "selectors_json": [
        "selectors.json",
        "retailers/selectors.json",
        "tools/phase1/selectors.json",
    ],
    "phase1_runners": [
        "tools/phase1/phase1_oilbot.py",
        "tools/phase1/run_live_once.py",
        "tools/phase1/phase1_runner.py",
        "tools/phase1/phase1_bots.py",
    ],
}

# ------------------------ checks -----------------------------

def check_backup_impl(all_files: List[Path]) -> Dict:
    hits = grep(all_files, BACKUP_CODE_PATTERNS)
    totals = {k: (len(v) > 0) for k,v in hits.items()}

    score_items = sum(1 for v in totals.values() if v)
    score_total = len(totals)
    score_pct = pct(score_items, score_total)

    return {
        "type": "backup_implementation",
        "patterns": {k: hits[k] for k in BACKUP_CODE_PATTERNS},
        "present_count": score_items,
        "total": score_total,
        "score_pct": score_pct,
        "summary": {
            "has_wrapper": totals.get(r"fetch_with_fallback", False),
            "has_archive_funcs": totals.get(r"fetch_from_archive", False),
            "has_priority_config": totals.get(r"archive_priority", False),
            "has_cli_flags": totals.get(r"--archive-first|--no-archive|--live-only|--archive-now|--prefer-archive", False),
            "has_wayback": totals.get(r"wayback|web\.archive\.org|CDX", False),
            "has_archive_today": totals.get(r"archive\.ph|archive\.today", False),
            "has_ghostarchive": totals.get(r"ghostarchive\.org", False),
            "has_memento": totals.get(r"memento|timemap|timegate|memgator", False),
            "has_arquivo": totals.get(r"arquivo\.pt", False),
            "has_perma": totals.get(r"perma\.cc", False),
        }
    }

def resolve_first_existing(root: Path, candidates: List[str]) -> Optional[Path]:
    for c in candidates:
        p = (root / c).resolve()
        if p.exists():
            return p
    return None

def check_retailers_csv(root: Path) -> Dict:
    path = resolve_first_existing(root, CANDIDATE_FILES["retailers_csv"])
    headers, rows = load_csv(path) if path else ([], [])
    issues = []
    if not path:
        issues.append("retailers.csv not found (checked common locations)")
    else:
        # headers present?
        for h in EXPECTED_RETAILER_COLUMNS:
            if h not in [x.lower() for x in headers]:
                issues.append(f"Missing column in {path.name}: {h}")

        # each retailer row exists?
        for code in RETAILERS:
            found = any((r.get("retailer","").lower()==code) or (r.get("code","").lower()==code) for r in rows)
            if not found:
                issues.append(f"Missing row for retailer: {code}")

    return {
        "type": "retailers_csv",
        "path": str(path) if path else None,
        "headers": headers,
        "row_count": len(rows),
        "issues": issues,
        "ok": len(issues)==0
    }

def check_selectors_json(root: Path) -> Dict:
    path = resolve_first_existing(root, CANDIDATE_FILES["selectors_json"])
    data = load_json(path) if path else None
    issues = []
    if not path:
        issues.append("selectors.json not found (checked common locations)")
    elif data is None:
        issues.append(f"selectors.json not parseable at {path}")
    else:
        for code, reqs in EXPECTED_SELECTORS.items():
            block = data.get(code, {})
            for key, pattern in reqs.items():
                # just check presence of key or value containing expected token
                val = ""
                if isinstance(block.get(key), str):
                    val = block.get(key, "")
                # Accept either exact key match OR something containing the phrase (for contains selectors)
                if not block or (key not in block and not any(re.search(pattern, (v or ""), re.IGNORECASE) for v in block.values() if isinstance(v, str))):
                    issues.append(f"{code}: missing/weak selector for {key} (expect something like '{pattern}')")

    return {
        "type": "selectors_json",
        "path": str(path) if path else None,
        "issues": issues,
        "ok": len(issues)==0
    }

def check_unblocking_impl(all_files: List[Path]) -> Dict:
    hits = grep(all_files, UNBLOCK_PATTERNS)
    totals = {k: (len(v) > 0) for k,v in hits.items()}
    score_items = sum(1 for v in totals.values() if v)
    score_total = len(totals)
    score_pct = pct(score_items, score_total)

    # finer-grained indicators
    present = {
        "consent_logic": totals.get(r"#onetrust-accept-btn-handler", False) and totals.get(r"Alles accepteren|Accepter tout", False),
        "store_picker": totals.get(r"Verander winkel|Zoek je winkel|preferred_store_name|store_selected", False),
        "pagination": totals.get(r"offSet|offset|withOffset|page=", False),
        "scroll_sim": totals.get(r"scroll|infinite|load more|IntersectionObserver", False),
        "cf_detection": totals.get(r"Cloudflare|cf-ray|captcha|bot|Access Denied", False),
        "referrer_chain": totals.get(r"Referer|Referrer|headers.*Referer", False),
        "rate_limit": totals.get(r"1\.5s|1500ms|jitter|random", False),
        "debug_dumps": totals.get(r"listing\.html|dump|debug", False),
        "red_day_logic": totals.get(r"red day|bad day|archive-first mode|ARCHIVE_MODE", False),
    }

    return {
        "type": "unblocking_retailers",
        "patterns": {k: hits[k] for k in UNBLOCK_PATTERNS},
        "score_pct": score_pct,
        "indicators": present
    }

def scan_phase1_runners(root: Path) -> Dict:
    found = []
    for rel in CANDIDATE_FILES["phase1_runners"]:
        p = (root / rel).resolve()
        if p.exists():
            found.append(str(p))
    return {"type":"phase1_runner_files","paths":found}

# ------------------------ reporting --------------------------

def to_markdown(report: Dict) -> str:
    md = []
    md.append(f"# Project Scan Report — {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    a = report["backup"]
    md.append("## A) Backup Implementation\n")
    md.append(f"- Coverage: **{a['present_count']}/{a['total']}** items → **{a['score_pct']}%**\n")
    md.append("**Signals found:**\n")
    for k,v in a["summary"].items():
        md.append(f"- {k}: {'YES' if v else 'no'}")
    md.append("\n")

    md.append("## B) Unblocking Retailers\n")
    b = report["unblocking"]
    md.append(f"- Coverage: **{b['score_pct']}%** (pattern presence)\n")
    md.append("**Key indicators:**\n")
    for k,v in b["indicators"].items():
        md.append(f"- {k}: {'YES' if v else 'no'}")
    md.append("\n")

    md.append("## retailers.csv\n")
    rc = report["retailers_csv"]
    md.append(f"- Path: {rc.get('path') or 'not found'}")
    md.append(f"- Rows: {rc.get('row_count', 0)}")
    if rc["issues"]:
        md.append("**Issues:**")
        for i in rc["issues"]:
            md.append(f"- {i}")
    else:
        md.append("- ✅ OK")
    md.append("\n")

    md.append("## selectors.json\n")
    sj = report["selectors_json"]
    md.append(f"- Path: {sj.get('path') or 'not found'}")
    if sj["issues"]:
        md.append("**Issues:**")
        for i in sj["issues"]:
            md.append(f"- {i}")
    else:
        md.append("- ✅ OK")
    md.append("\n")

    pr = report["phase1_runners"]
    md.append("## Phase 1 runner files\n")
    if pr["paths"]:
        for p in pr["paths"]:
            md.append(f"- {p}")
    else:
        md.append("- No common runner files found (looked in tools/phase1/*)")
    md.append("\n")

    md.append("---\nGenerated by `scan_backup_and_unblock.py`.\n")
    return "\n".join(md)

def console_summary(report: Dict) -> int:
    print()
    print(color("=== SCAN RESULTS ===", BOLD))
    # A) Backup
    a = report["backup"]
    c = GREEN if a["score_pct"] >= 70 else (YELLOW if a["score_pct"] >= 40 else RED)
    print(color(f"A) Backup Implementation: {a['score_pct']}% ({a['present_count']}/{a['total']})", c))
    # B) Unblocking
    b = report["unblocking"]
    c2 = GREEN if b["score_pct"] >= 70 else (YELLOW if b["score_pct"] >= 40 else RED)
    print(color(f"B) Unblocking Retailers:  {b['score_pct']}%", c2))

    # retailers.csv
    rc = report["retailers_csv"]
    print(color(f"- retailers.csv: {'OK' if rc['ok'] else 'issues found'}", GREEN if rc["ok"] else YELLOW))
    # selectors.json
    sj = report["selectors_json"]
    print(color(f"- selectors.json: {'OK' if sj['ok'] else 'issues found'}", GREEN if sj["ok"] else YELLOW))

    # indicators quick view
    ind = b["indicators"]
    print(color("  Key Indicators:", CYAN))
    for k, v in ind.items():
        print(f"   • {k:<16} : {'YES' if v else 'no'}")

    # exit code: fail if any major area weak
    fail = 0
    if a["score_pct"] < 60: fail += 1
    if b["score_pct"] < 60: fail += 1
    if not rc["ok"]: fail += 1
    if not sj["ok"]: fail += 1
    print()
    if fail:
        print(color("Result: SOME ITEMS MISSING — see debug/scan_report/report.md", YELLOW))
        return 1
    else:
        print(color("Result: ✅ LOOKS GOOD", GREEN))
        return 0

# ------------------------ main -------------------------------

def main():
    ap = argparse.ArgumentParser(description="Scan repo for backup/archiving + retailer-unblock readiness.")
    ap.add_argument("--root", default=".", help="Project root (default: .)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    files = find_files(root)

    report = {}
    report["backup"] = check_backup_impl(files)
    report["unblocking"] = check_unblocking_impl(files)
    report["retailers_csv"] = check_retailers_csv(root)
    report["selectors_json"] = check_selectors_json(root)
    report["phase1_runners"] = scan_phase1_runners(root)

    out_dir = root / "debug" / "scan_report"
    ensure_dir(out_dir)
    (out_dir / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (out_dir / "report.md").write_text(to_markdown(report), encoding="utf-8")

    rc = console_summary(report)
    sys.exit(rc)

if __name__ == "__main__":
    main()
