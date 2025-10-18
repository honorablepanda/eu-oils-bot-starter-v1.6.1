from __future__ import annotations

import argparse
import csv
import importlib
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# ---------- config ----------
EXPECTED = {
    "phase1_files": [
        "tools/phase1/phase1_oilbot.py",
        "tools/phase1/utils_playwright.py",
        "configs/oil_terms.yaml",
        "retailers.csv",
    ],
    "phase2_files": [
        "src/eopt/ids.py",
        "src/eopt/db_migrations/002_websites.sql",
        "tools/db/migrate_002_websites.py",
        "tools/phase2/seed_registry.py",
        "tools/discovery/discover.py",
        "tools/phase2/phase2_audit.py",
        "discovery/config.yaml",
        "requirements-phase2.txt",
    ],
    "dirs": [
        "logs",
        "exports",
        "retailers",
        "manifests",
        "discovery",
        "reports",
        "src",
    ],
}

WEBSITE_ID_RX = re.compile(r"^[a-z]{2}:[a-z0-9.-]+\.[a-z.]+$")

# ---------- helpers ----------
@dataclass
class Issue:
    severity: str  # P1 | P2 | P3
    code: str
    message: str
    path: Optional[str] = None
    extra: Optional[Dict[str, Any]] = None

def add(issues: List[Issue], sev: str, code: str, msg: str, path: Optional[Path] = None, **extra):
    issues.append(Issue(severity=sev, code=code, message=msg, path=str(path) if path else None, extra=extra or None))

def read_text_safe(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        try:
            return p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return None

def try_import(modname: str) -> Tuple[bool, Optional[str]]:
    try:
        importlib.import_module(modname)
        return True, None
    except Exception as e:
        return False, repr(e)

def try_yaml_load(p: Path) -> Tuple[Optional[Any], Optional[str]]:
    try:
        import yaml  # type: ignore
    except Exception as e:
        return None, f"PyYAML missing: {e}"
    try:
        return yaml.safe_load(read_text_safe(p) or "") or ({} if p.suffix == ".yaml" else None), None
    except Exception as e:
        return None, f"YAML parse error: {e}"

def glob_many(root: Path, pattern: str) -> List[Path]:
    return sorted(root.glob(pattern))

# ---------- checks ----------
def check_presence(root: Path, issues: List[Issue]) -> None:
    for rel in EXPECTED["dirs"]:
        p = root / rel
        if not p.exists():
            add(issues, "P2", "DIR_MISSING", f"Directory missing: {rel}", p)
    for rel in EXPECTED["phase1_files"] + EXPECTED["phase2_files"]:
        p = root / rel
        if not p.exists():
            sev = "P1" if "src/eopt/ids.py" in rel or "seed_registry.py" in rel else "P2"
            add(issues, sev, "FILE_MISSING", f"File missing: {rel}", p)

def check_pythonpath(root: Path, issues: List[Issue]) -> None:
    # Ensure src is importable in this session
    if str(root / "src") not in sys.path:
        add(issues, "P3", "PYTHONPATH_SRC", "src not on sys.path for this process; set $env:PYTHONPATH=(Resolve-Path .\\src).Path")

def check_imports_and_ids(root: Path, issues: List[Issue]) -> None:
    ok, err = try_import("yaml")
    if not ok:
        add(issues, "P2", "DEP_MISSING", "PyYAML not installed (needed to parse registry/manifests). Install PyYAML.", extra={"import_error": err})
    # Make src importable for this process
    if str(root / "src") not in sys.path:
        sys.path.append(str(root / "src"))
    try:
        ids = importlib.import_module("eopt.ids")
    except Exception as e:
        add(issues, "P1", "IMPORT_FAIL", "Cannot import eopt.ids; Phase 2 code not wired.", extra={"error": repr(e)})
        return
    # Test make_website_id
    try:
        wid = ids.make_website_id("BE", "https://www.carrefour.be")
        if wid != "be:carrefour.be":
            add(issues, "P2", "ID_CANONICAL", f"make_website_id('BE', 'https://www.carrefour.be') -> '{wid}' (expected 'be:carrefour.be')", path=root/"src/eopt/ids.py")
    except Exception as e:
        add(issues, "P1", "ID_RUNTIME", "make_website_id raised an exception", path=root/"src/eopt/ids.py", error=repr(e))

def check_sql_migration(root: Path, issues: List[Issue]) -> None:
    ddl = root / "src/eopt/db_migrations/002_websites.sql"
    if not ddl.exists(): 
        return
    sql = read_text_safe(ddl) or ""
    if "CREATE TABLE" not in sql or "websites" not in sql:
        add(issues, "P2", "DDL_WEBSITES", "websites DDL file found but does not create a 'websites' table", ddl)

def check_db(root: Path, issues: List[Issue], db_path: Optional[str]) -> None:
    if not db_path:
        # default path
        dbp = root / "data/eopt.sqlite"
    else:
        dbp = Path(db_path)
    if not dbp.exists():
        add(issues, "P3", "DB_ABSENT", f"Database not found at {dbp}. That's fine if you haven't created it yet.", dbp)
        return
    try:
        conn = sqlite3.connect(str(dbp))
        cur = conn.cursor()
        tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "websites" not in tables:
            add(issues, "P2", "DB_WEBSITES_MISSING", "Database exists but 'websites' table missing. Re-run migrate_002_websites.py.", dbp)
        # Optional: columns check for rows/snapshots
        for tname in ("rows", "snapshots"):
            if tname in tables:
                cols = [r[1] for r in cur.execute(f"PRAGMA table_info({tname})").fetchall()]
                if "website_id" not in cols:
                    add(issues, "P2", "DB_COL_MISSING", f"Table {tname} missing website_id column", dbp, table=tname)
        conn.close()
    except Exception as e:
        add(issues, "P1", "DB_OPEN_FAIL", f"Failed to open SQLite DB: {e}", dbp)

def check_registry_and_manifests(root: Path, issues: List[Issue]) -> None:
    reg = root / "retailers/registry.yaml"
    if not reg.exists():
        add(issues, "P2", "REGISTRY_MISSING", "retailers/registry.yaml not found (run seed_registry.py)")
        return
    data, err = try_yaml_load(reg)
    if err:
        add(issues, "P1", "REGISTRY_PARSE", err, reg)
        return
    if not isinstance(data, list):
        add(issues, "P1", "REGISTRY_SHAPE", "registry.yaml should be a YAML list of entries", reg)
        return
    # Validate entries
    seen_ids = set()
    seen_pair = set()
    for i, row in enumerate(data, 1):
        wid = str(row.get("website_id", ""))
        country = str(row.get("country", ""))
        dom = str(row.get("site_domain", "")).lower()
        klass = row.get("retailer_class", "")
        priority = row.get("priority", "")
        robots = row.get("robots_status", "")
        if not WEBSITE_ID_RX.match(wid):
            add(issues, "P1", "REGISTRY_ID_SHAPE", f"Invalid website_id '{wid}' at entry {i}", reg, entry=i)
        if (country, dom) in seen_pair:
            add(issues, "P1", "REGISTRY_DUP_DOMAIN", f"Duplicate (country,site_domain) {country},{dom}", reg)
        seen_pair.add((country, dom))
        if wid in seen_ids:
            add(issues, "P1", "REGISTRY_DUP_ID", f"Duplicate website_id {wid}", reg)
        seen_ids.add(wid)
        if klass not in {"grocery", "beauty", "pharmacy", "marketplace"}:
            add(issues, "P2", "REGISTRY_CLASS", f"Invalid retailer_class '{klass}' for {wid}", reg)
        if priority not in {"must_cover", "long_tail"}:
            add(issues, "P2", "REGISTRY_PRIORITY", f"Invalid priority '{priority}' for {wid}", reg)
        if robots not in {"allowed", "blocked", "review", ""}:
            add(issues, "P2", "REGISTRY_ROBOTS", f"robots_status should be allowed|blocked|review (got '{robots}')", reg)

    # Manifests
    mdir = root / "manifests"
    files = sorted(mdir.glob("*.yaml"))
    if len(files) < 27:
        add(issues, "P2", "MANIFEST_COUNT", f"Found {len(files)} manifests (<27).")
    for mf in files:
        doc, err2 = try_yaml_load(mf)
        if err2:
            add(issues, "P1", "MANIFEST_PARSE", f"{err2}", mf)
            continue
        if not isinstance(doc, dict):
            add(issues, "P1", "MANIFEST_SHAPE", "Manifest must be a YAML mapping (dict)", mf)
            continue
        if not doc.get("country"):
            add(issues, "P1", "MANIFEST_COUNTRY", "Missing 'country'", mf)
        if not doc.get("must_cover"):
            add(issues, "P1", "MANIFEST_MUST", "Missing or empty 'must_cover' list", mf)
        if "gates" not in doc:
            add(issues, "P2", "MANIFEST_GATES", "Missing 'gates' section", mf)

def check_discovery_outputs(root: Path, issues: List[Issue]) -> None:
    cands = sorted((root / "discovery").glob("candidates_*.csv"))
    if not cands:
        add(issues, "P3", "DISCOVERY_EMPTY", "No discovery candidates CSVs found (run tools/discovery/discover.py).")
    dup_count = 0
    for fp in cands:
        try:
            with fp.open("r", encoding="utf-8") as fh:
                rd = csv.DictReader(fh)
                cols = set(rd.fieldnames or [])
                must = {"country","site_domain","website_id","relevance_score","robots_status"}
                missing = must - cols
                if missing:
                    add(issues, "P2", "DISCOVERY_COLS", f"{fp.name} missing columns: {sorted(missing)}", fp)
                seen = set()
                hi = 0
                for row in rd:
                    sd = (row.get("site_domain","") or "").lower()
                    if sd in seen:
                        dup_count += 1
                    seen.add(sd)
                    try:
                        if float(row.get("relevance_score", 0)) >= 0.7:
                            hi += 1
                    except Exception:
                        pass
                # Informative thresholds only for BE/NL demo
                if fp.name.endswith("_BE.csv") or fp.name.endswith("_NL.csv"):
                    if hi < 5:
                        add(issues, "P2", "DISCOVERY_THRESHOLD", f"{fp.name}: high-confidence (<0.7) count {hi} (<5)", fp)
        except Exception as e:
            add(issues, "P1", "DISCOVERY_READ", f"Failed to read {fp.name}: {e}", fp)
    if dup_count > 0:
        add(issues, "P2", "DISCOVERY_DUP", f"Duplicates detected across discovery files: {dup_count}")

    # Proposed diffs exist?
    pr = root / "discovery/proposed_registry_diff.yaml"
    if not pr.exists():
        add(issues, "P3", "DISCOVERY_DIFF_REG", "proposed_registry_diff.yaml not found (expected after discovery).")
    pm = root / "discovery/proposed_manifests_diff"
    if not pm.exists():
        add(issues, "P3", "DISCOVERY_DIFF_MAN", "proposed_manifests_diff/ not found.")

def check_exports(root: Path, issues: List[Issue]) -> None:
    exp_dir = root / "exports"
    if not exp_dir.exists():
        add(issues, "P3", "EXPORTS_DIR", "exports/ directory not present (no runs yet).")
        return
    csvs = sorted(exp_dir.glob("*.csv"))
    if not csvs:
        add(issues, "P3", "EXPORTS_EMPTY", "No final export CSVs found in exports/.")
        return
    for fp in csvs:
        try:
            with fp.open("r", encoding="utf-8") as fh:
                rd = csv.reader(fh)
                header = next(rd, None)
                if header is None:
                    add(issues, "P1", "EXPORT_EMPTY", f"{fp.name} is empty", fp)
                    continue
                expected = ["retailer","product_name","quantity","price_eur"]
                if [h.strip() for h in header] != expected:
                    add(issues, "P2", "EXPORT_HEADER", f"{fp.name} header != {expected} (got {header})", fp)
                # Light sanity sample
                rows_checked = 0
                for row in rd:
                    if len(row) != 4:
                        add(issues, "P2", "EXPORT_COLCOUNT", f"{fp.name} has a row with {len(row)} columns (expected 4)", fp)
                        break
                    try:
                        float(row[3])
                    except Exception:
                        add(issues, "P2", "EXPORT_PRICE", f"{fp.name} contains non-numeric price_eur value: '{row[3]}'", fp)
                        break
                    rows_checked += 1
                    if rows_checked >= 25:
                        break
        except Exception as e:
            add(issues, "P1", "EXPORT_READ", f"Failed to read export {fp.name}: {e}", fp)

def check_phase1_logs(root: Path, issues: List[Issue]) -> None:
    log_root = root / "logs"
    runs = sorted(log_root.glob("run_*"))
    if not runs:
        add(issues, "P3", "LOGS_EMPTY", "No run_* directories found under logs/ (haven't run phase1 yet?).")
        return
    # Spot-check each retailer dir for listing artifact presence
    for r in runs:
        for store_dir in sorted(r.glob("*")):
            if not store_dir.is_dir():
                continue
            h = store_dir / "listing.html"
            p = store_dir / "listing.png"
            rcsv = store_dir / "rows.csv"
            if not h.exists():
                add(issues, "P3", "LISTING_HTML_MISSING", f"Missing listing.html in {store_dir.name}", h)
            if not p.exists():
                add(issues, "P3", "LISTING_PNG_MISSING", f"Missing listing.png in {store_dir.name}", p)
            if not rcsv.exists():
                add(issues, "P2", "ROWS_CSV_MISSING", f"Missing rows.csv in {store_dir.name}", rcsv)

def check_discovery_config(root: Path, issues: List[Issue]) -> None:
    cfgp = root / "discovery/config.yaml"
    if not cfgp.exists():
        add(issues, "P2", "DISCOVERY_CFG_MISSING", "discovery/config.yaml missing (used by discover.py).")
        return
    cfg, err = try_yaml_load(cfgp)
    if err:
        add(issues, "P1", "DISCOVERY_CFG_PARSE", err, cfgp)
        return
    if not isinstance(cfg, dict):
        add(issues, "P2", "DISCOVERY_CFG_SHAPE", "config.yaml should map ISO2 → settings", cfgp)
        return
    for iso2, block in cfg.items():
        if not isinstance(block, dict):
            add(issues, "P2", "DISCOVERY_CFG_BLOCK", f"{iso2}: config block must be a mapping", cfgp)
            continue
        if not block.get("query_terms"):
            add(issues, "P3", "DISCOVERY_CFG_TERMS", f"{iso2}: query_terms missing or empty", cfgp)

# ---------- report ----------
def write_reports(root: Path, issues: List[Issue], out_prefix: Optional[str]) -> Tuple[Path, Path]:
    ts = time.strftime("%Y%m%d-%H%M%S")
    out_dir = root / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    base = f"{out_prefix}_" if out_prefix else ""
    json_path = out_dir / f"{base}scan_report_{ts}.json"
    txt_path = out_dir / f"{base}scan_report_{ts}.txt"

    # JSON
    payload = {
        "summary": summarize(issues),
        "issues": [asdict(i) for i in issues],
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # TXT
    lines = []
    s = summarize(issues)
    lines.append("# Repo Scan Report\n")
    lines.append(f"Total: {s['total']}  |  P1: {s['P1']}  P2: {s['P2']}  P3: {s['P3']}\n")
    lines.append("----\n")
    for sev in ("P1", "P2", "P3"):
        block = [i for i in issues if i.severity == sev]
        if not block: 
            continue
        lines.append(f"## {sev} issues\n")
        for i in block:
            path = f" [{i.path}]" if i.path else ""
            extra = f" | extra={i.extra}" if i.extra else ""
            lines.append(f"- ({i.code}) {i.message}{path}{extra}")
        lines.append("")
    txt_path.write_text("\n".join(lines), encoding="utf-8")

    return json_path, txt_path

def summarize(issues: List[Issue]) -> Dict[str, int]:
    return {
        "total": len(issues),
        "P1": sum(1 for i in issues if i.severity == "P1"),
        "P2": sum(1 for i in issues if i.severity == "P2"),
        "P3": sum(1 for i in issues if i.severity == "P3"),
    }

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Scan EU Oils Bot repo for Phase 1/2 readiness.")
    ap.add_argument("--db", default=None, help="Path to SQLite DB (default: data/eopt.sqlite)")
    ap.add_argument("--out-prefix", default="phase",
                    help="Prefix for report filenames (default: 'phase')")
    args = ap.parse_args()

    root = Path(".").resolve()
    issues: List[Issue] = []

    # Checks
    check_presence(root, issues)
    check_pythonpath(root, issues)
    check_imports_and_ids(root, issues)
    check_sql_migration(root, issues)
    check_db(root, issues, args.db)
    check_registry_and_manifests(root, issues)
    check_discovery_config(root, issues)
    check_discovery_outputs(root, issues)
    check_exports(root, issues)
    check_phase1_logs(root, issues)

    j, t = write_reports(root, issues, args.out_prefix)
    s = summarize(issues)
    print(f"[SCAN] Done. P1={s['P1']} P2={s['P2']} P3={s['P3']} | Reports → {t}  &  {j}")

if __name__ == "__main__":
    main()
