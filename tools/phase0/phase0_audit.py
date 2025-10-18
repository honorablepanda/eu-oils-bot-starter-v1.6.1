#!/usr/bin/env python3
# tools/phase0/phase0_audit.py
from __future__ import annotations
import json, sys
from pathlib import Path
import pandas as pd
from hashlib import sha256

CANON_COLS = [
    "run_id","country","chain","retailer_code","website_id","site_domain","robots_status","mode",
    "product_name","net_qty_value","net_qty_unit","pack_count",
    "price_eur","unit_price_eur_per_l","ean","sku","source_url","timestamp_utc","store_context_json"
]
REQ_SHEETS = ["All_Data","Coverage","Pivots_Country_Chain","Changes","QA","Suspect"]

def hash_file(p: Path) -> str:
    h = sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/phase0/phase0_audit.py --run-id <RUN_ID>")
        sys.exit(2)
    run_id = sys.argv[2] if sys.argv[1] == "--run-id" else sys.argv[1]
    weekly = Path(f"exports/oils-prices_{run_id}.xlsx")
    master = Path("exports/oils-prices_MASTER.xlsx")
    manifest = Path(f"exports/_manifests/run_{run_id}.json")

    fail = 0
    if not weekly.exists(): print(f"[FAIL] Weekly missing: {weekly}"); fail += 1
    if not master.exists(): print(f"[FAIL] Master missing: {master}"); fail += 1
    if fail: sys.exit(fail)

    # sheets
    w = pd.read_excel(weekly, sheet_name=None)
    for s in REQ_SHEETS:
        if s not in w: print(f"[FAIL] Weekly missing sheet: {s}"); fail += 1

    m = pd.read_excel(master, sheet_name=None)
    for s in REQ_SHEETS:
        if s not in m: print(f"[FAIL] Master missing sheet: {s}"); fail += 1

    # canonical columns
    w_all = w.get("All_Data", pd.DataFrame())
    m_all = m.get("All_Data", pd.DataFrame())
    miss_w = [c for c in CANON_COLS if c not in w_all.columns]
    miss_m = [c for c in CANON_COLS if c not in m_all.columns]
    if miss_w: print(f"[FAIL] Weekly All_Data missing cols: {miss_w}"); fail += 1
    if miss_m: print(f"[FAIL] Master All_Data missing cols: {miss_m}"); fail += 1

    # website_id presence
    if "website_id" in w_all.columns and w_all["website_id"].isna().any():
        print("[FAIL] website_id contains nulls in Weekly"); fail += 1

    # idempotency hashes match manifest (if exists)
    if manifest.exists():
        meta = json.loads(manifest.read_text(encoding="utf-8"))
        want_w = (meta.get("artifacts",{}).get("weekly_xlsx") or {}).get("sha256")
        want_m = (meta.get("artifacts",{}).get("master_xlsx") or {}).get("sha256")
        got_w = hash_file(weekly)
        got_m = hash_file(master)
        if want_w and want_w != got_w:
            print(f"[FAIL] Weekly SHA mismatch vs manifest: {want_w} != {got_w}"); fail += 1
        if want_m and want_m != got_m:
            print(f"[FAIL] Master SHA mismatch vs manifest: {want_m} != {got_m}"); fail += 1

    if fail:
        print("[RESULT] ❌ Phase-0 audit FAILED")
        sys.exit(fail)
    print("[RESULT] ✅ Phase-0 audit PASS")

if __name__ == "__main__":
    main()
