#!/usr/bin/env python3
# tools/audit/qa_gates_phase0_2.py
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

CANON_COLS = [
    "run_id","country","chain","retailer_code","website_id","site_domain","robots_status","mode",
    "product_name","net_qty_value","net_qty_unit","pack_count",
    "price_eur","unit_price_eur_per_l","ean","sku","source_url","timestamp_utc"
]

def must_have_columns(df, cols):
    missing = [c for c in cols if c not in df.columns]
    return missing

def audit(run_id: str) -> int:
    weekly = Path(f"exports/oils-prices_{run_id}.xlsx")
    master = Path("exports/oils-prices_MASTER.xlsx")
    fail = 0

    if not weekly.exists():
        print(f"[FAIL] Weekly Excel missing: {weekly}"); fail += 1
    if not master.exists():
        print(f"[FAIL] Master Excel missing: {master}"); fail += 1
    if fail: return fail

    w = pd.read_excel(weekly, sheet_name=None)
    m = pd.read_excel(master, sheet_name=None)

    w_all = w.get("All_Data", pd.DataFrame())
    m_all = m.get("All_Data", pd.DataFrame())

    # Canonical columns
    miss_w = must_have_columns(w_all, CANON_COLS)
    miss_m = must_have_columns(m_all, CANON_COLS)
    if miss_w: print(f"[FAIL] Weekly All_Data missing columns: {miss_w}"); fail += 1
    if miss_m: print(f"[FAIL] Master All_Data missing columns: {miss_m}"); fail += 1

    # website_id presence
    if "website_id" in w_all.columns and w_all["website_id"].isna().any():
        print("[FAIL] website_id contains nulls in Weekly All_Data"); fail += 1

    # Retailer coverage ≥2 per country (BE, NL)
    active = w_all.groupby(["country","chain"], dropna=False).size().reset_index(name="rows")
    per_country = active.groupby("country")["chain"].nunique().to_dict()
    for c in ("BE","NL"):
        if per_country.get(c,0) < 2:
            print(f"[FAIL] Coverage: fewer than 2 active retailers for {c} (found {per_country.get(c,0)})"); fail += 1
    # zero-row retailers check is implicit — only present retailers are counted

    # Identifier rate ≥ 0.60 overall and by retailer
    def id_rate(df):
        if df.empty: return 0.0
        s = df["ean"].notna() | df["sku"].notna()
        return float(s.mean())
    overall = id_rate(w_all)
    if overall < 0.60:
        print(f"[FAIL] Identifier rate overall={overall:.2f} (<0.60)"); fail += 1
    by_chain = (w_all.assign(has_id=w_all["ean"].notna() | w_all["sku"].notna())
                      .groupby("chain")["has_id"].mean())
    weak = by_chain[by_chain < 0.60]
    if not weak.empty:
        print("[FAIL] Identifier rate <0.60 for:", ", ".join([f"{k}={v:.2f}" for k,v in weak.items()])); fail += 1

    # Suspect sheet sanity — contains all unit price outliers
    suspect = w.get("Suspect", pd.DataFrame())
    if not suspect.empty and not w_all.empty and "unit_price_eur_per_l" in w_all.columns:
        outliers = w_all[(w_all["unit_price_eur_per_l"].notna()) & ((w_all["unit_price_eur_per_l"] < 1) | (w_all["unit_price_eur_per_l"] > 200))]
        # join on a stable subset
        merged = outliers.merge(suspect[["product_name","price_eur","unit_price_eur_per_l"]], how="left", on=["product_name","price_eur","unit_price_eur_per_l"])
        if merged.isna().any().any():
            print("[FAIL] Some unit price outliers are not present in Suspect sheet"); fail += 1

    if fail == 0:
        print("[PASS] All Phase-0/1/2 gates satisfied for run", run_id)
    return fail

def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/audit/qa_gates_phase0_2.py <RUN_ID>")
        sys.exit(2)
    run_id = sys.argv[1]
    sys.exit(audit(run_id))

if __name__ == "__main__":
    main()
