# tools/phase2/phase2_audit.py
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import yaml

# -------------------------
# Registry / Manifests / Discovery
# -------------------------

ID_RX = re.compile(r"^[a-z]{2}:[a-z0-9.-]+\.[a-z.]+$")

def read_registry(p: Path) -> List[Dict]:
    return yaml.safe_load(p.read_text(encoding="utf-8")) or []

def read_manifest(p: Path) -> Dict:
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}

def audit_registry(reg: List[Dict]) -> List[str]:
    errs = []
    seen_ids, seen_pair = set(), set()
    for r in reg:
        wid = str(r.get("website_id",""))
        cc = str(r.get("country",""))
        dom = str(r.get("site_domain","")).lower()

        if not ID_RX.match(wid):
            errs.append(f"invalid website_id casing/shape: {wid}")

        if wid in seen_ids:
            errs.append(f"dup website_id: {wid}")
        seen_ids.add(wid)

        pair = (cc, dom)
        if pair in seen_pair:
            errs.append(f"dup (country,site_domain): {pair}")
        seen_pair.add(pair)

        if r.get("retailer_class") not in ("grocery","beauty","pharmacy","marketplace"):
            errs.append(f"class invalid: {wid}")
        if r.get("priority") not in ("must_cover","long_tail"):
            errs.append(f"priority invalid: {wid}")
        if r.get("robots_status") not in ("allowed","blocked","review"):
            errs.append(f"robots_status invalid: {wid}")
    return errs

def audit_manifests(mdir: Path) -> List[str]:
    errs = []
    files = sorted(mdir.glob("*.yaml"))
    if len(files) < 27:
        errs.append(f"manifests count {len(files)} < 27")
    for fp in files:
        m = read_manifest(fp)
        if not m.get("country"): errs.append(f"{fp.name}: missing country")
        if not m.get("must_cover"): errs.append(f"{fp.name}: must_cover empty")
        if "gates" not in m: errs.append(f"{fp.name}: gates missing")
        gates = m.get("gates", {})
        for k in ("min_retailers_active","max_zero_result_retailers","ean_or_sku_presence_rate","unit_price_sanity","wow_drop_threshold_pct"):
            if k not in gates: errs.append(f"{fp.name}: gate {k} missing")
    return errs

def audit_discovery(cdir: Path, threshold: float = 0.7) -> List[str]:
    errs = []
    csvs = list(cdir.glob("candidates_*.csv"))
    for fp in csvs:
        seen = set()
        robots_blank = 0
        hi = 0
        with fp.open("r", encoding="utf-8") as fh:
            rd = csv.DictReader(fh)
            rows = list(rd)
        for row in rows:
            rdoma = row.get("site_domain","").lower()
            if rdoma in seen:
                errs.append(f"{fp.name}: duplicate domain {rdoma}")
            seen.add(rdoma)
            if not row.get("robots_status"):
                robots_blank += 1
            try:
                if float(row.get("relevance_score",0)) >= threshold:
                    hi += 1
            except Exception:
                pass
        if robots_blank>0:
            errs.append(f"{fp.name}: robots_status missing on {robots_blank} rows")
        # Spec demo: BE and NL should have >=5 high-confidence suggestions
        if any(s in fp.name for s in ["_BE.csv","_NL.csv"]) and hi < 5:
            errs.append(f"{fp.name}: high-confidence < 5 (got {hi})")
    return errs

def coverage_matrix(reg: List[Dict], mdir: Path, out_csv: Path) -> None:
    from collections import defaultdict
    must_by_cc = defaultdict(int)
    for fp in mdir.glob("*.yaml"):
        m = read_manifest(fp)
        cc = m.get("country","")
        must_by_cc[cc] += len(m.get("must_cover",[]))
    cand_by_cc = defaultdict(int)
    ddir = Path("discovery")
    if ddir.exists():
        for fp in ddir.glob("candidates_*.csv"):
            cc = fp.stem.split("_")[-1]
            with fp.open("r", encoding="utf-8") as fh:
                cand_by_cc[cc] += max(0, sum(1 for _ in fh) - 1)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh)
        wr.writerow(["country","must_cover_count","candidates_count"])
        for cc in sorted(set(list(must_by_cc.keys()) + list(cand_by_cc.keys()))):
            wr.writerow([cc, must_by_cc.get(cc,0), cand_by_cc.get(cc,0)])

# -------------------------
# Canonical workbook / website_id / identifier / suspect / idempotency
# -------------------------

REQ_SHEETS = ["All_Data","Suspect","Coverage"]
CANON_COLS = [
    "country","chain","website_id","retailer_code",
    "product_name","normalized_name",
    "ean","sku",
    "net_qty_value","net_qty_unit","pack_count","pack_unit",
    "price_eur","currency",
    "unit_basis","unit_price_eur_per_unit","unit_price_display",
    "promo_price_eur","promo_text",
    "source_url","snapshot_url",
    "crawl_mode","run_id","ts_utc",
    "store_context",
]

def _hash(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

def _latest_weekly(exports_dir: Path) -> Optional[Path]:
    # Only accept true week-tagged files: oils-prices_YYYY-Www.xlsx
    cands = [p for p in exports_dir.glob("oils-prices_*.xlsx")
             if p.name != "oils-prices_MASTER.xlsx" and re.search(r"oils-prices_\d{4}-W\d{2}\.xlsx$", p.name)]
    if not cands:
        return None
    return max(cands, key=lambda p: p.stat().st_mtime)

def _load_weekly_and_master(exports_dir: Path) -> Tuple[Optional[Path], Optional[Path]]:
    weekly = _latest_weekly(exports_dir)
    master = exports_dir / "oils-prices_MASTER.xlsx"
    return weekly, (master if master.exists() else None)

def check_canonical_workbook(weekly: Path) -> List[str]:
    errs = []
    wb = pd.read_excel(weekly, sheet_name=None)
    for s in REQ_SHEETS:
        if s not in wb:
            errs.append(f"weekly missing sheet: {s}")
    if "All_Data" in wb:
        cols = list(wb["All_Data"].columns)
        if cols != CANON_COLS:
            errs.append(f"canonical columns mismatch (weekly/All_Data). Expected {CANON_COLS} Got {cols}")
    return errs

def _parse_store_ctx(v: str) -> Dict:
    try:
        if isinstance(v, str) and v.strip().startswith("{"):
            return json.loads(v)
    except Exception:
        pass
    return {}

def check_website_id_presence(weekly: Path) -> List[str]:
    errs = []
    df = pd.read_excel(weekly, sheet_name="All_Data")
    if "website_id" not in df.columns:
        return ["weekly: column 'website_id' missing"]
    missing = df["website_id"].isna().sum()
    if missing > 0:
        errs.append(f"weekly: website_id missing on {missing} rows")

    # Ensure website_id also appears inside store_context JSON and matches column
    bad_ctx = 0
    for wid, ctx in zip(df["website_id"], df["store_context"]):
        if pd.isna(wid):
            continue
        ctxj = _parse_store_ctx(ctx)
        if not ctxj or ctxj.get("website_id") != wid:
            bad_ctx += 1
    if bad_ctx > 0:
        errs.append(f"weekly: store_context.website_id mismatch/missing on {bad_ctx} rows")

    return errs

def check_coverage_gates(weekly: Path, min_active_per_country: int = 2) -> List[str]:
    errs = []
    df = pd.read_excel(weekly, sheet_name="All_Data")
    if df.empty:
        return ["weekly: All_Data is empty"]
    cov = df.groupby(["country","chain"]).size().reset_index(name="rows")
    act = cov.groupby("country")["chain"].nunique()
    bad = act[act < min_active_per_country]
    if not bad.empty:
        for cc, n in bad.items():
            errs.append(f"coverage gate: {cc} active retailers {n} < {min_active_per_country}")
    return errs

def check_identifier_rate(weekly: Path, threshold: float = 0.60) -> Tuple[List[str], float]:
    errs = []
    df = pd.read_excel(weekly, sheet_name="All_Data")
    if "ean" not in df.columns or "sku" not in df.columns:
        return (["weekly: 'ean'/'sku' column missing"], 0.0)
    rate = float(((df["ean"].notna()) | (df["sku"].notna())).mean()) if len(df) else 0.0
    if rate < threshold:
        errs.append(f"identifier rate {rate:.3f} < {threshold:.2f}")
    return errs, rate

def load_suspect_count(weekly: Path) -> int:
    try:
        sus = pd.read_excel(weekly, sheet_name="Suspect")
        return len(sus)
    except Exception:
        return 0

# -------------------------
# Orchestrator
# -------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", required=True, help="retailers/registry.yaml")
    ap.add_argument("--manifests", required=True, help="manifests/ directory")
    ap.add_argument("--candidates-dir", required=True, help="discovery/ directory")
    ap.add_argument("--out-matrix", default="reports/coverage_matrix.csv")
    # Control Spec checks
    ap.add_argument("--check-website-id", action="store_true")
    ap.add_argument("--check-canonical", action="store_true")
    ap.add_argument("--check-gates", action="store_true")
    ap.add_argument("--exports-dir", default="exports")
    args = ap.parse_args()

    errs: List[str] = []

    # Original audits
    reg = read_registry(Path(args.registry))
    errs += audit_registry(reg)
    errs += audit_manifests(Path(args.manifests))
    errs += audit_discovery(Path(args.candidates_dir), threshold=0.7)

    coverage_matrix(reg, Path(args.manifests), Path(args.out_matrix))

    # Canonical weekly/master audits
    exports_dir = Path(args.exports_dir)
    weekly, master = _load_weekly_and_master(exports_dir)
    if weekly is None or master is None:
        errs.append("weekly/master Excel missing in exports/")
    else:
        if args.check_canonical:
            errs += check_canonical_workbook(weekly)
        if args.check_website_id:
            errs += check_website_id_presence(weekly)
        if args.check_gates:
            errs += check_coverage_gates(weekly, min_active_per_country=2)

        # identifier + suspects (always useful to print)
        id_errs, id_rate = check_identifier_rate(weekly, threshold=0.60)
        errs += id_errs
        suspect_cnt = load_suspect_count(weekly)

    if errs:
        print("[AUDIT] FAIL")
        for e in errs:
            print(" -", e)
        if weekly and master:
            print(f"(weekly={weekly.name} sha256={_hash(weekly)})")
            print(f"(master={master.name} sha256={_hash(master)})")
    else:
        print("[AUDIT] PASS")
        if weekly and master:
            print(f"Weekly: {weekly.name} | hash={_hash(weekly)}")
            print(f"Master: {master.name} | hash={_hash(master)}")
            _, rate = check_identifier_rate(weekly, threshold=0.60)
            sus = load_suspect_count(weekly)
            print(f"Identifier rate: {rate:.3f} | Suspect rows: {sus}")
        print("Coverage matrix →", args.out_matrix)

if __name__ == "__main__":
    main()
