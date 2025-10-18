# src/eopt/cli.py
from __future__ import annotations
import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

# --- internal deps ---
try:
    from eopt.ids import make_website_id, _root_domain
except Exception:
    # soft shim if ids.py not on PYTHONPATH yet
    def _root_domain(u: str) -> str:
        from urllib.parse import urlparse
        try:
            netloc = urlparse(u).netloc or u
            parts = netloc.split(".")
            return ".".join(parts[-2:]) if len(parts) >= 2 else netloc
        except Exception:
            return ""
    def make_website_id(iso2: str, site_domain: str) -> str:
        return f"{(iso2 or '').upper()}:{site_domain}"

try:
    from eopt.exporters_normalized import (
        write_weekly_and_master,
        parse_quantity,
        unit_price_eur_per_L,
        CANON_COLS,
    )
except Exception as e:
    raise RuntimeError(
        "Missing eopt.exporters_normalized. Make sure src is on PYTHONPATH "
        "and that eopt/exporters_normalized.py exists."
    )

def _read_retailer_codes(csv_path: Path) -> Dict[Tuple[str, str], str]:
    """
    Map (ISO2, root_domain) -> retailer code from retailers.csv
    Uses base_url or category_url to compute root_domain.
    """
    mapping: Dict[Tuple[str, str], str] = {}
    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        rd = csv.DictReader(fh)
        for row in rd:
            code = (row.get("code") or "").strip()
            iso2 = (row.get("country") or "").strip().upper()
            base = (row.get("base_url") or "").strip()
            cat = (row.get("category_url") or "").strip()
            url_for_domain = base or cat
            if not code or not iso2 or not url_for_domain:
                continue
            dom = _root_domain(url_for_domain)
            if not dom:
                continue
            mapping[(iso2, dom)] = code
    return mapping

def _discover_targets(countries: List[str], manifests_dir: Path, retailers_csv: Path) -> Tuple[List[str], List[str]]:
    """
    Return (matched_codes, missing_ids) where missing_ids are website_ids that
    could not be mapped to a code via retailers.csv.
    """
    import yaml
    codes_by_domain = _read_retailer_codes(retailers_csv)
    matched: List[str] = []
    missing: List[str] = []

    for cc in countries:
        mf = manifests_dir / f"{cc.upper()}.yaml"
        if not mf.exists():
            print(f"[WARN] Manifest missing for {cc}: {mf}", file=sys.stderr)
            continue
        doc = yaml.safe_load(mf.read_text(encoding="utf-8")) or {}
        ids = [x.get("website_id") for x in (doc.get("must_cover") or []) if x.get("website_id")]
        ids += [x.get("website_id") for x in (doc.get("long_tail") or []) if x.get("website_id")]
        for wid in ids:
            try:
                iso, dom = wid.split(":", 1)
            except ValueError:
                missing.append(wid)
                continue
            code = codes_by_domain.get((iso.upper(), dom))
            if code:
                matched.append(code)
            else:
                missing.append(wid)

    # dedupe, stable-ish order
    seen = set()
    uniq_matched: List[str] = []
    for c in matched:
        if c not in seen:
            seen.add(c)
            uniq_matched.append(c)

    return uniq_matched, missing

def run_phase1_for_retailer(code: str, run_id: str, root: Path) -> List[Dict]:
    """
    Calls Phase 1 runner and returns rich dict rows suitable for normalization.
    Prefers tools.phase1.phase1_oilbot.run_one_for_cli; falls back if missing.
    """
    import importlib
    m = importlib.import_module("tools.phase1.phase1_oilbot")
    if hasattr(m, "run_one_for_cli"):
        return m.run_one_for_cli(code, run_id, root)

    # Fallback: adapt from run_one(...) -> list of Row dataclasses (best-effort)
    res = m.run_one(code, run_id, root)  # if signature differs, prefer run_one_for_cli
    rows = res.get("rows") or []
    out: List[Dict] = []
    for r in rows:
        out.append({
            "country": None,
            "chain": None,
            "retailer_code": code,
            "product_name": getattr(r, "product_name", None),
            "quantity": getattr(r, "quantity", None),
            "price_eur": getattr(r, "price_eur", None),
            "source_url": getattr(r, "source_url", None),
            "robots_status": None,
            "site_domain": _root_domain(getattr(r, "source_url", "") or ""),
            "mode": getattr(r, "mode", "live"),
            "ean": None,
            "sku": None,
        })
    return out

def canonicalize_rows(raw_rows: List[Dict], run_id: str) -> List[Dict]:
    """
    Map Phase-1 rich dicts -> canonical All_Data rows for weekly/master.
    """
    out = []
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    for r in raw_rows:
        country = r.get("country")
        chain = r.get("chain")
        code = r.get("retailer_code")
        site_domain = r.get("site_domain") or _root_domain(r.get("source_url", ""))
        website_id = make_website_id(country, site_domain) if country and site_domain else None

        qv, qu, pc, pu = parse_quantity(r.get("quantity") or "")
        unit_basis = "L" if qu in ("ml", "l") else None
        up = unit_price_eur_per_L(r.get("price_eur"), qv, qu, pc) if unit_basis == "L" else None
        unit_disp = f"{up} EUR/L" if up is not None else None

        store_ctx = {
            "website_id": website_id,
            "retailer_code": code,
            "robots_status": r.get("robots_status"),
            "mode": r.get("mode"),
        }

        row = {
            "country": country,
            "chain": chain,
            "website_id": website_id,
            "retailer_code": code,
            "product_name": r.get("product_name"),
            "normalized_name": r.get("normalized_name") or r.get("product_name"),
            "ean": r.get("ean"),
            "sku": r.get("sku"),
            "net_qty_value": qv,
            "net_qty_unit": qu,
            "pack_count": pc,
            "pack_unit": pu,
            "price_eur": r.get("price_eur"),
            "currency": "EUR",
            "unit_basis": unit_basis,
            "unit_price_eur_per_unit": up,
            "unit_price_display": unit_disp,
            "promo_price_eur": r.get("promo_price_eur"),
            "promo_text": r.get("promo_text"),
            "source_url": r.get("source_url"),
            "snapshot_url": r.get("snapshot_url"),
            "crawl_mode": r.get("mode"),
            "run_id": run_id,
            "ts_utc": ts,
            "store_context": json.dumps(store_ctx, ensure_ascii=False),
        }
        # ensure canonical columns exist (order is enforced by exporter)
        for k in CANON_COLS:
            row.setdefault(k, None)
        out.append(row)
    return out

def main():
    ap = argparse.ArgumentParser()
    # keep compatibility with "run" positional (optional)
    ap.add_argument("run", nargs="?", help="subcommand 'run'", default="run")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--countries", nargs="+", required=True)
    ap.add_argument("--mode", choices=["real", "synthetic"], default="real")
    # new helpers
    ap.add_argument("--list-targets", action="store_true",
                    help="List retailer codes that would run (derived from manifests + retailers.csv) and exit.")
    ap.add_argument("--targets", default="",
                    help="Override target retailer codes (comma-separated) instead of discovery.")
    # optional paths
    ap.add_argument("--manifests-dir", default="manifests")
    ap.add_argument("--retailers-csv", default="retailers.csv")
    ap.add_argument("--exports-dir", default="exports")
    ap.add_argument("--sqlite-path", default="data/eopt.sqlite")
    args = ap.parse_args()

    root = Path(".").resolve()
    countries = [c.upper() for c in args.countries]
    manifests_dir = Path(args.manifests_dir)
    retailers_csv = Path(args.retailers_csv)

    discovered, missing = _discover_targets(countries, manifests_dir, retailers_csv)

    # FIX: argparse stores --list-targets as list_targets
    if args.list_targets:
        print(json.dumps({
            "countries": countries,
            "targets": discovered,
            "unmapped_website_ids": missing
        }, ensure_ascii=False, indent=2))
        return

    if args.targets.strip():
        targets = [t.strip() for t in args.targets.split(",") if t.strip()]
        print(f"[INFO] Using explicit --targets override: {targets}")
    else:
        targets = discovered
        print(f"[INFO] Discovered targets from manifests: {targets}")
        if missing:
            print(f"[WARN] Unmapped website_ids (no retailer code in retailers.csv): {missing}", file=sys.stderr)

    all_rows: List[Dict] = []
    per_code: Dict[str, int] = {}

    if args.mode == "real":
        for code in targets:
            raw = run_phase1_for_retailer(code, args.run_id, root)
            per_code[code] = len(raw)
            all_rows.extend(raw)
            print(f"[INFO] {code}: {len(raw)} rows")
    else:
        # synthetic mode: keep rows as provided by any upstream mock; otherwise, empty
        print("[INFO] Synthetic mode: writing workbooks with current collected rows (none if not provided).")

    canonical = canonicalize_rows(all_rows, args.run_id)
    week_tag = args.run_id
    metrics = write_weekly_and_master(
        canonical,
        week_tag,
        Path(args.exports_dir),
        Path(args.sqlite_path),
    )
    print("[METRICS]", metrics)

if __name__ == "__main__":
    main()
