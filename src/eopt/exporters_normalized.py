# src/eopt/exporters_normalized.py
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

# ----------------------------
# Row model used by Phase-1 (tools.phase1.exporters imports this)
# ----------------------------
@dataclass
class Row:
    retailer: str
    product_name: str
    quantity: str
    price_eur: float
    source_url: str
    selector_used: str = ""
    mode: str = "live"   # "live" or "archive"
    stale: bool = False  # true when from snapshot/archive

def _sha256_file(fp: Path) -> str:
    h = hashlib.sha256()
    with fp.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def _looks_suspect(row: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Lightweight quarantine rules.
    Flag if price invalid, empty name, or quantity looks nonsense.
    """
    name = (row.get("product_name") or "").strip()
    qty  = (row.get("quantity") or "").strip().lower()
    price = _safe_float(row.get("price_eur"))

    if not name:
        return True, "empty_name"
    if price is None or price <= 0:
        return True, "bad_price"
    # very rough qty sanity (ok to be blank, but if present it should contain a unit or a multiplier)
    if qty and not any(k in qty for k in ["ml", "l", "cl", "x", "×"]):
        # allow common grams too, some retailers mix olive “pasta” etc; we still mark suspect rather than drop
        if "g" not in qty and "kg" not in qty:
            return True, "odd_quantity"
    return False, ""

# ----------------------------
# Public API used by Phase-1 runner
# ----------------------------
def write_rows_csv(rows: List[Row], run_dir: Path) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    out = run_dir / "phase1_rows.csv"
    df = pd.DataFrame([asdict(r) for r in rows])
    df.to_csv(out, index=False, encoding="utf-8")
    return out

def write_run_health(health: Dict[str, Any], logs_root: Path) -> Path:
    logs_root.mkdir(parents=True, exist_ok=True)
    out = logs_root / "run_health.json"
    # append-or-merge latest; keep simple for now (overwrite)
    out.write_text(json.dumps(health, ensure_ascii=False, indent=2), encoding="utf-8")
    return out

def merge_final_export(rows: List[Row], export_csv: Path) -> Path:
    export_csv.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([asdict(r) for r in rows])
    df.to_csv(export_csv, index=False, encoding="utf-8")
    return export_csv

# ----------------------------
# Phase-2/3 normalized exporter entry points
# ----------------------------
NORMALIZED_COLS = [
    "country", "chain", "retailer_code",
    "product_name", "quantity", "price_eur",
    "source_url", "robots_status", "site_domain",
    "mode", "ean", "sku", "website_id",
]

def _normalize_phase1_dicts(dict_rows: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Takes the rich dicts returned by tools.phase1.phase1_oilbot.run_one_for_cli().
    Returns (df_ok, df_suspect).
    """
    if not dict_rows:
        return pd.DataFrame(columns=NORMALIZED_COLS), pd.DataFrame(columns=NORMALIZED_COLS + ["suspect_reason"])

    # Build a full DF with missing columns filled
    df = pd.DataFrame(dict_rows)
    for col in NORMALIZED_COLS:
        if col not in df.columns:
            df[col] = None

    # Ensure correct column order (extra columns preserved but placed after)
    ordered = [c for c in NORMALIZED_COLS if c in df.columns]
    rest = [c for c in df.columns if c not in ordered]
    df = df[ordered + rest]

    # Quarantine suspects
    suspects: List[Dict[str, Any]] = []
    oks: List[Dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        is_bad, why = _looks_suspect(row)
        if is_bad:
            r = dict(row)
            r["suspect_reason"] = why
            suspects.append(r)
        else:
            oks.append(row)

    df_ok = pd.DataFrame(oks, columns=ordered + rest)
    df_sus = pd.DataFrame(suspects, columns=ordered + rest + ["suspect_reason"])
    return df_ok, df_sus

def export_weekly_and_master(dict_rows: List[Dict[str, Any]], exports_dir: Path, run_id: str) -> Dict[str, Any]:
    """
    Writes two Excel files:
      - weekly:   exports/oils-prices_<run_id>.xlsx
      - master:   exports/oils-prices_MASTER.xlsx   (append-new)
    Returns metrics incl. sha256 hashes.
    """
    exports_dir.mkdir(parents=True, exist_ok=True)
    weekly = exports_dir / f"oils-prices_{run_id}.xlsx"
    master = exports_dir / "oils-prices_MASTER.xlsx"

    df_ok, df_sus = _normalize_phase1_dicts(dict_rows)

    # Write weekly workbook
    with pd.ExcelWriter(weekly, engine="openpyxl", mode="w") as xw:
        df_ok.to_excel(xw, index=False, sheet_name="ok")
        if not df_sus.empty:
            df_sus.to_excel(xw, index=False, sheet_name="suspect")

    # Merge into master (append)
    if master.exists():
        prev = pd.read_excel(master, sheet_name="ok")
        # Align columns (avoid FutureWarning and dtype issues)
        for col in prev.columns:
            if col not in df_ok.columns:
                df_ok[col] = None
        for col in df_ok.columns:
            if col not in prev.columns:
                prev[col] = None
        prev = prev[df_ok.columns]  # same order
        df_master = pd.concat([prev, df_ok], ignore_index=True)
    else:
        df_master = df_ok.copy()

    with pd.ExcelWriter(master, engine="openpyxl", mode="w") as xw:
        df_master.to_excel(xw, index=False, sheet_name="ok")
        # keep a rolling suspect sheet as well (optional)
        if df_sus.empty:
            pd.DataFrame(columns=df_sus.columns).to_excel(xw, index=False, sheet_name="suspect")
        else:
            try:
                if master.exists():
                    prev_sus = pd.read_excel(master, sheet_name="suspect")
                    # align columns
                    for col in prev_sus.columns:
                        if col not in df_sus.columns:
                            df_sus[col] = None
                    for col in df_sus.columns:
                        if col not in prev_sus.columns:
                            prev_sus[col] = None
                    prev_sus = prev_sus[df_sus.columns]
                    df_sus_all = pd.concat([prev_sus, df_sus], ignore_index=True)
                else:
                    df_sus_all = df_sus
                df_sus_all.to_excel(xw, index=False, sheet_name="suspect")
            except Exception:
                # if anything odd, just write current suspects
                df_sus.to_excel(xw, index=False, sheet_name="suspect")

    metrics = {
        "identifier_rate_overall": float(
            (df_ok["ean"].notnull().sum() + df_ok["sku"].notnull().sum()) / max(len(df_ok), 1)
        ),
        "suspect_count": int(len(df_sus)),
        "weekly_path": str(weekly),
        "master_path": str(master),
        "weekly_sha256": _sha256_file(weekly),
        "master_sha256": _sha256_file(master),
    }
    return metrics
