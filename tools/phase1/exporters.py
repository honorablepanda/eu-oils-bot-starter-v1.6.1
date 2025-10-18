from __future__ import annotations
import csv, json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any

@dataclass
class Row:
    retailer: str
    product_name: str
    quantity: str
    price_eur: float
    # provenance (not included in final export)
    source_url: str = ""
    snapshot_url: str = ""
    selector_used: str = ""
    mode: str = "live"
    stale: bool = False

def write_rows_csv(rows: List[Row], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    f = out_dir / "rows.csv"
    with f.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["retailer","product_name","quantity","price_eur","source_url","snapshot_url","selector_used","mode","stale"]) 
        for r in rows:
            w.writerow([r.retailer, r.product_name, r.quantity, f"{r.price_eur:.2f}", r.source_url, r.snapshot_url, r.selector_used, r.mode, str(r.stale).lower()])

def write_run_health(data: Dict[str, Any], run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_health.json").write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")    

def merge_final_export(all_rows: List[Row], export_path: Path) -> None:
    export_path.parent.mkdir(parents=True, exist_ok=True)
    with export_path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["retailer","product_name","quantity","price_eur"]) 
        for r in all_rows:
            w.writerow([r.retailer, r.product_name, r.quantity, f"{r.price_eur:.2f}"])
