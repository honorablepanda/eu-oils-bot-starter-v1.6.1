
from __future__ import annotations
from typing import List, Dict
import csv
from pathlib import Path

def write_finder_report(rows: List[Dict], out_xlsx: Path):
    # Lightweight: write a CSV next to the XLSX name if openpyxl is unavailable.
    try:
        import openpyxl  # type: ignore
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "selected"
        if rows:
            ws.append(list(rows[0].keys()))
            for r in rows:
                ws.append([r.get(k,"") for k in rows[0].keys()])
        wb.save(out_xlsx)
    except Exception:
        out_csv = out_xlsx.with_suffix(".csv")
        if rows:
            with open(out_csv, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                w.writeheader()
                w.writerows(rows)
