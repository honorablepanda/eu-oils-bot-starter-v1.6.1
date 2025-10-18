#!/usr/bin/env python3
from pathlib import Path
import csv, shutil

CSV_CANDIDATES = [
    "retailers/retailers.csv",
    "retailers/registry.csv",
    "retailers.csv",
]

def read_rows(p: Path):
    with p.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        r = csv.DictReader(f)
        headers = r.fieldnames or []
        rows = [dict(x) for x in r]
    return headers, rows

def write_rows(p: Path, headers, rows):
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    root = Path(".").resolve()
    target = None
    for c in CSV_CANDIDATES:
        p = (root / c).resolve()
        if p.exists():
            target = p
            break
    if not target:
        print("No retailers.csv found in common locations.")
        return 1

    headers, rows = read_rows(target)

    # If 'retailer' already present, nothing to do.
    if any(h.lower() == "retailer" for h in headers):
        print(f"[OK] {target.name} already has 'retailer' column.")
        return 0

    # Prefer to mirror from 'code' if present.
    code_header = next((h for h in headers if h.lower()=="code"), None)
    new_headers = headers[:] + ["retailer"]

    # Fill new column
    for r in rows:
        r["retailer"] = r.get(code_header, r.get("retailer", "")) if code_header else r.get("retailer","")

    # Backup then write
    backup = target.with_suffix(target.suffix + ".bak")
    shutil.copyfile(target, backup)
    write_rows(target, new_headers, rows)

    print(f"[UPDATED] Added 'retailer' column to {target}\n[Backup] -> {backup}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
