from __future__ import annotations
import csv, json, sys
from pathlib import Path

ROOT = Path(".").resolve()
CSV_PATH = ROOT / "retailers.csv"
SEL_PATH = ROOT / "selectors.json"

REQUIRED_COLS = ["archive_priority", "prefer_archive"]

# sensible global defaults
GLOBAL_PRIORITY = ["wayback","ghost","memento","arquivo","ukwa","archivetoday"]
DEFAULT_PRIORITY = ",".join(GLOBAL_PRIORITY)
DEFAULT_PREFER = "false"

# optional per-retailer defaults you want baked in now
PINNED = {
    # code -> dict of column overrides
    "jumbo_nl":   {"prefer_archive":"true", "archive_priority": DEFAULT_PRIORITY},
    "carrefour_be": {"prefer_archive":"false", "archive_priority": DEFAULT_PRIORITY},
    "colruyt_be": {"prefer_archive":"false", "archive_priority": DEFAULT_PRIORITY},
    "ah_nl":     {"prefer_archive":"false", "archive_priority": DEFAULT_PRIORITY},
}

def ensure_csv_columns():
    if not CSV_PATH.exists():
        print(f"[ERR] retailers.csv not found at {CSV_PATH}")
        sys.exit(1)

    rows = []
    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    changed = False
    for col in REQUIRED_COLS:
        if col not in fieldnames:
            fieldnames.append(col)
            changed = True

    # fill defaults for new cols; apply PINNED overrides by code
    for r in rows:
        for col in REQUIRED_COLS:
            if r.get(col, "") == "":
                r[col] = DEFAULT_PREFER if col == "prefer_archive" else DEFAULT_PRIORITY
        code = (r.get("code") or "").strip()
        if code in PINNED:
            r.update(PINNED[code])

    # write back only if needed (or always to be explicit)
    with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] retailers.csv updated. added_cols={changed} rows={len(rows)}")

def ensure_selectors_archives():
    # load or create selectors.json
    if SEL_PATH.exists():
        try:
            data = json.loads(SEL_PATH.read_text(encoding="utf-8"))
        except Exception:
            print("[WARN] selectors.json invalid JSON; recreating minimal file.")
            data = {}
    else:
        data = {}

    archives = data.get("archives", {})
    # set only if missing (so we don't clobber future tweaks)
    archives.setdefault("global_priority", GLOBAL_PRIORITY)
    archives.setdefault("timeout_ms", 5000)
    archives.setdefault("bad_day_threshold", 2)
    archives.setdefault("cooldowns", {"unlock_hours": 12})
    data["archives"] = archives

    SEL_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] selectors.json archives section ensured at {SEL_PATH}")

def main():
    ensure_csv_columns()
    ensure_selectors_archives()
    print("[DONE] Step 1 archive config applied.")

if __name__ == "__main__":
    main()
