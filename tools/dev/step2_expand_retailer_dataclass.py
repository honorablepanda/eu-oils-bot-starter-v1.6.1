from __future__ import annotations
import re, sys, shutil, datetime as dt
from pathlib import Path

ROOT = Path(".").resolve()
PHASE1 = ROOT / "tools/phase1/phase1_oilbot.py"

# Fields to ensure on Retailer (safe, optional defaults)
WANTED = [
    ("website_id", "website_id: str | None = None"),
    ("prefer_wayback", "prefer_wayback: bool | str | None = None"),
    ("archive_providers", "archive_providers: str | None = None"),
    ("max_archive_lookback_days", "max_archive_lookback_days: int | None = None"),
    ("max_pages", "max_pages: int | None = None"),
    ("scroll_strategy", "scroll_strategy: str | None = None"),
    ("load_more_selector", "load_more_selector: str | None = None"),
    ("preferred_store_name", "preferred_store_name: str | None = None"),
    ("store_open_selector", "store_open_selector: str | None = None"),
    ("store_confirm_selector", "store_confirm_selector: str | None = None"),
    ("locale", "locale: str | None = None"),
    ("country", "country: str | None = None"),
]

def backup(fp: Path, bkdir: Path):
    bkdir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(fp, bkdir / (fp.name + ".bak"))

def main():
    if not PHASE1.exists():
        print(f"[ERR] Missing {PHASE1}"); sys.exit(1)

    txt = PHASE1.read_text(encoding="utf-8", errors="ignore")

    # Find the @dataclass Retailer block
    m = re.search(r"@dataclass\s*[\r\n]+class\s+Retailer\s*:\s*(?P<body>[\s\S]+?)(?=^[^\s#]|\Z)", txt, flags=re.MULTILINE)
    if not m:
        print("[ERR] Could not find @dataclass class Retailer")
        sys.exit(2)

    body = m.group("body")

    # Collect existing field names
    existing = set()
    for line in body.splitlines():
        ml = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", line)
        if ml:
            existing.add(ml.group(1))

    to_add_lines = []
    for name, decl in WANTED:
        if name not in existing:
            to_add_lines.append("    " + decl)

    if not to_add_lines:
        print("[OK] Retailer already has all desired fields. No change.")
        try:
            compile(txt, str(PHASE1), "exec")
            print("[OK] phase1_oilbot.py syntax valid.")
        except SyntaxError as e:
            print(f"[ERR] SyntaxError: {e}"); sys.exit(3)
        return

    # Insert the new fields before first def inside the class (or at end of body)
    insert_pos = m.end("body")
    mm = re.search(r"\n\s*def\s+\w+\(", body)
    if mm:
        insert_pos = m.start("body") + mm.start()

    new_body = body[: insert_pos - m.start("body")] + "\n" + "\n".join(to_add_lines) + "\n" + body[insert_pos - m.start("body"):]
    new_txt = txt[: m.start("body")] + new_body + txt[m.end("body"):]

    bkdir = ROOT / f"backups/expand_retailer_{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}"
    backup(PHASE1, bkdir)
    PHASE1.write_text(new_txt, encoding="utf-8")

    try:
        compile(new_txt, str(PHASE1), "exec")
    except SyntaxError as e:
        print(f"[ERR] SyntaxError after patch: {e}")
        print(f"[HINT] Backup at: {bkdir}")
        sys.exit(4)

    print(f"[OK] Added {len(to_add_lines)} field(s) to Retailer. Backup → {bkdir}")
    print("[OK] phase1_oilbot.py syntax valid.")

if __name__ == "__main__":
    main()
