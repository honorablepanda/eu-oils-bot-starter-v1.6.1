from __future__ import annotations
import re, sys, shutil, datetime as dt
from pathlib import Path

ROOT = Path(".").resolve()
PHASE1 = ROOT / "tools/phase1/phase1_oilbot.py"

def backup(fp: Path, bkdir: Path):
    bkdir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(fp, bkdir / (fp.name + ".bak"))

def main():
    if not PHASE1.exists():
        print(f"[ERR] Missing file: {PHASE1}")
        sys.exit(1)
    txt = PHASE1.read_text(encoding="utf-8", errors="ignore")

    # Already patched?
    if re.search(r"_ah_paginate_allowed\(\s*page\s*,\s*target_url", txt):
        print("[OK] AH call already present — no change.")
        return

    # Find the first line: page.goto(target_url, ...)
    pat = re.compile(r'^(?P<i>[ \t]*)page\.goto\(\s*target_url[^\n]*\)\s*$', re.MULTILINE)
    m = pat.search(txt)
    if not m:
        print("[ERR] Could not locate `page.goto(target_url, ...)` line to patch.")
        sys.exit(2)

    indent = m.group("i")
    original_line = m.group(0).strip()

    replacement = (
        f'{indent}if ret.code == "ah_nl":\n'
        f'{indent}    _ah_paginate_allowed(page, target_url, max_pages=int(ret.max_pages or 6))\n'
        f'{indent}else:\n'
        f'{indent}    {original_line}\n'
    )

    bkdir = ROOT / f"backups/ah_call_fix_{dt.datetime.now().strftime("%Y%m%d-%H%M%S")}"
    backup(PHASE1, bkdir)

    new_txt = txt[:m.start()] + replacement + txt[m.end():]
    PHASE1.write_text(new_txt, encoding="utf-8")
    print(f"[OK] Patched AH call. Backup → {bkdir}")

if __name__ == "__main__":
    main()
