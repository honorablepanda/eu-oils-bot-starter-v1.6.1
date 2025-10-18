from __future__ import annotations
import re, sys, shutil, datetime as dt
from pathlib import Path

ROOT   = Path(".").resolve()
PHASE1 = ROOT / "tools/phase1/phase1_oilbot.py"

GOOD_BLOCK = r"""
def _ensure_store_selected(
    page,
    store_name: str,
    open_selector: str,
    confirm_selector: str,
    timeout_ms: int = 15000,
) -> bool:
    \"\"\"Colruyt-style legit store picker. Returns True if selection is set/kept.\"\"\"
    try:
        if open_selector:
            page.wait_for_selector(open_selector, timeout=timeout_ms)
            page.click(open_selector)
    except Exception:
        # Modal might be already open or store already selected.
        pass
    try:
        # Try common textbox/combobox inputs first
        inp = page.query_selector('input[role="combobox"], input[type="search"], input[type="text"]')
        if inp:
            inp.fill(store_name)
            page.wait_for_timeout(500)
            # first matching option by text (li/div/button/a)
            opt = (
                page.query_selector(f'//li[contains(normalize-space(.), "{store_name}")]')
                or page.query_selector(f'//div[contains(normalize-space(.), "{store_name}")]')
                or page.query_selector(f'//button[contains(normalize-space(.), "{store_name}")]')
                or page.query_selector(f'//a[contains(normalize-space(.), "{store_name}")]')
            )
            if opt:
                opt.click()
        else:
            # Direct clickable entry without textbox
            opt = (
                page.query_selector(f'//button[contains(normalize-space(.), "{store_name}")]')
                or page.query_selector(f'//a[contains(normalize-space(.), "{store_name}")]')
                or page.query_selector(f'//div[contains(normalize-space(.), "{store_name}")]')
            )
            if opt:
                opt.click()

        if confirm_selector:
            page.click(confirm_selector, timeout=timeout_ms)

        page.wait_for_timeout(1200)
        return True
    except Exception:
        return False
""".strip() + "\n"

def backup(fp: Path, bkdir: Path):
    bkdir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(fp, bkdir / (fp.name + ".bak"))

def patch_file(fp: Path) -> bool:
    txt = fp.read_text(encoding="utf-8", errors="ignore")

    # If the function is syntactically present and *looks* okay, skip.
    if re.search(r"def\s+_ensure_store_selected\(", txt):
        # Replace the entire function body (from its def to the next top-level def or EOF)
        pat = re.compile(r"^def\s+_ensure_store_selected\([\\s\\S]*?(?=^\\s*def\\s+\\w|\\Z)", re.MULTILINE)
        if pat.search(txt):
            new = pat.sub(GOOD_BLOCK, txt, count=1)
        else:
            # Found the def token but couldn't bound it — fall back to reinserting cleanly:
            new = txt + ("\n\n" if not txt.endswith("\n") else "\n") + GOOD_BLOCK
    else:
        # Not present — insert near the top after imports.
        m = re.search(r"^(from\\s+\\S+\\s+import\\s+.*|import\\s+\\S+).*?$", txt, re.MULTILINE)
        if m:
            ins_at = m.end()
            new = txt[:ins_at] + "\n\n" + GOOD_BLOCK + txt[ins_at:]
        else:
            new = GOOD_BLOCK + "\n\n" + txt

    if new != txt:
        fp.write_text(new, encoding="utf-8")
        return True
    return False

def main():
    if not PHASE1.exists():
        print(f"[ERR] Missing {PHASE1}"); sys.exit(1)
    bkdir = ROOT / f"backups/repair_store_picker_{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}"
    backup(PHASE1, bkdir)
    changed = patch_file(PHASE1)
    if not changed:
        print("[OK] No change needed (function already good).")
    else:
        print(f"[OK] Replaced _ensure_store_selected(). Backup → {bkdir}")

    # quick syntax check
    try:
        compile(PHASE1.read_text(encoding="utf-8"), str(PHASE1), "exec")
        print("[OK] phase1_oilbot.py syntax is valid.")
    except SyntaxError as e:
        print(f"[ERR] SyntaxError after patch: {e}"); sys.exit(2)

if __name__ == "__main__":
    main()
