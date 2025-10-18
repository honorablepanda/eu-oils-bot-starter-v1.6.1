from __future__ import annotations
import sys, shutil, datetime as dt
from pathlib import Path

ROOT   = Path(".").resolve()
PHASE1 = ROOT / "tools/phase1/phase1_oilbot.py"

MIN_BLOCK = (
    "def _ensure_store_selected(page, store_name: str, open_selector: str, "
    "confirm_selector: str, timeout_ms: int = 15000) -> bool:\\n"
    "    # Legit one-time store picker. Returns True if selection is set/kept.\\n"
    "    try:\\n"
    "        if open_selector:\\n"
    "            page.wait_for_selector(open_selector, timeout=timeout_ms)\\n"
    "            page.click(open_selector)\\n"
    "    except Exception:\\n"
    "        pass\\n"
    "    try:\\n"
    "        inp = page.query_selector('input[role=\"combobox\"], input[type=\"search\"], input[type=\"text\"]')\\n"
    "        if inp:\\n"
    "            inp.fill(store_name)\\n"
    "            page.wait_for_timeout(500)\\n"
    "            # First matching option by visible text (li/div/button/a)\\n"
    "            opt = (\\n"
    "                page.query_selector(\"//li[contains(normalize-space(.), '\" + store_name + \"')]\")\\n"
    "                or page.query_selector(\"//div[contains(normalize-space(.), '\" + store_name + \"')]\")\\n"
    "                or page.query_selector(\"//button[contains(normalize-space(.), '\" + store_name + \"')]\")\\n"
    "                or page.query_selector(\"//a[contains(normalize-space(.), '\" + store_name + \"')]\")\\n"
    "            )\\n"
    "            if opt:\\n"
    "                opt.click()\\n"
    "        else:\\n"
    "            opt = (\\n"
    "                page.query_selector(\"//button[contains(normalize-space(.), '\" + store_name + \"')]\")\\n"
    "                or page.query_selector(\"//a[contains(normalize-space(.), '\" + store_name + \"')]\")\\n"
    "                or page.query_selector(\"//div[contains(normalize-space(.), '\" + store_name + \"')]\")\\n"
    "            )\\n"
    "            if opt:\\n"
    "                opt.click()\\n"
    "        if confirm_selector:\\n"
    "            page.click(confirm_selector, timeout=timeout_ms)\\n"
    "        page.wait_for_timeout(1200)\\n"
    "        return True\\n"
    "    except Exception:\\n"
    "        return False\\n"
)

def backup(fp: Path, bkdir: Path):
    bkdir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(fp, bkdir / (fp.name + ".bak"))

def remove_existing_block(txt: str) -> str:
    lines = txt.splitlines(True)
    start = None
    for i, line in enumerate(lines):
        if line.lstrip().startswith("def _ensure_store_selected("):
            start = i
            break
    if start is None:
        return txt  # nothing to remove

    start_indent = len(lines[start]) - len(lines[start].lstrip(" \t"))
    end = len(lines)
    for j in range(start + 1, len(lines)):
        s = lines[j].lstrip(" \t")
        indent = len(lines[j]) - len(s)
        if s.startswith("def ") and indent <= start_indent:
            end = j
            break
    return "".join(lines[:start] + lines[end:])

def insert_after_imports(txt: str, block: str) -> str:
    lines = txt.splitlines(True)
    last_imp_idx = -1
    for idx, line in enumerate(lines):
        s = line.lstrip()
        if s.startswith("import ") or (s.startswith("from ") and " import " in s):
            last_imp_idx = idx
    if last_imp_idx >= 0:
        insert_pos = last_imp_idx + 1
        return "".join(lines[:insert_pos] + ["\n", block, "\n"] + lines[insert_pos:])
    return block + "\n\n" + txt

def main():
    if not PHASE1.exists():
        print("[ERR] Missing {}".format(PHASE1)); sys.exit(1)

    bkdir = ROOT / "backups/force_fix_store_picker_{}".format(dt.datetime.now().strftime("%Y%m%d-%H%M%S"))
    backup(PHASE1, bkdir)

    original = PHASE1.read_text(encoding="utf-8", errors="ignore")
    stripped  = remove_existing_block(original)
    patched   = insert_after_imports(stripped, MIN_BLOCK)

    if patched != original:
        PHASE1.write_text(patched, encoding="utf-8")
        print("[OK] Wrote minimal _ensure_store_selected(). Backup → {}".format(bkdir))
    else:
        print("[OK] No change made (already minimal/clean). Backup → {}".format(bkdir))

    # Syntax check
    try:
        compile(PHASE1.read_text(encoding="utf-8"), str(PHASE1), "exec")
        print("[OK] phase1_oilbot.py syntax is valid.")
    except SyntaxError as e:
        print("[ERR] SyntaxError after patch: {}".format(e)); sys.exit(2)

if __name__ == "__main__":
    main()
