from __future__ import annotations
import re, sys, shutil, datetime as dt
from pathlib import Path

ROOT = Path(".").resolve()
PHASE1 = ROOT / "tools/phase1/phase1_oilbot.py"

def backup(fp: Path, bkdir: Path):
    bkdir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(fp, bkdir / (fp.name + ".bak"))

# --- code blocks to inject (kept small, self-contained) ----------------------

BLOCK_ARCHIVE_IMPORT = """
# --- archive helpers (injected) ---
try:
    from eopt.archives import fetch_listing as _archive_fetch_listing
except Exception:
    _archive_fetch_listing = None
"""

BLOCK_ARCHIVE_ORDER_FN = r'''
def _archive_provider_order(ret) -> list[str]:
    """
    Decide the provider order for this retailer.
    Sources (in priority): per-retailer CSV `archive_providers` (comma list),
    then global safe default.
    """
    default = ["wayback", "archive_today", "memento", "ghost", "nla"]
    raw = (getattr(ret, "archive_providers", None) or "").strip()
    if raw:
        order = [p.strip() for p in raw.split(",") if p.strip()]
        return order or default
    return default
'''

BLOCK_TRY_ARCHIVE_FN = r'''
def _try_archive_listing(ret, category_url: str, why: str = "fallback") -> list[dict]:
    """
    Archive ladder attempt. Returns normalized rows (or empty list).
    Requires eopt.archives.fetch_listing to be importable.
    """
    if _archive_fetch_listing is None:
        return []

    lookback = getattr(ret, "max_archive_lookback_days", None)
    try:
        lookback = int(lookback) if lookback is not None else None
    except Exception:
        lookback = None

    providers = _archive_provider_order(ret)
    prefer_wayback = getattr(ret, "prefer_wayback", None)
    if isinstance(prefer_wayback, str):
        prefer_wayback = prefer_wayback.strip().lower() in ("1","true","yes","y")

    if prefer_wayback and "wayback" in providers:
        providers = ["wayback"] + [p for p in providers if p != "wayback"]

    try:
        return _archive_fetch_listing(
            category_url=category_url,
            retailer_code=getattr(ret, "code", "unknown"),
            providers=providers,
            lookback_days=lookback,
            reason=why,
        ) or []
    except Exception:
        return []
'''

CALLSITE_MARKER_BEGIN = "# [EOPT_ARCHIVE_CALLSITE] BEGIN (do not remove)"
CALLSITE_MARKER_END   = "# [EOPT_ARCHIVE_CALLSITE] END"

# Fully balanced, wrapped in its own try/except so it never leaks an unterminated try:
CALLSITE_PATCH_CONTENT = r"""
    {mb}
    try:
        rows_archive = []
        should_try_archive = False

        # Prefer existing early-flip helper if present
        reason = None
        if 'should_flip_to_archive' in globals():
            try:
                reason = should_flip_to_archive(health, card_count or 0, jsonld_count or 0)
            except Exception:
                reason = None
        if reason:
            should_try_archive = True
            health["why_flip"] = reason

        if (not rows_live) and (card_count == 0):
            should_try_archive = True
            health.setdefault("why_flip", "empty_listing")

        if should_try_archive:
            rows_archive = _try_archive_listing(ret, target_url, health.get("why_flip","fallback")) or []
            if rows_archive:
                rows = rows_archive
                health["source"] = "archive"
            else:
                rows = rows_live
        else:
            rows = rows_live
    except Exception:
        # Failsafe: never break Phase-1 flow due to archive attempt
        rows = locals().get("rows_live", [])
    {me}
"""

def ensure_after_imports(txt: str, block: str) -> tuple[str, bool]:
    """Insert a block after the last import line (idempotent)."""
    if block.strip() in txt:
        return txt, False
    lines = txt.splitlines(True)
    last_imp = -1
    for i, line in enumerate(lines):
        s = line.lstrip()
        if s.startswith("import ") or (s.startswith("from ") and " import " in s):
            last_imp = i
    if last_imp >= 0:
        idx = last_imp + 1
        new = "".join(lines[:idx] + ["\n", block.strip(), "\n\n"] + lines[idx:])
        return new, True
    else:
        return block.strip() + "\n\n" + txt, True

def inject_helper(txt: str, sentinel_pat: str, helper_block: str) -> tuple[str, bool]:
    """Append helper if sentinel (function name) is missing."""
    if re.search(sentinel_pat, txt):
        return txt, False
    new = txt + ("\n\n" if not txt.endswith("\n") else "\n") + helper_block.strip() + "\n"
    return new, True

def remove_previous_callsite(txt: str) -> tuple[str, bool]:
    """Remove any previously injected callsite block by our markers (idempotent)."""
    begin = txt.find(CALLSITE_MARKER_BEGIN)
    if begin == -1:
        return txt, False
    end = txt.find(CALLSITE_MARKER_END, begin)
    if end == -1:
        # If END is missing, remove from begin to next blank line or end of file
        tail = txt.find("\n\n", begin)
        end = tail if tail != -1 else len(txt)
        return txt[:begin] + txt[end:], True
    end += len(CALLSITE_MARKER_END)
    return txt[:begin] + txt[end:], True

def patch_callsite(txt: str) -> tuple[str, bool]:
    """
    Locate 'rows_live =' assignment and insert our wrapped callsite block right after it.
    """
    # Clean up any previous injection
    txt, _ = remove_previous_callsite(txt)

    # Find 'rows_live ='
    m = re.search(r"^\s*rows_live\s*=\s*.*$", txt, flags=re.MULTILINE)
    if not m:
        return txt, False

    indent = re.match(r"^(\s*)", m.group(0)).group(1)
    block = CALLSITE_PATCH_CONTENT.format(mb=CALLSITE_MARKER_BEGIN, me=CALLSITE_MARKER_END)
    # Indent each line of the content to match surrounding block
    indented = "\n".join((indent + line if line.strip() else line) for line in block.splitlines(True))

    insert_at = m.end()
    patched = txt[:insert_at] + "\n" + indented + txt[insert_at:]
    return patched, True

def main():
    if not PHASE1.exists():
        print(f"[ERR] Missing {PHASE1}"); sys.exit(1)

    raw = PHASE1.read_text(encoding="utf-8", errors="ignore")
    bkdir = ROOT / f"backups/wire_archive_{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}"
    backup(PHASE1, bkdir)

    changed = False

    # 1) import hook (archive fetch)
    txt, ch = ensure_after_imports(raw, BLOCK_ARCHIVE_IMPORT)
    changed = changed or ch

    # 2) helper functions
    txt, ch = inject_helper(txt, r"def\s+_archive_provider_order\s*\(", BLOCK_ARCHIVE_ORDER_FN)
    changed = changed or ch
    txt, ch = inject_helper(txt, r"def\s+_try_archive_listing\s*\(", BLOCK_TRY_ARCHIVE_FN)
    changed = changed or ch

    # 3) callsite splice after rows_live (wrapped + marked)
    txt, ch = patch_callsite(txt)
    changed = changed or ch

    if not changed:
        print("[OK] No changes needed (already wired).")
        # still syntax check
        try:
            compile(txt, str(PHASE1), "exec")
            print("[OK] phase1_oilbot.py syntax valid.")
        except SyntaxError as e:
            print(f"[ERR] SyntaxError: {e}"); sys.exit(3)
        return

    # Syntax check before writing to disk (safer)
    try:
        compile(txt, str(PHASE1), "exec")
    except SyntaxError as e:
        print(f"[ERR] SyntaxError after patch (not written): {e}")
        print(f"[HINT] Backup at: {bkdir}")
        sys.exit(4)

    PHASE1.write_text(txt, encoding="utf-8")
    print(f"[OK] Archive ladder wired. Backup → {bkdir}")
    print("[OK] phase1_oilbot.py syntax valid.")

if __name__ == "__main__":
    main()
