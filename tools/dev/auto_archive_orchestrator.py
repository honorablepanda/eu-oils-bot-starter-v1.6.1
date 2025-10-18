# tools/dev/auto_archive_orchestrator.py
from __future__ import annotations
import csv
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

# ---------- Constants ----------
ROOT = Path(".").resolve()
RETAILERS_CSV = ROOT / "retailers.csv"
REPORTS_DIR = ROOT / "reports"
PHASE1_FILE = ROOT / "tools/phase1/phase1_oilbot.py"

DEFAULT_PROVIDERS = "wayback,archive_today,ghost,memento,commoncrawl"
DEFAULT_LOOKBACK_DAYS = "60"
DEFAULT_MAX_PAGES = "6"

RUN_CMD = [
    sys.executable, "-m", "eopt.cli", "run",
    # caller adds: --run-id, --countries, --mode, --targets
]

# ---------- Regex to parse console ----------
RE_WHY = re.compile(r"\bwhy_flip=([a-z_]+)")
RE_ARCH_TRY = re.compile(r"\barchive:\s+trying\s+([a-z_]+)", re.IGNORECASE)
RE_ARCH_OK = re.compile(r"\barchive:\s+success\s+provider=([a-z_]+)", re.IGNORECASE)
RE_ARCH_FAIL = re.compile(r"\barchive:\s+(?:fallback|failing)\b", re.IGNORECASE)
RE_INFO_ROWS = re.compile(r"^\[INFO\]\s+([a-z_]+):\s+(\d+)\s+rows", re.IGNORECASE)
RE_METRICS = re.compile(r"^\[METRICS\]\s+(\{.*\})\s*$")

# ---------- Data structures ----------
@dataclass
class TargetResult:
    target: str
    info_rows: Optional[int] = None
    why_flip: Optional[str] = None
    archive_tries: List[str] = None
    archive_success: Optional[str] = None
    archive_fallbacks: int = 0
    metrics: Dict = None
    stdout_path: Optional[str] = None

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["archive_tries"] = self.archive_tries or []
        d["metrics"] = self.metrics or {}
        return d

# ---------- CSV helpers ----------
def _read_csv(fp: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not fp.exists():
        raise FileNotFoundError(f"retailers.csv not found at {fp}")
    with fp.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.DictReader(f)
        rows = list(rdr)
        return list(rdr.fieldnames or []), rows

def _write_csv(fp: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    fp.parent.mkdir(parents=True, exist_ok=True)
    with fp.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def _ensure_cols(fieldnames: List[str], rows: List[Dict[str, str]], needed: List[str]) -> Tuple[List[str], List[Dict[str, str]], bool]:
    changed = False
    for col in needed:
        if col not in fieldnames:
            fieldnames.append(col)
            changed = True
    if changed:
        for r in rows:
            for col in needed:
                r.setdefault(col, "")
    return fieldnames, rows, changed

def _set_defaults_for_targets(rows: List[Dict[str, str]], targets: List[str]) -> bool:
    changed = False
    tset = {t.lower().strip() for t in targets}
    for r in rows:
        code = (r.get("code") or "").lower().strip()
        if code in tset:
            if (r.get("prefer_wayback") or "").strip().lower() not in {"true", "false"}:
                r["prefer_wayback"] = "true"; changed = True
            if not (r.get("archive_providers") or "").strip():
                r["archive_providers"] = DEFAULT_PROVIDERS; changed = True
            if not (r.get("max_archive_lookback_days") or "").strip():
                r["max_archive_lookback_days"] = DEFAULT_LOOKBACK_DAYS; changed = True
            if not (r.get("max_pages") or "").strip():
                r["max_pages"] = DEFAULT_MAX_PAGES; changed = True
    return changed

def _toggle_prefer_wayback(rows: List[Dict[str, str]], target: str, value: str) -> Tuple[bool, Optional[str]]:
    changed = False
    prev = None
    for r in rows:
        if (r.get("code") or "").lower().strip() == target.lower().strip():
            prev = (r.get("prefer_wayback") or "").strip()
            if prev.lower() != value.lower():
                r["prefer_wayback"] = value
                changed = True
            break
    return changed, prev

# ---------- Retailer dataclass field discovery ----------
def _discover_retailer_fields_from_phase1(pyfile: Path) -> List[str]:
    """
    Parse tools/phase1/phase1_oilbot.py to extract Retailer dataclass field names,
    so we can build a sanitized temporary retailers.csv for older schemas.
    """
    try:
        txt = pyfile.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return []
    # Find "@dataclass" followed by "class Retailer:"
    m = re.search(r"@dataclass\s*[\r\n]+class\s+Retailer\s*\((?:object)?\)\s*:\s*(.*?)^(?:\S|$)", txt, flags=re.DOTALL | re.MULTILINE)
    if not m:
        # Try a simpler block capture until next "class " or EOF
        m = re.search(r"@dataclass\s*[\r\n]+class\s+Retailer\s*:\s*(.*)", txt, flags=re.DOTALL)
        if not m:
            return []
    block = m.group(1)
    # Pull names of the form "name: type" at start-of-line indentation
    names = []
    for line in block.splitlines():
        # stop if we hit a new def/class
        if re.match(r"^\s*(def|class)\b", line):
            break
        m2 = re.match(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:", line)
        if m2:
            names.append(m2.group(1))
    return names

def _sanitize_rows_for_phase1(fieldnames: List[str], rows: List[Dict[str, str]], allowed: List[str]) -> Tuple[List[str], List[Dict[str, str]], bool]:
    if not allowed:
        return fieldnames, rows, False
    allowed_set = set(allowed)
    # Keep required CSV keys that phase1 may expect (always keep 'code' at least)
    core_keep = {"code"}
    new_fields = [c for c in fieldnames if (c in allowed_set or c in core_keep)]
    if new_fields == fieldnames:
        # nothing to change
        return fieldnames, rows, False
    sanitized = []
    for r in rows:
        sanitized.append({k: r.get(k, "") for k in new_fields})
    return new_fields, sanitized, True

# ---------- Runner ----------
def _run_cli(run_id: str, countries: List[str], mode: str, targets: List[str]) -> str:
    cmd = RUN_CMD + [
        "--run-id", run_id,
        "--countries", *countries,
        "--mode", mode,
        "--targets", ",".join(targets),
    ]
    env = os.environ.copy()
    if not env.get("PYTHONPATH"):
        env["PYTHONPATH"] = str((ROOT / "src").resolve())
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    return stdout + ("\n--- STDERR ---\n" + stderr if stderr.strip() else "")

def _parse_stdout(stdout: str) -> Dict[str, TargetResult]:
    results: Dict[str, TargetResult] = {}
    current: Optional[str] = None
    for line in stdout.splitlines():
        line = line.rstrip()

        m = RE_INFO_ROWS.search(line)
        if m:
            tgt, n = m.group(1), int(m.group(2))
            current = tgt
            res = results.get(tgt) or TargetResult(target=tgt, archive_tries=[], archive_fallbacks=0)
            res.info_rows = n
            results[tgt] = res
            continue

        mw = RE_WHY.search(line)
        if mw:
            res = results.get(current or "unknown") or TargetResult(target=current or "unknown", archive_tries=[], archive_fallbacks=0)
            res.why_flip = mw.group(1)
            results[res.target] = res
            continue

        ma = RE_ARCH_TRY.search(line)
        if ma:
            res = results.get(current or "unknown") or TargetResult(target=current or "unknown", archive_tries=[], archive_fallbacks=0)
            res.archive_tries.append(ma.group(1).lower())
            results[res.target] = res
            continue

        ms = RE_ARCH_OK.search(line)
        if ms:
            res = results.get(current or "unknown") or TargetResult(target=current or "unknown", archive_tries=[], archive_fallbacks=0)
            res.archive_success = ms.group(1).lower()
            results[res.target] = res
            continue

        mf = RE_ARCH_FAIL.search(line)
        if mf:
            res = results.get(current or "unknown") or TargetResult(target=current or "unknown", archive_tries=[], archive_fallbacks=0)
            res.archive_fallbacks += 1
            results[res.target] = res
            continue

        mm = RE_METRICS.search(line)
        if mm:
            try:
                metrics = json.loads(mm.group(1))
            except Exception:
                metrics = {}
            if results:
                for r in results.values():
                    r.metrics = metrics
            else:
                results["_run"] = TargetResult(target="_run", archive_tries=[], archive_fallbacks=0, metrics=metrics)
    return results

# ---------- Reporting ----------
def _save_report(run_id: str, countries: List[str], mode: str, targets: List[str], stdout: str, results: Dict[str, TargetResult]) -> Tuple[Path, Path]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base = f"archive_orch_{run_id}_{stamp}"
    txt = REPORTS_DIR / f"{base}.txt"
    jsn = REPORTS_DIR / f"{base}.json"

    lines = []
    lines.append(f"[ORCH] run_id={run_id} countries={countries} mode={mode} targets={targets}")
    lines.append("")
    for t in targets:
        r = results.get(t)
        if not r:
            lines.append(f"- {t}: NO PARSED OUTPUT")
            continue
        lines.append(f"- {t}: rows={r.info_rows} why_flip={r.why_flip} archive_tries={r.archive_tries} archive_success={r.archive_success} fallbacks={r.archive_fallbacks}")
    r0 = next(iter(results.values())) if results else None
    if r0 and r0.metrics:
        lines.append("")
        lines.append("[METRICS]")
        for k, v in r0.metrics.items():
            lines.append(f"  {k}: {v}")
    lines.append("\n[RAW]\n" + stdout)
    txt.write_text("\n".join(lines), encoding="utf-8")

    out = {
        "run_id": run_id,
        "countries": countries,
        "mode": mode,
        "targets": targets,
        "results": {k: v.to_dict() for k, v in results.items()},
        "raw_path": str(txt),
    }
    jsn.write_text(json.dumps(out, indent=2), encoding="utf-8")
    return txt, jsn

# ---------- Main ----------
def main(argv: List[str]) -> int:
    import argparse
    p = argparse.ArgumentParser(
        description="Archive-aware orchestrator: ensure CSV knobs, sanitize for phase1 schema, run, and report.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("--run-id", required=True, help="e.g., 2025-W42")
    p.add_argument("--countries", nargs="+", required=True, help="e.g., NL or BE NL")
    p.add_argument("--mode", default="real", choices=["real", "synthetic"])
    p.add_argument("--targets", required=True, help="Comma-separated codes (e.g., ah_nl,jumbo_nl,carrefour_be,colruyt_be)")
    p.add_argument("--force-archive-for", help="Temporarily set prefer_wayback=true for this target during the run")
    p.add_argument("--print-retailers-change", action="store_true", help="Print whether retailers.csv was modified")
    args = p.parse_args(argv)

    if not RETAILERS_CSV.exists():
        print(f"[ERR] retailers.csv not found at {RETAILERS_CSV}", file=sys.stderr)
        return 2

    targets = [t.strip() for t in args.targets.split(",") if t.strip()]

    # 1) Load retailers.csv
    fields, rows = _read_csv(RETAILERS_CSV)

    # 2) Ensure archive columns exist + defaults for selected targets
    needed_cols = ["prefer_wayback", "archive_providers", "max_archive_lookback_days", "max_pages"]
    fields, rows, added_cols = _ensure_cols(fields, rows, needed_cols)
    changed_defaults = _set_defaults_for_targets(rows, targets)

    # Backup and write if changed
    bk_path = None
    if added_cols or changed_defaults:
        bk_path = RETAILERS_CSV.with_suffix(".bak")
        shutil.copy2(RETAILERS_CSV, bk_path)
        _write_csv(RETAILERS_CSV, fields, rows)
        if args.print_retailers_change:
            print(f"[OK] retailers.csv updated. added_cols={added_cols} changed_defaults={changed_defaults} | backup={bk_path}")
    else:
        if args.print_retailers_change:
            print("[OK] retailers.csv: no changes needed.")

    # 3) Optional: temporarily force prefer_wayback for one target
    forced_tmp_bk = None
    force_target = (args.force_archive_for or "").strip()
    prev_value = None
    forced = False
    if force_target:
        fields, rows = _read_csv(RETAILERS_CSV)
        forced, prev_value = _toggle_prefer_wayback(rows, force_target, "true")
        if forced:
            forced_tmp_bk = RETAILERS_CSV.with_suffix(".tmp.bak")
            shutil.copy2(RETAILERS_CSV, forced_tmp_bk)
            _write_csv(RETAILERS_CSV, fields, rows)
            print(f"[OK] Temporarily forced prefer_wayback=true for {force_target} (prev='{prev_value}'). Backup={forced_tmp_bk}")

    # 4) Schema compatibility: sanitize a temp retailers.csv for phase1 Retailer dataclass
    phase1_fields = _discover_retailer_fields_from_phase1(PHASE1_FILE)
    original_csv_bytes = RETAILERS_CSV.read_bytes()
    tmp_sanitized_applied = False
    try:
        if phase1_fields:
            s_fields, s_rows, changed = _sanitize_rows_for_phase1(fields, rows, phase1_fields)
            if changed:
                # write sanitized CSV over retailers.csv (keep original in memory)
                _write_csv(RETAILERS_CSV, s_fields, s_rows)
                tmp_sanitized_applied = True

        # 5) Run the CLI
        stdout = _run_cli(args.run_id, args.countries, args.mode, targets)
        results = _parse_stdout(stdout)

    finally:
        # Always restore the original retailers.csv if we sanitized it
        if tmp_sanitized_applied:
            RETAILERS_CSV.write_bytes(original_csv_bytes)

        # Restore prefer_wayback if we forced it
        if force_target and forced:
            fields2, rows2 = _read_csv(RETAILERS_CSV)
            chg, _ = _toggle_prefer_wayback(rows2, force_target, prev_value or "")
            if chg:
                _write_csv(RETAILERS_CSV, fields2, rows2)
                print(f"[OK] Restored prefer_wayback for {force_target} to '{prev_value or ''}'")

    # 6) Report
    txt_path, js_path = _save_report(args.run_id, args.countries, args.mode, targets, stdout, results)
    print(f"[REPORT] {txt_path}")
    print(f"[REPORT] {js_path}")

    # 7) Friendly summary
    print("\n[SUMMARY]")
    for t in targets:
        r = results.get(t)
        if not r:
            print(f" - {t}: no parsed output")
            continue
        print(f" - {t}: rows={r.info_rows} flip={r.why_flip or '-'} provider={r.archive_success or '-'} fallbacks={r.archive_fallbacks}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
