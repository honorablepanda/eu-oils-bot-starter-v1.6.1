#!/usr/bin/env python3
import json, shutil
from pathlib import Path

PATHS = [
    "selectors.json",
    "retailers/selectors.json",
    "tools/phase1/selectors.json",
]

# Minimal required keys (values are defaults if missing)
REQUIRED = {
  "ah_nl": {
    "consent_accept_selector": "#onetrust-accept-btn-handler"
  },
  "jumbo_nl": {
    "consent_accept_selector": "#onetrust-accept-btn-handler"
  },
  "carrefour_be": {
    # We accept either a title attr or text match — upstream code can try both.
    "consent_accept_selector_nl": "button[title='Alles accepteren'], button:has-text('Alles accepteren')",
    "consent_accept_selector_fr": "button[title='Accepter tout'], button:has-text('Accepter tout')",
    # keep room for scroll if you later enable it:
    "load_more_selector": None
  },
  "colruyt_be": {
    "store_open_selector": "a:has-text('Verander winkel'), a:has-text('Zoek je winkel')",
    "store_search_selector": "input[placeholder*='Zoek']",
    "store_option_selector": "li:has-text('Halle')",
    "store_confirm_selector": "button:has-text('Bevestig'), button:has-text('Bevestigen')"
  }
}

def first_existing():
    for p in PATHS:
        q = Path(p).resolve()
        if q.exists():
            return q
    return None

def deep_fill(dst: dict, src: dict):
    for k, v in src.items():
        if isinstance(v, dict):
            if k not in dst or not isinstance(dst.get(k), dict):
                dst[k] = {}
            deep_fill(dst[k], v)
        else:
            if k not in dst or dst[k] in (None, "", []):
                dst[k] = v

def main():
    target = first_existing()
    if not target:
        print("selectors.json not found in common locations.")
        return 1
    raw = target.read_text(encoding="utf-8", errors="ignore")
    try:
        data = json.loads(raw) if raw.strip() else {}
    except Exception:
        print(f"Could not parse JSON at {target}")
        return 1

    before = json.dumps(data, sort_keys=True)
    for retailer, req in REQUIRED.items():
        data.setdefault(retailer, {})
        deep_fill(data[retailer], req)
    after = json.dumps(data, sort_keys=True)

    if before == after:
        print(f"[OK] selectors already contain required keys for AH/Jumbo/Carrefour/Colruyt.\n-> {target}")
        return 0

    backup = target.with_suffix(target.suffix + ".bak")
    shutil.copyfile(target, backup)
    target.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[UPDATED] {target}\n[Backup] -> {backup}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
