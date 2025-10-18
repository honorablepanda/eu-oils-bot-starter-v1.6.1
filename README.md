# EU Oils Price Bot — Starter Scaffold (Blueprint v1.6.1)

This is a **minimal, runnable scaffold** that implements the first chunk of the blueprint:
- Persistent Playwright profiles (`_pw_profile/<retailer_code>`).
- DDG resolver with guardrails + cache (saved to `logs/run_<id>/<code>/ddg.json`).
- Detectors for `auth_redirect`, `cf_challenge`, `cookie_wall`, `empty_listing` (heuristics).
- Bounded scroller and basic consent handling.
- JSON‑LD extraction (schema.org Product) + price/quantity normalization.
- Exporters that write `rows.csv` (with provenance) and merge a 4‑column final CSV.

> **Status**: This is a foundation meant to be extended with the rest of the blueprint
> (selector wizard, archive backends, policy engine persistence, PDP cache, etc.).

## Quickstart

1. **Install Python 3.10+** and Node/npm (for Playwright browsers).
2. **Create venv** and install deps:
   ```bash
   python -m venv .venv
   . .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -U pip
   pip install -e .
   python -m playwright install  # installs Chromium
   ```
3. **Run (first pass on an easy retailer like Vomar)**:
   ```bash
   python tools/phase1/phase1_oilbot.py --run-id 2025-W42R --targets vomar_nl --retailers retailers.csv
   ```

Artifacts will land under `logs/run_<id>/<code>/` and final CSV in `exports/oil_prices_<run-id>.csv`.

## Files & Folders
- `tools/phase1/phase1_oilbot.py` — Orchestrator
- `tools/phase1/utils_playwright.py` — Persistent contexts
- `tools/phase1/ddg_search.py` — Resolver (via DuckDuckGo HTML endpoint in-page)
- `tools/phase1/detectors.py` — Heuristic detectors
- `tools/phase1/scroller.py` — Bounded auto-scroll + load-more clicker
- `tools/phase1/parsers_jsonld.py` — JSON‑LD (schema.org/Product) extractor
- `tools/phase1/price_parser.py`, `tools/phase1/quantity_parser.py` — Normalizers
- `tools/phase1/exporters.py` — Writers for `rows.csv` + final 4‑col export
- `configs/oil_terms.yaml` — Keywords/brands for basic filtering
- `configs/warmup_sites.yaml` — Sites for session warm‑up (not yet wired — TODO)
- `retailers.csv` — Minimal seed with 2–4 example rows

## Notes
- This starter uses **Chromium**. If you get blocks, switch to `channel="chrome"` in utils to use your local Chrome.
- On CF/auth/cookie walls, the current behavior is **log & early flip** (archive not implemented yet). Add operator unlock & archives next.
- Deterministic behavior is seeded by `run_id + retailer_code` where applicable.

