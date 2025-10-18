from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Any
import json

from finder.core.cdx import summarize_archives
from finder.core.html_signals import oil_in_url, unit_tokens, currency_tokens, infer_locale_ok

def load_yaml(path: Path) -> dict:
    try:
        import yaml  # type: ignore
    except Exception:
        yaml = None
    if yaml:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    return {}

def _slug_dict(keywords: dict) -> Dict[str, List[str]]:
    return {k: [str(x).strip('/') for x in v] for k, v in (keywords or {}).items()}

def enrich_candidates(cands: List[Dict[str, Any]], keywords: dict, locales: dict, start_year: int = 2022) -> List[Dict[str, Any]]:
    slugs = _slug_dict(keywords)
    out = []
    for row in cands:
        url = row.get('original_url') or ''
        oil = row.get('oil') or ''
        arc = summarize_archives(url, start_year=start_year)
        row.update({
            'archive_hits': {'wayback': arc['wayback_hits'], 'arquivo': arc['arquivo_hits']},
            'months_with_snapshots': arc['months_with_snapshots'],
        })
        row_signals = row.get('signals') or {}
        row_signals.update({
            'oil_in_url': oil_in_url(url, slugs.get(oil, [])),
            'unit_tokens': False,
            'per_unit_price': False,
            'oil_in_title': False,
            'currency_tokens': False,
            'locale_ok': infer_locale_ok(row.get('country',''), url, locales or {}),
            'gtin_valid': False,
        })
        row['signals'] = row_signals
        out.append(row)
    return out

def run(input_path: Path, output_path: Path, products_path: Path, keywords_path: Path, locales_path: Path, start_year: int = 2022):
    cands = json.loads(input_path.read_text(encoding='utf-8'))
    keywords = load_yaml(keywords_path)
    locales = load_yaml(locales_path)
    enriched = enrich_candidates(cands, keywords, locales, start_year=start_year)
    output_path.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding='utf-8')
