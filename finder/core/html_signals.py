from __future__ import annotations
from typing import Dict, List
import re

def oil_in_url(url: str, oil_slugs: List[str]) -> bool:
    low = url.lower()
    return any(slug.lower().strip('/') in low for slug in oil_slugs if slug)

UNIT_RE = re.compile(r'(\d+(?:[\.,]\d+)?)\s*(l|liter|litre|l\b|ml|millilit(er|re)s?)', re.I)

def unit_tokens(text: str) -> bool:
    return bool(UNIT_RE.search(text or ''))

CURRENCY_RE = re.compile(r'(€|eur)', re.I)

def currency_tokens(text: str) -> bool:
    return bool(CURRENCY_RE.search(text or ''))

def infer_locale_ok(country: str, url: str, expected_locales: Dict[str, List[str]]) -> bool:
    langs = expected_locales.get(country.upper(), [])
    url_low = url.lower()
    return any(f'/{lang}/' in url_low or url_low.endswith(f'.{lang}') for lang in langs) or bool(langs)
