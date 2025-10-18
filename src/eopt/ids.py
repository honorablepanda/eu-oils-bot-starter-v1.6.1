from __future__ import annotations
from urllib.parse import urlparse
import tldextract

# Use a local PSL (no first-run network fetch) for determinism
_EXTRACTOR = tldextract.TLDExtract(suffix_list_urls=None)

def _root_domain(host: str) -> str:
    """
    Return the registrable domain (e.g., 'carrefour.be' for 'www.carrefour.be').
    Always lowercase. Falls back to the input host if parsing is incomplete.
    """
    if not host:
        return ""
    ext = _EXTRACTOR(host)
    if not ext.domain or not ext.suffix:
        return host.lower()
    return f"{ext.domain}.{ext.suffix}".lower()

def _to_host(s: str) -> str:
    s = (s or "").strip()
    if "://" in s:
        try:
            return urlparse(s).netloc or s
        except Exception:
            return s
    return s

def make_website_id(iso2: str, site_domain_or_url: str) -> str:
    """
    Deterministic website_id: '<iso2_lower>:<root_domain_lower>'
    """
    iso = (iso2 or "").strip().lower()
    host = _to_host(site_domain_or_url)
    root = _root_domain(host)
    return f"{iso}:{root}"
